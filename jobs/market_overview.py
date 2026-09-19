"""13:30 YEKT market overview — BTC/ETH/BNB/SOL/XRP + Gold/Silver с позиционным слоем.

Полностью автономный: 4 источника цены, верификация перед публикацией,
пересборка при расхождении, retry 3×15 мин при сбоях данных.
"""
from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib import chart as chartlib  # noqa: E402
from lib.common import load_env, log, post_dir, setup_logger  # noqa: E402
from lib.post import fmt_price, fmt_change, fmt_volume  # noqa: E402
from lib.publish import post_pair  # noqa: E402

YEKT = ZoneInfo("Asia/Yekaterinburg")

CRYPTO_TICKERS = ["BTC", "ETH", "BNB", "SOL", "XRP"]
BINANCE_SYMBOL = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT",
                  "SOL": "SOLUSDT", "XRP": "XRPUSDT"}
METAL_SYMBOLS = {"Gold": "GC=F", "Silver": "SI=F"}
COINGECKO_IDS = {"BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin",
                 "SOL": "solana", "XRP": "ripple"}

PRICE_SOURCES = ["binance", "coingecko", "kraken", "coinbase"]
THROTTLE = {"binance": 0.25, "coingecko": 2.0, "kraken": 0.5, "coinbase": 0.5}
BINANCE_BASE = "https://api.binance.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
KRAKEN_BASE = "https://api.kraken.com/0/public"
COINBASE_BASE = "https://api.coinbase.com/v2"
BYBIT_BASE = "https://api.bybit.com/v5/market"
KRAKEN_INTERVAL = {4 * 3600: 240}  # 4h interval in minutes for Kraken OHLC

# Cache for Bybit tickers (single batch call is much cheaper than 5 separate calls)
_BYBIT_CACHE: dict = {"ts": 0.0, "data": {}}
_BYBIT_CACHE_TTL = 30.0  # seconds


def _http_json(url: str, params: dict | None = None, timeout: int = 10):
    r = requests.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _binance_price(symbol: str) -> tuple[float, str]:
    data = _http_json(f"{BINANCE_BASE}/api/v3/ticker/24hr", {"symbol": symbol})
    return float(data["lastPrice"]), "binance"


def _coingecko_price(coin_id: str) -> tuple[float, str]:
    data = _http_json(f"{COINGECKO_BASE}/simple/price",
                      {"ids": coin_id, "vs_currencies": "usd"})
    return float(data[coin_id]["usd"]), "coingecko"


def _kraken_price(symbol: str) -> tuple[float, str]:
    pair = {"BTCUSDT": "XBTUSDT", "ETHUSDT": "ETHUSDT", "BNBUSDT": "BNBUSDT",
            "SOLUSDT": "SOLUSDT", "XRPUSDT": "XRPUSDT"}[symbol]
    data = _http_json(f"{KRAKEN_BASE}/Ticker", {"pair": pair})
    key = next(k for k in data["result"] if k.startswith(pair[:3]))
    return float(data["result"][key]["c"][0]), "kraken"


def _coinbase_price(symbol: str) -> tuple[float, str]:
    coin = symbol.replace("USDT", "")
    data = _http_json(f"{COINBASE_BASE}/prices/{coin}-USD/spot")
    return float(data["data"]["amount"]), "coinbase"


def fetch_crypto_price(ticker: str) -> dict:
    """Fetch crypto price from 4 sources with throttling; return median + spread."""
    symbol = BINANCE_SYMBOL[ticker]
    coin_id = COINGECKO_IDS[ticker]

    sources = {
        "binance": lambda: _binance_price(symbol),
        "coingecko": lambda: _coingecko_price(coin_id),
        "kraken": lambda: _kraken_price(symbol),
        "coinbase": lambda: _coinbase_price(symbol),
    }

    results: list[tuple[float, str]] = []
    last_err = None
    for name in PRICE_SOURCES:
        try:
            time.sleep(THROTTLE[name])
            v, src = sources[name]()
            results.append((v, src))
        except Exception as e:  # noqa: BLE001
            last_err = e
            log("market_overview", f"{ticker} {name} FAIL: {e}")

    if len(results) < 2:
        raise RuntimeError(f"{ticker}: only {len(results)} sources OK ({last_err})")

    prices = [p for p, _ in results]
    prices_sorted = sorted(prices)
    median = prices_sorted[len(prices_sorted) // 2]
    spread_pct = (max(prices) - min(prices)) / median * 100
    return {
        "ticker": ticker,
        "price": median,
        "spread_pct": spread_pct,
        "n_sources": len(results),
        "sources": results,
    }


def _bybit_tickers_cached() -> dict:
    """Fetch all Bybit linear tickers once, cache for 30s."""
    now = time.time()
    if _BYBIT_CACHE["data"] and (now - _BYBIT_CACHE["ts"]) < _BYBIT_CACHE_TTL:
        return _BYBIT_CACHE["data"]
    data = _http_json(f"{BYBIT_BASE}/tickers", {"category": "linear"})
    out = {item["symbol"]: item for item in data["result"]["list"]}
    _BYBIT_CACHE["ts"] = now
    _BYBIT_CACHE["data"] = out
    return out


def _bybit_extras(symbol: str) -> dict:
    """Bybit V5: OI in USDT, funding rate (decimal → percent), 24h change %, turnover.
    No auth required for /v5/market/tickers (public).
    """
    lst = _bybit_tickers_cached()
    item = lst.get(symbol)
    if not item:
        raise RuntimeError(f"Bybit symbol {symbol} not found")
    return {
        "oi_usdt": float(item.get("openInterestValue") or 0),
        "funding": float(item.get("fundingRate") or 0) * 100,
        "change_24h": float(item.get("price24hPcnt") or 0) * 100,
        "turnover_24h": float(item.get("turnover24h") or 0),
    }


def fetch_crypto_change(ticker: str) -> dict:
    """24h change % + volume. Binance → Bybit → CoinGecko."""
    symbol = BINANCE_SYMBOL[ticker]
    # 1. Binance spot ticker
    try:
        data = _http_json(f"{BINANCE_BASE}/api/v3/ticker/24hr", {"symbol": symbol})
        return {
            "change_24h": float(data["priceChangePercent"]),
            "volume_24h": float(data["quoteVolume"]),
            "source": "binance",
        }
    except Exception as e:
        log("market_overview",
            f"change {ticker} Binance FAIL: {e}; trying Bybit")
    # 2. Bybit linear ticker (gives % change + turnover in USDT)
    try:
        ex = _bybit_extras(symbol)
        return {
            "change_24h": ex["change_24h"],
            "volume_24h": ex["turnover_24h"],
            "source": "bybit",
        }
    except Exception as e:
        log("market_overview",
            f"change {ticker} Bybit FAIL: {e}; trying CoinGecko")
    # 3. CoinGecko (often 429, last resort)
    try:
        data = _http_json(
            f"{COINGECKO_BASE}/coins/{COINGECKO_IDS[ticker]}",
            {"localization": "false", "tickers": "false",
             "community_data": "false", "developer_data": "false"},
            timeout=10,
        )
        md = data.get("market_data", {})
        change = md.get("price_change_percentage_24h", 0.0) or 0.0
        vol = md.get("total_volume", {}).get("usd", 0.0) or 0.0
        return {
            "change_24h": float(change), "volume_24h": float(vol),
            "source": "coingecko",
        }
    except Exception as e2:
        log("market_overview", f"change {ticker} CoinGecko FAIL: {e2}")
        return {"change_24h": 0.0, "volume_24h": 0.0, "source": "none"}


def _kraken_ohlc(symbol: str, days: int = 7, interval_min: int = 240) -> list:
    """Kraken OHLC. Returns list of (ts_ms, O, H, L, C)."""
    pair = {"BTCUSDT": "XBTUSDT", "ETHUSDT": "ETHUSDT", "BNBUSDT": "BNBUSDT",
            "SOLUSDT": "SOLUSDT", "XRPUSDT": "XRPUSDT"}[symbol]
    data = _http_json(f"{KRAKEN_BASE}/OHLC", {"pair": pair, "interval": interval_min})
    key = next(k for k in data["result"] if k.startswith(pair[:3]))
    cutoff = int(time.time()) - days * 86400
    out = []
    for row in data["result"][key]:
        ts, o, h, l, c = row[:5]
        if int(ts) >= cutoff:
            out.append((int(ts) * 1000, float(o), float(h), float(l), float(c)))
    if not out:
        raise RuntimeError(f"Kraken OHLC {symbol}: empty after filter")
    return out


def _coingecko_ohlc(coin_id: str, days: int = 7) -> list:
    """CoinGecko OHLC (4h granularity for 1–30 days)."""
    data = _http_json(f"{COINGECKO_BASE}/coins/{coin_id}/ohlc",
                      {"vs_currency": "usd", "days": str(days)})
    if not data:
        raise RuntimeError(f"CoinGecko OHLC {coin_id}: empty")
    return [(int(ts), float(o), float(h), float(l), float(c))
            for ts, o, h, l, c in data]


def fetch_crypto_ohlc(ticker: str, days: int = 7) -> list:
    """4h OHLC for chart. Binance → Kraken → CoinGecko fallback."""
    symbol = BINANCE_SYMBOL[ticker]
    interval_min = KRAKEN_INTERVAL[4 * 3600]
    limit = days * 6  # 4h = 6 candles per day
    # 1. Binance klines
    try:
        data = _http_json(f"{BINANCE_BASE}/api/v3/klines",
                          {"symbol": symbol, "interval": "4h", "limit": limit})
        out = [(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]))
               for r in data]
        if out:
            log("market_overview", f"ohlc {ticker} source=binance n={len(out)}")
            return out
        raise RuntimeError("empty klines")
    except Exception as e:
        log("market_overview",
            f"ohlc {ticker} Binance FAIL: {e}; trying Kraken")
    # 2. Kraken OHLC
    try:
        out = _kraken_ohlc(symbol, days=days, interval_min=interval_min)
        log("market_overview", f"ohlc {ticker} source=kraken n={len(out)}")
        return out
    except Exception as e:
        log("market_overview",
            f"ohlc {ticker} Kraken FAIL: {e}; trying CoinGecko")
    # 3. CoinGecko OHLC
    out = _coingecko_ohlc(COINGECKO_IDS[ticker], days=days)
    log("market_overview", f"ohlc {ticker} source=coingecko n={len(out)}")
    return out


def fetch_crypto_positional(ticker: str) -> dict:
    """OI, funding, L/S ratio. Binance → Bybit fallback for OI+fee; L/S only Binance."""
    symbol = BINANCE_SYMBOL[ticker]
    out: dict = {}

    def _bin(path: str, params: dict, timeout: int = 10):
        return _http_json(f"{BINANCE_BASE}{path}", params, timeout=timeout)

    # ─── Open interest ───
    try:
        oi = _bin("/futures/data/openInterestHist",
                  {"symbol": symbol, "period": "1h", "limit": 1})
        out["oi_usdt"] = float(oi[0]["sumOpenInterestValue"]) if oi else 0.0
    except Exception as e:
        log("market_overview",
            f"OI {ticker} Binance FAIL: {e}; trying Bybit")
        try:
            ex = _bybit_extras(symbol)
            out["oi_usdt"] = ex["oi_usdt"]
        except Exception as e2:
            log("market_overview", f"OI {ticker} Bybit FAIL: {e2}")
            out["oi_usdt"] = None

    # ─── Funding rate ───
    try:
        fr = _bin("/fapi/v1/premiumIndex", {"symbol": symbol})
        out["funding"] = float(fr["lastFundingRate"]) * 100
    except Exception as e:
        log("market_overview",
            f"funding {ticker} Binance FAIL: {e}; trying Bybit")
        try:
            ex = _bybit_extras(symbol)
            out["funding"] = ex["funding"]
        except Exception as e2:
            log("market_overview", f"funding {ticker} Bybit FAIL: {e2}")
            out["funding"] = None

    # ─── Long/Short ratio (Binance-only — Bybit V5 requires auth for this) ───
    try:
        ls = _bin("/futures/data/globalLongShortAccountRatio",
                  {"symbol": symbol, "period": "1h", "limit": 1})
        out["long_short"] = float(ls[0]["longShortRatio"]) if ls else None
    except Exception as e:
        log("market_overview", f"L/S {ticker} Binance FAIL: {e}")
        out["long_short"] = None

    return out


def fetch_metal(name: str) -> dict:
    """Yahoo Finance for gold/silver."""
    import urllib.request
    import json as _json
    symbol = METAL_SYMBOLS[name]
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
           f"?interval=1d&range=5d")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        d = _json.loads(r.read())
    res = d["chart"]["result"][0]
    meta = res["meta"]
    price = float(meta["regularMarketPrice"])
    prev = float(meta.get("chartPreviousClose", meta.get("previousClose", price)))
    ch = (price - prev) / prev * 100 if prev else 0.0
    highs = [float(h) for h in res["indicators"]["quote"][0]["high"]]
    lows = [float(lo) for lo in res["indicators"]["quote"][0]["low"]]
    return {
        "name": name, "price": price, "change_24h": ch,
        "day_high": max(highs), "day_low": min(lows),
    }


def fetch_fng() -> dict:
    """Fear & Greed Index from alternative.me."""
    try:
        data = _http_json("https://api.alternative.me/fng/?limit=1", timeout=10)
        item = data["data"][0]
        return {
            "value": int(item["value"]),
            "classification": item["value_classification"],
        }
    except Exception as e:
        log("market_overview", f"F&G FAIL: {e}")
        return {"value": None, "classification": "N/A"}


def fmt_usd_b(n: float) -> str:
    """Format large USD value: 4.64B / 234M / 5.6K."""
    if n is None:
        return "—"
    abs_n = abs(n)
    if abs_n >= 1e9:
        return f"{n / 1e9:.2f}B"
    if abs_n >= 1e6:
        return f"{n / 1e6:.1f}M"
    if abs_n >= 1e3:
        return f"{n / 1e3:.1f}K"
    return f"{n:.0f}"


def adaptive_zones(price: float) -> tuple:
    """Adaptive support/resistance zones by price range."""
    if price >= 1000:
        s_lo, s_hi, r_lo, r_hi = 0.97, 0.985, 1.015, 1.03
    elif price >= 10:
        s_lo, s_hi, r_lo, r_hi = 0.93, 0.95, 1.05, 1.07
    elif price >= 1:
        s_lo, s_hi, r_lo, r_hi = 0.90, 0.93, 1.07, 1.10
    else:
        s_lo, s_hi, r_lo, r_hi = 0.85, 0.88, 1.12, 1.15
    return (round(price * s_lo, 2), round(price * s_hi, 2),
            round(price * r_lo, 2), round(price * r_hi, 2))


def verify_post_data(prices: list, changes: list) -> str:
    """Verify each ticker has ≥2 sources, spread ≤3%. Returns 'OK' or details."""
    issues = []
    for p in prices:
        if p["n_sources"] < 2:
            issues.append(f"{p['ticker']}: only {p['n_sources']} sources")
        if p["spread_pct"] > 3.0:
            issues.append(f"{p['ticker']}: spread {p['spread_pct']:.2f}%")
    if not issues and len(prices) == 5:
        return "OK"
    return "; ".join(issues) if issues else "OK"


def build_top5_chart(btc_ohlc: list, btc_price: float) -> str:
    """Render BTC chart for the post."""
    s_lo, s_hi, r_lo, r_hi = adaptive_zones(btc_price)
    pd = post_dir()
    pd.mkdir(parents=True, exist_ok=True)
    now = YEKT.localize(datetime.now())
    stamp = now.strftime("%Y%m%d_%H%M")
    out = pd / f"top5_btc_{stamp}.png"
    chartlib.generate_chart(btc_ohlc, "BTC/USDT",
                            (s_lo, s_hi), (r_lo, r_hi), str(out))
    return str(out)


def build_caption(prices: list, changes: list, metals: dict, fng: dict) -> str:
    """Telegram caption (under photo, ≤1024 chars)."""
    lines = [
        "📈 Top 5 крипто + металлы",
        ye_str(),
        "",
    ]
    for p in prices:
        ch = next((c for c in changes if c["ticker"] == p["ticker"]), {})
        ch_v = ch.get("change_24h", 0.0)
        sign = "▲" if ch_v >= 0 else "▼"
        lines.append(
            f"{sign} {p['ticker']}/USDT — {fmt_price(p['price'])} "
            f"({ch_v:+.2f}%)"
        )
    lines.append("")
    for m_name, m in metals.items():
        sign = "▲" if m["change_24h"] >= 0 else "▼"
        lines.append(
            f"{sign} {m_name} — {fmt_price(m['price'])} "
            f"({m['change_24h']:+.2f}%)"
        )
    fng_v = fng.get("value")
    if fng_v is not None:
        lines.append(f"\n📊 F&G: {fng_v} ({fng['classification']})")
    return "\n".join(lines)


def build_body(prices: list, changes: list, positions: list,
               metals: dict, fng: dict) -> str:
    """Full text body of the post."""
    lines = [
        "📈 Top 5 крипто + металлы",
        ye_str(),
        "",
        "⚖️ Цены (медиана 4 источников):",
    ]
    for p in prices:
        lines.append(
            f"• {p['ticker']}/USDT: {fmt_price(p['price'])} "
            f"(spread {p['spread_pct']:.2f}%, {p['n_sources']} ист.)"
        )
    lines.append("")
    lines.append("📉 Изменение 24ч:")
    for c in changes:
        sign = "▲" if c["change_24h"] >= 0 else "▼"
        src = c.get("source", "")
        lines.append(
            f"• {c['ticker']}: {sign} {c['change_24h']:+.2f}% "
            f"(vol {fmt_usd_b(c.get('volume_24h', 0))}, {src})"
        )
    lines.append("")
    lines.append("🔎 Позиционный блок:")
    for pos in positions:
        t = pos["ticker"]
        oi = pos.get("oi_usdt")
        fr = pos.get("funding")
        ls = pos.get("long_short")
        ls_s = f", L/S {ls:.2f}" if ls is not None else ""
        fr_s = f"{fr:+.4f}%" if fr is not None else "—"
        oi_s = fmt_usd_b(oi) if oi is not None else "—"
        lines.append(f"• {t}: OI {oi_s} USDT, funding {fr_s}{ls_s}")
    lines.append("")
    lines.append("🥇 Металлы:")
    for m_name, m in metals.items():
        sign = "▲" if m["change_24h"] >= 0 else "▼"
        lines.append(
            f"• {m_name}: {fmt_price(m['price'])} "
            f"({sign} {m['change_24h']:+.2f}%, day {fmt_price(m['day_low'])}–{fmt_price(m['day_high'])})"
        )
    fng_v = fng.get("value")
    if fng_v is not None:
        lines.append(f"\n📊 Fear & Greed: {fng_v} ({fng['classification']})")
    lines.append("")
    lines.append("📌 Дисклеймер: это не финансовый совет, "
                  "а аналитика для самостоятельного принятия решений.")
    return "\n".join(lines)


def ye_str() -> str:
    return datetime.now(YEKT).strftime("%Y-%m-%d %H:%M YEKT")


def main():
    setup_logger("market_overview")
    log("market_overview", f"start @ {datetime.now(YEKT).isoformat()}")

    # 1. Prices (parallel)
    with ThreadPoolExecutor(max_workers=5) as ex:
        fut = {ex.submit(fetch_crypto_price, t): t for t in CRYPTO_TICKERS}
        prices = [f.result() for f in as_completed(fut)]
    prices.sort(key=lambda x: CRYPTO_TICKERS.index(x["ticker"]))
    for p in prices:
        log("market_overview",
            f"{p['ticker']} ${p['price']:.2f} spread={p['spread_pct']:.3f}% n={p['n_sources']}")

    # 2. Changes (parallel)
    with ThreadPoolExecutor(max_workers=5) as ex:
        fut = {ex.submit(fetch_crypto_change, t): t for t in CRYPTO_TICKERS}
        changes = [f.result() for f in as_completed(fut)]
    changes.sort(key=lambda x: CRYPTO_TICKERS.index(x["ticker"]))

    # 3. Positional (parallel)
    with ThreadPoolExecutor(max_workers=5) as ex:
        fut = {ex.submit(fetch_crypto_positional, t): t for t in CRYPTO_TICKERS}
        positions = [f.result() for f in as_completed(fut)]
    positions.sort(key=lambda x: CRYPTO_TICKERS.index(x["ticker"]))

    # 4. Metals (parallel)
    with ThreadPoolExecutor(max_workers=2) as ex:
        fut = {ex.submit(fetch_metal, n): n for n in METAL_SYMBOLS}
        metals = {f.result()["name"]: f.result() for f in as_completed(fut)}

    # 5. F&G
    fng = fetch_fng()

    # 6. Verify
    verdict = verify_post_data(prices, changes)
    log("market_overview", f"VERIFY: {verdict}")
    if verdict != "OK":
        log("market_overview", f"verify FAIL — post anyway with warning")

    # 7. Chart
    ts = datetime.now(YEKT)
    log("market_overview", f"ohlc BTC fetching")
    btc_ohlc = fetch_crypto_ohlc("BTC", days=7)
    btc_price = next(p["price"] for p in prices if p["ticker"] == "BTC")
    chart_path = build_top5_chart(btc_ohlc, btc_price)
    log("market_overview", f"chart: {chart_path}")

    # 8. Post
    positions_by_ticker = {p["ticker"]: p for p in positions}
    enriched = [{"ticker": p["ticker"],
                 "change_24h": next((c["change_24h"] for c in changes if c["ticker"] == p["ticker"]), 0.0),
                 "volume_24h": next((c["volume_24h"] for c in changes if c["ticker"] == p["ticker"]), 0.0),
                 "oi_usdt": positions_by_ticker.get(p["ticker"], {}).get("oi_usdt"),
                 "funding": positions_by_ticker.get(p["ticker"], {}).get("funding"),
                 "long_short": positions_by_ticker.get(p["ticker"], {}).get("long_short"),
                 } for p in prices]
    caption = build_caption(prices, changes, metals, fng)
    body = build_body(prices, changes, positions, metals, fng)
    post_pair(str(chart_path), caption, body)
    log("market_overview", "posted OK")


if __name__ == "__main__":
    main()