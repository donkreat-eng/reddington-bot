"""13:30 YEKT market overview — BTC/ETH/BNB/SOL/XRP + Gold/Silver цены в одном сообщении.

Sources (auto-fallback on runner-side geo-blocks):
  • Prices:        Binance → CoinGecko → Kraken → Coinbase (median, spread ≤ 3%)
  • Change 24h:    Binance → Bybit → OKX → CoinGecko
  • OI/Funding:    Binance → Bybit → OKX
  • OHLC (chart):  Binance → Kraken → CoinGecko
  • Gold/Silver:   Yahoo Finance
  • Fear & Greed:  alternative.me

Telegram channel: Reddington Trade📈 (id=-1003226574019).
Posting order: chart (top5 BTC) → caption → body.
"""
from __future__ import annotations

import io
import os
import json
import time
import math
import logging
import requests
import statistics
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.publish import post_pair
from lib.chart import generate_chart as _chart_generate

YE_TZ = timezone(timedelta(hours=5))
LOG = logging.getLogger("market_overview")
if not LOG.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
    LOG.addHandler(h)
    LOG.setLevel(logging.INFO)

BINANCE_BASE = "https://api.binance.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
KRAKEN_BASE = "https://api.kraken.com/0/public"
COINBASE_BASE = "https://api.coinbase.com/v2"
BYBIT_BASE = "https://api.bybit.com/v5/market"
OKX_BASE = "https://www.okx.com"

TICKERS = ["BTC", "ETH", "BNB", "SOL", "XRP"]
COINGECKO_IDS = {
    "BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin",
    "SOL": "solana", "XRP": "ripple",
}
BINANCE_SYMBOLS = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT",
    "SOL": "SOLUSDT", "XRP": "XRPUSDT",
}
BYBIT_SYMBOLS = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT",
    "SOL": "SOLUSDT", "XRP": "XRPUSDT",
}
OKX_SWAP_INST = {
    "BTC": "BTC-USDT-SWAP", "ETH": "ETH-USDT-SWAP", "BNB": "BNB-USDT-SWAP",
    "SOL": "SOL-USDT-SWAP", "XRP": "XRP-USDT-SWAP",
}
KRAKEN_PAIRS = {
    "BTC": "XBTUSD", "ETH": "ETHUSD", "BNB": "BNBUSD",
    "SOL": "SOLUSD", "XRP": "XRPUSD",
}
KRAKEN_INTERVAL = 240  # minutes (4h)

_BYBIT_CACHE = {"ts": 0.0, "data": {}}
_BYBIT_CACHE_TTL = 30.0


def _http_json(url: str, params: dict | None = None, timeout: float = 10.0):
    r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": "reddington-bot/1.0"})
    r.raise_for_status()
    return r.json()


# ---------------- Prices ----------------

def _binance_price(symbol: str):
    data = _http_json(f"{BINANCE_BASE}/api/v3/ticker/24hr", {"symbol": symbol})
    return float(data["lastPrice"])


def _coingecko_price(ticker: str):
    coin_id = COINGECKO_IDS[ticker]
    data = _http_json(f"{COINGECKO_BASE}/simple/price", {"ids": coin_id, "vs_currencies": "usd"})
    return float(data[coin_id]["usd"])


def _kraken_price(pair: str):
    data = _http_json(f"{KRAKEN_BASE}/Ticker", {"pair": pair})
    key = next(k for k in data["result"] if k != "last")
    return float(data["result"][key]["c"][0])


def _coinbase_price(symbol: str):
    data = _http_json(f"{COINBASE_BASE}/prices/{symbol}-USD/spot")
    return float(data["data"]["amount"])


def fetch_crypto_price(ticker: str):
    sym_b = BINANCE_SYMBOLS[ticker]
    sym_c = KRAKEN_PAIRS[ticker]
    sym_cb = ticker if ticker != "BTC" else "BTC"
    sources = {
        "binance": lambda: _binance_price(sym_b),
        "coingecko": lambda: _coingecko_price(ticker),
        "kraken": lambda: _kraken_price(sym_c),
        "coinbase": lambda: _coinbase_price(sym_cb),
    }
    prices = []
    for name, fn in sources.items():
        try:
            p = fn()
            prices.append((name, p))
            LOG.info(f"price {ticker} {name} = {p}")
        except Exception as e:
            LOG.info(f"price {ticker} {name} FAIL: {type(e).__name__}: {e}")
    if not prices:
        return None
    vals = [p for _, p in prices]
    median = statistics.median(vals)
    spread = (max(vals) - min(vals)) / median * 100 if median else 0
    return {"ticker": ticker, "price": median, "spread_pct": spread, "n_sources": len(prices), "sources": dict(prices)}


# ---------------- Change 24h + Volume ----------------

def _bybit_tickers_cached():
    now = time.time()
    if now - _BYBIT_CACHE["ts"] < _BYBIT_CACHE_TTL and _BYBIT_CACHE["data"]:
        return _BYBIT_CACHE["data"]
    out = {}
    for tkr in TICKERS:
        try:
            data = _http_json(f"{BYBIT_BASE}/tickers", {"category": "linear", "symbol": BYBIT_SYMBOLS[tkr]})
            lst = data.get("result", {}).get("list", [])
            if lst:
                out[tkr] = lst[0]
        except Exception as e:
            LOG.info(f"bybit ticker {tkr} FAIL: {e}")
    _BYBIT_CACHE["ts"] = now
    _BYBIT_CACHE["data"] = out
    return out


def _bybit_extras(ticker: str):
    info = _BYBIT_CACHE["data"].get(ticker)
    if not info:
        return None
    try:
        last = float(info["lastPrice"])
        prev = float(info["prevPrice24h"])
        chg = (last - prev) / prev * 100 if prev else 0
        return {
            "change_24h_pct": chg,
            "turnover_24h_usdt": float(info.get("turnover24h", 0) or 0),
            "last_price": last,
        }
    except Exception as e:
        LOG.info(f"bybit extras {ticker} FAIL: {e}")
        return None


def _okx_swap(inst_id: str):
    return _http_json(f"{OKX_BASE}/api/v5/market/ticker", {"instId": inst_id})


def _okx_funding(inst_id: str):
    return _http_json(f"{OKX_BASE}/api/v5/public/funding-rate", {"instId": inst_id})


def _okx_oi(inst_id: str, mark_price: float):
    data = _http_json(f"{OKX_BASE}/api/v5/public/open-interest", {"instId": inst_id})
    arr = data.get("data", [])
    if not arr:
        return None
    oi_ccy = float(arr[0].get("oiCcy", 0) or 0)
    return oi_ccy * mark_price


def _okx_extras(ticker: str):
    inst = OKX_SWAP_INST[ticker]
    try:
        t = _okx_swap(inst)
        arr = t.get("data", [])
        if not arr:
            return None
        d = arr[0]
        last = float(d["last"])
        open24 = float(d["open24h"])
        chg = (last - open24) / open24 * 100 if open24 else 0
        vol_ccy = float(d.get("volCcy24h", 0) or 0)
        turnover = float(d.get("vol24h", 0) or 0)  # in quote ccy (USDT)
        return {
            "change_24h_pct": chg,
            "turnover_24h_usdt": turnover,
            "last_price": last,
            "vol_ccy_24h": vol_ccy,
        }
    except Exception as e:
        LOG.info(f"okx extras {ticker} FAIL: {e}")
        return None


def fetch_crypto_change(ticker: str):
    sym = BINANCE_SYMBOLS[ticker]
    # Try Binance first
    try:
        data = _http_json(f"{BINANCE_BASE}/api/v3/ticker/24hr", {"symbol": sym}, timeout=8)
        return {
            "ticker": ticker,
            "change_24h_pct": float(data["priceChangePercent"]),
            "volume_24h_usdt": float(data["quoteVolume"]),
            "source": "binance",
        }
    except Exception as e:
        LOG.info(f"change {ticker} binance FAIL: {type(e).__name__}")
    # Bybit
    try:
        _bybit_tickers_cached()
        ex = _bybit_extras(ticker)
        if ex:
            return {
                "ticker": ticker,
                "change_24h_pct": ex["change_24h_pct"],
                "volume_24h_usdt": ex["turnover_24h_usdt"],
                "source": "bybit",
            }
    except Exception as e:
        LOG.info(f"change {ticker} bybit FAIL: {type(e).__name__}")
    # OKX
    try:
        ex = _okx_extras(ticker)
        if ex:
            return {
                "ticker": ticker,
                "change_24h_pct": ex["change_24h_pct"],
                "volume_24h_usdt": ex["turnover_24h_usdt"],
                "source": "okx",
            }
    except Exception as e:
        LOG.info(f"change {ticker} okx FAIL: {type(e).__name__}")
    # CoinGecko last
    try:
        coin_id = COINGECKO_IDS[ticker]
        data = _http_json(f"{COINGECKO_BASE}/coins/{coin_id}", {"localization": "false", "tickers": "false", "community_data": "false", "developer_data": "false"}, timeout=8)
        chg = data.get("market_data", {}).get("price_change_percentage_24h")
        vol = data.get("market_data", {}).get("total_volume", {}).get("usd")
        return {
            "ticker": ticker,
            "change_24h_pct": float(chg) if chg is not None else 0.0,
            "volume_24h_usdt": float(vol) if vol is not None else 0.0,
            "source": "coingecko",
        }
    except Exception as e:
        LOG.info(f"change {ticker} coingecko FAIL: {type(e).__name__}")
    return {"ticker": ticker, "change_24h_pct": 0.0, "volume_24h_usdt": 0.0, "source": "none"}


# ---------------- OHLC ----------------

def _kraken_ohlc(pair: str, days: int = 7):
    data = _http_json(f"{KRAKEN_BASE}/OHLC", {"pair": pair, "interval": KRAKEN_INTERVAL})
    key = next(k for k in data["result"] if k != "last")
    rows = data["result"][key]
    cutoff = int(time.time()) - days * 86400
    out = []
    for r in rows:
        ts, o, h, l, c = r[0], r[1], r[2], r[3], r[4]
        if ts >= cutoff:
            out.append((ts * 1000, float(o), float(h), float(l), float(c)))
    return out


def _coingecko_ohlc(coin_id: str, days: int = 7):
    data = _http_json(f"{COINGECKO_BASE}/coins/{coin_id}/ohlc", {"vs_currency": "usd", "days": str(days)})
    return [(r[0], r[1], r[2], r[3], r[4]) for r in data]


def fetch_crypto_ohlc(ticker: str, days: int = 7):
    sym = BINANCE_SYMBOLS[ticker]
    try:
        data = _http_json(f"{BINANCE_BASE}/api/v3/klines", {"symbol": sym, "interval": "4h", "limit": days * 6})
        return [(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in data], "binance"
    except Exception as e:
        LOG.info(f"ohlc {ticker} binance FAIL: {e}")
    try:
        rows = _kraken_ohlc(KRAKEN_PAIRS[ticker], days=days)
        if rows:
            return rows, "kraken"
    except Exception as e:
        LOG.info(f"ohlc {ticker} kraken FAIL: {e}")
    try:
        rows = _coingecko_ohlc(COINGECKO_IDS[ticker], days=days)
        if rows:
            return rows, "coingecko"
    except Exception as e:
        LOG.info(f"ohlc {ticker} coingecko FAIL: {e}")
    return [], "none"


# ---------------- Positional: OI / Funding / L-S ----------------

def fetch_crypto_positional(ticker: str):
    sym = BINANCE_SYMBOLS[ticker]
    out = {"ticker": ticker, "open_interest_usdt": None, "funding_rate_pct": None, "long_short_ratio": None, "source_oi": None, "source_funding": None, "source_ls": None}
    # OI + funding: Binance → Bybit → OKX
    oi = None
    fund = None
    try:
        data = _http_json(f"{BINANCE_BASE}/futures/data/openInterestHist", {"symbol": sym, "period": "5m", "limit": 1})
        if data:
            oi_btc = float(data[0]["sumOpenInterest"])
            mark = _http_json(f"{BINANCE_BASE}/api/v3/ticker/price", {"symbol": sym})
            oi = oi_btc * float(mark["price"])
            out["source_oi"] = "binance"
        data_f = _http_json(f"{BINANCE_BASE}/fapi/v1/premiumIndex", {"symbol": sym})
        fund = float(data_f.get("lastFundingRate", 0)) * 100
        out["source_funding"] = "binance"
    except Exception as e:
        LOG.info(f"positional {ticker} binance FAIL: {type(e).__name__}")
    if oi is None:
        try:
            _bybit_tickers_cached()
            info = _BYBIT_CACHE["data"].get(ticker)
            if info:
                oi = float(info.get("openInterestValue", 0) or 0)
                out["source_oi"] = "bybit"
                fund_b = float(info.get("fundingRate", 0) or 0) * 100
                if fund is None:
                    fund = fund_b
                    out["source_funding"] = "bybit"
        except Exception as e:
            LOG.info(f"positional {ticker} bybit FAIL: {e}")
    if oi is None:
        try:
            ex = _okx_extras(ticker)
            mark = ex["last_price"] if ex else None
            if mark:
                oi = _okx_oi(OKX_SWAP_INST[ticker], mark)
                if oi:
                    out["source_oi"] = "okx"
        except Exception as e:
            LOG.info(f"positional {ticker} okx oi FAIL: {e}")
    if fund is None:
        try:
            data = _okx_funding(OKX_SWAP_INST[ticker])
            arr = data.get("data", [])
            if arr:
                fund = float(arr[0].get("fundingRate", 0)) * 100
                out["source_funding"] = "okx"
        except Exception as e:
            LOG.info(f"positional {ticker} okx fund FAIL: {e}")
    out["open_interest_usdt"] = oi
    out["funding_rate_pct"] = fund
    # L-S ratio: Binance only (public endpoint)
    try:
        data = _http_json(f"{BINANCE_BASE}/futures/data/globalLongShortAccountRatio", {"symbol": sym, "period": "5m", "limit": 1})
        if data:
            out["long_short_ratio"] = float(data[0]["longShortRatio"])
            out["source_ls"] = "binance"
    except Exception as e:
        LOG.info(f"positional {ticker} ls binance FAIL: {type(e).__name__}")
    return out


# ---------------- Metals ----------------

def fetch_metal():
    out = {"gold_usd": None, "silver_usd": None, "source": "yahoo"}
    try:
        import yfinance as yf
    except Exception:
        yf = None
    if yf:
        try:
            g = yf.Ticker("GC=F").history(period="2d")
            if not g.empty:
                out["gold_usd"] = float(g["Close"].iloc[-1])
            s = yf.Ticker("SI=F").history(period="2d")
            if not s.empty:
                out["silver_usd"] = float(s["Close"].iloc[-1])
            return out
        except Exception as e:
            LOG.info(f"yahoo FAIL: {e}")
    return out


# ---------------- Fear & Greed ----------------

def fetch_fng():
    try:
        data = _http_json("https://api.alternative.me/fng/", {"limit": 1})
        d = data["data"][0]
        return {"value": int(d["value"]), "classification": d["value_classification"]}
    except Exception as e:
        LOG.info(f"fng FAIL: {e}")
        return None


# ---------------- Verify ----------------

def verify_post_data(prices: dict, changes: dict) -> tuple[bool, str]:
    issues = []
    for t in TICKERS:
        p = prices.get(t)
        if not p or p.get("n_sources", 0) < 2:
            issues.append(f"{t} price sources<2")
            continue
        if p["spread_pct"] > 3.0:
            issues.append(f"{t} spread {p['spread_pct']:.2f}%")
    if issues:
        return False, "; ".join(issues)
    return True, "OK"


# ---------------- Formatting ----------------

def fmt_price(v: float) -> str:
    if v is None:
        return "—"
    if v >= 1000:
        return f"{v:,.2f}".replace(",", " ")
    if v >= 1:
        return f"{v:,.2f}"
    return f"{v:.4f}"


def fmt_chg(v: float) -> str:
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"


def fmt_vol(v: float) -> str:
    if v is None or v == 0:
        return "—"
    if v >= 1e9:
        return f"${v/1e9:.2f}B"
    if v >= 1e6:
        return f"${v/1e6:.1f}M"
    if v >= 1e3:
        return f"${v/1e3:.1f}K"
    return f"${v:.0f}"


def fmt_oi(v: float) -> str:
    if v is None or v == 0:
        return "—"
    if v >= 1e9:
        return f"${v/1e9:.2f}B"
    if v >= 1e6:
        return f"${v/1e6:.1f}M"
    return f"${v/1e3:.0f}K"


# ---------------- Chart ----------------

def build_top5_chart(prices: dict, ohlc_by_ticker: dict, out_dir: Path):
    """Generate BTC chart (top of Top-5)."""
    ticker = "BTC"
    rows = ohlc_by_ticker.get(ticker, [])
    if not rows:
        return None
    p = prices.get(ticker, {})
    price = p.get("price")
    if not price:
        return None
    # Adaptive support/resistance zones (price ±2%, ±5%)
    support = (price * 0.98, price * 0.95)
    resistance = (price * 1.02, price * 1.05)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(YE_TZ).strftime("%Y%m%d_%H%M")
    chart_path = out_dir / f"top5_{ticker.lower()}_{ts}.png"
    try:
        _chart_generate(rows, ticker, support, resistance, str(chart_path))
        return chart_path
    except Exception as e:
        LOG.error(f"chart generate FAIL: {type(e).__name__}: {e}")
        return None


# ---------------- Caption + Body ----------------

def build_caption(prices: dict, changes: dict) -> str:
    lines = ["📊 *Обзор рынка* — BTC/ETH/BNB/SOL/XRP\n"]
    for t in TICKERS:
        p = prices.get(t, {})
        c = changes.get(t, {})
        price = fmt_price(p.get("price"))
        chg = fmt_chg(c.get("change_24h_pct"))
        lines.append(f"• *{t}* — ${price} ({chg})")
    return "\n".join(lines)


def build_body(prices: dict, changes: dict, positionals: dict, metals: dict, fng: dict | None) -> str:
    now = datetime.now(YE_TZ).strftime("%d.%m.%Y %H:%M YEKT")
    lines = [f"🕒 _{now}_\n"]
    lines.append("━━━ *Топ-5* ━━━")
    for t in TICKERS:
        p = prices.get(t, {})
        c = changes.get(t, {})
        pos = positionals.get(t, {})
        lines.append(
            f"*{t}* — ${fmt_price(p.get('price'))}  {fmt_chg(c.get('change_24h_pct'))}\n"
            f"   Vol: {fmt_vol(c.get('volume_24h_usdt'))}  |  OI: {fmt_oi(pos.get('open_interest_usdt'))}\n"
            f"   Fund: {fmt_chg(pos.get('funding_rate_pct'))}  |  L/S: {pos.get('long_short_ratio') or '—'}"
        )
    lines.append("")
    lines.append("━━━ *Металлы* ━━━")
    g = metals.get("gold_usd")
    s = metals.get("silver_usd")
    lines.append(f"🥇 Gold: ${fmt_price(g)}/oz")
    lines.append(f"🥈 Silver: ${fmt_price(s)}/oz")
    if fng:
        lines.append("")
        lines.append(f"🌡 Fear & Greed: *{fng['value']}* ({fng['classification']})")
    lines.append("")
    lines.append("_Источники: Binance/CoinGecko/Kraken/Coinbase (цены), Bybit/OKX (OI/funding), Yahoo (металлы), alternative.me (F&G)._")
    return "\n".join(lines)


# ---------------- Main ----------------

def main():
    LOG.info("start @ %s", datetime.now(YE_TZ).isoformat())

    # Fetch prices (parallel)
    prices: dict = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(fetch_crypto_price, t): t for t in TICKERS}
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                res = fut.result()
                if res:
                    prices[t] = res
            except Exception as e:
                LOG.error(f"price {t} CRASH: {e}")

    for t in TICKERS:
        p = prices.get(t)
        if p:
            LOG.info(f"{t} ${fmt_price(p['price'])} spread={p['spread_pct']:.3f}% n={p['n_sources']}")

    # Fetch changes (parallel, separate so price first)
    changes: dict = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(fetch_crypto_change, t): t for t in TICKERS}
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                changes[t] = fut.result()
            except Exception as e:
                LOG.error(f"change {t} CRASH: {e}")
    for t in TICKERS:
        c = changes.get(t)
        if c:
            LOG.info(f"change {t} {fmt_chg(c['change_24h_pct'])} source={c['source']}")

    # OHLC for chart (sequential OK)
    ohlc_by_ticker: dict = {}
    for t in TICKERS:
        rows, src = fetch_crypto_ohlc(t, days=7)
        if rows:
            ohlc_by_ticker[t] = rows
            LOG.info(f"ohlc {t} source={src} n={len(rows)}")

    # Positional
    positionals: dict = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(fetch_crypto_positional, t): t for t in TICKERS}
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                positionals[t] = fut.result()
            except Exception as e:
                LOG.error(f"positional {t} CRASH: {e}")

    # Metals + F&G
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_metals = ex.submit(fetch_metal)
        f_fng = ex.submit(fetch_fng)
        metals = f_metals.result()
        fng = f_fng.result()

    if metals.get("gold_usd"):
        LOG.info(f"Gold ${metals['gold_usd']:.2f} Silver ${metals.get('silver_usd', 0):.2f}")

    # Verify
    ok, msg = verify_post_data(prices, changes)
    LOG.info(f"VERIFY {msg}")
    if not ok:
        LOG.warning(f"verify failed but posting anyway: {msg}")

    # Chart
    repo_root = Path(__file__).resolve().parent.parent
    posts_dir = repo_root / "posts"
    chart_path = build_top5_chart(prices, ohlc_by_ticker, posts_dir)
    if chart_path:
        LOG.info(f"chart saved: {chart_path} ({chart_path.stat().st_size} B)")

    # Build text
    caption = build_caption(prices, changes)
    body = build_body(prices, changes, positionals, metals, fng)

    # Post
    if chart_path and chart_path.exists():
        post_pair(str(chart_path), caption, body)
    else:
        LOG.warning("no chart, posting text only via post_pair with placeholder")
        # create placeholder
        posts_dir.mkdir(parents=True, exist_ok=True)
        placeholder = posts_dir / f"placeholder_{datetime.now(YE_TZ).strftime('%Y%m%d_%H%M')}.txt"
        placeholder.write_text(caption + "\n\n" + body)
        post_pair(str(placeholder), caption, body)

    LOG.info("posted OK")


if __name__ == "__main__":
    main()
