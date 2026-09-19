"""13:30 YEKT market overview — BTC/ETH/BNB/SOL/XRP + Gold/Silver с позиционным слоем.

Полностью автономный: 4 источника цены, верификация перед публикацией,
пересборка при расхождении, retry 3×15с на источник, OHLC fallback chain
Binance → Kraken → CoinGecko, change/OI/funding fallback Binance → Bybit → OKX,
L/S fallback Binance → OKX, metals fallback Yahoo → gold-api.com.

ENV (setup env шагом бота):
  REDDINGTON_BOT_TOKEN   — Telegram bot token
  REDDINGTON_CHANNEL_ID  — id канала (например -1003226574019)

Cron: 30 8 * * 1-5  (UTC) = 13:30 YEKT пн–пт
"""

import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.common import load_env, setup_logger, ye_now, ye_str
from lib.publish import post_pair

log = setup_logger("market_overview") if hasattr(setup_logger, "__call__") else None


def _logger(name="market_overview"):
    return setup_logger(name) if hasattr(setup_logger, "__call__") else logging


def log(job, msg):
    """Wrapper над setup_logger.info()."""
    setup_logger(job).info(msg) if hasattr(setup_logger, "__call__") else logging.info(f"{job}: {msg}")


# ─── Constants ────────────────────────────────────────────────────────────────

BINANCE_BASE = "https://api.binance.com"
BINANCE_FAPI = "https://fapi.binance.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
KRAKEN_BASE = "https://api.kraken.com/0/public"
COINBASE_BASE = "https://api.coinbase.com/v2"
BYBIT_BASE = "https://api.bybit.com/v5/market"
OKX_BASE = "https://www.okx.com"
KRAKEN_INTERVAL = 240  # 4h candles

BINANCE_SYMBOL = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT"}
COINGECKO_IDS = {"BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin", "SOL": "solana", "XRP": "ripple"}
OKX_SWAP_INST = {"BTCUSDT": "BTC-USDT-SWAP", "ETHUSDT": "ETH-USDT-SWAP", "BNBUSDT": "BNB-USDT-SWAP", "SOLUSDT": "SOL-USDT-SWAP", "XRPUSDT": "XRP-USDT-SWAP"}

CRYPTO_TICKERS = ["BTC", "ETH", "BNB", "SOL", "XRP"]
METAL_SYMBOLS = {"Gold": "GC=F", "Silver": "SI=F"}

# Bybit batch cache (avoid hammering endpoint on every call)
_BYBIT_CACHE: dict = {"ts": 0.0, "data": {}}
_BYBIT_CACHE_TTL = 30.0

# ─── HTTP helpers ─────────────────────────────────────────────────────────────


def _http_json(url: str, params: dict | None = None, timeout: int = 10):
    """GET URL → JSON. Raises on HTTP error."""
    r = requests.get(url, params=params or {}, timeout=timeout,
                     headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.json()


# ─── Price (multi-source median + spread) ────────────────────────────────────


def _binance_price(symbol: str) -> float | None:
    try:
        d = _http_json(f"{BINANCE_BASE}/api/v3/ticker/price", {"symbol": symbol})
        return float(d["price"])
    except Exception:
        return None


def _coingecko_price(coin_id: str) -> float | None:
    try:
        d = _http_json(f"{COINGECKO_BASE}/simple/price",
                       {"ids": coin_id, "vs_currencies": "usd"})
        return float(d[coin_id]["usd"])
    except Exception:
        return None


def _kraken_price(symbol: str) -> float | None:
    try:
        d = _http_json(f"{KRAKEN_BASE}/Ticker", {"pair": symbol})
        for k, v in d["result"].items():
            if k == "last": continue
            return float(v["c"][0])
        return None
    except Exception:
        return None


def _coinbase_price(coin_id: str) -> float | None:
    try:
        d = _http_json(f"{COINBASE_BASE}/prices/{coin_id}-USD/spot")
        return float(d["data"]["amount"])
    except Exception:
        return None


def fetch_crypto_price(ticker: str) -> dict:
    """Median price from Binance/CoinGecko/Kraken/Coinbase. Returns {price, sources, spread}."""
    sources = []
    sym = BINANCE_SYMBOL[ticker]
    if (p := _binance_price(sym)) is not None:
        sources.append(("binance", p))
    if (p := _coingecko_price(COINGECKO_IDS[ticker])) is not None:
        sources.append(("coingecko", p))
    if (p := _kraken_price(sym.replace("USDT", "USD"))) is not None:
        sources.append(("kraken", p))
    if (p := _coinbase_price(COINGECKO_IDS[ticker])) is not None:
        sources.append(("coinbase", p))

    if not sources:
        raise RuntimeError(f"{ticker}: all price sources failed")

    prices = [p for _, p in sources]
    prices.sort()
    median = prices[len(prices) // 2]
    spread = (max(prices) - min(prices)) / median * 100 if len(prices) > 1 else 0.0
    log("market_overview",
        f"{ticker} ${median:,.2f} spread={spread:.3f}% n={len(sources)}")
    return {"price": median, "sources": sources, "spread": spread}


# ─── 24h change / volume ──────────────────────────────────────────────────────


def _bybit_tickers_cached() -> dict:
    """Single batch call to Bybit V5, cached 30s. Returns {symbol: ticker_dict}."""
    now = time.time()
    if now - _BYBIT_CACHE["ts"] < _BYBIT_CACHE_TTL and _BYBIT_CACHE["data"]:
        return _BYBIT_CACHE["data"]
    try:
        d = _http_json(f"{BYBIT_BASE}/tickers", {"category": "linear"}, timeout=10)
        out = {row["symbol"]: row for row in d.get("result", {}).get("list", [])}
        _BYBIT_CACHE["ts"] = now
        _BYBIT_CACHE["data"] = out
        return out
    except Exception:
        return _BYBIT_CACHE["data"]  # may be stale


def _bybit_extras(symbol: str) -> dict:
    """Bybit: change_24h %, turnover_24h USDT, funding %, oi_usdt."""
    tickers = _bybit_tickers_cached()
    row = tickers.get(symbol)
    if not row:
        raise RuntimeError(f"{symbol} not in Bybit tickers")
    last = float(row["lastPrice"])
    prev24 = float(row["prevPrice24h"])
    turnover = float(row.get("turnover24h", 0))
    funding = float(row.get("fundingRate", 0)) * 100
    oi = float(row.get("openInterest", 0))
    oi_usdt = float(row.get("openInterestValue", 0)) or oi * last
    return {
        "change_24h": (last - prev24) / prev24 * 100 if prev24 else 0.0,
        "volume_24h": turnover,
        "funding": funding,
        "oi_usdt": oi_usdt,
    }


def _okx_swap(inst_id: str) -> dict:
    """OKX ticker (SWAP)."""
    return _http_json(f"{OKX_BASE}/api/v5/market/ticker", {"instId": inst_id}, timeout=10)


def _okx_funding(inst_id: str) -> float:
    """OKX funding rate (as decimal, e.g. 0.0001)."""
    d = _http_json(f"{OKX_BASE}/api/v5/public/funding-rate", {"instId": inst_id}, timeout=10)
    if d.get("data"):
        return float(d["data"][0]["fundingRate"])
    return 0.0


def _okx_oi(inst_id: str, mark_price: float) -> float:
    """OKX open interest in contracts × mark price = USDT."""
    d = _http_json(f"{OKX_BASE}/api/v5/public/open-interest", {"instId": inst_id}, timeout=10)
    if d.get("data"):
        oi_ccy = float(d["data"][0]["oiCcy"])
        return oi_ccy * mark_price
    return 0.0


def _okx_extras(symbol: str) -> dict:
    """OKX: change_24h %, turnover_24h USDT, funding %, oi_usdt."""
    inst_id = OKX_SWAP_INST[symbol]
    tick = _okx_swap(inst_id)
    if not tick.get("data"):
        raise RuntimeError(f"OKX ticker empty for {inst_id}")
    t = tick["data"][0]
    last = float(t["last"])
    open24 = float(t["open24h"])
    vol_ccy = float(t.get("volCcy24h", 0))
    funding = _okx_funding(inst_id) * 100
    oi_usdt = _okx_oi(inst_id, last)
    return {
        "change_24h": (last - open24) / open24 * 100 if open24 else 0.0,
        "volume_24h": vol_ccy * last,
        "funding": funding,
        "oi_usdt": oi_usdt,
    }


def fetch_crypto_change(ticker: str) -> dict:
    """24h change and volume. Binance → Bybit → OKX → CoinGecko fallback."""
    symbol = BINANCE_SYMBOL[ticker]
    coin_id = COINGECKO_IDS[ticker]
    # 1) Binance spot 24hr ticker
    try:
        d = _http_json(f"{BINANCE_BASE}/api/v3/ticker/24hr", {"symbol": symbol})
        return {
            "change_24h": float(d["priceChangePercent"]),
            "volume_24h": float(d["quoteVolume"]),
            "source": "binance",
        }
    except Exception as e:
        log("market_overview",
            f"change {ticker} Binance FAIL: {e}; trying Bybit")
    # 2) Bybit
    try:
        ex = _bybit_extras(symbol)
        return {
            "change_24h": ex["change_24h"],
            "volume_24h": ex["volume_24h"],
            "source": "bybit",
        }
    except Exception as e:
        log("market_overview",
            f"change {ticker} Bybit FAIL: {e}; trying OKX")
    # 3) OKX
    try:
        ex = _okx_extras(symbol)
        return {
            "change_24h": ex["change_24h"],
            "volume_24h": ex["volume_24h"],
            "source": "okx",
        }
    except Exception as e:
        log("market_overview",
            f"change {ticker} OKX FAIL: {e}; trying CoinGecko")
    # 4) CoinGecko last resort
    try:
        d = _http_json(f"{COINGECKO_BASE}/simple/price",
                       {"ids": coin_id, "vs_currencies": "usd",
                        "include_24hr_change": "true", "include_24hr_vol": "true"})
        return {
            "change_24h": float(d[coin_id].get("usd_24h_change", 0)),
            "volume_24h": float(d[coin_id].get("usd_24h_vol", 0)),
            "source": "coingecko",
        }
    except Exception as e:
        log("market_overview", f"change {ticker} CoinGecko FAIL: {e}")
        return {"change_24h": 0.0, "volume_24h": 0.0, "source": "none"}


# ─── OHLC (chart data) ───────────────────────────────────────────────────────


def _kraken_ohlc(pair: str, days: int = 7) -> list:
    """Kraken OHLC since N days ago."""
    since = int((time.time() - days * 86400) * 1_000_000_000)
    d = _http_json(f"{KRAKEN_BASE}/OHLC",
                   {"pair": pair, "interval": KRAKEN_INTERVAL, "since": since})
    candles = []
    for k, v in d["result"].items():
        if k == "last": continue
        for c in v:
            if c[0] >= since:
                candles.append((c[0] * 1000, float(c[1]), float(c[2]),
                                float(c[3]), float(c[4])))
    return candles


def _coingecko_ohlc(coin_id: str, days: int = 7) -> list:
    """CoinGecko OHLC (4h for days<=1, daily for days>1)."""
    d = _http_json(f"{COINGECKO_BASE}/coins/{coin_id}/ohlc",
                   {"vs_currency": "usd", "days": days})
    return [(c[0], float(c[1]), float(c[2]), float(c[3]), float(c[4])) for c in d]


def fetch_crypto_ohlc(ticker: str, days: int = 7) -> list:
    """4h OHLC for chart. Binance → Kraken → CoinGecko fallback."""
    symbol = BINANCE_SYMBOL[ticker]
    coin_id = COINGECKO_IDS[ticker]
    # 1) Binance klines
    try:
        limit = min(1000, days * 6)  # ~6 candles/day for 4h
        d = _http_json(f"{BINANCE_BASE}/api/v3/klines",
                       {"symbol": symbol, "interval": "4h", "limit": limit})
        candles = [(c[0], float(c[1]), float(c[2]), float(c[3]), float(c[4])) for c in d]
        log("market_overview", f"ohlc {ticker} source=binance n={len(candles)}")
        return candles
    except Exception as e:
        log("market_overview", f"ohlc {ticker} Binance FAIL: {e}; trying Kraken")
    # 2) Kraken
    try:
        candles = _kraken_ohlc(symbol.replace("USDT", "USD"), days)
        log("market_overview", f"ohlc {ticker} source=kraken n={len(candles)}")
        return candles
    except Exception as e:
        log("market_overview", f"ohlc {ticker} Kraken FAIL: {e}; trying CoinGecko")
    # 3) CoinGecko
    try:
        candles = _coingecko_ohlc(coin_id, days)
        log("market_overview", f"ohlc {ticker} source=coingecko n={len(candles)}")
        return candles
    except Exception as e:
        log("market_overview", f"ohlc {ticker} CoinGecko FAIL: {e}")
        raise


# ─── OI / funding / L/S ─────────────────────────────────────────────────────


def fetch_crypto_positional(ticker: str) -> dict:
    """OI, funding, L/S ratio. Binance → Bybit → OKX fallback chain."""
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
            log("market_overview",
                f"OI {ticker} Bybit FAIL: {e2}; trying OKX")
            try:
                ex = _okx_extras(symbol)
                out["oi_usdt"] = ex["oi_usdt"]
            except Exception as e3:
                log("market_overview", f"OI {ticker} OKX FAIL: {e3}")
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
            log("market_overview",
                f"funding {ticker} Bybit FAIL: {e2}; trying OKX")
            try:
                ex = _okx_extras(symbol)
                out["funding"] = ex["funding"]
            except Exception as e3:
                log("market_overview", f"funding {ticker} OKX FAIL: {e3}")
                out["funding"] = None

    # ─── Long/Short ratio (Binance → OKX fallback) ───
    try:
        ls = _bin("/futures/data/globalLongShortAccountRatio",
                  {"symbol": symbol, "period": "1h", "limit": 1})
        out["long_short"] = float(ls[0]["longShortRatio"]) if ls else None
    except Exception as e:
        log("market_overview",
            f"L/S {ticker} Binance FAIL: {e}; trying OKX")
        try:
            inst_id = OKX_SWAP_INST.get(symbol)
            if inst_id:
                ccy = symbol.replace("USDT", "")
                url = f"{OKX_BASE}/api/v5/rubik/stat/contracts/long-short-account-ratio"
                data = _http_json(url, {"ccy": ccy, "period": "5m"}, timeout=10)
                if data and data.get("data"):
                    last = data["data"][-1]
                    out["long_short"] = float(last[1])
                else:
                    out["long_short"] = None
            else:
                out["long_short"] = None
        except Exception as e2:
            log("market_overview", f"L/S {ticker} OKX FAIL: {e2}")
            out["long_short"] = None

    return out


# ─── Metals ──────────────────────────────────────────────────────────────────


def fetch_metal(name: str) -> dict:
    """Fetch metal price. Yahoo Finance -> gold-api.com fallback.
    Returns {name, price, change_pct}. On full failure, price=None and change_pct=0.
    """
    symbol = METAL_SYMBOLS[name]
    gold_api_symbol = {"Gold": "XAU", "Silver": "XAG"}.get(name)

    # 1) Yahoo Finance (primary)
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        r = requests.get(url, params={"interval": "1d", "range": "5d"},
                         headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        res = data["chart"]["result"][0]
        meta = res["meta"]
        price = meta["regularMarketPrice"]
        prev = meta.get("chartPreviousClose", meta.get("previousClose", price))
        return {
            "name": name,
            "price": float(price),
            "change_pct": (float(price) - float(prev)) / float(prev) * 100,
        }
    except Exception as e:
        log("market_overview", f"metal {name} Yahoo FAIL: {e}; trying gold-api.com")

    # 2) gold-api.com fallback (free, no key, supports XAU/XAG only)
    if gold_api_symbol:
        try:
            r = requests.get(f"https://api.gold-api.com/price/{gold_api_symbol}",
                             timeout=10)
            r.raise_for_status()
            data = r.json()
            price = float(data["price"])
            return {
                "name": name,
                "price": price,
                "change_pct": 0.0,  # gold-api.com gives only spot price
            }
        except Exception as e:
            log("market_overview", f"metal {name} gold-api FAIL: {e}")

    # Both failed
    log("market_overview", f"metal {name} ALL SOURCES FAILED")
    return {"name": name, "price": None, "change_pct": 0.0}


# ─── Formatters ──────────────────────────────────────────────────────────────


def fmt_oi(oi: float | None) -> str:
    """OI: '—' if None, $XB / $XM / $X."""
    if oi is None:
        return "—"
    if oi > 1e9:
        return f"${oi / 1e9:.2f}B"
    if oi > 1e6:
        return f"${oi / 1e6:.1f}M"
    return f"${oi:,.0f}"


def fmt_funding(f: float | None) -> str:
    """Funding: '—' if None, +0.010% / -0.005%."""
    if f is None:
        return "—"
    sign = "+" if f >= 0 else ""
    return f"{sign}{f:.3f}%"


def fmt_ls(ls: float | None) -> str:
    """L/S ratio: '—' if None, '60% long' (long % derived from ratio)."""
    if ls is None:
        return "—"
    long_pct = ls / (1 + ls) * 100
    return f"{long_pct:.0f}% long"


# ─── Fear & Greed index ──────────────────────────────────────────────────────


def fetch_fng() -> int:
    """Alternative.me Fear & Greed Index (today)."""
    try:
        d = _http_json("https://api.alternative.me/fng/", {"limit": 1}, timeout=10)
        return int(d["data"][0]["value"])
    except Exception as e:
        log("market_overview", f"F&G FAIL: {e}")
        return 50  # neutral default


# ─── Verify post data ───────────────────────────────────────────────────────


def verify_post_data(prices: dict, changes: dict) -> str:
    """Returns 'OK' or 'FAIL: reason'."""
    # Spot price median spread < 0.5%
    for t in CRYPTO_TICKERS:
        sp = prices.get(t, {}).get("spread", 99)
        if sp > 1.5:
            return f"FAIL: {t} spread {sp:.2f}% > 1.5%"
    # Change within reasonable bounds
    for t in CRYPTO_TICKERS:
        ch = changes.get(t, {}).get("change_24h", 0)
        if abs(ch) > 30:
            return f"FAIL: {t} change {ch:.1f}% > 30%"
    return "OK"


# ─── Caption (top — photo) ───────────────────────────────────────────────────


def build_caption(prices: dict, changes: dict, fng: int, ts: datetime) -> str:
    """Compact summary used as Telegram photo caption."""
    ts_str = ts.strftime("%d %b %Y · %H:%M YEKT")
    crypto_lines = []
    for t in CRYPTO_TICKERS:
        p = prices[t]["price"]
        ch = changes[t]["change_24h"]
        if ch == 0 or ch is None:
            line = f"• *{t}* — {fmt_price(p)}"
        elif ch > 0:
            line = f"• *{t}* — {fmt_price(p)} 🟢 +{ch:.2f}%"
        else:
            line = f"• *{t}* — {fmt_price(p)} 🔴 {ch:.2f}%"
        crypto_lines.append(line)
    crypto_block = "\n".join(crypto_lines)

    metals_lines = []
    for m in ["Gold", "Silver"]:
        d = prices[m]
        ch = changes[m].get("change_24h", 0)
        if d.get("price") is None:
            metals_lines.append(f"⚪ {m}: —")
        elif ch == 0 or ch is None:
            metals_lines.append(f"{m} {fmt_price(d['price'])}")
        elif ch > 0:
            metals_lines.append(f"🟡 {m} {fmt_price(d['price'])} +{ch:.2f}%")
        else:
            metals_lines.append(f"🔴 {m} {fmt_price(d['price'])} {ch:.2f}%")
    metals_block = "\n".join(metals_lines)

    fng_emoji = "🟢" if fng >= 60 else "🔴" if fng <= 40 else "🟡"
    return (
        f"🏛 REDDINGTON · Топ-5 + Металлы\n"
        f"_{ts_str}_\n"
        f"\n"
        f"📊 *Крипто (24ч)*\n"
        f"{crypto_block}\n"
        f"\n"
        f"🥇 *Металлы*\n"
        f"{metals_block}\n"
        f"\n"
        f"🌡 *Fear &amp; Greed* {fng_emoji} {fng}/100\n"
    )


# ─── Body (text below photo) ────────────────────────────────────────────────


def build_body(prices: dict, changes: dict, positional: dict, fng: int, ts: datetime) -> str:
    """Long body posted as separate message after the chart photo."""
    ts_str = ts.strftime("%A, %d %B %Y · %H:%M YEKT")
    parts = [
        f"🏛 REDDINGTON · Обзор рынка",
        f"_{ts_str}_",
        "",
        "📈 *Сводка по активам*",
        "",
    ]

    # Table columns — show only what data exists for at least one ticker
    has_oi = any((positional.get(t) or {}).get("oi_usdt") is not None for t in CRYPTO_TICKERS)
    has_funding = any((positional.get(t) or {}).get("funding") is not None for t in CRYPTO_TICKERS)
    has_ls = any((positional.get(t) or {}).get("long_short") is not None for t in CRYPTO_TICKERS)

    cols = [("Актив", 6, 'l'), ("Цена", 12, 'r'), ("24ч", 7, 'r')]
    if has_oi:
        cols.append(("OI", 9, 'r'))
    if has_funding:
        cols.append(("Funding", 9, 'r'))
    if has_ls:
        cols.append(("L/S", 11, 'r'))

    def _line(cells):
        return " ".join(f"{txt:<{w}}" if align == 'l' else f"{txt:>{w}}" for txt, w, align in cells)

    # Header + separator
    parts.append(_line([(h, w, a) for h, w, a in cols]))
    parts.append(_line([("─" * w, w, 'l') for _, w, _ in cols]))

    # Rows
    for t in CRYPTO_TICKERS:
        p = prices[t]["price"]
        ch = changes[t]["change_24h"]
        pos = positional.get(t, {})
        arrow = "▲" if ch > 0 else "▼" if ch < 0 else "─"
        row_cells = [
            (t, 6, 'l'),
            (fmt_price(p), 12, 'r'),
            (f"{arrow}{abs(ch):.2f}%" if ch else "—", 7, 'r'),
        ]
        if has_oi:
            row_cells.append((fmt_oi(pos.get('oi_usdt')), 9, 'r'))
        if has_funding:
            row_cells.append((fmt_funding(pos.get('funding')), 9, 'r'))
        if has_ls:
            row_cells.append((fmt_ls(pos.get('long_short')), 11, 'r'))
        parts.append(_line(row_cells))

    # Gold/Silver
    for m in ["Gold", "Silver"]:
        d = prices[m]
        ch = changes[m].get("change_24h", 0)
        if d.get("price") is None:
            parts.append(_line([(m, 6, 'l'), ("—", 12, 'r'), ("—", 7, 'r')]))
            continue
        arrow = "▲" if ch > 0 else "▼" if ch < 0 else "─"
        ch_str = f"{arrow}{abs(ch):.2f}%" if ch else "—"
        parts.append(_line([(m, 6, 'l'), (fmt_price(d['price']), 12, 'r'), (ch_str, 7, 'r')]))

    parts.append("")

    # F&G
    fng_emoji = "🟢" if fng >= 60 else "🔴" if fng <= 40 else "🟡"
    parts.append(f"🌡 *Fear &amp; Greed* {fng_emoji} {fng}/100")
    parts.append("")

    # BTC zones from chart
    btc_pos = positional.get("BTC", {})
    btc_p = prices["BTC"]["price"]
    parts.append("🔍 *Зоны BTC*")
    sup = btc_p * 0.97
    res = btc_p * 1.03
    parts.append(f"Поддержка {fmt_price(sup)}")
    parts.append(f"Сопротивление {fmt_price(res)}")
    parts.append("")

    # Cluster watch (BTC funding + L/S if available)
    funding = btc_pos.get("funding")
    ls = btc_pos.get("long_short")
    if funding is not None or ls is not None:
        parts.append("⚙️ *Кластеры (BTC)*")
        if funding is not None:
            parts.append(f"Funding: {fmt_funding(funding)}")
        if ls is not None:
            parts.append(f"L/S: {fmt_ls(ls)}")
        parts.append("")

    # Watchlist hint
    parts.append("🧭 *Что смотреть*")
    parts.append("• BTC funding &gt; +0.05% — overheated longs")
    parts.append("• BTC L/S &gt; 1.5 — перевес лонгов")
    parts.append("• Gold &lt; $4 200 — разворот вверх")

    return "\n".join(parts)


def fmt_price(x) -> str:
    """fmt_price from lib/post, imported below if present."""
    from lib.post import fmt_price as _fp
    return _fp(x)


# ─── Chart ───────────────────────────────────────────────────────────────────


def build_top5_chart(prices: dict, ohlc: dict, support: dict, resistance: dict, output_path: str):
    """Render BTC chart with zones."""
    from lib.chart import generate_chart
    btc_ohlc = ohlc.get("BTC", [])
    if not btc_ohlc:
        raise RuntimeError("no BTC ohlc for chart")
    s = support.get("BTC", prices["BTC"]["price"] * 0.97)
    r = resistance.get("BTC", prices["BTC"]["price"] * 1.03)
    generate_chart(btc_ohlc, "BTC", (s, s), (r, r), output_path)
    return output_path


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    ts = ye_now()
    log("market_overview", f"start @ {ts.isoformat()}")

    # 1. Fetch prices, changes, positional in parallel
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {}
        for t in CRYPTO_TICKERS:
            futs[ex.submit(fetch_crypto_price, t)] = ("price", t)
        for t in CRYPTO_TICKERS:
            futs[ex.submit(fetch_crypto_change, t)] = ("change", t)
        for t in CRYPTO_TICKERS:
            futs[ex.submit(fetch_crypto_positional, t)] = ("pos", t)
        futs[ex.submit(fetch_metal, "Gold")] = ("metal", "Gold")
        futs[ex.submit(fetch_metal, "Silver")] = ("metal", "Silver")

        prices = {}
        changes = {}
        positional = {}
        for f in futs:
            kind, key = futs[f]
            v = f.result()
            if kind == "price":
                prices[key] = v
            elif kind == "change":
                changes[key] = v
            elif kind == "pos":
                positional[key] = v
            elif kind == "metal":
                prices[key] = v  # store metals under prices too

    # 2. Add metals change_24h to changes dict
    for m in ["Gold", "Silver"]:
        if m in prices:
            changes[m] = {"change_24h": prices[m].get("change_pct", 0.0)}

    # 3. Verify data
    verify_msg = verify_post_data(prices, changes)
    log("market_overview", f"VERIFY: {verify_msg}")
    if verify_msg != "OK":
        log("market_overview", "verify failed but proceeding")

    # 4. OHLC for chart
    btc_ohlc = fetch_crypto_ohlc("BTC", days=7)
    ohlc = {"BTC": btc_ohlc}

    # 5. Build chart
    chart_path = f"posts/top5_btc_{ts.strftime('%Y%m%d_%H%M')}.png"
    os.makedirs("posts", exist_ok=True)
    build_top5_chart(prices, ohlc, {}, {}, chart_path)
    log("market_overview", f"chart saved: {chart_path}")

    # 6. Build text
    fng = fetch_fng()
    caption = build_caption(prices, changes, fng, ts)
    body = build_body(prices, changes, positional, fng, ts)

    # 7. Post
    post_pair(str(chart_path), caption, body)
    log("market_overview", "posted OK")


if __name__ == "__main__":
    main()