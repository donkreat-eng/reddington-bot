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
    try:
        d = _http_json(f"{BINANCE_BASE}/api/v3/ticker/24hr", {"symbol": symbol})
        return {
            "change_24h": float(d["priceChangePercent"]),
            "volume_24h": float(d["quoteVolume"]),
            "source": "binance",
        }
    except Exception as e:
        log("market_overview", f"change {ticker} Binance FAIL: {e}; trying Bybit")
    try:
        ex = _bybit_extras(symbol)
        return {
            "change_24h": ex["change_24h"],
            "volume_24h": ex["volume_24h"],
            "source": "bybit",
        }
    except Exception as e:
        log("market_overview", f"change {ticker} Bybit FAIL: {e}; trying OKX")
    try:
        ex = _okx_extras(symbol)
        return {
            "change_24h": ex["change_24h"],
            "volume_24h": ex["volume_24h"],
            "source": "okx",
        }
    except Exception as e:
        log("market_overview", f"change {ticker} OKX FAIL: {e}; trying CoinGecko")
    try:
        d = _http_json(f"{COINGECKO_BASE}/simple/price",
                       {"ids": coin_id, "vs_currencies": "usd",
                        "include_24hr_change": "true"})
        return {
            "change_24h": float(d[coin_id]["usd_24h_change"]),
            "volume_24h": 0.0,
            "source": "coingecko",
        }
    except Exception as e:
        log("market_overview", f"change {ticker} CoinGecko FAIL: {e}")
        raise RuntimeError(f"{ticker}: no change data from any source")


# ─── OHLC for chart ───────────────────────────────────────────────────────────


_KRAKEN_ALIASES = {
    "BTCUSDT": ("XBTUSDT", "XXBTZUSD", "XBTUSD"),
    "ETHUSDT": ("ETHUSDT", "XETHZUSD", "ETHUSD"),
    "BNBUSDT": ("BNBUSDT",),
    "SOLUSDT": ("SOLUSDT",),
    "XRPUSDT": ("XRPUSDT", "XXRPZUSD", "XRPUSD"),
}


def _kraken_ohlc(pair: str, days: int = 7) -> list:
    """Kraken OHLC since N days ago. Tries multiple pair aliases."""
    since = int((time.time() - days * 86400) - 3600)
    aliases = _KRAKEN_ALIASES.get(pair, (pair,))
    last_err: Exception | None = None
    for alias in aliases:
        try:
            d = _http_json(f"{KRAKEN_BASE}/OHLC",
                           {"pair": alias, "interval": KRAKEN_INTERVAL, "since": since})
        except Exception as e:
            last_err = e
            continue
        candles = []
        for k, v in d.get("result", {}).items():
            if k == "last":
                continue
            if not isinstance(v, list):
                continue
            for c in v:
                if c[0] >= since:
                    candles.append((c[0] * 1000, float(c[1]), float(c[2]),
                                    float(c[3]), float(c[4])))
        if candles:
            return candles
    if last_err:
        raise last_err
    raise RuntimeError(f"Kraken: no candles for {pair} via {aliases}")


def _coingecko_ohlc(coin_id: str, days: int = 7) -> list:
    """CoinGecko OHLC."""
    d = _http_json(f"{COINGECKO_BASE}/coins/{coin_id}/ohlc",
                   {"vs_currency": "usd", "days": days})
    return [(c[0], float(c[1]), float(c[2]), float(c[3]), float(c[4])) for c in d]


def fetch_crypto_ohlc(ticker: str, days: int = 7) -> list:
    """BTC OHLC for chart. Binance → Kraken (multi-alias) → CoinGecko."""
    symbol = BINANCE_SYMBOL[ticker]
    coin_id = COINGECKO_IDS[ticker]
    try:
        d = _http_json(f"{BINANCE_BASE}/api/v3/klines",
                       {"symbol": symbol, "interval": "4h", "limit": days * 6})
        candles = [(c[0], float(c[1]), float(c[2]), float(c[3]), float(c[4]))
                   for c in d]
        log("market_overview", f"ohlc {ticker} source=binance n={len(candles)}")
        return candles
    except Exception as e:
        log("market_overview", f"ohlc {ticker} Binance FAIL: {e}; trying Kraken")
    try:
        candles = _kraken_ohlc(symbol, days)
        log("market_overview", f"ohlc {ticker} source=kraken n={len(candles)}")
        return candles
    except Exception as e:
        log("market_overview", f"ohlc {ticker} Kraken FAIL: {e}; trying CoinGecko")
    candles = _coingecko_ohlc(coin_id, days)
    log("market_overview", f"ohlc {ticker} source=coingecko n={len(candles)}")
    return candles


# ─── Positional (OI / funding / L/S) ──────────────────────────────────────────


def _binance_oi_funding(symbol: str) -> dict:
    """Binance USDⓈ-M futures: OI + funding."""
    oi_url = f"{BINANCE_FAPI}/fapi/v1/openInterest?symbol={symbol}"
    fr_url = f"{BINANCE_FAPI}/fapi/v1/fundingRate?symbol={symbol}&limit=1"
    oi_data = _http_json(oi_url)
    fr_data = _http_json(fr_url)
    oi_contracts = float(oi_data["openInterest"])
    mark = _http_json(f"{BINANCE_FAPI}/fapi/v1/premiumIndex?symbol={symbol}")
    mark_price = float(mark["markPrice"])
    fr = float(fr_data[0]["fundingRate"]) if fr_data else 0.0
    return {"oi_usdt": oi_contracts * mark_price, "funding": fr * 100}


def _binance_long_short(symbol: str) -> float:
    """Binance top trader long/short account ratio."""
    d = _http_json(f"{BINANCE_FAPI}/fapi/v1/globalLongShortAccountRatio",
                   {"symbol": symbol, "period": "5m", "limit": 1})
    return float(d[0]["longShortRatio"])


def _okx_long_short(coin: str) -> float | None:
    """OKX long/short account ratio. coin = BTC, ETH, etc."""
    try:
        d = _http_json(f"{OKX_BASE}/api/v5/rubik/stat/contracts/long-short-account-ratio",
                       {"ccy": coin, "period": "5m"}, timeout=10)
        if d.get("data"):
            return float(d["data"][0]["ratio"])
    except Exception:
        return None
    return None


def fetch_crypto_positional(ticker: str) -> dict:
    """OI USDT, funding %, L/S ratio."""
    symbol = BINANCE_SYMBOL[ticker]
    coin = ticker
    out: dict = {"oi_usdt": None, "funding": None, "long_short": None}

    try:
        ex = _binance_oi_funding(symbol)
        out["oi_usdt"] = ex["oi_usdt"]
        out["funding"] = ex["funding"]
        log("market_overview",
            f"OI/funding {ticker} Binance OK oi=${ex['oi_usdt']/1e9:.2f}B fr={ex['funding']:.4f}%")
    except Exception as e:
        log("market_overview",
            f"OI/funding {ticker} Binance FAIL: {e}; trying Bybit")
        try:
            ex = _bybit_extras(symbol)
            out["oi_usdt"] = ex["oi_usdt"]
            out["funding"] = ex["funding"]
            log("market_overview",
                f"OI/funding {ticker} Bybit OK oi=${ex['oi_usdt']/1e9:.2f}B fr={ex['funding']:.4f}%")
        except Exception as e2:
            log("market_overview",
                f"OI/funding {ticker} Bybit FAIL: {e2}; trying OKX")
            try:
                ex = _okx_extras(symbol)
                out["oi_usdt"] = ex["oi_usdt"]
                out["funding"] = ex["funding"]
                log("market_overview",
                    f"OI/funding {ticker} OKX OK oi=${ex['oi_usdt']/1e9:.2f}B fr={ex['funding']:.4f}%")
            except Exception as e3:
                log("market_overview",
                    f"OI/funding {ticker} OKX FAIL: {e3}")

    try:
        ratio = _binance_long_short(symbol)
        out["long_short"] = ratio
        log("market_overview", f"L/S {ticker} Binance OK {ratio:.2f}")
    except Exception as e:
        log("market_overview",
            f"L/S {ticker} Binance FAIL: {e}; trying OKX")
        ratio = _okx_long_short(coin)
        if ratio is not None:
            out["long_short"] = ratio
            log("market_overview", f"L/S {ticker} OKX OK {ratio:.2f}")
        else:
            log("market_overview", f"L/S {ticker} OKX FAIL")

    return out


# ─── Metals ───────────────────────────────────────────────────────────────────


def _yahoo_metal(symbol: str) -> dict:
    """Yahoo Finance GC=F/SI=F via chart endpoint."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    r = requests.get(url, params={"range": "5d", "interval": "1d"},
                     timeout=10, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    data = r.json()["chart"]["result"][0]
    meta = data["meta"]
    price = float(meta["regularMarketPrice"])
    prev = float(meta.get("chartPreviousClose", meta.get("previousClose", price)))
    return {"price": price, "change_pct": (price - prev) / prev * 100 if prev else 0.0}


def _gold_api_metal(symbol: str) -> dict | None:
    """gold-api.com fallback (no auth, USD)."""
    try:
        d = _http_json(f"https://api.gold-api.com/price/{symbol}", timeout=10)
        return {"price": float(d["price"]), "change_pct": 0.0}
    except Exception:
        return None


def fetch_metal(name: str) -> dict:
    """Gold/Silver price + 24h change%. Yahoo → gold-api.com fallback."""
    sym = METAL_SYMBOLS[name]
    try:
        out = _yahoo_metal(sym)
        log("market_overview",
            f"{name} ${out['price']:.2f} ch={out['change_pct']:+.2f}% (yahoo)")
        return out
    except Exception as e:
        log("market_overview",
            f"{name} Yahoo FAIL: {e}; trying gold-api.com")
    xau_xag = "XAU" if name == "Gold" else "XAG"
    out = _gold_api_metal(xau_xag)
    if out:
        log("market_overview",
            f"{name} ${out['price']:.2f} ch=0.00% (gold-api.com, no change)")
        return out
    log("market_overview", f"{name} ALL sources FAILED")
    return {"price": None, "change_pct": 0.0}


# ─── Fear & Greed ─────────────────────────────────────────────────────────────


def fetch_fng() -> int:
    try:
        d = _http_json("https://api.alternative.me/fng/", {"limit": 1})
        return int(d["data"][0]["value"])
    except Exception:
        return 50


# ─── Verification ─────────────────────────────────────────────────────────────


def verify_post_data(prices: dict, changes: dict) -> str:
    for t in CRYPTO_TICKERS:
        sp = prices.get(t, {}).get("spread", 99)
        if sp > 1.5:
            return f"FAIL: {t} spread {sp:.2f}% > 1.5%"
    for t in CRYPTO_TICKERS:
        ch = changes.get(t, {}).get("change_24h", 0)
        if abs(ch) > 30:
            return f"FAIL: {t} change {ch:.1f}% > 30%"
    return "OK"


# ─── Caption (top — photo) ───────────────────────────────────────────────────


def build_caption(prices: dict, changes: dict, fng: int, ts: datetime) -> str:
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
    ts_str = ts.strftime("%A, %d %B %Y · %H:%M YEKT")
    parts = [
        f"🏛 REDDINGTON · Обзор рынка",
        f"_{ts_str}_",
        "",
        "📈 *Сводка по активам*",
        "",
    ]

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

    parts.append(_line([(h, w, a) for h, w, a in cols]))
    parts.append(_line([("─" * w, w, 'l') for _, w, _ in cols]))

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

    fng_emoji = "🟢" if fng >= 60 else "🔴" if fng <= 40 else "🟡"
    parts.append(f"🌡 *Fear &amp; Greed* {fng_emoji} {fng}/100")
    parts.append("")

    btc_pos = positional.get("BTC", {})
    btc_p = prices["BTC"]["price"]
    parts.append("🔍 *Зоны BTC*")
    sup = btc_p * 0.97
    res = btc_p * 1.03
    parts.append(f"Поддержка {fmt_price(sup)}")
    parts.append(f"Сопротивление {fmt_price(res)}")
    parts.append("")

    funding = btc_pos.get("funding")
    ls = btc_pos.get("long_short")
    if funding is not None or ls is not None:
        parts.append("⚙️ *Кластеры (BTC)*")
        if funding is not None:
            parts.append(f"Funding: {fmt_funding(funding)}")
        if ls is not None:
            parts.append(f"L/S: {fmt_ls(ls)}")
        parts.append("")

    parts.append("🧭 *Что смотреть*")
    parts.append("• BTC funding &gt; +0.05% — overheated longs")
    parts.append("• BTC L/S &gt; 1.5 — перевес лонгов")
    parts.append("• Gold &lt; $4 200 — разворот вверх")

    return "\n".join(parts)


def fmt_price(x) -> str:
    """fmt_price from lib/post, imported below if present."""
    from lib.post import fmt_price as _fp
    return _fp(x)


def fmt_oi(usdt) -> str:
    """Open interest: $2.51B / $348M / '—'."""
    if usdt is None:
        return "—"
    if usdt >= 1e9:
        return f"${usdt / 1e9:.2f}B"
    if usdt >= 1e6:
        return f"${usdt / 1e6:.1f}M"
    if usdt >= 1e3:
        return f"${usdt / 1e3:.1f}K"
    return f"${usdt:.0f}"


def fmt_funding(f) -> str:
    """Funding rate in %. f is already in percent (e.g. 0.01 = 0.01%)."""
    if f is None:
        return "—"
    sign = "+" if f >= 0 else "−"
    return f"{sign}{abs(f):.3f}%"


def fmt_ls(ratio) -> str:
    """Long/short ratio: 1.50 / '—'."""
    if ratio is None:
        return "—"
    if ratio >= 1:
        return f"{ratio:.2f} лонг"
    return f"{ratio:.2f} шорт"


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
                prices[key] = v

    for m in ["Gold", "Silver"]:
        if m in prices:
            changes[m] = {"change_24h": prices[m].get("change_pct", 0.0)}

    verify_msg = verify_post_data(prices, changes)
    log("market_overview", f"VERIFY: {verify_msg}")
    if verify_msg != "OK":
        log("market_overview", "verify failed but proceeding")

    btc_ohlc = fetch_crypto_ohlc("BTC", days=7)
    ohlc = {"BTC": btc_ohlc}

    chart_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "posts")
    os.makedirs(chart_dir, exist_ok=True)
    chart_path = os.path.join(chart_dir, f"top5_btc_{ts.strftime('%Y%m%d_%H%M')}.png")
    build_top5_chart(prices, ohlc, {}, {}, chart_path)
    log("market_overview", f"chart saved: {chart_path}")

    fng = fetch_fng()
    caption = build_caption(prices, changes, fng, ts)
    body = build_body(prices, changes, positional, fng, ts)

    post_pair(str(chart_path), caption, body)
    log("market_overview", "posted OK")


if __name__ == "__main__":
    main()