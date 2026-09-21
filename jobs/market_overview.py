"""13:30 YEKT market overview — BTC/ETH/BNB/SOL/XRP with Bybit/OKX fallback chain.

Self-contained: requests + zoneinfo only. No DB or external deps. Designed for
GitHub Actions cron run on weekdays at 13:30 Yekaterinburg (08:30 UTC).
"""

import json as _json
import logging
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
from zoneinfo import ZoneInfo

# --- Constants --------------------------------------------------------------

BINANCE_BASE = "https://api.binance.com"
BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"
KRAKEN_BASE = "https://api.kraken.com"
COINBASE_BASE = "https://api.exchange.coinbase.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

YAHOO_SYMBOLS = {"Gold": "GC=F", "Silver": "SI=F"}
GOLD_API_IDS = {"Gold": "XAU", "Silver": "XAG"}

CRYPTO_TICKERS = ["BTC", "ETH", "BNB", "SOL", "XRP"]

SYMBOLS = {t: f"{t}USDT" for t in CRYPTO_TICKERS}

OKX_SWAP_INST = {t: f"{t}-USDT-SWAP" for t in CRYPTO_TICKERS}

COINGECKO_IDS = {"BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin",
                 "SOL": "solana", "XRP": "ripple"}

YE_TZ = ZoneInfo("Asia/Yekaterinburg")

# throttling between failed sources (seconds)
THROTTLE = {"binance": 1.0, "bybit": 0.5, "okx": 0.5, "kraken": 1.0,
            "coingecko": 0.5, "coinbase": 0.5, "yahoo": 1.0, "gold-api": 0.3}

log = logging.getLogger("market_overview")
if not log.handlers:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")


# ─── HTTP helpers ──────────────────────────────────────────────────────────

def _http_json(url, params=None, headers=None, timeout=15):
    r = requests.get(url, params=params or {}, headers=headers or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _http_get(url, params=None, headers=None, timeout=15):
    return requests.get(url, params=params or {}, headers=headers or {}, timeout=timeout)


# ─── Price fetcher (4-source) ──────────────────────────────────────────────

def _from_binance_price(symbol):
    data = _http_json(f"{BINANCE_BASE}/api/v3/ticker/24hr", {"symbol": symbol})
    return float(data["lastPrice"])


def _from_kraken_price(symbol):
    pair = SYMBOLS_KRAKEN.get(symbol, symbol)
    data = _http_json(f"{KRAKEN_BASE}/Ticker", {"pair": pair})
    res = data.get("result") or {}
    for k, v in res.items():
        if isinstance(v, dict) and v.get("c"):
            return float(v["c"][0])
    raise RuntimeError(f"Kraken price: no ticker for {symbol}")


def _from_coinbase_price(symbol):
    coin = symbol.replace("USDT", "-USD").replace("USD", "-USD")
    # Coinbase uses BTC-USD style; BTCUSDT -> BTC-USD
    base = SYMBOLS.get(symbol, symbol).replace("USDT", "")
    coin = f"{base}-USD"
    data = _http_json(f"{COINBASE_BASE}/prices/{coin}/spot")
    return float(data["data"]["amount"])


def _from_coingecko_price(symbol):
    coin_id = COINGECKO_IDS[symbol]
    data = _http_json(f"{COINGECKO_BASE}/simple/price",
                      {"ids": coin_id, "vs_currencies": "usd"})
    return float(data[coin_id]["usd"])


SYMBOLS_KRAKEN = {"BTCUSDT": "XBTUSD", "ETHUSDT": "ETHUSD", "BNBUSDT": "BNBUSD",
                  "SOLUSDT": "SOLUSD", "XRPUSDT": "XRPUSD"}


def fetch_crypto_price(symbol, max_attempts=2):
    """Median of {binance, kraken, coinbase, coingecko} with retry."""
    sources = {"binance": _from_binance_price, "kraken": _from_kraken_price,
               "coinbase": _from_coinbase_price, "coingecko": _from_coingecko_price}
    last_err = None
    for _ in range(max_attempts):
        prices = []
        srcs = []
        for name, fn in sources.items():
            try:
                p = fn(symbol)
                prices.append(p)
                srcs.append(name)
            except Exception as e:  # noqa: BLE001
                last_err = e
                continue
        if len(prices) >= 2:
            med = statistics.median(prices)
            spread = (max(prices) - min(prices)) / med * 100 if med else 0.0
            return med, spread, len(prices), srcs
        time.sleep(2)
    raise RuntimeError(f"price FAIL for {symbol}: {last_err}")


# ─── Change (24h %) ────────────────────────────────────────────────────────

def _change_binance(symbol):
    data = _http_json(f"{BINANCE_BASE}/api/v3/ticker/24hr", {"symbol": symbol})
    return float(data["priceChangePercent"])


def _change_bybit(symbol):
    data = _http_json(f"{BYBIT_BASE}/v5/market/tickers",
                      {"category": "spot", "symbol": symbol})
    rows = data.get("result", {}).get("list") or []
    if not rows:
        raise RuntimeError("Bybit: empty list")
    row = rows[0]
    last = float(row["lastPrice"])
    open24 = float(row["open24h"])
    return (last - open24) / open24 * 100


def _change_okx(symbol):
    inst = OKX_SWAP_INST[symbol]
    data = _http_json(f"{OKX_BASE}/api/v5/market/ticker", {"instId": inst})
    rows = data.get("data") or []
    if not rows:
        raise RuntimeError("OKX: empty data")
    row = rows[0]
    last = float(row["last"])
    open24 = float(row["open24h"])
    return (last - open24) / open24 * 100


def _change_coingecko(symbol):
    coin_id = COINGECKO_IDS[symbol]
    data = _http_json(f"{COINGECKO_BASE}/simple/price",
                      {"ids": coin_id, "vs_currencies": "usd",
                       "include_24hr_change": "true"})
    return float(data[coin_id]["usd_24h_change"])


def fetch_crypto_change(symbol):
    """24h change % with fallback chain."""
    for name in ("binance", "bybit", "okx", "coingecko"):
        fn = {"binance": _change_binance, "bybit": _change_bybit,
              "okx": _change_okx, "coingecko": _change_coingecko}[name]
        try:
            v = fn(symbol)
            return v, name
        except Exception as e:  # noqa: BLE001
            log.info(f"{name} FAIL change {symbol}: {e}")
            time.sleep(THROTTLE[name])
    return 0.0, "none"


# ─── OHLC ──────────────────────────────────────────────────────────────────

def _binance_ohlc(symbol, days):
    limit = days * 6  # 4h candles
    raw = _http_json(f"{BINANCE_BASE}/api/v3/klines",
                     {"symbol": symbol, "interval": "4h", "limit": limit})
    return [(int(t[0]), float(t[1]), float(t[2]), float(t[3]), float(t[4]))
            for t in raw], "binance"


def _kraken_ohlc(pair, days=7):
    """Try multiple Kraken pair aliases. Returns (candles, source)."""
    # Kraken pair aliases: query alias → actual response key.
    # Kraken often returns data under a different name than requested.
    ALIASES = {
        "BTCUSDT": ["XBTUSD", "XXBTZUSD"],
        "ETHUSDT": ["ETHUSD", "XETHZUSD", "ETHUSDT"],
        "BNBUSDT": ["BNBUSD"],
        "SOLUSDT": ["SOLUSD", "SOLUSDT"],
        "XRPUSDT": ["XRPUSD", "XXRPZUSD"],
    }
    aliases = ALIASES.get(pair, [pair])
    cutoff = int(time.time()) - days * 86400 - 3600  # 1h slack
    for alias in aliases:
        try:
            data = _http_json(f"{KRAKEN_BASE}/OHLC",
                              {"pair": alias, "interval": 240})
            res = (data.get("result") or {})
            for k, v in res.items():
                if isinstance(v, list) and v:
                    candles = [(int(c[0]) * 1000, float(c[1]), float(c[2]),
                                float(c[3]), float(c[4]))
                               for c in v if int(c[0]) >= cutoff]
                    if candles:
                        return candles, f"kraken:{k}"
        except Exception as e:  # noqa: BLE001
            log.info(f"kraken alias FAIL {pair} via {alias}: {e}")
            continue
    raise RuntimeError(f"Kraken: no candles for {pair} via {aliases}")


def _coingecko_ohlc(symbol, days):
    # symbol like "BTCUSDT" — strip USDT/USD to look up coin id
    base = symbol.replace("USDT", "").replace("USD", "")
    coin_id = COINGECKO_IDS.get(base)
    if not coin_id:
        raise RuntimeError(f"CoinGecko: unknown coin for {symbol}")
    data = _http_json(f"{COINGECKO_BASE}/coins/{coin_id}/ohlc",
                      {"vs_currency": "usd", "days": days})
    return [(int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]))
            for c in data], "coingecko"


def fetch_crypto_ohlc(symbol, days=7):
    """Binance → Kraken → CoinGecko fallback. Returns (candles, source)."""
    for name, fn in (("binance", lambda: _binance_ohlc(symbol, days)),
                      ("kraken",  lambda: _kraken_ohlc(symbol, days)),
                      ("coingecko", lambda: _coingecko_ohlc(symbol, days))):
        try:
            candles, source = fn()
            if candles:
                return candles, source
        except Exception as e:  # noqa: BLE001
            log.info(f"{name} FAIL ohlc {symbol}: {e}")
            time.sleep(THROTTLE.get(name, 0.5))
    raise RuntimeError(f"no ohlc for {symbol}")


# ─── Positional data (OI / funding / L-S) ──────────────────────────────────

def _binance_oi_funding(symbol):
    oi = _http_json(f"https://fapi.binance.com/fapi/v1/openInterest",
                    {"symbol": symbol})
    funding = _http_json(f"https://fapi.binance.com/fapi/v1/premiumIndex",
                         {"symbol": symbol})
    return float(oi["openInterest"]) * float(funding["markPrice"]), \
           float(funding["lastFundingRate"]) * 100


def _binance_long_short(symbol):
    data = _http_json(f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
                      {"symbol": symbol, "period": "5m", "limit": 1})
    rows = data or []
    if not rows:
        raise RuntimeError("Binance L/S: empty")
    return float(rows[0]["longShortRatio"])


def _bybit_oi(symbol):
    data = _http_json(f"{BYBIT_BASE}/v5/market/tickers",
                      {"category": "linear", "symbol": symbol})
    rows = data.get("result", {}).get("list") or []
    if not rows:
        raise RuntimeError("Bybit OI: empty")
    return float(rows[0]["openInterest"]) * float(rows[0]["markPrice"])


def _bybit_funding(symbol):
    data = _http_json(f"{BYBIT_BASE}/v5/market/funding/history",
                      {"category": "linear", "symbol": symbol, "limit": 1})
    rows = data.get("result", {}).get("list") or []
    if not rows:
        raise RuntimeError("Bybit funding: empty")
    return float(rows[0]["fundingRate"]) * 100


def _okx_oi(symbol):
    inst = OKX_SWAP_INST[symbol]
    data = _http_json(f"{OKX_BASE}/api/v5/market/open-interest",
                      {"instId": inst})
    rows = data.get("data") or []
    if not rows:
        raise RuntimeError("OKX OI: empty")
    oi_ccy = float(rows[0]["oi"])
    # convert to USDT via mark price
    ticker = _http_json(f"{OKX_BASE}/api/v5/market/ticker", {"instId": inst})
    trows = ticker.get("data") or []
    if not trows:
        raise RuntimeError("OKX OI: no ticker")
    mark = float(trows[0]["markPx"])
    return oi_ccy * mark


def _okx_funding(symbol):
    inst = OKX_SWAP_INST[symbol]
    data = _http_json(f"{OKX_BASE}/api/v5/public/funding-rate",
                      {"instId": inst})
    rows = data.get("data") or []
    if not rows:
        raise RuntimeError("OKX funding: empty")
    return float(rows[0]["fundingRate"]) * 100


def fetch_crypto_positional(symbol):
    """Fetch OI (USDT), funding %, L/S ratio with 3-source fallback."""
    out = {"oi_usdt": None, "funding": None, "long_short": None}

    # OI (USDT)
    for name, fn in (("binance", lambda: _binance_oi_funding(symbol)[0]),
                     ("bybit", lambda: _bybit_oi(symbol)),
                     ("okx", lambda: _okx_oi(symbol))):
        try:
            v = fn()
            out["oi_usdt"] = v
            log.info(f"{name} OK oi {symbol}: ${v/1e6:.2f}M")
            break
        except Exception as e:  # noqa: BLE001
            log.info(f"{name} FAIL oi {symbol}: {e}")
            time.sleep(THROTTLE.get(name, 0.5))

    # Funding
    for name, fn in (("binance", lambda: _binance_oi_funding(symbol)[1]),
                     ("bybit", lambda: _bybit_funding(symbol)),
                     ("okx", lambda: _okx_funding(symbol))):
        try:
            v = fn()
            out["funding"] = v
            log.info(f"{name} OK funding {symbol}: {v:.4f}%")
            break
        except Exception as e:  # noqa: BLE001
            log.info(f"{name} FAIL funding {symbol}: {e}")
            time.sleep(THROTTLE.get(name, 0.5))

    # L/S ratio
    for name, fn in (("binance", lambda: _binance_long_short(symbol)),):
        try:
            ratio = fn()
            out["long_short"] = ratio
            log.info(f"{name} OK L/S {symbol}: {ratio:.2f}")
            break
        except Exception as e:  # noqa: BLE001
            log.info(f"{name} FAIL L/S {symbol}: {e}")
            time.sleep(THROTTLE.get(name, 0.5))

    if out["long_short"] is None:
        # OKX long-short-account-ratio fallback
        # Response shape: data = [[ts_ms, ratio], ...] (list of lists).
        try:
            base = symbol.replace("USDT", "")
            lr = _http_json(f"{OKX_BASE}/api/v5/rubik/stat/contracts/long-short-account-ratio",
                            {"ccy": base, "period": "5m"})
            if lr.get("data") and isinstance(lr["data"], list) and lr["data"]:
                row = lr["data"][-1]  # most recent bar
                ratio = float(row[1]) if len(row) > 1 else None
                if ratio is not None:
                    out["long_short"] = ratio
                    log.info(f"okx OK L/S {symbol}: {ratio:.2f}")
        except Exception as e:  # noqa: BLE001
            log.info(f"okx FAIL L/S {symbol}: {e}")

    return out


# ─── Metals ────────────────────────────────────────────────────────────────

def fetch_metal(yahoo_symbol, gold_api_id):
    """gold-api.com → Yahoo fallback. Returns {price, change_pct, source}.

    gold-api.com is more reliable than Yahoo (no rate limit on shared runners);
    Yahoo is only used to recover the 24h change % when gold-api returns a
    flat price.
    """
    # 1. gold-api.com (primary)
    try:
        data = _http_json(f"https://api.gold-api.com/price/{gold_api_id}", timeout=10)
        price = float(data["price"])
        # Try Yahoo for the 24h change (best effort, ignore failures).
        ch = 0.0
        try:
            yd = _http_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}",
                            {"interval": "1d", "range": "5d"}, timeout=10)
            meta = yd["chart"]["result"][0]["meta"]
            prev = float(meta.get("chartPreviousClose") or meta.get("previousClose") or price)
            yahoo_price = float(meta.get("regularMarketPrice") or price)
            ch = (yahoo_price - prev) / prev * 100 if prev else 0.0
        except Exception:
            pass
        return {"price": price, "change_pct": ch, "source": "gold-api"}
    except Exception as e:  # noqa: BLE001
        log.info(f"gold-api FAIL {gold_api_id}: {e}")
    # 2. Yahoo (secondary)
    try:
        data = _http_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}",
                          {"interval": "1d", "range": "5d"}, timeout=10)
        meta = data["chart"]["result"][0]["meta"]
        price = float(meta["regularMarketPrice"])
        prev = float(meta.get("chartPreviousClose") or meta.get("previousClose") or price)
        ch = (price - prev) / prev * 100 if prev else 0.0
        return {"price": price, "change_pct": ch, "source": "yahoo"}
    except Exception as e:  # noqa: BLE001
        log.info(f"yahoo FAIL {yahoo_symbol}: {e}")
    return {"price": None, "change_pct": 0.0, "source": "none"}


# ─── F&G ───────────────────────────────────────────────────────────────────

def fetch_fng():
    try:
        data = _http_json("https://api.alternative.me/fng/?limit=1", timeout=10)
        return int(data["data"][0]["value"]), int(data["data"][0]["value_classification"])
    except Exception as e:  # noqa: BLE001
        log.info(f"fng FAIL: {e}")
        return None, "Unknown"


# ─── Formatting helpers ────────────────────────────────────────────────────

def fmt_price(p):
    if p is None:
        return "—"
    if p >= 1000:
        return f"${p:,.0f}"
    return f"${p:,.2f}"


def fmt_pct(p):
    if p is None:
        return "—"
    arrow = "▲" if p >= 0 else "▼"
    return f"{arrow}{abs(p):.2f}%"


def fmt_oi(v):
    if v is None:
        return "—"
    if v >= 1e9:
        return f"${v/1e9:.2f}B"
    if v >= 1e6:
        return f"${v/1e6:.1f}M"
    return f"${v:,.0f}"


def fmt_funding(f):
    if f is None:
        return "—"
    sign = "+" if f >= 0 else "−"
    return f"{sign}{abs(f):.3f}%"


def fmt_ls(ratio):
    if ratio is None:
        return "—"
    long_pct = ratio / (1 + ratio) * 100
    return f"{long_pct:.0f}% лонг"


# ─── Caption / Body ────────────────────────────────────────────────────────

def build_caption(prices, changes, metals, fng):
    parts = ["🏛 REDDINGTON · Топ-5 + Металлы"]
    parts.append(f"_{datetime.now(YE_TZ).strftime('%d %b %Y · %H:%M')} YEKT_")
    parts.append("")

    parts.append("📊 Крипто (24ч)")
    for t in CRYPTO_TICKERS:
        p = prices.get(t, (None, 0, 0, []))[0]
        ch = changes.get(t, (None, "none"))[0]
        if p is None:
            continue
        emoji = "🟢" if (ch or 0) >= 0 else "🔴"
        ch_str = f" {fmt_pct(ch)}" if ch is not None else ""
        parts.append(f"{emoji} {t} {fmt_price(p)}{ch_str}")

    parts.append("")
    parts.append("🥇 Металлы")
    for k in ("Gold", "Silver"):
        m = metals.get(k, {})
        p = m.get("price")
        ch = m.get("change_pct")
        if p is None:
            continue
        emoji = "🟡" if k == "Gold" else "⚪"
        ch_str = f" {fmt_pct(ch)}" if ch and ch != 0 else ""
        parts.append(f"{emoji} {k} {fmt_price(p)}{ch_str}")

    fng_val, fng_class = fng
    if fng_val is not None:
        fng_emoji = "🟢" if fng_val >= 55 else ("🔴" if fng_val <= 45 else "🟡")
        parts.append("")
        parts.append(f"🌡 Fear & Greed {fng_emoji} {fng_val}/100 · {fng_class}")

    return "\n".join(parts)


def build_body(prices, changes, positional, metals, fng, tech):
    parts = []
    parts.append("🏛 REDDINGTON · Обзор рынка")
    parts.append(f"_{datetime.now(YE_TZ).strftime('%A, %d %B %Y · %H:%M')} YEKT_")
    parts.append("")

    has_oi = any(positional.get(t, {}).get("oi_usdt") for t in CRYPTO_TICKERS)
    has_funding = any(positional.get(t, {}).get("funding") is not None for t in CRYPTO_TICKERS)
    has_ls = any(positional.get(t, {}).get("long_short") for t in CRYPTO_TICKERS)

    parts.append("📈 Сводка по активам")
    headers = ["Актив", "Цена", "24ч"]
    if has_oi:
        headers.append("OI")
    if has_funding:
        headers.append("Funding")
    if has_ls:
        headers.append("L/S")
    widths = [8, 14, 10]
    if has_oi:
        widths.append(8)
    if has_funding:
        widths.append(10)
    if has_ls:
        widths.append(10)

    parts.append(" ".join(h.ljust(w) for h, w in zip(headers, widths)))
    parts.append("─" * sum(widths))

    for t in CRYPTO_TICKERS:
        p = prices.get(t, (None, 0, 0, []))[0]
        ch = changes.get(t, (None, "none"))[0]
        if p is None:
            continue
        row_parts = [t.ljust(widths[0]), fmt_price(p).rjust(widths[1]),
                     (fmt_pct(ch) if ch is not None else "—").rjust(widths[2])]
        if has_oi:
            oi = positional.get(t, {}).get("oi_usdt")
            row_parts.append(fmt_oi(oi).rjust(widths[len(row_parts)]))
        if has_funding:
            fr = positional.get(t, {}).get("funding")
            row_parts.append(fmt_funding(fr).rjust(widths[len(row_parts)]))
        if has_ls:
            ls = positional.get(t, {}).get("long_short")
            row_parts.append(fmt_ls(ls).rjust(widths[len(row_parts)]))
        parts.append(" ".join(row_parts))

    # Metals rows
    for k in ("Gold", "Silver"):
        m = metals.get(k, {})
        p = m.get("price")
        ch = m.get("change_pct")
        if p is None:
            continue
        ch_str = fmt_pct(ch) if ch and ch != 0 else "—"
        parts.append(f"{k} {fmt_price(p)} {ch_str}")

    parts.append("")
    fng_val, fng_class = fng
    if fng_val is not None:
        fng_emoji = "🟢" if fng_val >= 55 else ("🔴" if fng_val <= 45 else "🟡")
        parts.append(f"🌡 Fear & Greed {fng_emoji} {fng_val}/100 · {fng_class}")

    btc_pos = positional.get("BTC", {})
    sup = tech.get("btc", {}).get("support")
    res = tech.get("btc", {}).get("resistance")
    if sup and res:
        parts.append("")
        parts.append("🎯 Зоны BTC")
        parts.append(f"Поддержка {fmt_price(sup)}")
        parts.append(f"Сопротивление {fmt_price(res)}")

    return "\n".join(parts)


# ─── Chart (BTC 7d) ────────────────────────────────────────────────────────

def build_top5_chart():
    """Render BTC chart with support/resistance zones.

    Wrapper around lib.chart.generate_chart (which loads make_v6 from
    channel-preview/). Returns chart_path on success.
    """
    symbol = "BTCUSDT"
    try:
        candles, source = fetch_crypto_ohlc(symbol, days=7)
    except Exception as e:
        raise RuntimeError(f"no BTC ohlc for chart: {e}")
    if not candles:
        raise RuntimeError(f"no BTC ohlc for chart ({source or 'empty'})")
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    chart_dir = os.path.join(repo_root, "posts")
    os.makedirs(chart_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).astimezone()
    chart_path = os.path.join(chart_dir, f"top5_btc_{ts.strftime('%Y%m%d_%H%M')}.png")
    last_close = float(candles[-1][4])
    sup = last_close * 0.97
    res = last_close * 1.03
    from lib.chart import generate_chart
    generate_chart(candles, "BTC/USDT", (sup, sup), (res, res), chart_path)
    return chart_path


# --- Main ----------------------------------------------------------------

def main():
    if was_posted_recently("market_overview", min_age_minutes=60):
        log.info("skipped: market_overview posted recently")
        return

    ts = datetime.now(timezone.utc)
    # 1. prices
    prices = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_crypto_price, SYMBOLS[t]): t for t in CRYPTO_TICKERS}
        for f in futs:
            t = futs[f]
            try:
                median, spread, n, srcs = f.result()
                prices[t] = (median, spread, n, srcs)
                log.info(f"{t} ${median:.2f} spread={spread:.3f}% n={n} srcs={srcs}")
            except Exception as e:  # noqa: BLE001
                prices[t] = (None, 0.0, 0, [])
                log.error(f"{t} FAIL price: {e}")
                raise

    # 2. changes
    changes = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_crypto_change, SYMBOLS[t]): t for t in CRYPTO_TICKERS}
        for f in futs:
            t = futs[f]
            try:
                ch, src = f.result()
                changes[t] = (ch, src)
                log.info(f"{t} change {ch:+.2f}% src={src}")
            except Exception as e:  # noqa: BLE001
                log.error(f"{t} FAIL change: {e}")
                changes[t] = (None, "none")

    # 3. positional (OI / funding / L/S)
    positional = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_crypto_positional, SYMBOLS[t]): t for t in CRYPTO_TICKERS}
        for f in futs:
            t = futs[f]
            try:
                positional[t] = f.result()
            except Exception as e:  # noqa: BLE001
                log.error(f"{t} FAIL positional: {e}")
                positional[t] = {"oi_usdt": None, "funding": None, "long_short": None}

    # 4. metals
    metals = {}
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {ex.submit(fetch_metal, YAHOO_SYMBOLS[k], GOLD_API_IDS[k]): k
                for k in ("Gold", "Silver")}
        for f in futs:
            k = futs[f]
            try:
                metals[k] = f.result()
            except Exception as e:  # noqa: BLE001
                log.error(f"{k} FAIL metal: {e}")
                metals[k] = {"price": None, "change_pct": 0.0, "source": "none"}

    # 5. fng
    with ThreadPoolExecutor(max_workers=3) as ex:
        gold_f = ex.submit(fetch_metal, "GC=F", "XAU")
        silver_f = ex.submit(fetch_metal, "SI=F", "XAG")
        fng_f = ex.submit(fetch_fng)
        metals = {"Gold": gold_f.result(), "Silver": silver_f.result()}
        fng = fng_f.result()
    # 6. verify
    if not verify_post_data(prices, changes):
        log.error("verify FAIL")
        return

    # 7. build chart
    try:
        chart_path = build_top5_chart()
    except Exception as e:
        log.error(f"chart FAIL: {e}")
        return

    # 8. build caption + body
    tech = {"btc": {}, "watchlist": []}
    caption = build_caption(prices, changes, metals, fng)
    body = build_body(prices, changes, positional, metals, fng, tech)


if __name__ == "__main__":
    main()