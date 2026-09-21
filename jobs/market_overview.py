"""13:30 YEKT market overview — BTC/ETH/BNB/SOL/XRP + Gold/Silver с позиционным слоем.

Полностью автономный. Перенеси в cron через post_pair(job_name="market_overview", min_age_minutes=60).
Источники (auto-fallback, без ключей):
  • prices — Binance → Bybit → Kraken → Coinbase → CoinGecko (медиана + spread)
  • change_24h — Binance → Bybit → OKX → CoinGecko
  • ohlc (top5 chart) — Binance → Kraken → CoinGecko
  • OI / funding — Binance → Bybit → OKX (long-short-account-ratio для L/S через OKX rubik)
  • metals — Yahoo Finance GC=F / SI=F → api.gold-api.com/{XAU,XAG}
  • fear & greed — alternative.me
"""
import base64
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.publish import post_pair, was_posted_recently, mark_posted

log = logging.getLogger("market_overview")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- Tickers --------------------------------------------------------------

CRYPTO_TICKERS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
BINANCE_SYMBOL = {"BTCUSDT": "BTCUSDT", "ETHUSDT": "ETHUSDT", "BNBUSDT": "BNBUSDT",
                   "SOLUSDT": "SOLUSDT", "XRPUSDT": "XRPUSDT"}
COINBASE_PRODUCT = {"BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD", "BNBUSDT": "BNB-USD",
                     "SOLUSDT": "SOL-USD", "XRPUSDT": "XRP-USD"}
COINGECKO_ID = {"BTCUSDT": "bitcoin", "ETHUSDT": "ethereum", "BNBUSDT": "binancecoin",
                 "SOLUSDT": "solana", "XRPUSDT": "ripple"}

COINBASE_BASE = "https://api.exchange.coinbase.com"
BYBIT_BASE = "https://api.bybit.com"
KRAKEN_INTERVAL = 60  # seconds for Kraken OHLC throttle

OKX_BASE = "https://www.okx.com"
OKX_SWAP_INST = {"BTCUSDT": "BTC-USDT-SWAP", "ETHUSDT": "ETH-USDT-SWAP",
                  "BNBUSDT": "BNB-USDT-SWAP", "SOLUSDT": "SOL-USDT-SWAP",
                  "XRPUSDT": "XRP-USDT-SWAP"}

# Kraken OHLC: SDK maps USDT pairs to varying Kraken codes
_KRAKEN_ALIASES = {
    "BTCUSDT": ["XBTUSDT", "XXBTZUSD", "XBTUSD"],
    "ETHUSDT": ["ETHUSDT", "XETHZUSD", "ETHUSD"],
    "BNBUSDT": ["BNBUSD"],
    "SOLUSDT": ["SOLUSD"],
    "XRPUSDT": ["XRPUSD", "XXRPZUSD"],
}

# Per-source throttle to avoid rate limits on repeated calls
_BYBIT_CACHE = {}
_BYBIT_CACHE_TTL = 30

# --- HTTP helpers ---------------------------------------------------------

def _http_json(url, params=None, headers=None, timeout=15):
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _http_get(url, params=None, headers=None, timeout=15):
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


# --- Prices (median across 4 sources) ------------------------------------

def _from_binance_price(symbol):
    return _http_json("https://api.binance.com/api/v3/ticker/price",
                       {"symbol": symbol})["price"]


def _from_kraken_price(symbol):
    pairs = ["XBTUSDT"] if symbol == "BTCUSDT" else [symbol]
    for pair in pairs:
        j = _http_json("https://api.kraken.com/0/public/Ticker",
                       {"pair": pair})
        for v in j["result"].values():
            return v["c"][0]
    raise RuntimeError(f"kraken fail {symbol}")


def _from_coinbase_price(symbol):
    p = _http_json(f"{COINBASE_BASE}/products/{COINBASE_PRODUCT[symbol]}/ticker")["price"]
    return p


def _from_coingecko_price(symbol):
    j = _http_json(f"https://api.coingecko.com/api/v3/simple/price",
                    {"ids": COINGECKO_ID[symbol], "vs_currencies": "usd"})
    return str(j[COINGECKO_ID[symbol]]["usd"])


def fetch_crypto_price(symbol):
    """4-source consensus price with median + spread filter (no auth)."""
    sources = [_from_binance_price, _from_kraken_price, _from_coinbase_price, _from_coingecko_price]
    prices, sources_used = [], []
    for src in sources:
        try:
            p = float(src(symbol))
            prices.append(p)
            sources_used.append(src.__name__)
        except Exception as e:
            log.info(f"{src.__name__} FAIL {symbol}: {e}")
    if not prices:
        return {"price": None, "sources": [], "spread": None}
    prices.sort()
    median = (prices[len(prices)//2 - 1] + prices[len(prices)//2]) / 2 if len(prices) > 1 else prices[0]
    spread = (max(prices) - min(prices)) / median * 100 if median else None
    return {"price": median, "sources": sources_used, "spread": spread}


# --- Change 24h -----------------------------------------------------------

def _change_binance(symbol):
    j = _http_json("https://api.binance.com/api/v3/ticker/24hr",
                    {"symbol": symbol})
    return float(j["priceChangePercent"])


def _change_bybit(symbol):
    now = time.time()
    cache_key = f"ch_{symbol}"
    if cache_key in _BYBIT_CACHE and now - _BYBIT_CACHE[cache_key]["ts"] < _BYBIT_CACHE_TTL:
        return _BYBIT_CACHE[cache_key]["val"]
    j = _http_json(f"{BYBIT_BASE}/v5/market/tickers",
                    {"category": "spot", "symbol": symbol})
    rows = j["result"]["list"]
    if not rows:
        raise RuntimeError(f"bybit empty {symbol}")
    pct = float(rows[0]["price24hPcnt"]) * 100
    _BYBIT_CACHE[cache_key] = {"ts": now, "val": pct}
    return pct


def _change_okx(symbol):
    inst = OKX_SWAP_INST.get(symbol)
    if not inst:
        raise RuntimeError(f"okx no inst {symbol}")
    j = _http_json(f"{OKX_BASE}/api/v5/market/ticker",
                    {"instId": inst})
    rows = j.get("data", [])
    if not rows:
        raise RuntimeError(f"okx empty {symbol}")
    last = float(rows[0]["last"])
    open24 = float(rows[0]["open24h"])
    if not open24:
        raise RuntimeError("okx zero open24")
    return (last - open24) / open24 * 100


def _change_coingecko(symbol):
    j = _http_json(f"https://api.coingecko.com/api/v3/simple/price",
                    {"ids": COINGECKO_ID[symbol], "vs_currencies": "usd",
                      "include_24hr_change": "true"})
    return float(j[COINGECKO_ID[symbol]]["usd_24h_change"])


def fetch_crypto_change(symbol):
    for src in [_change_binance, _change_bybit, _change_okx, _change_coingecko]:
        try:
            return {"change": src(symbol), "source": src.__name__}
        except Exception as e:
            log.info(f"{src.__name__} FAIL change {symbol}: {e}")
    return {"change": 0.0, "source": None}


# --- OHLC -----------------------------------------------------------------

def _binance_ohlc(symbol, days):
    j = _http_json("https://api.binance.com/api/v3/klines",
                    {"symbol": symbol, "interval": "1h", "limit": days * 24})
    return [{"ts": k[0], "open": float(k[1]), "high": float(k[2]),
              "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])}
             for k in j]


def _kraken_ohlc(pair, days=7):
    """Try multiple Kraken pair aliases; raise if all empty so next fallback fires."""
    aliases = _KRAKEN_ALIASES.get(pair, [pair])
    since = int(time.time() - days * 86400 - 3600)  # 1h slack for hourly bars
    last_err = None
    for pair_alias in aliases:
        try:
            j = _http_json("https://api.kraken.com/0/public/OHLC",
                            {"pair": pair_alias, "interval": 60, "since": since})
        except Exception as e:
            last_err = e
            continue
        res = j.get("result", {})
        if not res:
            last_err = RuntimeError(f"empty result {pair_alias}")
            continue
        # Kraken returns key inside last dict
        candles = []
        for v in res.values():
            if isinstance(v, list) and v and isinstance(v[0], (list, tuple)):
                candles = v
                break
        if candles:
            return [{"ts": int(c[0]) * 1000, "open": float(c[1]),
                      "high": float(c[2]), "low": float(c[3]),
                      "close": float(c[4]), "volume": float(c[6])}
                     for c in candles]
    raise RuntimeError(f"Kraken no candles for {pair} via {aliases}: {last_err}")


def _coingecko_ohlc(symbol, days):
    j = _http_json(f"https://api.coingecko.com/api/v3/coins/{COINGECKO_ID[symbol]}/ohlc",
                    {"vs_currency": "usd", "days": days})
    return [{"ts": k[0], "open": float(k[1]), "high": float(k[2]),
              "low": float(k[3]), "close": float(k[4]), "volume": 0.0}
             for k in j]


def fetch_crypto_ohlc(symbol, days=7):
    """3-step fallback for BTC hourly candles (used by chart)."""
    # 1) Binance (ray-1) ...
    try:
        return _binance_ohlc(symbol, days), "binance"
    except Exception as e:
        log.info(f"binance FAIL ohlc {symbol}: {e}")
    # 2) Kraken
    try:
        return _kraken_ohlc(symbol, days), "kraken"
    except Exception as e:
        log.info(f"trying CoinGecko; kraken FAIL {symbol}: {e}")
    # 3) CoinGecko last resort
    try:
        return _coingecko_ohlc(symbol, days), "coingecko"
    except Exception as e:
        log.info(f"coingecko FAIL ohlc {symbol}: {e}")
    raise RuntimeError(f"no candles for {symbol}")


# --- OI / funding / L-S ---------------------------------------------------

def _binance_oi_funding(symbol):
    j = _http_json("https://fapi.binance.com/fapi/v1/openInterest",
                    {"symbol": symbol})
    oi_btc = float(j["openInterest"])
    mark_j = _http_json("https://fapi.binance.com/fapi/v1/premiumIndex",
                         {"symbol": symbol})
    mark = float(mark_j["markPrice"])
    oi_usdt = oi_btc * mark
    return oi_usdt


def _bybit_oi_funding(symbol):
    j = _http_json(f"{BYBIT_BASE}/v5/market/tickers",
                    {"category": "linear", "symbol": symbol})
    rows = j["result"]["list"]
    if not rows:
        raise RuntimeError(f"bybit linear empty {symbol}")
    oi_usdt = float(rows[0]["openInterest"]) * float(rows[0]["markPrice"])
    return oi_usdt


def _binance_long_short(symbol):
    j = _http_json("https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
                    {"symbol": symbol, "period": "5m", "limit": 1})
    return float(j[0]["longShortRatio"])


def fetch_crypto_positional(symbol):
    """Returns dict with oi_usdt, funding, long_short; OKX fallback for OI/funding/L-S."""
    result = {"oi_usdt": None, "funding": None, "long_short": None}
    # --- OI ---
    for src in [_binance_oi_funding, _bybit_oi_funding, lambda s: _okx_oi_with_mark(s)]:
        try:
            result["oi_usdt"] = src(symbol)
            break
        except Exception as e:
            log.info(f"{src.__name__ if hasattr(src,'__name__') else 'okx'} FAIL oi {symbol}: {e}")
    # --- funding ---
    try:
        funding = _http_json("https://fapi.binance.com/fapi/v1/premiumIndex",
                              {"symbol": symbol})["lastFundingRate"]
        result["funding"] = float(funding)
    except Exception as e:
        log.info(f"binance FAIL funding {symbol}: {e}")
        for inst in [OKX_SWAP_INST.get(symbol)]:
            if not inst:
                continue
            try:
                fr_j = _http_json(f"{OKX_BASE}/api/v5/public/funding-rate",
                                   {"instId": inst})
                if fr_j.get("data"):
                    result["funding"] = float(fr_j["data"][0]["fundingRate"])
                    break
            except Exception as e:
                log.info(f"okx FAIL funding {symbol}: {e}")
    # --- L/S ---
    try:
        result["long_short"] = _binance_long_short(symbol)
    except Exception as e:
        log.info(f"binance FAIL ls {symbol}: {e}")
        # fallback to OKX long-short-account-ratio
        inst = OKX_SWAP_INST.get(symbol)
        if inst:
            ccy = inst.split("-")[0]
            try:
                ls_j = _http_json(f"{OKX_BASE}/api/v5/rubik/stat/contracts/long-short-account-ratio",
                                   {"ccy": ccy, "period": "5m"})
                if ls_j.get("data"):
                    result["long_short"] = float(ls_j["data"][0]["longShortRatio"])
            except Exception as e:
                log.info(f"okx FAIL ls {symbol}: {e}")
    return result


def _okx_oi_with_mark(symbol):
    inst = OKX_SWAP_INST[symbol]
    j = _http_json(f"{OKX_BASE}/api/v5/public/open-interest",
                    {"instId": inst, "instType": "SWAP"})
    rows = j.get("data", [])
    if not rows:
        raise RuntimeError("okx oi empty")
    oi_ccy = float(rows[0]["oiCcy"])
    if oi_ccy == 0:
        raise RuntimeError("okx oi zero")
    t_j = _http_json(f"{OKX_BASE}/api/v5/market/ticker",
                     {"instId": inst})
    return oi_ccy * float(t_j["data"][0]["last"])


# --- Metals ---------------------------------------------------------------

def fetch_metal(yahoo_symbol, gold_api_id):
    """Two-level fallback: Yahoo Finance → api.gold-api.com/{XAU|XAG}."""
    # 1) Yahoo Finance
    try:
        chart = _http_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}",
                            {"interval": "1d", "range": "5d"})
        meta = chart["chart"]["result"][0]["meta"]
        price = float(meta["regularMarketPrice"])
        prev = float(meta["chartPreviousClose"] or meta.get("previousClose", price))
        ch = (price - prev) / prev * 100 if prev else 0.0
        return {"price": price, "change_pct": ch}
    except Exception as e:
        log.info(f"yahoo FAIL {yahoo_symbol}: {e}")
    # 2) api.gold-api.com
    try:
        j = _http_json(f"https://api.gold-api.com/price/{gold_api_id}")
        price = float(j["price"])
        # No history in free tier — return 0% change
        return {"price": price, "change_pct": 0.0}
    except Exception as e:
        log.info(f"gold-api FAIL {gold_api_id}: {e}")
    return {"price": None, "change_pct": 0.0}


# --- Fear & Greed ---------------------------------------------------------

def fetch_fng():
    try:
        j = _http_json("https://api.alternative.me/fng/",
                        {"limit": 1, "format": "json"})
        return int(j["data"][0]["value"])
    except Exception as e:
        log.info(f"fng FAIL: {e}")
        return None


# --- Verify + sanity ------------------------------------------------------

def verify_post_data(prices, changes):
    missing = [k for k,v in prices.items() if not v.get("price")]
    if missing:
        raise RuntimeError(f"missing prices: {missing}")
    return True


# --- Formatting helpers ---------------------------------------------------

USD_DASH = "\u2014"


def fmt_price(p):
    if p is None:
        return USD_DASH
    return f"${p:,.2f}"


def fmt_change(c):
    if c is None or c == 0:
        return ""
    sign = "\u25B2" if c > 0 else "\u25BC"
    return f"{sign}{abs(c):.2f}%"


def fmt_oi(usdt):
    if usdt is None:
        return USD_DASH
    if usdt >= 1e9:
        return f"${usdt/1e9:.2f}B"
    if usdt >= 1e6:
        return f"${usdt/1e6:.1f}M"
    return f"${usdt/1e3:.1f}K"


def fmt_funding(f):
    if f is None:
        return USD_DASH
    sign = "+" if f >= 0 else "\u2212"
    return f"{sign}{abs(f)*100:.3f}%"


def fmt_ls(ratio):
    if ratio is None:
        return USD_DASH
    long_pct = ratio / (1 + ratio) * 100
    return f"{long_pct:.0f}% \u043B\u043E\u043D\u0433"


# --- Caption + body --------------------------------------------------------

def build_caption(prices, changes, metals, fng):
    """Single-line text with crypto + metals sections."""
    parts = []
    parts.append("\U0001F3DB REDDINGTON \u00B7 \u0422\u043E\u043F-5 + \u041C\u0435\u0442\u0430\u043B\u043B\u044B")
    now = datetime.now(timezone.utc).astimezone()
    parts.append(f"_{now.strftime('%d %b %Y \u00B7 %H:%M')} YEKT_")
    parts.append("\U0001F4CA \u041A\u0440\u0438\u043F\u0442\u043E (24\u0447)")
    for sym in CRYPTO_TICKERS:
        name = sym.replace("USDT", "")
        p = prices[sym]["price"]
        ch = changes[sym]
        if ch.get("change") in (None, 0):
            emoji = "\u26AA"
            change_str = ""
        else:
            emoji = "\U0001F7E2" if ch["change"] > 0 else "\U0001F534"
            change_str = f" {fmt_change(ch['change'])}"
        parts.append(f"{emoji} {name} {fmt_price(p)}{change_str}")
    parts.append("\U0001F947 \u041C\u0435\u0442\u0430\u043B\u043B\u044B")
    gold = metals.get("Gold")
    if gold and gold["price"]:
        gold_ch = gold["change_pct"]
        gold_emoji = "\U0001F7E2" if gold_ch > 0 else ("\U0001F534" if gold_ch < 0 else "\U0001F7E1")
        gold_str = f" +{gold_ch:.2f}%" if gold_ch and gold_ch > 0 else (f" {gold_ch:.2f}%" if gold_ch and gold_ch < 0 else "")
        parts.append(f"{gold_emoji} Gold {fmt_price(gold['price'])}{gold_str}")
    silver = metals.get("Silver")
    if silver and silver["price"]:
        s_ch = silver["change_pct"]
        s_emoji = "\U0001F7E2" if s_ch > 0 else ("\U0001F534" if s_ch < 0 else "\U0001F7E1")
        s_str = f" +{s_ch:.2f}%" if s_ch and s_ch > 0 else (f" {s_ch:.2f}%" if s_ch and s_ch < 0 else "")
        parts.append(f"\u26AA Silver {fmt_price(silver['price'])}{s_str}")
    if fng is not None:
        fng_emoji = "\U0001F7E2" if fng >= 50 else "\U0001F534"
        parts.append(f"\U0001F321 Fear & Greed {fng_emoji} {fng}/100")
    return "\n".join(parts)


def build_body(prices, changes, positional, metals, fng, tech):
    """Long-form text body with summary table + clusters."""
    rows = []
    has_oi = any(positional[sym]["oi_usdt"] for sym in CRYPTO_TICKERS)
    has_funding = any(positional[sym]["funding"] for sym in CRYPTO_TICKERS)
    has_ls = any(positional[sym]["long_short"] for sym in CRYPTO_TICKERS)
    cols = ["\u0410\u043A\u0442\u0438\u0432", "\u0426\u0435\u043D\u0430", "24\u0447"]
    if has_oi:
        cols.append("OI")
    if has_funding:
        cols.append("Funding")
    if has_ls:
        cols.append("L/S")
    widths = [10, 14, 7]
    if has_oi:
        widths.append(10)
    if has_funding:
        widths.append(10)
    if has_ls:
        widths.append(12)
    for sym in CRYPTO_TICKERS:
        name = sym.replace("USDT", "")
        p = prices[sym]["price"]
        ch = changes[sym]
        if p is None:
            row = [name, USD_DASH, ""]
        else:
            c = ch.get("change")
            row = [name, fmt_price(p), fmt_change(c) if c else ""]
        if has_oi:
            row.append(fmt_oi(positional[sym]["oi_usdt"]))
        if has_funding:
            row.append(fmt_funding(positional[sym]["funding"]))
        if has_ls:
            row.append(fmt_ls(positional[sym]["long_short"]))
        rows.append(row)
    lines = []
    now = datetime.now(timezone.utc).astimezone()
    lines.append("\U0001F3DB REDDINGTON \u00B7 \u041E\u0431\u0437\u043E\u0440 \u0440\u044B\u043D\u043A\u0430")
    lines.append(f"_{now.strftime('%A, %d %B %Y \u00B7 %H:%M')} YEKT_")
    lines.append("\n\U0001F4C8 \u0421\u0432\u043E\u0434\u043A\u0430 \u043F\u043E \u0430\u043A\u0442\u0438\u0432\u0430\u043C")
    lines.append("  " + " | ".join(c.ljust(w) for c, w in zip(cols, widths)))
    lines.append("  " + "-+-".join("-" * w for w in widths))
    for row in rows:
        lines.append("  " + " | ".join(c.ljust(w) for c, w in zip(row, widths)))
    lines.append("")
    gold = metals.get("Gold")
    if gold and gold["price"]:
        lines.append(f"  Gold {fmt_price(gold['price'])} {fmt_change(gold['change_pct']) if gold['change_pct'] else ''}")
    silver = metals.get("Silver")
    if silver and silver["price"]:
        lines.append(f"  Silver {fmt_price(silver['price'])} {fmt_change(silver['change_pct']) if silver['change_pct'] else ''}")
    if fng is not None:
        fng_emoji = "\U0001F7E2" if fng >= 50 else "\U0001F534"
        lines.append(f"\n\U0001F321 Fear & Greed {fng_emoji} {fng}/100")
    if tech and tech.get("btc") and tech["btc"].get("support"):
        t = tech["btc"]
        lines.append(f"\n\U0001F50D \u041A\u043B\u0430\u0441\u0442\u0435\u0440\u044B (BTC)")
        lines.append(f"  \u041F\u043E\u0434\u0434\u0435\u0440\u0436\u043A\u0430 {fmt_price(t['support'])}")
        lines.append(f"  \u0421\u043E\u043F\u0440\u043E\u0442\u0438\u0432\u043B\u0435\u043D\u0438\u0435 {fmt_price(t['resistance'])}")
    if tech and tech.get("watchlist"):
        lines.append("\n\U0001F9ED \u0427\u0442\u043E \u0441\u043C\u043E\u0442\u0440\u0435\u0442\u044C")
        for w in tech["watchlist"]:
            lines.append(f"  \u2022 {w}")
    return "\n".join(lines)


# --- Chart ----------------------------------------------------------------

def build_top5_chart():
    from lib.make_v6 import draw_tv_chart
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
    draw_tv_chart(candles, out_path=chart_path, symbol="BTC/USDT",
                   title=f"BTC/USDT 7d ({source})")
    return chart_path


# --- Main ----------------------------------------------------------------

def main():
    if was_posted_recently("market_overview", min_age_minutes=60):
        log.info("skipped: market_overview posted recently")
        return

    # 1. Crypto prices (parallel)
    with ThreadPoolExecutor(max_workers=5) as ex:
        prices = {sym: ex.submit(fetch_crypto_price, sym).result()
                   for sym in CRYPTO_TICKERS}
    log.info(f"prices OK")
    # 2a. Sanity: require BTC price
    if not prices.get("BTCUSDT", {}).get("price"):
        log.error("missing BTC price — abort")
        return

    # 3. change_24h
    changes = {sym: fetch_crypto_change(sym) for sym in CRYPTO_TICKERS}
    # 4. positional
    positional = {sym: fetch_crypto_positional(sym) for sym in CRYPTO_TICKERS}
    # 5. metals + fng (parallel)
    with ThreadPoolExecutor(max_workers=2) as ex:
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

    # 9. post (with dedup)
    post_pair(chart_path, caption, body,
              job_name="market_overview", min_age_minutes=60)
    mark_posted("market_overview")


if __name__ == "__main__":
    main()
