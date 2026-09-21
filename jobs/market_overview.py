"""13:30 YEKT market overview — BTC/ETH/BNB/SOL/XRP + Gold/Silver.

Источники (auto-fallback):
  Prices:    binance → bybit → okx → coingecko → coinbase → kraken
  OHLC:      binance → kraken → coingecko
  OI:        binance → bybit → okx
  Funding:   binance → bybit → okx
  Long/Short: okx (long-short-account-ratio 5m)
  Metals:    gold-api.com (XAU/XAG) primary + yahoo fallback

Cron: 30 8 * * 1-5 UTC = 13:30 YEKT пн–пт
"""

import json as _json
import base64
import os
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.common import load_env, setup_logger, ye_now, ye_str
from lib.publish import post_pair, was_posted_recently, mark_posted

load_env()
_logger = setup_logger("market_overview")


def log(job, msg):
    _logger.info(f"{job}: {msg}")

CRYPTO_TICKERS = ["BTC", "ETH", "BNB", "SOL", "XRP"]
COINGECKO_IDS = {"BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin", "SOL": "solana", "XRP": "ripple"}

YAHOO_SYMBOLS = {"Gold": "GC=F", "Silver": "SI=F"}
OKX_BASE = "https://www.okx.com"
OKX_SWAP_INST = {"BTC": "BTC-USDT-SWAP", "ETH": "ETH-USDT-SWAP", "BNB": "BNB-USDT-SWAP", "SOL": "SOL-USDT-SWAP", "XRP": "XRP-USDT-SWAP"}

GOLD_API = "https://api.gold-api.com/price"

# === HTTP ===

def _http_json(url, timeout=8, headers=None):
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "reddington/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return _json.loads(r.read().decode("utf-8", errors="ignore"))


def _http_text(url, timeout=8, headers=None):
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "reddington/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="ignore")


# === Prices ===

def _change_binance(symbol):
    try:
        d = _http_json(f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}")
        return float(d["lastPrice"]), float(d["priceChangePercent"])
    except Exception:
        return None


def _change_bybit(symbol):
    try:
        d = _http_json(f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}")
        lst = d.get("result", {}).get("list", [])
        if not lst:
            return None
        return float(lst[0]["lastPrice"]), float(lst[0]["price24hPcnt"])
    except Exception:
        return None


def _change_okx(symbol):
    try:
        d = _http_json(f"https://www.okx.com/api/v5/market/ticker?instId={symbol}")
        lst = d.get("data", [])
        if not lst:
            return None
        return float(lst[0]["last"]), float(lst[0]["24hChgPct"]) * 100
    except Exception:
        return None


def _change_coingecko(symbol):
    try:
        coin = COINGECKO_IDS.get(symbol.replace("USDT", "").replace("USD", ""))
        if not coin:
            return None
        d = _http_json(f"https://api.coingecko.com/api/v3/simple/price?ids={coin}&vs_currencies=usd&include_24hr_change=true")
        v = d.get(coin, {})
        if "usd" not in v or "usd_24h_change" not in v:
            return None
        return float(v["usd"]), float(v["usd_24h_change"])
    except Exception:
        return None


def _change_coinbase(symbol):
    try:
        d = _http_json(f"https://api.exchange.coinbase.com/products/{symbol}/ticker")
        return float(d["price"]), None
    except Exception:
        return None


def _change_kraken(symbol):
    try:
        pair = symbol.replace("USDT", "USD")
        d = _http_json(f"https://api.kraken.com/0/public/Ticker?pair={pair}")
        result = d.get("result", {})
        if not result:
            return None
        first = next(iter(result.values()))
        return float(first["c"][0]), None
    except Exception:
        return None


def fetch_crypto_change(symbol):
    """Returns (price, change_pct). Tries 6 sources in order."""
    for fn in (_change_binance, _change_bybit, _change_okx, _change_coingecko, _change_coinbase, _change_kraken):
        r = fn(symbol)
        if r and r[0]:
            return r[0], r[1] if r[1] is not None else 0.0
    return None, None


# === OHLC ===

_KRAKEN_ALIASES = {
    "BTCUSDT": ["XBTUSDT", "XXBTZUSD", "XBTUSD", "XBTUSDC"],
    "ETHUSDT": ["ETHUSDT", "XETHZUSD", "ETHUSD", "ETHUSDC"],
    "BNBUSDT": ["BNBUSD", "BNBUSDT", "BNBUSDC"],
    "SOLUSDT": ["SOLUSD", "SOLUSDT", "SOLUSDC"],
    "XRPUSDT": ["XRPUSD", "XXRPZUSD", "XRPUSDT", "XRPUSDC"],
}


def _binance_ohlc(symbol, days=7):
    try:
        limit = max(50, days * 24)
        d = _http_json(f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1h&limit={limit}")
        return [(int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4])) for c in d]
    except Exception as e:
        log("ohlc", f"binance fail {symbol}: {e}")
        return []


def _kraken_ohlc(symbol, days=7):
    aliases = _KRAKEN_ALIASES.get(symbol, [symbol])
    since = int(time.time() - days * 86400)
    last_err = None
    for pair in aliases:
        try:
            url = f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval=60&since={since}"
            d = _http_json(url)
            if d.get("error"):
                last_err = d["error"]
                continue
            result = d.get("result", {})
            candles = []
            for k, v in result.items():
                if k == "last":
                    continue
                if isinstance(v, list):
                    for c in v:
                        candles.append((int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4])))
            if candles:
                log("ohlc", f"kraken OK {symbol} via {pair}: {len(candles)} candles")
                return candles
        except Exception as e:
            last_err = e
            continue
    log("ohlc", f"kraken fail {symbol}: {last_err}")
    return []


def _coingecko_ohlc(symbol, days=7):
    try:
        coin = COINGECKO_IDS.get(symbol.replace("USDT", "").replace("USD", ""))
        if not coin:
            return []
        d = _http_json(f"https://api.coingecko.com/api/v3/coins/{coin}/ohlc?vs_currency=usd&days={days}")
        if not isinstance(d, list):
            return []
        return [(int(c[0]), float(c[1]), float(c[1]), float(c[1]), float(c[2])) for c in d]
    except Exception as e:
        log("ohlc", f"coingecko fail {symbol}: {e}")
        return []


def fetch_crypto_ohlc(symbol, days=7):
    for fn in (_binance_ohlc, _kraken_ohlc, _coingecko_ohlc):
        candles = fn(symbol, days)
        if candles:
            return candles
    return []


# === Open Interest & Funding (multi-source) ===

def _oi_binance(symbol):
    try:
        d = _http_json(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}")
        return float(d["openInterest"]) * float(_http_json(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}")["price"])
    except Exception:
        return None


def _oi_bybit(symbol):
    try:
        d = _http_json(f"https://api.bybit.com/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime=5min&limit=1")
        lst = d.get("result", {}).get("list", [])
        if not lst:
            return None
        return float(lst[0]["openInterest"]) * float(lst[0]["price"])
    except Exception:
        return None


def _oi_okx(symbol):
    try:
        d = _http_json(f"https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-volume?ccy={symbol.replace('USDT','').replace('USD','')}&instType=SWAP")
        lst = d.get("data", [])
        if not lst:
            return None
        return float(lst[0]["oi"]) * 1_000_000  # USD
    except Exception:
        return None


def fetch_crypto_oi(symbol):
    for fn in (_oi_binance, _oi_bybit, _oi_okx):
        r = fn(symbol)
        if r:
            return r
    return None


def _funding_binance(symbol):
    try:
        d = _http_json(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}")
        return float(d["lastFundingRate"]) * 100
    except Exception:
        return None


def _funding_bybit(symbol):
    try:
        d = _http_json(f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&limit=1")
        lst = d.get("result", {}).get("list", [])
        if not lst:
            return None
        return float(lst[0]["fundingRate"]) * 100
    except Exception:
        return None


def _funding_okx(symbol):
    try:
        d = _http_json(f"https://www.okx.com/api/v5/public/funding-rate?instId={symbol.replace('USDT','-USDT-SWAP')}")
        lst = d.get("data", [])
        if not lst:
            return None
        return float(lst[0]["fundingRate"]) * 100
    except Exception:
        return None


def fetch_crypto_funding(symbol):
    for fn in (_funding_binance, _funding_bybit, _funding_okx):
        r = fn(symbol)
        if r is not None:
            return r
    return None


def fetch_crypto_positional(symbol):
    """Returns dict {oi, funding, long_short}."""
    out = {"oi": None, "funding": None, "long_short": None}

    # OI
    for fn in (_oi_binance, _oi_bybit, _oi_okx):
        r = fn(symbol)
        if r:
            out["oi"] = r
            break

    # Funding
    for fn in (_funding_binance, _funding_bybit, _funding_okx):
        r = fn(symbol)
        if r is not None:
            out["funding"] = r
            break

    # Long/Short via OKX rubik
    inst_id = OKX_SWAP_INST.get(symbol.replace("USDT", ""))
    if inst_id:
        ccy = symbol.replace("USDT", "")
        try:
            d = _http_json(f"{OKX_BASE}/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy={ccy}&period=5m")
            lst = d.get("data", [])
            if lst:
                row = lst[-1]
                if isinstance(row, list) and len(row) >= 2:
                    out["long_short"] = float(row[1])
                elif isinstance(row, dict):
                    out["long_short"] = float(row.get("ratio", 0))
        except Exception as e:
            log("ls", f"okx fail {symbol}: {e}")

    return out


# === Metals ===

def _yahoo_quote(symbol):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol)}?interval=1d&range=2d"
        d = _http_json(url)
        result = d.get("chart", {}).get("result", [])
        if not result:
            return None
        meta = result[0].get("meta", {})
        price = meta.get("regularMarketPrice")
        prev = meta.get("chartPreviousClose") or meta.get("previousClose")
        if price is None or prev is None:
            return None
        ch = (price - prev) / prev * 100 if prev else 0.0
        return float(price), float(ch)
    except Exception as e:
        log("metals", f"yahoo fail {symbol}: {e}")
        return None


def _goldapi_price(metal):
    try:
        d = _http_json(f"{GOLD_API}/{metal}")
        if "price" not in d:
            return None
        return float(d["price"]), 0.0
    except Exception as e:
        log("metals", f"gold-api fail {metal}: {e}")
        return None


def fetch_metal(name):
    """Returns dict {price, change_pct}. gold-api primary, yahoo for change_pct."""
    sym = YAHOO_SYMBOLS[name]
    api_sym = "XAU" if name == "Gold" else "XAG"
    price = None
    change = 0.0
    p = _goldapi_price(api_sym)
    if p:
        price = p[0]
    y = _yahoo_quote(sym)
    if y:
        if price is None:
            price = y[0]
        change = y[1]
    if price is None:
        return {"price": None, "change_pct": 0.0}
    return {"price": price, "change_pct": change}


# === Formatting ===

def fmt_price(p):
    if p is None:
        return "—"
    if p >= 1000:
        return f"${p:,.2f}"
    return f"${p:,.4f}"


def fmt_oi(usdt):
    if usdt is None:
        return "—"
    if usdt >= 1_000_000_000:
        return f"${usdt/1e9:.2f}B"
    if usdt >= 1_000_000:
        return f"${usdt/1e6:.1f}M"
    return f"${usdt:,.0f}"


def fmt_funding(f):
    if f is None:
        return "—"
    sign = "+" if f >= 0 else "−"
    return f"{sign}{abs(f):.3f}%"


def fmt_ls(ratio):
    if ratio is None:
        return "—"
    return f"{ratio:.2f} лонг/шорт"


def fmt_ch(pct):
    if pct is None or pct == 0:
        return "—"
    arrow = "▲" if pct > 0 else "▼"
    return f"{arrow}{abs(pct):.2f}%"


def sign(p):
    return "▲" if (p or 0) > 0 else ("▼" if (p or 0) < 0 else "—")


# === Build body ===

def build_body(prices, changes, ois, funds, lss, gold, silver):
    lines = []
    has_oi = any(ois.get(t) is not None for t in CRYPTO_TICKERS)
    has_funding = any(funds.get(t) is not None for t in CRYPTO_TICKERS)
    has_ls = any(lss.get(t) is not None for t in CRYPTO_TICKERS)

    cols = ["Актив", "Цена", "24ч"]
    if has_oi:
        cols.append("OI")
    if has_funding:
        cols.append("Funding")
    if has_ls:
        cols.append("L/S")

    lines.append("📈 Сводка по активам")
    lines.append("")
    lines.append(" | ".join(cols))
    lines.append(" | ".join(["---"] * len(cols)))

    for t in CRYPTO_TICKERS:
        row = [t, fmt_price(prices.get(t)), sign(changes.get(t) or 0) + (f" {abs(changes.get(t) or 0):.2f}%" if changes.get(t) else "")]
        if has_oi:
            row.append(fmt_oi(ois.get(t)))
        if has_funding:
            row.append(fmt_funding(funds.get(t)))
        if has_ls:
            row.append(fmt_ls(lss.get(t)))
        lines.append(" | ".join(row))

    # Metals
    lines.append("")
    gold_ch = (gold or {}).get("change_pct")
    silver_ch = (silver or {}).get("change_pct")
    lines.append(f"🥇 Gold {fmt_price((gold or {}).get('price'))} {sign(gold_ch or 0)}{abs(gold_ch or 0):.2f}%" if gold_ch else f"🥇 Gold {fmt_price((gold or {}).get('price'))} —")
    lines.append(f"🥈 Silver {fmt_price((silver or {}).get('price'))} {sign(silver_ch or 0)}{abs(silver_ch or 0):.2f}%" if silver_ch else f"🥈 Silver {fmt_price((silver or {}).get('price'))} —")

    return "\n".join(lines)


# === Chart ===

def build_top5_chart(btc_ohlc):
    from lib.chart import generate_chart

    if not btc_ohlc:
        raise RuntimeError("no BTC ohlc for chart")

    last_close = btc_ohlc[-1][4]
    support = last_close * 0.97
    resistance = last_close * 1.03

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    chart_dir = os.path.join(repo_root, "posts")
    os.makedirs(chart_dir, exist_ok=True)
    ye = ye_now()
    chart_path = os.path.join(chart_dir, f"top5_btc_{ye.strftime('%Y%m%d_%H%M')}.png")

    generate_chart(btc_ohlc, "BTC/USDT", (support, support), (resistance, resistance), chart_path)
    return chart_path


# === Main ===

def main():
    log("market_overview", "tick")

    # Dedup
    if was_posted_recently("market_overview", min_age_minutes=60):
        log("market_overview", "skip — posted recently")
        return

    # Sanity: require BTC price
    btc_price, _ = fetch_crypto_change("BTCUSDT")
    if not btc_price:
        log("market_overview", "FATAL: no BTC price — skipping post")
        return

    # Fetch all in parallel
    with ThreadPoolExecutor(max_workers=20) as ex:
        ch_futs = {t: ex.submit(fetch_crypto_change, f"{t}USDT") for t in CRYPTO_TICKERS}
        pos_futs = {t: ex.submit(fetch_crypto_positional, f"{t}USDT") for t in CRYPTO_TICKERS}
        ohlc_fut = ex.submit(fetch_crypto_ohlc, "BTCUSDT", 7)
        gold_f = ex.submit(fetch_metal, "Gold")
        silver_f = ex.submit(fetch_metal, "Silver")

    prices = {}
    changes = {}
    for t, f in ch_futs.items():
        p, c = f.result()
        prices[t] = p
        changes[t] = c

    ois, funds, lss = {}, {}, {}
    for t, f in pos_futs.items():
        d = f.result()
        ois[t] = d["oi"]
        funds[t] = d["funding"]
        lss[t] = d["long_short"]

    gold = gold_f.result()
    silver = silver_f.result()
    btc_ohlc = ohlc_fut.result()

    # Build chart
    try:
        chart_path = build_top5_chart(btc_ohlc)
    except Exception as e:
        log("chart", f"FAIL: {e}")
        return

    # Build text
    body = build_body(prices, changes, ois, funds, lss, gold, silver)

    # Fear & Greed
    try:
        fg = _http_json("https://api.alternative.me/fng/?limit=1")
        fg_v = fg.get("data", [{}])[0].get("value", "?")
        fg_l = fg.get("data", [{}])[0].get("value_classification", "")
        fg_line = f"🌡 Fear & Greed 🟢 {fg_v}/100 ({fg_l})"
    except Exception:
        fg_line = "🌡 Fear & Greed —"

    ye = ye_now()
    caption = f"🏛 REDDINGTON · Обзор рынка\n{ye.strftime('%A, %d %B %Y · %H:%M YEKT')}\n\n{fg_line}\n\n📊 Топ-5 (24ч) — см. картинку"

    # Post
    post_pair(chart_path, caption, body, job_name="market_overview", min_age_minutes=60)
    log("market_overview", "posted OK")
    mark_posted("market_overview")


if __name__ == "__main__":
    main()
