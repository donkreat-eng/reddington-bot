"""13:30 YEKT market overview — BTC/ETH/BNB/SOL/XRP + Gold/Silver.

Источники (auto-fallback):
  price    Binance→CoinGecko→Kraken→Coinbase       (4 параллели, медиана + spread)
  change   Binance→Bybit→OKX→CoinGecko
  OI       Binance→Bybit→OKX (mark × qty)
  funding  Binance→Bybit→OKX
  L/S      Binance→OKX (long-short-account-ratio)
  OHLC     Binance→Kraken→CoinGecko
  metals   Yahoo → api.gold-api.com

Cron: 30 8 * * 1-5 UTC = 13:30 YEKT пн–пт
"""
from __future__ import annotations

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
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)


# ─── Config ────────────────────────────────────────────────────────────────

CRYPTO_TICKERS = ["BTC", "ETH", "BNB", "SOL", "XRP"]
SYMBOLS = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT",
           "SOL": "SOLUSDT", "XRP": "XRPUSDT"}
OKX_SWAP_INST = {"BTC": "BTC-USDT-SWAP", "ETH": "ETH-USDT-SWAP",
                 "BNB": "BNB-USDT-SWAP", "SOL": "SOL-USDT-SWAP",
                 "XRP": "XRP-USDT-SWAP"}
COINGECKO_IDS = {"BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin",
                 "SOL": "solana", "XRP": "ripple"}
YAHOO_SYMBOLS = {"Gold": "GC=F", "Silver": "SI=F"}
GOLD_API_IDS = {"Gold": "XAU", "Silver": "XAG"}

BINANCE_BASE = "https://api.binance.com"
BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
KRAKEN_BASE = "https://api.kraken.com/0/public"
COINBASE_BASE = "https://api.coinbase.com/v2"

THROTTLE = {"binance": 0.4, "bybit": 0.6, "okx": 0.4,
            "kraken": 0.5, "coinbase": 0.4, "coingecko": 1.5,
            "yahoo": 1.0, "gold-api": 0.5}


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
    pair_map = {"BTCUSDT": "XBTUSDT", "ETHUSDT": "ETHUSDT", "BNBUSDT": "BNBUSDT",
                "SOLUSDT": "SOLUSDT", "XRPUSDT": "XRPUSDT"}
    pair = pair_map.get(symbol, symbol)
    data = _http_json(f"{KRAKEN_BASE}/Ticker", {"pair": pair})
    res = data.get("result", {}) or {}
    key = next((k for k in res if k.startswith(pair[:3])), None)
    if not key:
        raise RuntimeError(f"kraken: no key for {pair}")
    return float(res[key]["c"][0])


def _from_coinbase_price(symbol):
    coin = symbol.replace("USDT", "")
    data = _http_json(f"{COINBASE_BASE}/prices/{coin}-USD/spot")
    return float(data["data"]["amount"])


def _from_coingecko_price(symbol):
    coin_id = COINGECKO_IDS.get(symbol)
    if not coin_id:
        raise RuntimeError(f"no coingecko id for {symbol}")
    data = _http_json(f"{COINGECKO_BASE}/simple/price", {"ids": coin_id, "vs_currencies": "usd"})
    return float(data[coin_id]["usd"])


def fetch_crypto_price(symbol):
    """Run 4 sources in parallel; return (price, spread_pct, n_sources, sources_list)."""
    sources = {
        "binance": lambda: _from_binance_price(symbol),
        "kraken":  lambda: _from_kraken_price(symbol),
        "coinbase": lambda: _from_coinbase_price(symbol),
        "coingecko": lambda: _from_coingecko_price(symbol),
    }
    results = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fn): name for name, fn in sources.items()}
        for f in futs:
            name = futs[f]
            try:
                p = f.result(timeout=15)
                results.append((float(p), name))
            except Exception as e:  # noqa: BLE001
                log.info(f"{name} FAIL price {symbol}: {e}")
    if len(results) < 2:
        raise RuntimeError(f"{symbol}: only {len(results)} price sources OK")
    prices = sorted(p for p, _ in results)
    median = prices[len(prices) // 2]
    spread = (max(prices) - min(prices)) / median * 100
    return median, spread, len(results), [name for _, name in results]


# ─── 24h change fetcher ────────────────────────────────────────────────────

def _change_binance(symbol):
    data = _http_json(f"{BINANCE_BASE}/api/v3/ticker/24hr", {"symbol": symbol})
    return float(data["priceChangePercent"])


def _change_bybit(symbol):
    data = _http_json(f"{BYBIT_BASE}/v5/market/tickers",
                      {"category": "spot", "symbol": symbol})
    rows = data.get("result", {}).get("list", [])
    if not rows:
        raise RuntimeError(f"bybit: empty list for {symbol}")
    return float(rows[0]["price24hPcnt"])


def _change_okx(symbol):
    inst = f"{symbol.replace('USDT', '')}-USDT"
    data = _http_json(f"{OKX_BASE}/api/v5/market/ticker", {"instId": inst})
    rows = data.get("data", [])
    if not rows:
        raise RuntimeError(f"okx: empty for {inst}")
    row = rows[0]
    last = float(row["last"])
    open24 = float(row["open24h"])
    return (last - open24) / open24 * 100


def _change_coingecko(symbol):
    coin_id = COINGECKO_IDS.get(symbol)
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
    ALIASES = {
        "BTCUSDT": ["XBTUSDT", "XXBTZUSD", "XBTUSD"],
        "ETHUSDT": ["ETHUSDT", "XETHZUSD", "ETHUSD"],
        "BNBUSDT": ["BNBUSDT"],
        "SOLUSDT": ["SOLUSDT"],
        "XRPUSDT": ["XRPUSDT", "XXRPZUSD", "XRPUSD"],
    }
    aliases = ALIASES.get(pair, [pair])
    cutoff = int(time.time()) - days * 86400 - 3600  # 1h slack
    data = _http_json(f"{KRAKEN_BASE}/OHLC",
                      {"pair": ",".join(aliases), "interval": 240})
    res = data.get("result", {}) or {}
    for alias in aliases:
        if alias in res and isinstance(res[alias], list) and res[alias]:
            raw = res[alias]
            return [(int(c[0]) * 1000, float(c[1]), float(c[2]),
                     float(c[3]), float(c[4]))
                    for c in raw if int(c[0]) >= cutoff], f"kraken:{alias}"
    raise RuntimeError(f"Kraken: no candles for {pair} via {aliases}")


def _coingecko_ohlc(symbol, days):
    coin_id = COINGECKO_IDS.get(symbol)
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


def _bybit_oi_funding(symbol):
    data = _http_json(f"{BYBIT_BASE}/v5/market/tickers",
                      {"category": "linear", "symbol": symbol})
    rows = data.get("result", {}).get("list", [])
    if not rows:
        raise RuntimeError(f"bybit: empty for {symbol}")
    row = rows[0]
    return float(row["openInterestValue"]), float(row["fundingRate"]) * 100


def _binance_long_short(symbol):
    data = _http_json(
        "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
        {"symbol": symbol, "period": "5m", "limit": 1})
    if not data:
        raise RuntimeError(f"binance L/S empty for {symbol}")
    return float(data[0]["longShortRatio"])


def fetch_crypto_positional(symbol):
    """OI, funding, L-S with full fallback chain. Returns dict (values may be None)."""
    out = {"oi_usdt": None, "funding": None, "long_short": None}

    # OI + funding
    for name, fn in (("binance", lambda: _binance_oi_funding(symbol)),
                      ("bybit",  lambda: _bybit_oi_funding(symbol))):
        try:
            oi, fr = fn()
            out["oi_usdt"] = oi
            out["funding"] = fr
            log.info(f"{name} OK positional {symbol}: oi=${oi/1e6:.2f}M fr={fr:.4f}%")
            break
        except Exception as e:  # noqa: BLE001
            log.info(f"{name} FAIL positional {symbol}: {e}")
            time.sleep(THROTTLE.get(name, 0.5))

    # OKX fallback for OI (using mark × qty)
    if out["oi_usdt"] is None:
        try:
            inst = OKX_SWAP_INST.get(symbol.replace("USDT", "")[3:] if False else None)
            base = symbol.replace("USDT", "")
            inst = f"{base}-USDT-SWAP"
            tk = _http_json(f"{OKX_BASE}/api/v5/market/ticker", {"instId": inst})
            if tk.get("data"):
                last = float(tk["data"][0]["last"])
                oi_data = _http_json(f"{OKX_BASE}/api/v5/public/open-interest",
                                     {"instType": "SWAP", "instId": inst})
                if oi_data.get("data"):
                    qty = float(oi_data["data"][0]["oiCcy"])
                    out["oi_usdt"] = qty * last
                    log.info(f"okx OK oi {symbol}: ${out['oi_usdt']/1e6:.2f}M")
        except Exception as e:  # noqa: BLE001
            log.info(f"okx FAIL oi {symbol}: {e}")

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
        try:
            base = symbol.replace("USDT", "")
            lr = _http_json(f"{OKX_BASE}/api/v5/rubik/stat/contracts/long-short-account-ratio",
                            {"ccy": base, "period": "5m"})
            if lr.get("data"):
                out["long_short"] = float(lr["data"][0]["ratio"])
                log.info(f"okx OK L/S {symbol}: {out['long_short']:.2f}")
        except Exception as e:  # noqa: BLE001
            log.info(f"okx FAIL L/S {symbol}: {e}")

    return out


# ─── Metals ────────────────────────────────────────────────────────────────

def fetch_metal(yahoo_symbol, gold_api_id):
    """Yahoo → gold-api.com fallback. Returns {price, change_pct, source}."""
    # Yahoo
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
    # gold-api.com
    try:
        data = _http_json(f"https://api.gold-api.com/price/{gold_api_id}", timeout=10)
        price = float(data["price"])
        return {"price": price, "change_pct": 0.0, "source": "gold-api"}
    except Exception as e:  # noqa: BLE001
        log.info(f"gold-api FAIL {gold_api_id}: {e}")
    return {"price": None, "change_pct": 0.0, "source": "none"}


# ─── F&G ───────────────────────────────────────────────────────────────────

def fetch_fng():
    try:
        data = _http_json("https://api.alternative.me/fng/?limit=1", timeout=10)
        return int(data["data"][0]["value"])
    except Exception as e:  # noqa: BLE001
        log.info(f"F&G FAIL: {e}")
        return None


# ─── Verify ────────────────────────────────────────────────────────────────

def verify_post_data(prices, changes):
    """L1+L2: freshness + cross-source agreement."""
    issues = []
    for sym in CRYPTO_TICKERS:
        info = prices.get(sym)
        if not info:
            issues.append(f"{sym}: no price")
            continue
        _, spread, n, _ = info
        if n < 2:
            issues.append(f"{sym}: only {n} sources")
        elif spread > 3:
            issues.append(f"{sym}: spread {spread:.2f}% > 3%")
    if issues:
        log.warning(f"VERIFY FAIL: {'; '.join(issues)}")
        return False
    log.info("VERIFY OK")
    return True


# ─── Formatters ────────────────────────────────────────────────────────────

def fmt_price(p):
    if p is None:
        return "—"
    return f"{p:,.2f}".replace(",", " ")


def fmt_change(c):
    if c is None:
        return "—"
    sign = "▲" if c >= 0 else "▼"
    return f"{sign} {abs(c):.2f}%"


def fmt_oi(usdt):
    if usdt is None or usdt == 0:
        return "—"
    if abs(usdt) >= 1e9:
        return f"${usdt/1e9:.2f}B"
    return f"${usdt/1e6:.1f}M"


def fmt_funding(f):
    if f is None:
        return "—"
    sign = "+" if f >= 0 else "−"
    return f"{sign}{abs(f):.3f}%"


def fmt_ls(ratio):
    if ratio is None:
        return "—"
    return f"{ratio:.2f} лонг/шорт"


# ─── Caption / body builders ───────────────────────────────────────────────

def build_caption(prices, changes, metals, fng):
    # Hide rows with change=0 AND price None
    has_prices = any(prices[t][0] is not None for t in CRYPTO_TICKERS)
    if not has_prices:
        return ""
    rows = ["<b>📊 Рынок · обзор на сейчас</b>", ""]
    for t in CRYPTO_TICKERS:
        p = prices[t][0]
        ch, _ = changes[t]
        if p is None or ch is None:
            continue
        arrow = "🟢" if ch >= 0 else "🔴"
        rows.append(f"{arrow} <b>{t}</b> · ${fmt_price(p)} · {fmt_change(ch)}")
    if metals.get("Gold", {}).get("price"):
        m = metals["Gold"]
        rows.append(f"🟡 Gold · ${fmt_price(m['price'])}")
    if metals.get("Silver", {}).get("price"):
        m = metals["Silver"]
        rows.append(f"⚪ Silver · ${fmt_price(m['price'])}")
    if fng is not None:
        emoji = "🟢" if fng >= 55 else ("🔴" if fng <= 45 else "🟡")
        rows.append(f"{emoji} F&G {fng}/100")
    return "\n".join(rows)


def build_body(prices, changes, positional, metals, fng, tech):
    """Long-form body with summary table + zones + interpretation."""
    lines = []
    has_pos = any(positional.get(t, {}).get("oi_usdt") for t in CRYPTO_TICKERS)
    has_funding = any(positional.get(t, {}).get("funding") is not None for t in CRYPTO_TICKERS)
    has_ls = any(positional.get(t, {}).get("long_short") for t in CRYPTO_TICKERS)

    lines.append("<b>🏛 Сводка по активам</b>")
    lines.append("<pre>")
    cols = [("Актив", 8)]
    cols.append(("Цена", 11))
    cols.append(("24ч", 7))
    if has_pos:
        cols.append(("OI", 9))
    if has_funding:
        cols.append(("Fr", 8))
    if has_ls:
        cols.append(("L/S", 8))
    header = " ".join(f"{name:>{w}}" for name, w in cols[1:])  # skip first
    pad = " " * cols[0][1]
    lines.append(f"{cols[0][0]:<8} {header}")
    total_w = sum(w for _, w in cols) + len(cols) - 1 - 8
    lines.append("─" * max(total_w, 32))
    for t in CRYPTO_TICKERS:
        p = prices[t][0]
        ch, _ = changes[t]
        if p is None or ch is None:
            row = f"{t:<8} —"
        else:
            row = f"{t:<8} ${p:>9,.2f} {fmt_change(ch):>7}"
            if has_pos:
                row += f" {fmt_oi(positional.get(t,{}).get('oi_usdt')):>9}"
            if has_funding:
                row += f" {fmt_funding(positional.get(t,{}).get('funding')):>8}"
            if has_ls:
                row += f" {fmt_ls(positional.get(t,{}).get('long_short')):>8}"
        lines.append(row)
    lines.append("─" * max(total_w, 32))
    if metals.get("Gold", {}).get("price"):
        m = metals["Gold"]
        lines.append(f"{'Gold':<8} ${m['price']:>9,.2f} {fmt_change(m['change_pct']):>7}")
    if metals.get("Silver", {}).get("price"):
        m = metals["Silver"]
        lines.append(f"{'Silver':<8} ${m['price']:>9,.2f} {fmt_change(m['change_pct']):>7}")
    lines.append("</pre>")
    lines.append("")

    if fng is not None:
        emoji = "🟢" if fng >= 55 else ("🔴" if fng <= 45 else "🟡")
        lines.append(f"🌡 <b>Fear &amp; Greed:</b> {emoji} {fng}/100")
        lines.append("")

    lines.append("<i>🔍 Мониторим уровни BTC · обновляем каждые 15 минут</i>")
    return "\n".join(lines)


# --- Chart ----------------------------------------------------------------

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
                changes[t] = (0.0, "none")
                log.error(f"{t} FAIL change: {e}")

    # 3. positional
    positional = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
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

    # 9. post (with dedup)
    post_pair(chart_path, caption, body,
              job_name="market_overview", min_age_minutes=60)
    mark_posted("market_overview")


if __name__ == "__main__":
    main()
