"""13:30 YEKT market overview — BTC/ETH/BNB/SOL/XRP + Gold/Silver с позиционным слоем.
Источники (auto-fallback):
  price:    Binance → CoinGecko → Kraken → Coinbase (median + spread check)
  change:   Binance spot → Bybit linear → OKX swap → CoinGecko
  ohlc:     Binance klines → Kraken OHLC → CoinGecko OHLC
  OI/fund:  Binance futures → Bybit → OKX (L/S — только Binance)
  metals:   Yahoo Finance (GC=F, SI=F)
"""
from __future__ import annotations

import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from lib.chart import generate_chart
from lib.common import load_env, log, post_dir, setup_logger
from lib.publish import post_pair

# ─── Constants ───
BINANCE_BASE = "https://api.binance.com"
COINBASE_BASE = "https://api.exchange.coinbase.com"
BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"

KRAKEN_INTERVAL = 240
_BYBIT_CACHE: dict = {"ts": 0.0, "data": {}}
_BYBIT_CACHE_TTL = 30.0
_OKX_CACHE: dict = {"ts": 0.0, "data": {}}
_OKX_CACHE_TTL = 30.0

OKX_SWAP_INST = {
    "BTCUSDT": "BTC-USDT-SWAP",
    "ETHUSDT": "ETH-USDT-SWAP",
    "BNBUSDT": "BNB-USDT-SWAP",
    "SOLUSDT": "SOL-USDT-SWAP",
    "XRPUSDT": "XRP-USDT-SWAP",
}

BINANCE_SYMBOL = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT",
    "SOL": "SOLUSDT", "XRP": "XRPUSDT",
}
COINGECKO_IDS = {
    "BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin",
    "SOL": "solana", "XRP": "ripple",
}
TICKERS = ["BTC", "ETH", "BNB", "SOL", "XRP"]


# ─── HTTP helpers ───
def _http_json(url: str, params: dict | None = None, timeout: int = 10,
               headers: dict | None = None) -> dict | list:
    if params:
        qs = urllib.parse.urlencode(params)
        url = f"{url}?{qs}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "market_overview/1.0",
        "Accept": "application/json",
        **(headers or {}),
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body else {}


def _http_get(url: str, timeout: int = 10, headers: dict | None = None) -> str:
    req = urllib.request.Request(url, headers={
        "User-Agent": "market_overview/1.0",
        "Accept": "application/json",
        **(headers or {}),
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8")


# ─── Spot prices (4 sources with median) ───
def _binance_price(symbol: str) -> float | None:
    try:
        d = _http_json(f"{BINANCE_BASE}/api/v3/ticker/price",
                       {"symbol": symbol})
        return float(d["price"])
    except Exception as e:
        log("market_overview", f"price {symbol} binance FAIL: {e}")
        return None


def _coingecko_price(coin_id: str) -> float | None:
    try:
        d = _http_json("https://api.coingecko.com/api/v3/simple/price",
                       {"ids": coin_id, "vs_currencies": "usd"})
        return float(d[coin_id]["usd"])
    except Exception as e:
        log("market_overview", f"price {coin_id} coingecko FAIL: {e}")
        return None


def _kraken_price(symbol: str) -> float | None:
    pair = symbol.replace("USDT", "USD") if symbol.endswith("USDT") else symbol
    try:
        d = _http_json("https://api.kraken.com/0/public/Ticker",
                       {"pair": pair})
        key = next(iter(d.get("result", {})), None)
        if key:
            return float(d["result"][key]["c"][0])
    except Exception as e:
        log("market_overview", f"price {symbol} kraken FAIL: {e}")
    return None


def _coinbase_price(symbol: str) -> float | None:
    pair = symbol.replace("USDT", "USD") if symbol.endswith("USDT") else symbol
    try:
        d = _http_json(f"{COINBASE_BASE}/products/{pair}/ticker")
        return float(d["price"])
    except Exception as e:
        log("market_overview", f"price {symbol} coinbase FAIL: {e}")
        return None


def fetch_crypto_price(ticker: str) -> dict:
    symbol = BINANCE_SYMBOL[ticker]
    coin_id = COINGECKO_IDS[ticker]
    prices: list[float] = []
    src_names: list[str] = []
    for src_name, fn in (
        ("binance", lambda: _binance_price(symbol)),
        ("coingecko", lambda: _coingecko_price(coin_id)),
        ("kraken", lambda: _kraken_price(symbol)),
        ("coinbase", lambda: _coinbase_price(symbol)),
    ):
        p = fn()
        if p is not None:
            prices.append(p)
            src_names.append(src_name)
    if not prices:
        raise RuntimeError(f"all price sources failed for {ticker}")
    median = statistics.median(prices)
    if len(prices) >= 2:
        spread = (max(prices) - min(prices)) / median * 100
    else:
        spread = 0.0
    return {"ticker": ticker, "price": median, "spread": spread,
            "sources": src_names, "n_sources": len(prices)}


# ─── Bybit V5 helpers ───
def _bybit_tickers_cached(force: bool = False) -> dict:
    """Return dict of linear tickers keyed by symbol (e.g. BTCUSDT)."""
    now = time.time()
    if not force and (now - _BYBIT_CACHE["ts"]) < _BYBIT_CACHE_TTL \
            and _BYBIT_CACHE["data"]:
        return _BYBIT_CACHE["data"]
    data = {}
    for cat in ("linear",):
        try:
            d = _http_json(f"{BYBIT_BASE}/v5/market/tickers",
                           {"category": cat})
            for row in d.get("result", {}).get("list", []):
                data[row["symbol"]] = row
        except Exception as e:
            log("market_overview", f"bybit tickers {cat} FAIL: {e}")
    _BYBIT_CACHE["ts"] = now
    _BYBIT_CACHE["data"] = data
    return data


def _bybit_extras(symbol: str) -> dict:
    """Extract: change_24h %, turnover_24h USDT, funding %, oi_usdt."""
    tickers = _bybit_tickers_cached()
    if symbol not in tickers:
        raise RuntimeError(f"bybit {symbol} not in tickers")
    row = tickers[symbol]
    last = float(row["lastPrice"])
    prev24 = float(row["prevPrice24h"])
    turnover = float(row["turnover24h"])
    change = (last - prev24) / prev24 * 100 if prev24 else 0.0
    funding = float(row.get("fundingRate", 0)) * 100
    oi = float(row.get("openInterest", 0))
    oi_usdt = oi * last
    return {"change_24h": change, "turnover_24h": turnover,
            "funding": funding, "oi_usdt": oi_usdt, "price": last}


# ─── OKX V5 helpers (no auth, works on Cloudflare-blocked IPs) ───
def _okx_swap(inst_id: str) -> dict:
    """GET /api/v5/market/ticker?instId=... — last, open24h, volCcy24h."""
    d = _http_json(f"{OKX_BASE}/api/v5/market/ticker",
                   {"instId": inst_id})
    rows = d.get("data", [])
    if not rows:
        raise RuntimeError(f"okx ticker empty for {inst_id}")
    return rows[0]


def _okx_funding(inst_id: str) -> float:
    """Return fundingRate as percent."""
    d = _http_json(f"{OKX_BASE}/api/v5/public/funding-rate",
                   {"instId": inst_id})
    rows = d.get("data", [])
    if not rows:
        raise RuntimeError(f"okx funding empty for {inst_id}")
    return float(rows[0]["fundingRate"]) * 100


def _okx_oi(inst_id: str, mark_price: float) -> float:
    """Return OI in USDT = oiCcy * mark_price."""
    d = _http_json(f"{OKX_BASE}/api/v5/public/open-interest",
                   {"instId": inst_id})
    rows = d.get("data", [])
    if not rows:
        raise RuntimeError(f"okx oi empty for {inst_id}")
    oiCcy = float(rows[0]["oiCcy"])
    return oiCcy * mark_price


def _okx_extras(symbol: str) -> dict:
    """Extract: change_24h %, turnover_24h USDT, funding %, oi_usdt."""
    inst_id = OKX_SWAP_INST.get(symbol)
    if not inst_id:
        raise RuntimeError(f"okx no inst for {symbol}")
    t = _okx_swap(inst_id)
    last = float(t["last"])
    open24 = float(t["open24h"])
    vol_ccy = float(t.get("volCcy24h", 0))  # volume in base ccy
    # OKX volCcy24h is base ccy (e.g. BTC), volCcy24hQuote missing in ticker — use last * volCcy
    turnover = vol_ccy * last
    change = (last - open24) / open24 * 100 if open24 else 0.0
    funding = _okx_funding(inst_id)
    oi_usdt = _okx_oi(inst_id, last)
    return {"change_24h": change, "turnover_24h": turnover,
            "funding": funding, "oi_usdt": oi_usdt, "price": last}


# ─── 24h change + volume ───
def fetch_crypto_change(ticker: str) -> dict:
    """Binance → Bybit → OKX → CoinGecko."""
    symbol = BINANCE_SYMBOL[ticker]
    # 1. Binance spot 24hr ticker (gives %change + quoteVolume in USDT)
    try:
        d = _http_json(f"{BINANCE_BASE}/api/v3/ticker/24hr",
                       {"symbol": symbol})
        return {"change_24h": float(d["priceChangePercent"]),
                "volume_24h": float(d["quoteVolume"]),
                "source": "binance"}
    except Exception as e:
        log("market_overview",
            f"change {ticker} Binance FAIL: {e}; trying Bybit")
    # 2. Bybit linear ticker (gives % change + turnover in USDT)
    try:
        ex = _bybit_extras(symbol)
        return {"change_24h": ex["change_24h"],
                "volume_24h": ex["turnover_24h"],
                "source": "bybit"}
    except Exception as e:
        log("market_overview",
            f"change {ticker} Bybit FAIL: {e}; trying OKX")
    # 3. OKX swap ticker (no auth, works where Bybit 403 on Cloudflare-blocked IPs)
    try:
        ex = _okx_extras(symbol)
        return {"change_24h": ex["change_24h"],
                "volume_24h": ex["turnover_24h"],
                "source": "okx"}
    except Exception as e:
        log("market_overview",
            f"change {ticker} OKX FAIL: {e}; trying CoinGecko")
    # 4. CoinGecko (last resort; no turnover)
    cg_id = COINGECKO_IDS[ticker]
    try:
        d = _http_json("https://api.coingecko.com/api/v3/simple/price",
                       {"ids": cg_id,
                        "vs_currencies": "usd",
                        "include_24hr_change": "true"})
        chg = float(d[cg_id].get("usd_24h_change", 0))
        return {"change_24h": chg, "volume_24h": 0.0, "source": "coingecko"}
    except Exception as e:
        log("market_overview", f"change {ticker} CoinGecko FAIL: {e}")
        return {"change_24h": 0.0, "volume_24h": 0.0, "source": "none"}


# ─── OHLC fallback chain ───
def _kraken_ohlc(symbol: str, minutes: int, days: int) -> list[tuple[int, float, float, float, float]]:
    pair = symbol.replace("USDT", "USD") if symbol.endswith("USDT") else symbol
    cutoff = int(time.time()) - days * 86400
    candles: list[tuple[int, float, float, float, float]] = []
    since = cutoff
    while True:
        d = _http_json("https://api.kraken.com/0/public/OHLC",
                       {"pair": pair, "interval": minutes, "since": since})
        key = next((k for k in d.get("result", {}) if k != "last"), None)
        if not key:
            break
        rows = d["result"][key]
        for r in rows:
            ts, o, h, l, c = r[:5]
            if ts < cutoff:
                continue
            candles.append((int(ts) * 1000, float(o), float(h), float(l), float(c)))
        last_ts = int(rows[-1][0])
        if last_ts >= int(time.time()) - minutes * 60:
            break
        since = last_ts
        if len(rows) < 50:
            break
    return candles


def _coingecko_ohlc(coin_id: str, days: int) -> list[tuple[int, float, float, float, float]]:
    d = _http_json(f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc",
                   {"vs_currency": "usd", "days": str(days)})
    return [(int(t), float(o), float(h), float(l), float(c))
            for t, o, h, l, c in d]


def fetch_crypto_ohlc(ticker: str, days: int = 7, limit: int = 42) -> list[tuple[int, float, float, float, float]]:
    """4h OHLC for chart. Binance → Kraken → CoinGecko fallback."""
    symbol = BINANCE_SYMBOL[ticker]
    try:
        raw = _http_json(f"{BINANCE_BASE}/api/v3/klines",
                         {"symbol": symbol, "interval": "4h", "limit": limit})
        log("market_overview", f"ohlc {ticker} source=binance n={len(raw)}")
        return [(int(t[0]), float(t[1]), float(t[2]), float(t[3]), float(t[4]))
                for t in raw]
    except Exception as e:
        log("market_overview", f"ohlc {ticker} Binance FAIL: {e}; trying Kraken")
    try:
        candles = _kraken_ohlc(symbol, minutes=240, days=days)
        if candles:
            log("market_overview", f"ohlc {ticker} source=kraken n={len(candles)}")
            return candles
    except Exception as e:
        log("market_overview", f"ohlc {ticker} Kraken FAIL: {e}; trying CoinGecko")
    candles = _coingecko_ohlc(COINGECKO_IDS[ticker], days=days)
    log("market_overview", f"ohlc {ticker} source=coingecko n={len(candles)}")
    return candles


# ─── OI / funding / L/S ───
def fetch_crypto_positional(ticker: str) -> dict:
    """OI, funding, L/S ratio. Binance → Bybit → OKX; L/S only Binance."""
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

    # ─── Long/Short ratio (Binance-only — Bybit V5 requires auth for this) ───
    try:
        ls = _bin("/futures/data/globalLongShortAccountRatio",
                  {"symbol": symbol, "period": "1h", "limit": 1})
        out["long_short"] = float(ls[0]["longShortRatio"]) if ls else None
    except Exception as e:
        log("market_overview", f"L/S {ticker} Binance FAIL: {e}")
        out["long_short"] = None

    return out


# ─── Metals via Yahoo Finance ───
def _yahoo_quote(symbol: str) -> dict:
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
           "?interval=1d&range=5d")
    d = _http_json(url)
    r = d["chart"]["result"][0]
    meta = r["meta"]
    closes = [float(c) for c in r["indicators"]["quote"][0].get("close", [])
              if c is not None]
    price = float(meta.get("regularMarketPrice") or (closes[-1] if closes else 0))
    prev = float(meta.get("chartPreviousClose") or (closes[-2] if len(closes) > 1 else price))
    change = (price - prev) / prev * 100 if prev else 0.0
    return {"price": price, "change_24h": change}


def fetch_metal(symbol: str, name: str) -> dict:
    try:
        q = _yahoo_quote(symbol)
        return {"name": name, "price": q["price"], "change_24h": q["change_24h"]}
    except Exception as e:
        log("market_overview", f"metal {name} ({symbol}) FAIL: {e}")
        return {"name": name, "price": 0.0, "change_24h": 0.0}


# ─── Fear & Greed Index ───
def fetch_fng() -> dict:
    try:
        d = _http_json("https://api.alternative.me/fng/",
                       {"limit": 1, "format": "json"})
        e = d["data"][0]
        return {"value": int(e["value"]), "label": e["value_classification"]}
    except Exception as e:
        log("market_overview", f"F&G FAIL: {e}")
        return {"value": 0, "label": "n/a"}


# ─── Verification ───
def verify_post_data(prices: dict[str, dict]) -> str:
    """L1: every ticker has ≥1 source; L2: ≥2 sources and spread ≤3%."""
    for t, p in prices.items():
        n = p.get("n_sources", 0)
        spread = p.get("spread", 999)
        if n < 1:
            return f"L1 FAIL: {t} has no sources"
        if n < 2:
            log("market_overview", f"L2 WARN: {t} only {n} source(s)")
        elif spread > 3.0:
            return f"L2 FAIL: {t} spread {spread:.2f}% > 3%"
    return "OK"


# ─── Formatting ───
def fmt_price(p: float) -> str:
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.2f}"
    return f"{p:.4f}"


def fmt_chg(c: float) -> str:
    sign = "+" if c >= 0 else ""
    return f"{sign}{c:.2f}%"


def fmt_vol(v: float) -> str:
    if v >= 1e9:
        return f"${v/1e9:.2f}B"
    if v >= 1e6:
        return f"${v/1e6:.0f}M"
    if v >= 1e3:
        return f"${v/1e3:.0f}K"
    return f"${v:.0f}"


def fmt_oi(v: float | None) -> str:
    if v is None:
        return "—"
    return fmt_vol(v)


def adaptive_zones(price: float) -> tuple[tuple[float, float], tuple[float, float]]:
    """Return (support_range, resistance_range) adaptive to price magnitude."""
    if price >= 10000:
        span = price * 0.025
        return (price - span * 1.5, price - span * 0.5), \
               (price + span * 0.5, price + span * 1.5)
    if price >= 1000:
        span = price * 0.030
        return (price - span * 1.5, price - span * 0.5), \
               (price + span * 0.5, price + span * 1.5)
    if price >= 100:
        span = price * 0.035
        return (price - span * 1.5, price - span * 0.5), \
               (price + span * 0.5, price + span * 1.5)
    if price >= 10:
        span = price * 0.040
        return (price - span * 1.5, price - span * 0.5), \
               (price + span * 0.5, price + span * 1.5)
    if price >= 1:
        span = price * 0.045
        return (price - span * 1.5, price - span * 0.5), \
               (price + span * 0.5, price + span * 1.5)
    span = price * 0.06
    return (price - span * 1.5, price - span * 0.5), \
           (price + span * 0.5, price + span * 1.5)


# ─── Chart ───
def build_top5_chart(ohlc: list[tuple[int, float, float, float, float]],
                     ticker: str, price: float,
                     support: tuple[float, float],
                     resistance: tuple[float, float],
                     output: Path) -> Path:
    return generate_chart(ohlc, ticker, support, resistance, str(output))


# ─── Caption + body ───
def build_caption(date_str: str) -> str:
    return (f"📊 Обзор рынка {date_str} YEKT\n"
            f"Крипто + Металлы + Позиционный слой")


def build_body(prices: dict[str, dict], changes: dict[str, dict],
               positional: dict[str, dict], metals: dict[str, dict],
               fng: dict, verify: str) -> str:
    lines: list[str] = []
    lines.append("📈 <b>Топ-5 + Металлы — Технический + Позиционный слой</b>")
    lines.append("")
    # Crypto section
    lines.append("<b>Крипто (медиана из 4 источников)</b>")
    for t in TICKERS:
        p = prices.get(t, {})
        c = changes.get(t, {})
        pos = positional.get(t, {})
        chg = c.get("change_24h", 0)
        src = c.get("source", "?")
        lines.append(
            f"• <b>{t}/USDT</b> ${fmt_price(p.get('price', 0))} "
            f"{fmt_chg(chg)} "
            f"OI {fmt_oi(pos.get('oi_usdt'))} "
            f"fund {pos.get('funding') if pos.get('funding') is not None else '—'}%"
            f"  (src: {src}, n={p.get('n_sources', 0)})"
        )
    lines.append("")
    # Metals section
    lines.append("<b>Металлы (Yahoo)</b>")
    for sym, m in metals.items():
        lines.append(
            f"• <b>{m.get('name', sym)}</b> ${fmt_price(m.get('price', 0))} "
            f"{fmt_chg(m.get('change_24h', 0))}"
        )
    lines.append("")
    # Sentiment
    lines.append(f"<b>Сентимент:</b> Fear & Greed = "
                 f"{fng.get('value', 0)} ({fng.get('label', 'n/a')})")
    lines.append("")
    # Verification
    lines.append(f"<b>Verification:</b> {verify}")
    lines.append("")
    lines.append("<i>Источники: Binance/CoinGecko/Kraken/Coinbase (price), "
                 "Bybit/OKX/CoinGecko (change), Binance/Bybit/OKX (OI/funding), "
                 "Yahoo (metals).</i>")
    return "\n".join(lines)


# ─── Main ───
def main() -> None:
    setup_logger("market_overview")
    log("market_overview", "start @ " +
        datetime.now(timezone(timedelta(hours=5))).isoformat(timespec="seconds"))

    # Step 1: fetch prices in parallel (4 sources each)
    with ThreadPoolExecutor(max_workers=10) as pool:
        price_futs = {t: pool.submit(fetch_crypto_price, t) for t in TICKERS}
        prices = {t: f.result() for t, f in price_futs.items()}
        for t, p in prices.items():
            log("market_overview",
                f"{t} ${fmt_price(p['price'])} spread={p['spread']:.3f}% n={p['n_sources']}")

    # Step 2: fetch changes + positional in parallel
    with ThreadPoolExecutor(max_workers=10) as pool:
        change_futs = {t: pool.submit(fetch_crypto_change, t) for t in TICKERS}
        pos_futs = {t: pool.submit(fetch_crypto_positional, t) for t in TICKERS}
        changes = {t: f.result() for t, f in change_futs.items()}
        positional = {t: f.result() for t, f in pos_futs.items()}

    # Step 3: metals + F&G
    with ThreadPoolExecutor(max_workers=4) as pool:
        metals_futs = {
            "GC=F": pool.submit(fetch_metal, "GC=F", "Золото"),
            "SI=F": pool.submit(fetch_metal, "SI=F", "Серебро"),
        }
        metals = {sym: f.result() for sym, f in metals_futs.items()}
        fng_fut = pool.submit(fetch_fng)
        fng = fng_fut.result()

    for sym, m in metals.items():
        log("market_overview",
            f"{m['name']} ${fmt_price(m['price'])} {fmt_chg(m['change_24h'])}")

    # Step 4: verify
    verify = verify_post_data(prices)
    log("market_overview", f"VERIFY: {verify}")
    if verify != "OK":
        log("market_overview", f"ABORT: verification failed: {verify}")
        return

    # Step 5: chart for BTC (anchor ticker)
    btc_ohlc = fetch_crypto_ohlc("BTC", days=7, limit=42)
    btc_price = prices["BTC"]["price"]
    sup, res = adaptive_zones(btc_price)
    chart_dir = post_dir() / datetime.now().strftime("%Y-%m-%d")
    chart_dir.mkdir(parents=True, exist_ok=True)
    chart_path = chart_dir / f"top5_btc_{datetime.now().strftime('%H%M')}.png"
    build_top5_chart(btc_ohlc, "BTC", btc_price, sup, res, chart_path)
    log("market_overview", f"chart saved: {chart_path}")

    # Step 6: caption + body
    now_ye = datetime.now(timezone(timedelta(hours=5)))
    date_str = now_ye.strftime("%d.%m.%Y %H:%M")
    caption = build_caption(date_str)
    body = build_body(prices, changes, positional, metals, fng, verify)

    # Step 7: post
    post_pair(str(chart_path), caption, body)
    log("market_overview", "posted OK")


if __name__ == "__main__":
    main()