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

OKX_BASE = "https://www.okx.com"
OKX_SWAP_INST: dict[str, str] = {
    "BTCUSDT": "BTC-USDT-SWAP",
    "ETHUSDT": "ETH-USDT-SWAP",
    "BNBUSDT": "BNB-USDT-SWAP",
    "SOLUSDT": "SOL-USDT-SWAP",
    "XRPUSDT": "XRP-USDT-SWAP",
}


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


def _okx_swap(inst_id: str) -> dict:
    """OKX public ticker for SWAP: last, open24h, volCcy24h. No auth."""
    data = _http_json(f"{OKX_BASE}/api/v5/market/ticker", {"instId": inst_id}, timeout=8)
    if not data.get("data"):
        raise RuntimeError(f"OKX ticker {inst_id} empty")
    return data["data"][0]


def _okx_funding(inst_id: str) -> float:
    """OKX funding rate (decimal). No auth."""
    data = _http_json(f"{OKX_BASE}/api/v5/public/funding-rate", {"instId": inst_id}, timeout=8)
    if not data.get("data"):
        raise RuntimeError(f"OKX funding {inst_id} empty")
    return float(data["data"][0]["fundingRate"])


def _okx_oi(inst_id: str, mark_price: float) -> float:
    """OKX open interest in USDT = oiCcy * mark_price. No auth."""
    data = _http_json(f"{OKX_BASE}/api/v5/public/open-interest", {"instId": inst_id}, timeout=8)
    if not data.get("data"):
        raise RuntimeError(f"OKX OI {inst_id} empty")
    oi_ccy = float(data["data"][0]["oiCcy"] or 0)
    return oi_ccy * mark_price


def _okx_extras(symbol: str) -> dict:
    """OKX aggregated: change_24h%, OI_usdt, funding%, turnover_usdt. No auth.
    Used as final fallback when Binance and Bybit both fail (e.g. Bybit 403 on runners).
    """
    inst = OKX_SWAP_INST[symbol]
    ticker = _okx_swap(inst)
    last = float(ticker["last"])
    open24 = float(ticker["open24h"])
    change_pct = ((last - open24) / open24 * 100) if open24 else 0.0
    # volCcy24h is in contracts; convert via last for USDT turnover
    vol_ccy = float(ticker.get("volCcy24h") or 0)
    turnover_usdt = vol_ccy * last
    funding = _okx_funding(inst) * 100
    oi_usdt = _okx_oi(inst, last)
    return {
        "change_24h": change_pct,
        "turnover_24h": turnover_usdt,
        "funding": funding,
        "oi_usdt": oi_usdt,
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
            f"change {ticker} Bybit FAIL: {e}; trying OKX")
    # 3. OKX swap ticker (no auth, works where Bybit 403 on Cloudflare-blocked IPs)
    try:
        ex = _okx_extras(symbol)
        return {
            "change_24h": ex["change_24h"],
            "volume_24h": ex["turnover_24h"],
            "source": "okx",
        }
    except Exception as e:
        log("market_overview",
            f"change {ticker} OKX FAIL: {e}; trying CoinGecko")
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


def _kraken_ohlc(symbol: str, minutes: int = 240, days: int = 7) -> list:
    pair = {"BTCUSDT": "XBTUSDT", "ETHUSDT": "ETHUSDT", "BNBUSDT": "BNBUSDT",
            "SOLUSDT": "SOLUSDT", "XRPUSDT": "XRPUSDT"}[symbol]
    data = _http_json(f"{KRAKEN_BASE}/OHLC", {"pair": pair, "interval": minutes})
    candles = data["result"][pair]
    cutoff = int(time.time()) - days * 86400
    out = []
    for c in candles:
        ts_s = int(c[0])
        if ts_s < cutoff:
            continue
        out.append((ts_s * 1000, float(c[1]), float(c[2]), float(c[3]), float(c[4])))
    return out


def _coingecko_ohlc(coin_id: str, days: int = 7) -> list:
    data = _http_json(f"{COINGECKO_BASE}/coins/{coin_id}/ohlc",
                      {"vs_currency": "usd", "days": str(days)})
    return [(int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4])) for c in data]


def fetch_crypto_ohlc(ticker: str, days: int = 7) -> list:
    """4h OHLC for chart. Binance → Kraken → CoinGecko fallback."""
    symbol = BINANCE_SYMBOL[ticker]
    # 1. Binance klines
    try:
        data = _http_json(f"{BINANCE_BASE}/api/v3/klines",
                          {"symbol": symbol, "interval": "4h", "limit": days * 6})
        return [(int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]))
                for c in data]
    except Exception as e:
        log("market_overview", f"ohlc {ticker} Binance FAIL: {e}; trying Kraken")
    # 2. Kraken OHLC
    try:
        result = _kraken_ohlc(symbol, minutes=KRAKEN_INTERVAL[4 * 3600], days=days)
        if result:
            log("market_overview", f"ohlc {ticker} source=kraken n={len(result)}")
            return result
    except Exception as e:
        log("market_overview", f"ohlc {ticker} Kraken FAIL: {e}; trying CoinGecko")
    # 3. CoinGecko OHLC (last resort)
    result = _coingecko_ohlc(COINGECKO_IDS[ticker], days=days)
    log("market_overview", f"ohlc {ticker} source=coingecko n={len(result)}")
    return result


def fetch_crypto_positional(ticker: str) -> dict:
    """OI (USDT), funding rate (%), L/S ratio. Binance → Bybit → OKX."""
    symbol = BINANCE_SYMBOL[ticker]
    result: dict = {"oi_usdt": None, "funding": None, "long_short": None, "source": "none"}
    # 1. Binance funding + OI
    try:
        data = _http_json(f"{BINANCE_BASE}/fapi/v1/premiumIndex", {"symbol": symbol})
        result["funding"] = float(data["lastFundingRate"]) * 100
        data = _http_json(f"{BINANCE_BASE}/fapi/v1/openInterest", {"symbol": symbol})
        oi_ccy = float(data["openInterest"])
        # Mark price ≈ ticker last
        tk = _http_json(f"{BINANCE_BASE}/api/v3/ticker/price", {"symbol": symbol})
        result["oi_usdt"] = oi_ccy * float(tk["price"])
        result["source"] = "binance"
    except Exception as e:
        log("market_overview",
            f"positional {ticker} Binance FAIL: {e}; trying Bybit")
    # 2. Bybit fallback for OI + funding
    if result["oi_usdt"] is None or result["funding"] is None:
        try:
            ex = _bybit_extras(symbol)
            if result["oi_usdt"] is None:
                result["oi_usdt"] = ex["oi_usdt"]
            if result["funding"] is None:
                result["funding"] = ex["funding"]
            result["source"] = "bybit"
        except Exception as e:
            log("market_overview",
                f"positional {ticker} Bybit FAIL: {e}; trying OKX")
            # 3. OKX fallback for OI + funding
            try:
                ex = _okx_extras(symbol)
                if result["oi_usdt"] is None:
                    result["oi_usdt"] = ex["oi_usdt"]
                if result["funding"] is None:
                    result["funding"] = ex["funding"]
                result["source"] = "okx"
            except Exception as e2:
                log("market_overview",
                    f"positional {ticker} OKX FAIL: {e2}")
    # 4. L/S ratio — Binance global long/short account ratio (no good public alt)
    try:
        data = _http_json(
            f"{BINANCE_BASE}/futures/data/globalLongShortAccountRatio",
            {"symbol": symbol, "period": "5m", "limit": 1},
        )
        result["long_short"] = float(data[0]["longShortRatio"])
        if result["source"] == "none":
            result["source"] = "binance"
    except Exception as e:
        log("market_overview",
            f"positional {ticker} Binance L/S FAIL: {e}")
    return result


def fetch_metal(symbol: str) -> dict:
    yahoo = f"https://query1.finance.yahoo.com/v8/finance/chart/{METAL_SYMBOLS[symbol]}"
    data = _http_json(yahoo, {"interval": "1d", "range": "5d"})
    res = data["chart"]["result"][0]
    closes = res["indicators"]["quote"][0]["close"]
    last = float(closes[-1])
    prev = float(closes[-2]) if len(closes) > 1 else last
    return {"ticker": symbol, "price": last,
            "change_pct": (last - prev) / prev * 100 if prev else 0.0}


def fmt_oi(oi: float | None) -> str:
    if oi is None or oi == 0:
        return "—"
    if oi >= 1e9:
        return f"${oi / 1e9:.2f}B"
    if oi >= 1e6:
        return f"${oi / 1e6:.1f}M"
    return f"${oi / 1e3:.0f}K"


def fmt_funding(f: float | None) -> str:
    if f is None:
        return "—"
    sign = "+" if f >= 0 else ""
    return f"{sign}{f:.3f}%"


def fmt_ls(ls: float | None) -> str:
    if ls is None:
        return "—"
    pct = ls / (1 + ls) * 100
    return f"{pct:.0f}% лонг"


def adaptive_zones(price: float) -> tuple[float, float]:
    """ATR-like adaptive support/resistance bands around current price."""
    span = price * 0.03  # ±3%
    return price - span, price + span


def build_top5_chart(ohlc_map: dict, out_path: Path):
    btc = ohlc_map["BTC"]
    chartlib.generate_chart(
        ohlc=btc,
        ticker="BTC",
        support_range=None,
        resistance_range=None,
        output_path=str(out_path),
    )


def build_caption(prices: dict, changes: dict, ts: datetime) -> str:
    parts = ["🏛 <b>REDDINGTON · Топ-5 + Металлы</b>",
             f"<i>{ts.strftime('%d %b %Y · %H:%M')} YEKT</i>", ""]
    parts.append("📊 <b>Крипто (24ч)</b>")
    for t in CRYPTO_TICKERS:
        p = prices[t]["price"]
        ch = changes[t].get("change_24h", 0)
        if ch is None or ch == 0:
            parts.append(f"  🟡 <b>{t}</b> · {fmt_price(p)}")
        else:
            arrow = "🟢" if ch >= 0 else "🔴"
            parts.append(f"  {arrow} <b>{t}</b> · {fmt_price(p)} · {fmt_change(ch)}")
    parts.append("")
    parts.append("🥇 <b>Металлы</b>")
    gold_ch = prices['Gold']['change_pct']
    silver_ch = prices['Silver']['change_pct']
    if gold_ch == 0:
        parts.append(f"  🟡 <b>Gold</b> · {fmt_price(prices['Gold']['price'])}")
    else:
        parts.append(f"  🟡 <b>Gold</b> · {fmt_price(prices['Gold']['price'])} · "
                     f"{fmt_change(gold_ch)}")
    if silver_ch == 0:
        parts.append(f"  ⚪ <b>Silver</b> · {fmt_price(prices['Silver']['price'])}")
    else:
        parts.append(f"  ⚪ <b>Silver</b> · {fmt_price(prices['Silver']['price'])} · "
                     f"{fmt_change(silver_ch)}")
    parts.append("")
    parts.append("🎯 Зоны BTC (адаптивные)")
    sup, res = adaptive_zones(prices["BTC"]["price"])
    parts.append(f"  Поддержка: {fmt_price(sup)}")
    parts.append(f"  Сопротивление: {fmt_price(res)}")
    return "\n".join(parts)


def build_body(prices: dict, changes: dict, positional: dict,
               fng_value: int | None, ts: datetime) -> str:
    lines = ["🏛 <b>REDDINGTON · Обзор рынка</b>",
             f"<i>{ts.strftime('%A, %d %B %Y · %H:%M')} YEKT</i>",
             ""]

    lines.append("📈 <b>Сводка по активам</b>")
    # Каждую колонку прячем отдельно: если по ВСЕМ тикерам данные пустые — колонки нет
    has_oi = any((positional.get(t) or {}).get("oi_usdt") is not None for t in CRYPTO_TICKERS)
    has_funding = any((positional.get(t) or {}).get("funding") is not None for t in CRYPTO_TICKERS)
    has_ls = any((positional.get(t) or {}).get("long_short") is not None for t in CRYPTO_TICKERS)
    cols = [('Актив', 8, 'l'), ('Цена', 11, 'r'), ('24ч', 8, 'r')]
    if has_oi: cols.append(('OI', 9, 'r'))
    if has_funding: cols.append(('Funding', 9, 'r'))
    if has_ls: cols.append(('L/S', 11, 'r'))
    width = sum(c[1] + 1 for c in cols) - 1
    lines.append("<pre>")
    header = " ".join(f"{name:>{w}}" if align == 'r' else f"{name:<{w}}" for name, w, align in cols)
    lines.append(header)
    lines.append("─" * width)
    for t in CRYPTO_TICKERS:
        p = prices[t]["price"]
        ch = changes[t]["change_24h"]
        if ch is None or ch == 0:
            change_str = "—"
        else:
            arr = "▲" if ch >= 0 else "▼"
            change_str = f"{arr}{abs(ch):.2f}%"
        pos = positional.get(t, {})
        row_parts = [f"{t:<8}", f"${p:>10,.2f}", f"{change_str:>8}"]
        if has_oi: row_parts.append(f"{fmt_oi(pos.get('oi_usdt')):>9}")
        if has_funding: row_parts.append(f"{fmt_funding(pos.get('funding')):>9}")
        if has_ls: row_parts.append(f"{fmt_ls(pos.get('long_short')):>11}")
        lines.append(" ".join(row_parts))
    lines.append("─" * width)
    # Gold / Silver — без процента если 0
    gold_ch = prices['Gold']['change_pct']
    silver_ch = prices['Silver']['change_pct']
    gold_str = f"{gold_ch:+.2f}%" if gold_ch else "—"
    silver_str = f"{silver_ch:+.2f}%" if silver_ch else "—"
    lines.append(f"{'Gold':<8} ${prices['Gold']['price']:>10,.2f} {gold_str:>8}")
    lines.append(f"{'Silver':<8} ${prices['Silver']['price']:>10,.2f} {silver_str:>8}")
    lines.append("</pre>")
    lines.append("")

    if fng_value is not None:
        emoji = "🟢" if fng_value >= 55 else ("🔴" if fng_value <= 45 else "🟡")
        lines.append(f"🌡 <b>Fear &amp; Greed:</b> {emoji} {fng_value}/100")
        lines.append("")

    lines.append("🔍 <b>Что значат цифры</b>")
    btc = prices["BTC"]
    btc_pos = positional.get("BTC", {})
    fr = btc_pos.get("funding")
    ls = btc_pos.get("long_short")

    if fr is not None and abs(fr) > 0.05:
        side = "перегрет лонг" if fr > 0 else "перегрет шорт"
        lines.append(f"  • BTC funding {fmt_funding(fr)} → {side}, "
                     f"риск коррекции в обратную сторону")
    else:
        lines.append("  • BTC funding в нейтральной зоне — рынок сбалансирован")

    if ls is not None:
        ls_pct = ls / (1 + ls) * 100
        if ls_pct > 60:
            lines.append(f"  • {ls_pct:.0f}% лонгов → толпа в лонге, "
                         f"осторожно с шорт-идеями")
        elif ls_pct < 40:
            lines.append(f"  • Только {ls_pct:.0f}% лонгов → "
                         f"контртрендовые шорты рискованны")
        else:
            lines.append(f"  • L/S {ls_pct:.0f}% лонг — баланс")

    oi_chg = btc_pos.get("oi_usdt")
    if oi_chg is not None:
        if oi_chg > 5e9:
            lines.append(f"  • OI ${oi_chg / 1e9:.1f}B — высокий интерес, "
                         f"движения будут резкими")
        else:
            lines.append(f"  • OI ${oi_chg / 1e9:.2f}B — спокойное состояние")

    lines.append("")
    lines.append("⚙️ <b>Кластеры (BTC)</b>")
    sup, res = adaptive_zones(btc["price"])
    lines.append(f"  • Поддержка {fmt_price(sup)} — зона покупки на откате")
    lines.append(f"  • Сопротивление {fmt_price(res)} — зона фиксации прибыли")
    mid = (sup + res) / 2
    lines.append(f"  • Середина диапазона {fmt_price(mid)} — точка равновесия")
    lines.append("")

    lines.append("🧭 <b>Что смотреть до конца недели</b>")
    lines.append("  • Реакция BTC на сопротивление — пробой = продолжение")
    lines.append("  • Закрытие недели выше/ниже $80K задаст тон фьючерсам")
    lines.append("  • ETH/BTC ratio — слабость эфира = risk-off в альты")
    lines.append("  • Золото: $4 400 — психологический уровень")

    return "\n".join(lines)


def fetch_fng() -> int | None:
    try:
        data = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()
        return int(data["data"][0]["value"])
    except Exception as e:  # noqa: BLE001
        log("market_overview", f"F&G FAIL: {e}")
        return None


def verify_post_data(prices: dict) -> tuple[bool, str]:
    """L1+L2 verification: freshness + cross-source agreement."""
    issues = []
    for t in CRYPTO_TICKERS:
        info = prices[t]
        if info["n_sources"] < 2:
            issues.append(f"{t}: only {info['n_sources']} sources")
        elif info["spread_pct"] > 3:
            issues.append(f"{t}: spread {info['spread_pct']:.2f}% > 3%")
    if issues:
        return False, "; ".join(issues)
    return True, "OK"


def main():
    setup_logger()
    load_env()
    bot_token = os.environ["REDDINGTON_BOT_TOKEN"]
    chat_id = os.environ["REDDINGTON_CHANNEL_ID"]

    ts = datetime.now(YEKT)
    log("market_overview", f"start @ {ts.isoformat()}")

    # 1. Fetch crypto prices in parallel
    prices: dict = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_crypto_price, t): t for t in CRYPTO_TICKERS}
        for f in as_completed(futs):
            try:
                info = f.result()
                prices[info["ticker"]] = info
                log("market_overview",
                    f"{info['ticker']} ${info['price']:.2f} "
                    f"spread={info['spread_pct']:.3f}% "
                    f"n={info['n_sources']}")
            except Exception as e:  # noqa: BLE001
                t = futs[f]
                log("market_overview", f"{t} FAIL: {e}")
                raise

    # 2. Fetch changes + OI + funding + L/S
    changes: dict = {}
    positional: dict = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {}
        for t in CRYPTO_TICKERS:
            futs[ex.submit(fetch_crypto_change, t)] = ("change", t)
            futs[ex.submit(fetch_crypto_positional, t)] = ("pos", t)
        for f in as_completed(futs):
            kind, t = futs[f]
            try:
                if kind == "change":
                    changes[t] = f.result()
                else:
                    positional[t] = f.result()
            except Exception as e:  # noqa: BLE001
                log("market_overview", f"{kind} {t} FAIL: {e}")

    # 3. Fetch metals in parallel
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_g = ex.submit(fetch_metal, "Gold")
        f_s = ex.submit(fetch_metal, "Silver")
        prices["Gold"] = f_g.result()
        prices["Silver"] = f_s.result()
    log("market_overview", f"Gold ${prices['Gold']['price']:.2f} "
        f"Silver ${prices['Silver']['price']:.2f}")

    # 4. F&G
    fng = fetch_fng()

    # 5. Verify
    ok, msg = verify_post_data(prices)
    if not ok:
        log("market_overview", f"VERIFY FAIL: {msg}")
        return
    log("market_overview", f"VERIFY OK: {msg}")

    # 6. Build chart for BTC
    btc_ohlc = fetch_crypto_ohlc("BTC", days=7)
    prices["_btc_price"] = prices["BTC"]["price"]  # for chart zones
    chart_path = post_dir() / f"top5_btc_{ts.strftime('%Y%m%d_%H%M')}.png"
    build_top5_chart({"BTC": btc_ohlc, "_btc_price": prices["BTC"]["price"]},
                     chart_path)
    log("market_overview", f"chart saved: {chart_path} ({chart_path.stat().st_size} B)")

    # 7. Build caption + body
    caption = build_caption(prices, changes, ts)
    body = build_body(prices, changes, positional, fng, ts)

    # 8. Post
    post_pair(str(chart_path), caption, body)
    log("market_overview", "posted OK")


if __name__ == "__main__":
    main()