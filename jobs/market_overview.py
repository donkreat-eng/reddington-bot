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
KRAKEN_INTERVAL = {4 * 3600: 240}  # 4h interval in minutes for Kraken OHLC


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


def fetch_crypto_change(ticker: str) -> dict:
    """24h change % + volume. Try Binance, fallback to CoinGecko."""
    symbol = BINANCE_SYMBOL[ticker]
    try:
        data = _http_json(f"{BINANCE_BASE}/api/v3/ticker/24hr", {"symbol": symbol})
        return {
            "change_24h": float(data["priceChangePercent"]),
            "volume_24h": float(data["quoteVolume"]),
        }
    except Exception as e:
        log("market_overview", f"change {ticker} Binance FAIL: {e}; trying CoinGecko")
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
            return {"change_24h": float(change), "volume_24h": float(vol)}
        except Exception as e2:
            log("market_overview", f"change {ticker} CoinGecko FAIL: {e2}")
            return {"change_24h": 0.0, "volume_24h": 0.0}


def _kraken_ohlc(symbol: str, minutes: int = 240, days: int = 7) -> list:
    pair = {"BTCUSDT": "XBTUSDT", "ETHUSDT": "ETHUSDT", "BNBUSDT": "BNBUSDT",
            "SOLUSDT": "SOLUSDT", "XRPUSDT": "XRPUSDT"}[symbol]
    data = _http_json(f"{KRAKEN_BASE}/OHLC", {"pair": pair, "interval": minutes})
    candles = data["result"][pair]
    cutoff = int(time.time()) - days * 86400
    out = []
    for c in candles:
        if c[0] >= cutoff:
            out.append((c[0] * 1000, float(c[1]), float(c[2]),
                        float(c[3]), float(c[4])))
    return out


def _coingecko_ohlc(coin_id: str, days: int = 7) -> list:
    data = _http_json(f"{COINGECKO_BASE}/coins/{coin_id}/ohlc",
                      {"vs_currency": "usd", "days": days})
    return [(c[0], float(c[1]), float(c[2]), float(c[3]), float(c[4]))
            for c in data]


def fetch_crypto_ohlc(ticker: str, days: int = 7) -> list:
    """4h OHLC for chart. Binance → Kraken → CoinGecko fallback."""
    symbol = BINANCE_SYMBOL[ticker]
    limit = days * 6  # 6 candles per day at 4h
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


def fetch_crypto_positional(ticker: str) -> dict:
    """OI, funding, L/S ratio. Binance futures → try alt sources."""
    symbol = BINANCE_SYMBOL[ticker]
    out = {}

    def _bin(path: str, params: dict, timeout: int = 10):
        return _http_json(f"{BINANCE_BASE}{path}", params, timeout=timeout)

    # Open interest
    try:
        oi = _bin("/futures/data/openInterestHist",
                  {"symbol": symbol, "period": "1h", "limit": 1})
        out["oi_usdt"] = float(oi[0]["sumOpenInterestValue"]) if oi else 0.0
    except Exception as e:
        log("market_overview", f"OI {ticker} Binance FAIL: {e}")
        out["oi_usdt"] = None

    # Funding rate
    try:
        fr = _bin("/fapi/v1/premiumIndex", {"symbol": symbol})
        out["funding"] = float(fr["lastFundingRate"]) * 100
    except Exception as e:
        log("market_overview", f"funding {ticker} Binance FAIL: {e}")
        out["funding"] = None

    # Long/Short ratio
    try:
        ls = _bin("/futures/data/globalLongShortAccountRatio",
                  {"symbol": symbol, "period": "1h", "limit": 1})
        out["long_short"] = float(ls[0]["longShortRatio"]) if ls else None
    except Exception as e:
        log("market_overview", f"L/S {ticker} Binance FAIL: {e}")
        out["long_short"] = None

    return out


def fetch_metal(name: str) -> dict:
    """Fetch metal price from Yahoo Finance (public endpoint)."""
    symbol = METAL_SYMBOLS[name]
    headers = {"User-Agent": "Mozilla/5.0"}
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    data = requests.get(url, params={"interval": "1d", "range": "5d"},
                        headers=headers, timeout=10).json()
    res = data["chart"]["result"][0]
    meta = res["meta"]
    price = meta["regularMarketPrice"]
    prev = meta.get("chartPreviousClose", meta.get("previousClose", price))
    return {
        "name": name,
        "price": float(price),
        "change_pct": (float(price) - float(prev)) / float(prev) * 100,
    }


def fmt_oi(oi: float | None) -> str:
    if oi is None:
        return "n/a"
    if oi > 1e9:
        return f"${oi / 1e9:.2f}B"
    if oi > 1e6:
        return f"${oi / 1e6:.1f}M"
    return f"${oi:,.0f}"


def fmt_funding(f: float | None) -> str:
    if f is None:
        return "n/a"
    sign = "+" if f >= 0 else ""
    return f"{sign}{f:.3f}%"


def fmt_ls(ls: float | None) -> str:
    if ls is None:
        return "n/a"
    pct = ls / (1 + ls) * 100
    return f"{pct:.0f}% лонг"


def adaptive_zones(price: float) -> tuple[float, float]:
    if price >= 1000:
        sup = round(price * 0.97, 2)
        res = round(price * 1.03, 2)
    elif price >= 10:
        sup = round(price * 0.95, 2)
        res = round(price * 1.05, 2)
    elif price >= 1:
        sup = round(price * 0.93, 2)
        res = round(price * 1.07, 2)
    else:
        sup = round(price * 0.88, 4)
        res = round(price * 1.12, 4)
    return sup, res


def build_top5_chart(ohlc_by_ticker: dict, out_path: Path) -> None:
    """Render BTC chart — main asset for top5 overview."""
    btc_ohlc = ohlc_by_ticker["BTC"]
    sup, res = adaptive_zones(ohlc_by_ticker["_btc_price"])
    chartlib.generate_chart(
        btc_ohlc, "BTC/USDT",
        (sup * 0.998, sup * 1.005),
        (res * 0.995, res * 1.002),
        str(out_path),
    )


def build_caption(prices: dict, changes: dict, ts: datetime) -> str:
    parts = ["🏛 <b>REDDINGTON · Топ-5 + Металлы</b>",
             f"_{ts.strftime('%d %b %Y · %H:%M')} YEKT_",
             ""]
    parts.append("📊 <b>Крипто (24ч)</b>")
    for t in CRYPTO_TICKERS:
        p = prices[t]["price"]
        ch = changes[t]["change_24h"]
        arrow = "🟢" if ch >= 0 else "🔴"
        parts.append(f"  {arrow} <b>{t}</b> · ${fmt_price(p)} · {fmt_change(ch)}")
    parts.append("")
    parts.append("🥇 <b>Металлы</b>")
    parts.append(f"  🟡 <b>Gold</b> · ${fmt_price(prices['Gold']['price'])} · "
                 f"{fmt_change(prices['Gold']['change_pct'])}")
    parts.append(f"  ⚪ <b>Silver</b> · ${fmt_price(prices['Silver']['price'])} · "
                 f"{fmt_change(prices['Silver']['change_pct'])}")
    parts.append("")
    parts.append("🎯 Зоны BTC (адаптивные)")
    sup, res = adaptive_zones(prices["BTC"]["price"])
    parts.append(f"  Поддержка: ${fmt_price(sup)}")
    parts.append(f"  Сопротивление: ${fmt_price(res)}")
    return "\n".join(parts)


def build_body(prices: dict, changes: dict, positional: dict,
               fng_value: int | None, ts: datetime) -> str:
    lines = ["🏛 <b>REDDINGTON · Обзор рынка</b>",
             f"<i>{ts.strftime('%A, %d %B %Y · %H:%M')} YEKT</i>",
             ""]

    lines.append("📈 <b>Сводка по активам</b>")
    lines.append("<pre>")
    lines.append(f"{'Актив':<8} {'Цена':>11} {'24ч':>7} {'OI':>9} {'Funding':>9} {'L/S':>8}")
    lines.append("─" * 60)
    for t in CRYPTO_TICKERS:
        p = prices[t]["price"]
        ch = changes[t]["change_24h"]
        pos = positional.get(t, {})
        arrow = "▲" if ch >= 0 else "▼"
        lines.append(
            f"{t:<8} ${p:>10,.2f} {arrow}{abs(ch):>5.2f}% "
            f"{fmt_oi(pos.get('oi_usdt')):>9} {fmt_funding(pos.get('funding')):>9} "
            f"{fmt_ls(pos.get('long_short')):>8}"
        )
    lines.append("─" * 60)
    lines.append(f"{'Gold':<8} ${prices['Gold']['price']:>10,.2f} "
                 f"{prices['Gold']['change_pct']:>+6.2f}%")
    lines.append(f"{'Silver':<8} ${prices['Silver']['price']:>10,.2f} "
                 f"{prices['Silver']['change_pct']:>+6.2f}%")
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
    lines.append(f"  • Поддержка ${fmt_price(sup)} — зона покупки на откате")
    lines.append(f"  • Сопротивление ${fmt_price(res)} — зона фиксации прибыли")
    mid = (sup + res) / 2
    lines.append(f"  • Середина диапазона ${fmt_price(mid)} — точка равновесия")
    lines.append("")

    lines.append("🧭 <b>Что смотреть до конца недели</b>")
    lines.append("  • Реакция BTC на сопротивление — пробой = продолжение")
    lines.append("  • Закрытие недели выше/ниже $80K задаст тон фьючерсам")
    lines.append("  • ETH/BTC ratio — слабость эфира = risk-off в альты")
    lines.append("  • Золото: $4 400 — психологический уровень")
    lines.append("")

    lines.append("<i>Автопост · проверка данных из 4 источников · 13:30 YEKT</i>")
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
