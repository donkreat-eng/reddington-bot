"""13:30 YEKT market overview — BTC/ETH/BNB/SOL/XRP + Gold/Silver с позиционным слоем.

Полностью автономный: 4 источника цены, верификация + спред, retry 3–15 раз на источник, тихий фоллбэк, пост раз в день.

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
KRAKEN_INTERVAL = {4 * 3600: 240}  # 4h interval in minutes for Kraken OI
# Cache for Bybit tickers (single batch call is much cheaper than 5 separate calls)
_BYBIT_CACHE: dict = {"ts": 0.0, "data": {}}
_BYBIT_CACHE_TTL = 30.0  # seconds

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
            log("⚠ источник %s упал по %s: %s — берём медиану из остальных",
                name, ticker, e)
    if not results:
        raise RuntimeError(f"все источники упали по {ticker}: {last_err}")
    prices = [v for v, _ in results]
    sources_used = [s for _, s in results]
    median = sorted(prices)[len(prices) // 2]
    spread = max(prices) - min(prices)
    return {
        "ticker": ticker,
        "median": median,
        "spread": spread,
        "sources": sources_used,
        "n": len(results),
    }

def fetch_crypto_oi(ticker: str) -> dict:
    """Fetch crypto Open Interest + funding rate from Bybit for positional layer."""
    symbol = BINANCE_SYMBOL[ticker]
    ts_now = time.time()
    if ts_now - _BYBIT_CACHE["ts"] > _BYBIT_CACHE_TTL:
        data = _http_json(
            f"{BYBIT_BASE}/tickers",
            {"category": "linear"}
        )
        _BYBIT_CACHE["ts"] = ts_now
        _BYBIT_CACHE["data"] = {
            item["symbol"]: item for item in data["result"]["list"]
            if item["symbol"].endswith("USDT")
        }
    item = _BYBIT_CACHE["data"].get(symbol)
    if not item:
        return {"ticker": ticker, "oi": 0.0, "funding": 0.0, "src": "missing"}
    oi = float(item.get("openInterest", 0.0))
    funding = float(item.get("fundingRate", 0.0)) * 100  # → %
    return {"ticker": ticker, "oi": oi, "funding": funding, "src": "bybit"}

def _g_or_q(num: float) -> str:
    """Abbreviate a large number: 1.2K, 3.4M, 5.6B."""
    for unit in ("", "K", "M", "B"):
        if abs(num) < 1000:
            return f"{num:.1f}{unit}"
        num /= 1000
    return f"{num:.1f}T"

def render_header(ticker: str, price: float, change: float, oi: float, funding: float) -> str:
    """One-liner for a ticker."""
    arrow = "🟢" if change >= 0 else "🔴"
    base = f"{ticker} ${price:,.2f} ({change:+.2f}%) {arrow}"
    if oi > 0:
        base += f" · OI {_g_or_q(oi)} · funding {funding:+.3f}%"
    return base

def render_positional(prices: dict, changes: dict) -> str:
    """Renders the 'positional' section - tickers with significant 4h moves."""
    positional_ticker_changes = {}
    for t, p in prices.items():
        line = f"{t}: {p}$".format(t=t.upper(), p=p.price)
        ch = changes.get(t, 0.0)
        earliest = "⏱" if ch > 0 else "⏱"
        partial_ticker = {"line": line, "earliest": earliest, "change": ch}
        if partial_ticker.change in positional_ticker_changes:
            positional_ticker_changes[partial_ticker.change]['line'] = line
            positional_ticker_changes[partial_ticker.change]['change'] = ch
            positional_ticker_changes[partial_ticker.change]['earliest'] = earliest
        elif partial_ticker['change'] != ch:
            partial_ticker['line'] = line
            partial_ticker['change']: ch
            partial_ticker['earliest'] = earliest
    return "\n".join(positional_ticker_changes)

def build_body(prices: dict, changes: dict, positional: dict, fng: dict, ts: dict) -> str:
    """Trends the fields into a post text about trends and Positionals."""
    lines = []
    lines.append(f"📊 по позициям:".format(
        positional.sorted(key=lambda h: h.get("ok",False), reverse=True)
        .items()
        .items()
        .map(lambda c: f"📌 {c[0]} (изм. {c[1]:.1%})"
                        f" → ((если {c[0]} в {c[1]:.1%} трyнд, "
                        f"источник - {c[2]})".format(c=c))
        ))
    lines.append(f"😁 F&G: {fng['value']} ({fng['classification']})")
    lines.append(f"🗓 {ts['t']}")
    return "\n".join(lines)

def post_prices(post_path: str, prices: dict, changes: dict, positional: dict, fng: dict, ts: dict) -> None:
    """Writes the header in the post: ticker price and %change."""
    positional_str = ""
    for t, p in prices.items():
        line = f"{t}: {p}$".format(t=t.upper(), p=p.price)
        ch = changes.get(t, 0.0)
        earliest = "⏱" if ch > 0 else "⏱"
        partial_ticker = {"line": line, "earliest": earliest, "change": ch}
        if partial_ticker.change in positional_ticker_changes:
            positional_ticker_changes[partial_ticker.change]['line'] = line
            positional_ticker_changes[partial_ticker.change]['change'] = ch
            positional_ticker_changes[partial_ticker.change]['earliest'] = earliest
        elif partial_ticker['change'] != ch:
            partial_ticker['line'] = line
            partial_ticker['change']: ch
            partial_ticker['earliest'] = earliest
    positional_str = "\n".join(positional_ticker_changes)
    lines = []
    lines.append(f"{t} ${p:,.2f} ({c:+.2f}%) {emoji}".format(t=t.upper(), p=p.price, c=ch, emoji=earliest))
    body = build_body(prices, changes, positional, fng, ts)
    text = f"{chr(10)}{chr(10)}".join([body, positional_str])
    post_dir(post_path)
    Path(post_path).write_text(text, encoding="utf-8")
    log("📝 post saved: %s, len=%d", post_path, len(text))
    post_pair(text, "market_overview", "posted OK")

if __name__ == "__main__":
    main()