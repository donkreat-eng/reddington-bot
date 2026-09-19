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
    """24h change % + volume. Binance → Bybit → OKX → CoinGecko."""
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
    # 4. CoinGecko (often 429, last resort)
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