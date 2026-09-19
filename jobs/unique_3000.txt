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
    data = _h