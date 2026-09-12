"""Multi-source price fetching with median + retry."""
import json
import time
import statistics
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError
from concurrent.futures import ThreadPoolExecutor, as_completed

from common import setup_logger

logger = setup_logger("fetch")

TIMEOUT = 8
MAX_AGE_SEC = 180
_last_call = {}


def _throttle(name, min_delay=0.5):
    now = time.time()
    last = _last_call.get(name, 0)
    wait = min_delay - (now - last)
    if wait > 0:
        time.sleep(wait)
    _last_call[name] = time.time()


def _http(url, headers=None, timeout=TIMEOUT, throttle_name=None, min_delay=0.5):
    if throttle_name:
        _throttle(throttle_name, min_delay)
    try:
        req = Request(url, headers=headers or {})
        with urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        logger.warning(f"fetch failed: {url} -> {e}")
        return None


def fetch_coingecko(coin_id="bitcoin", vs="usd"):
    url = (f"https://api.coingecko.com/api/v3/simple/price"
           f"?ids={coin_id}&vs_currencies={vs}&include_24hr_change=true"
           f"&include_24hr_vol=true&include_market_cap=true")
    data = _http(url, throttle_name="coingecko", min_delay=2.0)
    if not data or coin_id not in data:
        return None
    d = data[coin_id]
    return {
        "source": "coingecko",
        "price": float(d[vs]),
        "change_24h": float(d.get(f"{vs}_24h_change", 0)),
        "volume_24h": float(d.get(f"{vs}_24h_vol", 0)),
        "market_cap": float(d.get(f"{vs}_market_cap", 0)),
        "ts": time.time(),
    }


def fetch_binance(symbol="BTCUSDT"):
    url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
    data = _http(url, throttle_name="binance", min_delay=0.2)
    if not data or "lastPrice" not in data:
        return None
    return {
        "source": "binance",
        "price": float(data["lastPrice"]),
        "change_24h": float(data.get("priceChangePercent", 0)),
        "volume_24h": float(data.get("quoteVolume", 0)),
        "ts": time.time(),
    }


def fetch_kraken(symbol="BTCUSD"):
    url = f"https://api.kraken.com/0/public/Ticker?pair={symbol}"
    data = _http(url, throttle_name="kraken", min_delay=0.5)
    if not data or "result" not in data or not data["result"]:
        return None
    pair_data = next(iter(data["result"].values()))
    return {
        "source": "kraken",
        "price": float(pair_data["c"][0]),
        "change_24h": (float(pair_data["c"][0]) - float(pair_data["o"])) / float(pair_data["o"]) * 100,
        "volume_24h": float(pair_data.get("v", [0, 0])[1]),
        "ts": time.time(),
    }


def fetch_coinbase(symbol="BTC-USD"):
    url = f"https://api.coinbase.com/v2/prices/{symbol}/spot"
    data = _http(url, throttle_name="coinbase", min_delay=0.5)
    if not data or "data" not in data:
        return None
    return {
        "source": "coinbase",
        "price": float(data["data"]["amount"]),
        "change_24h": 0,
        "volume_24h": 0,
        "ts": time.time(),
    }


def fetch_ohlc(coin_id="bitcoin", vs="usd", days=7):
    """Fetch OHLC candles from Binance klines."""
    symbol_map = {
        "bitcoin": "BTCUSDT",
        "ethereum": "ETHUSDT",
        "solana": "SOLUSDT",
        "binancecoin": "BNBUSDT",
        "ripple": "XRPUSDT",
        "cardano": "ADAUSDT",
        "avalanche-2": "AVAXUSDT",
        "polkadot": "DOTUSDT",
        "chainlink": "LINKUSDT",
        "toncoin": "TONUSDT",
        "sui": "SUIUSDT",
    }
    symbol = symbol_map.get(coin_id, "BTCUSDT")
    interval = "1h"
    limit = days * 24 if days <= 30 else 500
    url = (f"https://api.binance.com/api/v3/klines"
           f"?symbol={symbol}&interval={interval}&limit={limit}")
    data = _http(url, throttle_name="binance", min_delay=0.3)
    if not data:
        url = (f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc"
               f"?vs_currency={vs}&days={days}")
        return _http(url, throttle_name="coingecko", min_delay=2.0)
    ohlc = []
    for row in data:
        ohlc.append([row[0], float(row[1]), float(row[2]), float(row[3]), float(row[4])])
    return ohlc


def fetch_price_parallel(coin_id="bitcoin", symbol="BTCUSDT"):
    """Fetch from all available sources in parallel."""
    sources = [
        ("coingecko", lambda: fetch_coingecko(coin_id)),
        ("binance", lambda: fetch_binance(symbol)),
        ("kraken", lambda: fetch_kraken(symbol.replace("USDT", "USD"))),
        ("coinbase", lambda: fetch_coinbase(symbol.replace("USDT", "-USD"))),
    ]
    results = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fn): name for name, fn in sources}
        for fut in as_completed(futures, timeout=TIMEOUT + 2):
            name = futures[fut]
            try:
                r = fut.result()
                if r:
                    results.append(r)
                    logger.info(f"  {name}: ${r['price']:,.2f}")
            except Exception as e:
                logger.warning(f"  {name} error: {e}")
    return results


def consensus_price(results, max_age=MAX_AGE_SEC):
    """Return verified price from multi-source fetch."""
    now = time.time()
    fresh = [r for r in results if (now - r["ts"]) <= max_age]
    if not fresh:
        raise ValueError("No fresh data from any source")
    prices = [r["price"] for r in fresh]
    median_p = statistics.median(prices)
    spread = (max(prices) - min(prices)) / median_p * 100
    if spread > 3.0 and len(prices) >= 3:
        sorted_p = sorted(prices)
        trimmed = sorted_p[1:-1]
        if trimmed:
            median_p = statistics.median(trimmed)
            spread = (max(trimmed) - min(trimmed)) / median_p * 100
    changes = [r.get("change_24h", 0) for r in fresh if "change_24h" in r]
    vols = [r.get("volume_24h", 0) for r in fresh if "volume_24h" in r]
    if spread < 0.3:
        agreement = "OK"
    elif spread < 1.0:
        agreement = "MINOR_DRIFT"
    elif spread < 3.0:
        agreement = "MAJOR_DRIFT"
    else:
        agreement = "CRITICAL"
    return {
        "price": round(median_p, 2),
        "change_24h": round(statistics.median(changes) if changes else 0, 2),
        "volume_24h": max(vols) if vols else 0,
        "sources": [r["source"] for r in fresh],
        "agreement": agreement,
        "spread_pct": round(spread, 3),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_with_retry(coin_id="bitcoin", symbol="BTCUSDT", max_attempts=3, retry_delay=900):
    """Fetch with retry logic."""
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            results = fetch_price_parallel(coin_id, symbol)
            verified = consensus_price(results)
            if verified["sources"]:
                logger.info(f"attempt {attempt}: {verified['agreement']} "
                            f"price=${verified['price']:,.2f} "
                            f"sources={verified['sources']} "
                            f"spread={verified['spread_pct']}%")
                return verified
            raise ValueError("No sources returned data")
        except Exception as e:
            last_err = e
            logger.warning(f"attempt {attempt} failed: {e}")
            if attempt < max_attempts:
                logger.info(f"retry in {retry_delay}s")
                time.sleep(retry_delay)
    raise RuntimeError(f"All {max_attempts} attempts failed: {last_err}")
