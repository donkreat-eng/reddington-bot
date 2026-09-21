"""Multi-source price fetching with median + retry."""
import json
import time
import statistics
import random
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError
from concurrent.futures import ThreadPoolExecutor, as_completed
from lib.common import setup_logger, log

logger = setup_logger("fetch")

_TIMEOUT = 10
_MAX_AGE_SEC = 120


def _http(url, throttle_name=None, min_delay=0.3):
    """HTTP GET with throttle + JSON parse. Returns dict or None."""
    if throttle_name:
        _throttle(throttle_name, min_delay)
    try:
        req = Request(url, headers={"User-Agent": "reddington-bot/1.0"})
        with urlopen(req, timeout=_TIMEOUT) as r:
            data = json.loads(r.read().decode())
        return data
    except (URLError, json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning(f"fetch fail {url[:80]}: {e}")
        return None


_LAST = {0}
def _throttle(name, min_delay):
    last = _LAST.get(name, 0)
    elapsed = time.time() - last
    if elapsed < min_delay:
        time.sleep(min_delay - elapsed)
    _LAST[name] = time.time()


# === Per-coin fetchers ===
def fetch_coingecko(coin_id="bitcoin", vs="usd"):
    """CoinGecko simple price."""
    url = (f"https://api.coingecko.com/api/v3/simple/price"
           f"?ids={coin_id}&vs_currencies={vs}"
           f"&include_24hr_change=true&include_24hr_vol=true")
    data = _http(url, throttle_name="coingecko", min_delay=1.5)
    if not data or coin_id not in data or vs not in data[coin_id]:
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
    """Binance 24h ticker."""
    url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
    data = _http(url, throttle_name="binance", min_delay=0.3)
    if not data or "lastPrice" not in data:
        return None
    return {
        "source": "binance",
        "price": float(data["lastPrice"]),
        "change_24h": float(data["priceChangePercent"]),
        "volume_24h": float(data["quoteVolume"]),
        "ts": time.time(),
    }


def fetch_kraken(symbol="BTCUSD"):
    """Kraken public ticker."""
    url = f"https://api.kraken.com/0/public/Ticker?pair={symbol}"
    data = _http(url, throttle_name="kraken", min_delay=0.5)
    if not data or not data.get("result"):
        return None
    result = data["result"]
    key = list(result.keys())[0]
    t = result[key]
    return {
        "source": "kraken",
        "price": float(t["c"][0]),
        "change_24h": 0,  # Kraken doesn't return % change in this endpoint
        "volume_24h": float(t.get("v", [0, 0])[1]),
        "ts": time.time(),
    }


def fetch_coinbase(symbol="BTC-USD"):
    """Coinbase spot price."""
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


# === OHLC fetcher ===
def fetch_ohlc(coin_id="bitcoin", vs="usd", days=7):
    """Fetch OHLC candles. Prefer Binance klines (more reliable than CoinGecko OHLC)."""
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
        # Fallback to CoinGecko
        url = (f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc"
               f"?vs_currency={vs}&days={days}")
        return _http(url, throttle_name="coingecko", min_delay=2.0)
    # Binance klines: [openTime, open, high, low, close, volume, ...]
    ohlc = []
    for row in data:
        ohlc.append([row[0], float(row[1]), float(row[2]), float(row[3]), float(row[4])])
    return ohlc


# === Multi-source parallel fetch ===
def fetch_price_parallel(coin_id="bitcoin", symbol="BTCUSDT"):
    """Fetch from all available sources in parallel."""
    sources = [
        ("coingecko", lambda: fetch_coingecko(coin_id)),
        ("binance", lambda: fetch_binance(symbol)),
        ("kraken", lambda: fetch_kraken(symbol.replace("USDT", "USD"))),
        ("coinbase", lambda: fetch_coinbase(symbol.replace("USDT", "-USD"))),
    ]
    results = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(fn): name for name, fn in sources}
        for fut in as_completed(futures, timeout=_TIMEOUT + 2):
            name = futures[fut]
            try:
                r = fut.result()
                if r:
                    results.append(r)
                    logger.info(f"  {name}: ${r['price']:,.2f}")
            except Exception as e:
                logger.warning(f"  {name} error: {e}")
    return results


def consensus_price(results, max_age=_MAX_AGE_SEC):
    """Return verified price from multi-source fetch.
    Returns dict with: price, change_24h, volume_24h, sources, agreement.
    Raises ValueError on insufficient data."""
    now = time.time()
    fresh = [r for r in results if (now - r["ts"]) <= max_age]
    if not fresh:
        raise ValueError("No fresh data from any source")

    prices = [r["price"] for r in fresh]
    median_p = statistics.median(prices)
    spread = (max(prices) - min(prices)) / median_p * 100

    if spread > 3.0:
        # Try excluding outliers
        sorted_p = sorted(prices)
        trimmed = sorted_p[1:-1] if len(sorted_p) >= 3 else sorted_p
        if trimmed:
            median_p = statistics.median(trimmed)
            spread = (max(trimmed) - min(trimmed)) / median_p * 100

    # Use median change/volume from fresh sources
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


def fetch_with_retry(coin_id="bitcoin", symbol="BTCUSDT",
                     max_attempts=3, retry_delay=900):
    """Fetch with retry logic. Returns verified dict or raises."""
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            results = fetch_price_parallel(coin_id, symbol)
            verified = consensus_price(results)
            if verified["sources"]:
                logger.info(
                    f"attempt {attempt}: {verified['agreement']} "
                    f"price=${verified['price']:,.2f} "
                    f"sources={verified['sources']} "
                    f"spread={verified['spread_pct']}%"
                )
                return verified
            raise ValueError("No sources returned data")
        except Exception as e:
            last_err = e
            logger.warning(f"attempt {attempt} failed: {e}")
            if attempt < max_attempts:
                logger.info(f"retry in {retry_delay}s")
                time.sleep(retry_delay)
    raise RuntimeError(f"All {max_attempts} attempts failed: {last_err}")


if __name__ == "__main__":
    import sys
    cid = sys.argv[1] if len(sys.argv) > 1 else "bitcoin"
    sym = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
    print(json.dumps(fetch_with_retry(cid, sym), indent=2))
