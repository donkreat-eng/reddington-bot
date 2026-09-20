"""Price fetching with multi-source consensus and retry logic."""
import json
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from statistics import median

import requests

from lib.common import setup_logger, log

logger = setup_logger("fetch")

# Per-source throttling — avoid 429 from public APIs
_LAST = {}
_MIN_DELAY = {
    "coingecko": 2.0,
    "binance": 0.2,
    "kraken": 0.5,
    "coinbase": 0.5,
    "paprika": 1.0,
}
_session = requests.Session()
_session.headers.update({"User-Agent": "reddington-bot/1.0"})


def _throttle(name):
    min_delay = _MIN_DELAY.get(name, 0.3)
    last = _LAST.get(name, 0)
    elapsed = time.time() - last
    if elapsed < min_delay:
        time.sleep(min_delay - elapsed)
    _LAST[name] = time.time()


# --- Source: CoinGecko ---
def _from_coingecko(coin_id, symbol):
    try:
        _throttle("coingecko")
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {"ids": coin_id, "vs_currencies": "usd", "include_24hr_change": "true",
                  "include_24hr_vol": "true", "include_market_cap": "true"}
        r = _session.get(url, params=params, timeout=10)
        if r.status_code == 429:
            return None
        r.raise_for_status()
        data = r.json().get(coin_id, {})
        if "usd" not in data:
            return None
        return {
            "source": "coingecko",
            "price": float(data["usd"]),
            "change_24h": float(data.get("usd_24h_change", 0)),
            "volume_24h": float(data.get("usd_24h_vol", 0)),
            "market_cap": float(data.get("usd_market_cap", 0)),
        }
    except Exception as e:
        logger.warning(f"coingecko {coin_id} fail: {e}")
        return None


# --- Source: Binance ---
def _from_binance(coin_id, symbol):
    try:
        _throttle("binance")
        url = "https://api.binance.com/api/v3/ticker/24hr"
        # symbol уже приходит с USDT (например "BTCUSDT"), Binance его и ждёт
        params = {"symbol": symbol}
        r = _session.get(url, params=params, timeout=10)
        if r.status_code == 429:
            return None
        r.raise_for_status()
        data = r.json()
        return {
            "source": "binance",
            "price": float(data["lastPrice"]),
            "change_24h": float(data["priceChangePercent"]),
            "volume_24h": float(data["quoteVolume"]),
        }
    except Exception as e:
        logger.warning(f"binance {symbol} fail: {e}")
        return None


# --- Source: Kraken ---
def _from_kraken(coin_id, symbol):
    try:
        _throttle("kraken")
        kraken_pairs = {"BTC": "XBT", "DOGE": "XDG"}
        # strip USDT/USD → получить base ("BTCUSDT" → "BTC" → "XBT")
        base_sym = symbol.replace("USDT", "").replace("USDC", "").replace("USD", "")
        base = kraken_pairs.get(base_sym, base_sym)
        url = "https://api.kraken.com/0/public/Ticker"
        params = {"pair": f"{base}USD"}
        r = _session.get(url, params=params, timeout=10)
        r.raise_for_status()
        result = r.json().get("result", {})
        if not result:
            return None
        key = list(result.keys())[0]
        t = result[key]
        return {
            "source": "kraken",
            "price": float(t["c"][0]),
            "change_24h": float(t.get("p", [0, 0])[0]) / float(t["o"]) * 100 if "o" in t else 0,
            "volume_24h": float(t.get("v", [0, 0])[1]),
        }
    except Exception as e:
        logger.warning(f"kraken {symbol} fail: {e}")
        return None


# --- Source: Coinbase ---
def _from_coinbase(coin_id, symbol):
    try:
        _throttle("coinbase")
        # symbol = "BTCUSDT" → strip USDT → "BTC" → URL = "BTC-USD" (200)
        base_sym = symbol.replace("USDT", "").replace("USDC", "").replace("USD", "")
        url = "https://api.coinbase.com/v2/prices/{}-USD/spot".format(base_sym)
        r = _session.get(url, timeout=10)
        r.raise_for_status()
        price = float(r.json()["data"]["amount"])
        # Coinbase spot endpoint doesn't return 24h change; estimate 0
        return {
            "source": "coinbase",
            "price": price,
            "change_24h": 0.0,
            "volume_24h": 0.0,
        }
    except Exception as e:
        logger.warning(f"coinbase {symbol} fail: {e}")
        return None


# --- Parallel fetch ---
SOURCE_MAP = {
    "coingecko": _from_coingecko,
    "binance": _from_binance,
    "kraken": _from_kraken,
    "coinbase": _from_coinbase,
}


def fetch_price_parallel(coin_id, symbol):
    """Fetch price from all sources in parallel. Return list of source results."""
    results = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fn, coin_id, symbol): name for name, fn in SOURCE_MAP.items()}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                results.append(res)
    return results


def consensus_price(results):
    """Build a consensus: median price, agreement label, spread."""
    if not results:
        return None
    trimmed = [r for r in results if r.get("price")]
    if not trimmed:
        return None
    prices = [r["price"] for r in trimmed]
    median_p = median(prices)
    spread = (max(prices) - min(prices)) / median_p * 100

    # Use median change/volume from fresh sources
    changes = [r.get("change_24h", 0) for r in trimmed if "change_24h" in r]
    vols = [r.get("volume_24h", 0) for r in trimmed if "volume_24h" in r]

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
        "sources": [r["source"] for r in trimmed],
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
