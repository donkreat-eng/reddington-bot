"""Multi-source price fetching with median + retry."""
import json
import time
import random
import urllib.request
import urllib.error
import urllib.parse
import statistics
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("fetch")
TIMEOUT = 12
MAX_AGE_SEC = 120
_LAST = {}

# Per-source rate limit (seconds)
_MIN_DELAY = {
    "coingecko": 1.5,
    "binance": 0.5,
    "kraken": 1.0,
    "coinbase": 0.5,
    "coingecko_global": 2.0,
    "coingecko_coin": 2.0,
}


def _throttle(name, min_delay=0.5):
    last = _LAST.get(name, 0)
    elapsed = time.time() - last
    if elapsed < min_delay:
        time.sleep(min_delay - elapsed)
    _LAST[name] = time.time()


def _http(url, headers=None, timeout=TIMEOUT, throttle_name=None, min_delay=0.5):
    """HTTP GET with throttle + error handling."""
    if throttle_name:
        _throttle(throttle_name, min_delay=min_delay)
    else:
        time.sleep(0.1)
    req = urllib.request.Request(url, headers={"User-Agent": "reddington-bot/1.0", "Accept": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        if e.code == 429:
            logger.warning(f"http 429 rate limit: {url[:80]}")
        elif e.code == 451:
            logger.warning(f"http 451 geo-blocked: {url[:80]}")
        else:
            logger.warning(f"http {e.code}: {url[:80]}")
        return None
    except Exception as e:
        logger.warning(f"http error: {e}")
        return None


# === Source 1: CoinGecko ===
def fetch_coingecko(coin_id="bitcoin", vs="usd"):
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies={vs}&include_24hr_change=true"
    d = _http(url, throttle_name="coingecko", min_delay=1.5)
    if not d or coin_id not in d:
        return None
    block = d[coin_id]
    return {
        "source": "coingecko",
        "price": float(block.get(vs, 0)),
        "change_24h": float(block.get(f"{vs}_24h_change", 0)),
        "volume_24h": 0,
        "ts": time.time(),
    }


# === Source 2: Binance ===
def fetch_binance(symbol="BTCUSDT"):
    url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
    data = _http(url, throttle_name="binance", min_delay=0.5)
    if not data:
        return None
    return {
        "source": "binance",
        "price": float(data.get("lastPrice", 0)),
        "change_24h": float(data.get("priceChangePercent", 0)),
        "volume_24h": float(data.get("quoteVolume", 0)),
        "ts": time.time(),
    }


# === Source 3: Kraken ===
def fetch_kraken(symbol="BTCUSD"):
    url = f"https://api.kraken.com/0/public/Ticker?pair={symbol}"
    data = _http(url, throttle_name="kraken", min_delay=1.0)
    if not data or "result" not in data or not data["result"]:
        return None
    pair_key = list(data["result"].keys())[0]
    pair_data = data["result"][pair_key]
    last = float(pair_data.get("c", ["0"])[0])
    open_p = float(pair_data.get("o", 0))
    change_pct = ((last - open_p) / open_p * 100) if open_p > 0 else 0
    return {
        "source": "kraken",
        "price": last,
        "change_24h": change_pct,
        "volume_24h": float(pair_data.get("v", [0])[1]),
        "ts": time.time(),
    }


# === Source 4: Coinbase ===
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


# === OHLC fetcher ===
def fetch_ohlc(coin_id="bitcoin", vs="usd", days=7):
    """Fetch OHLC candles. Prefer Binance klines (more reliable than CoinGecko OHLC)."""
    symbol_map = {
        "bitcoin": "BTCUSDT",
        "ethereum": "ETHUSDT",
        "solana": "SOLUSDT",
        "binancecoin": "BNBUSDT",
        "ripple": "XRPUSDT",
    }
    symbol = symbol_map.get(coin_id, f"{coin_id.upper()}USDT")
    interval = "1d" if days and days >= 1 else "1h"
    limit = (days * 24) if interval == "1h" else min(days, 1000)

    # Try Binance first
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    data = _http(url, throttle_name="binance", min_delay=0.5)
    if data and isinstance(data, list) and len(data) > 0:
        ohlc = []
        for k in data:
            ohlc.append({
                "ts": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
            })
        return ohlc

    # Fallback: CoinGecko OHLC
    cg_id = {"bitcoin": "bitcoin", "ethereum": "ethereum"}.get(coin_id, coin_id)
    cg_url = f"https://api.coingecko.com/api/v3/coins/{cg_id}/ohlc?vs_currency={vs}&days={days}"
    cg_data = _http(cg_url, throttle_name="coingecko", min_delay=2.0)
    if cg_data and isinstance(cg_data, list):
        ohlc = []
        for k in cg_data:
            ohlc.append({
                "ts": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
            })
        return ohlc

    return None


# === Crypto detail: market cap + dominance ===
def _coingecko_coin(coin_id="bitcoin", vs="usd"):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false&tickers=false&community_data=false&developer_data=false"
    d = _http(url, throttle_name="coingecko_coin", min_delay=2.0)
    if not d or "market_data" not in d:
        return None
    cap = (d["market_data"].get("market_cap") or {}).get(vs)
    return {"market_cap": cap}


def _coingecko_global(vs="usd"):
    url = "https://api.coingecko.com/api/v3/global"
    d = _http(url, throttle_name="coingecko_global", min_delay=2.0)
    if not d or "data" not in d:
        return None
    total = (d["data"].get("total_market_cap") or {}).get(vs)
    return {"total": total}


def fetch_crypto_detail(coin_id="bitcoin", vs="usd"):
    """Fetch market_cap + dominance for a coin via CoinGecko (parallel)."""
    cap = None
    total = None
    try:
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_cap = ex.submit(_coingecko_coin, coin_id, vs)
            f_total = ex.submit(_coingecko_global, vs)
            try:
                r = f_cap.result(timeout=10)
                if r:
                    cap = r["market_cap"]
            except Exception as e:
                logger.warning(f"coingecko coin fetch failed: {e}")
            try:
                total = f_total.result(timeout=10)
            except Exception as e:
                logger.warning(f"coingecko global fetch failed: {e}")
    except Exception as e:
        logger.warning(f"fetch_crypto_detail pool failed: {e}")

    dominance = None
    if cap and total and total > 0:
        dominance = round(cap / total * 100, 2)

    return {"market_cap": cap, "dominance": dominance}


# === Multi-source parallel fetch ===
def fetch_price_parallel(coin_id="bitcoin", symbol="BTCUSDT"):
    """Fetch from all available sources in parallel."""
    sources = [
        ("coingecko", fetch_coingecko, (coin_id, "usd"), {}),
        ("binance", fetch_binance, (symbol,), {}),
        ("kraken", fetch_kraken, (symbol,), {}),
        ("coinbase", fetch_coinbase, (symbol.replace("USDT", "-USD").replace("BTC", "BTC"),), {}),
    ]
    results = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {}
        for name, fn, args, kwargs in sources:
            try:
                f = ex.submit(fn, *args, **kwargs)
                futures[f] = name
            except Exception as e:
                logger.warning(f"submit {name} failed: {e}")
        for f in as_completed(futures):
            name = futures[f]
            try:
                r = f.result(timeout=8)
                if r and r.get("price"):
                    results[name] = r
            except Exception as e:
                logger.warning(f"{name} failed: {e}")
    return results


def consensus_price(results, max_age=MAX_AGE_SEC):
    """Return verified price from multi-source fetch.
    Returns dict with: price, change_24h, volume_24h, sources, agreement.
    Raises ValueError on insufficient data."""
    now = time.time()
    fresh = [r for r in results.values() if (now - r["ts"]) <= max_age]
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
    changes = [r.get("change_24h", 0) for r in fresh if "change_24h" in r and r.get("change_24h") is not None]
    vols = [r.get("volume_24h", 0) for r in fresh if r.get("volume_24h")]

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