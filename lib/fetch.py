"""Multi-source price fetching with median + retry."""
import json
import time
import statistics
import random
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.common import setup_logger

logger = setup_logger("fetch")

TIMEOUT = 8
MAX_AGE_SEC = 180

# Throttle for free public APIs
_last_call = {}


def _throttle(name, min_delay=0.5):
    """Sleep so we don't hammer the same provider."""
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
        logger.warning(f"fetch failed: {url} → {e}")
        return None


# === Source 1: CoinGecko ===
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


# === Source 2: Binance ===
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


# === Source 3: CoinPaprika ===
def fetch_paprika(symbol="btc-bitcoin", vs="usd"):
    url = f"https://api.coinpaprika.com/v1/tickers/{symbol}?quotes={vs}"
    data = _http(url, throttle_name="paprika", min_delay=1.0)
    if not data or "quotes" not in data or vs not in data["quotes"]:
        return None
    q = data["quotes"][vs]
    return {
        "source": "paprika",
        "price": float(q["price"]),
        "change_24h": float(q.get("percent_change_24h", 0)),
        "volume_24h": float(q.get("volume_24h", 0)),
        "ts": time.time(),
    }


# === Source 4: Kraken ===
def fetch_kraken(symbol="BTCUSD"):
    """Kraken public ticker. symbol e.g. BTCUSD, ETHUSD, XAUUSD."""
    url = f"https://api.kraken.com/0/public/Ticker?pair={symbol}"
    data = _http(url, throttle_name="kraken", min_delay=0.5)
    if not data or "result" not in data or not data["result"]:
        return None
    pair_data = next(iter(data["result"].values()))
    return {
        "source": "kraken",
        "price": float(pair_data["c"][0]),
        "change_24h": float(pair_data.get("o", 0)) and
                      (float(pair_data["c"][0]) - float(pair_data["o"])) / float(pair_data["o"]) * 100,
        "volume_24h": float(pair_data.get("v", [0, 0])[1]),
        "ts": time.time(),
    }


# === Source 5: Coinbase ===
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


# === Crypto detail (market cap + dominance) ===
def _coingecko_coin(coin_id="bitcoin"):
    """Fetch CoinGecko /coins/{id} market data. Returns market_cap (USD) or None."""
    url = (f"https://api.coingecko.com/api/v3/coins/{coin_id}"
           f"?localization=false&tickers=false&community_data=false"
           f"&developer_data=false&sparkline=false")
    data = _http(url, throttle_name="coingecko", min_delay=2.0)
    if not data or "market_data" not in data:
        return None
    cap = data["market_data"].get("market_cap", {}).get("usd")
    return float(cap) if cap else None


def _coingecko_global():
    """Fetch CoinGecko /global total market cap. Returns total USD or None."""
    data = _http("https://api.coingecko.com/api/v3/global", throttle_name="coingecko", min_delay=2.0)
    if not data or "data" not in data:
        return None
    total = data["data"].get("total_market_cap", {}).get("usd")
    return float(total) if total else None


def fetch_crypto_detail(coin_id="bitcoin"):
    """Return {'market_cap': USD|None, 'dominance': %|None} for given coin.

    Uses parallel CoinGecko calls. On error returns zeros (downstream formatters
    print '—'). Falls back gracefully when CoinGecko rate-limits or 404s.
    """
    cap = None
    total = None
    try:
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_cap = ex.submit(_coingecko_coin, coin_id)
            f_total = ex.submit(_coingecko_global)
            try:
                cap = f_cap.result(timeout=10)
            except Exception as e:
                logger.warning(f"coingecko coin fetch failed: {e}")
            try:
                total = f_total.result(timeout=10)
            except Exception as e:
                logger.warning(f"coingecko global fetch failed: {e}")
    except Exception as e:
        logger.warning(f"fetch_crypto_detail pool failed: {e}")

    # CoinGecko may return an error dict on rate limit / 404 — guard types.
    if isinstance(cap, dict):
        cap = None
    if isinstance(total, dict):
        total = None

    dominance = None
    if cap and total and isinstance(total, (int, float)) and total > 0:
        dominance = round(cap / total * 100, 2)

    return {"market_cap": cap, "dominance": dominance}


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
        for fut in as_completed(futures, timeout=TIMEOUT + 2):
            name = futures[fut]
            try:
                r = fut.result()
                if r and isinstance(r, dict) and "price" in r:
                    results.append(r)
            except Exception:
                pass
    return results


def consensus_price(results, max_spread_pct=1.5):
    """Compute median price across sources. Reject if spread too wide.

    Returns dict with price/sources/spread_pct/change_24h (median)/volume_24h.
    """
    if not results:
        return None
    prices = [r["price"] for r in results]
    median = statistics.median(prices)
    spread = (max(prices) - min(prices)) / median * 100 if median else 0

    changes = [r.get("change_24h", 0) for r in results if isinstance(r.get("change_24h", 0), (int, float))]
    volumes = [r.get("volume_24h", 0) for r in results if isinstance(r.get("volume_24h", 0), (int, float))]
    change_24h = statistics.median(changes) if changes else 0
    volume_24h = statistics.median(volumes) if volumes else 0

    if spread > max_spread_pct:
        return {
            "price": median, "sources": [r["source"] for r in results],
            "spread_pct": round(spread, 3), "agreement": False,
            "change_24h": change_24h, "volume_24h": volume_24h,
            "raw": results,
        }

    return {
        "price": round(median, 2),
        "sources": [r["source"] for r in results],
        "spread_pct": round(spread, 3),
        "agreement": True,
        "change_24h": round(change_24h, 2),
        "volume_24h": round(volume_24h, 2),
        "raw": results,
    }


def fetch_with_retry(coin_id="bitcoin", symbol="BTCUSDT", attempts=2):
    """Try up to `attempts` rounds of parallel multi-source fetch."""
    last = None
    for i in range(1, attempts + 1):
        results = fetch_price_parallel(coin_id, symbol)
        verified = consensus_price(results)
        if verified and verified.get("agreement"):
            return verified
        last = verified
        if i < attempts:
            time.sleep(10)
    if last:
        # last attempt failed spread check — still return for caller to decide
        return last
    return None


if __name__ == "__main__":
    cid = sys.argv[1] if len(sys.argv) > 1 else "bitcoin"
    sym = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
    print(json.dumps(fetch_with_retry(cid, sym), indent=2))
