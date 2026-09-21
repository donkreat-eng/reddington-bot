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

TIMEOUT = 10
_LAST = {}
USER_AGENT = "Mozilla/5.0 (compatible; ReddingtonBot/1.0)"


def _throttle(name, min_delay=0.5):
    last = _LAST.get(name, 0)
    now = time.time()
    if now - last < min_delay:
        time.sleep(min_delay - (now - last))
    _LAST[name] = time.time()


def _http(url, headers=None, timeout=TIMEOUT, throttle_name=None, min_delay=0.5):
    if throttle_name:
        _throttle(throttle_name, min_delay)
    req = Request(url)
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Accept", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            if not body:
                return None
            return json.loads(body)
    except (URLError, TimeoutError, json.JSONDecodeError) as e:
        logger.warning(f"http fail {url[:80]}: {e}")
        return None
    except Exception as e:
        logger.warning(f"http error {url[:80]}: {e}")
        return None


# === Source 1: CoinGecko ===
def fetch_coingecko(coin_id="bitcoin", vs="usd"):
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies={vs}&include_24hr_change=true&include_24hr_vol=true&include_market_cap=true"
    data = _http(url, throttle_name="coingecko", min_delay=2.0)
    if not data or coin_id not in data:
        return None
    d = data[coin_id]
    return {
        "source": "coingecko",
        "price": float(d.get(vs, 0)),
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
        "market_cap": 0,
        "ts": time.time(),
    }


# === Source 3: CoinPaprika ===
def fetch_paprika(symbol="btc-bitcoin", vs="usd"):
    url = f"https://api.coinpaprika.com/v1/tickers/{symbol}?quotes={vs}"
    data = _http(url, throttle_name="paprika", min_delay=1.0)
    if not data or "quotes" not in data:
        return None
    q = data["quotes"].get(vs, {})
    return {
        "source": "paprika",
        "price": float(q.get("price", 0)),
        "change_24h": float(q.get("percent_change_24h", 0)),
        "volume_24h": float(q.get("volume_24h", 0)),
        "market_cap": float(q.get("market_cap", 0)),
        "ts": time.time(),
    }


# === Source 4: Kraken ===
def fetch_kraken(symbol="BTCUSD"):
    pair = symbol
    if pair.endswith("USDT"):
        pair = pair[:-1]  # BTCUSDT -> BTCUSD (Kraken uses USD not USDT)
    url = f"https://api.kraken.com/0/public/Ticker?pair={pair}"
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
    cap = (data.get("market_data") or {}).get("market_cap", {}).get("usd")
    return float(cap) if cap else None


def _coingecko_global():
    """Fetch CoinGecko /global total market cap (USD). Returns float or None."""
    url = "https://api.coingecko.com/api/v3/global"
    data = _http(url, throttle_name="coingecko", min_delay=2.0)
    if not data or "data" not in data:
        return None
    total = (data.get("data") or {}).get("total_market_cap", {}).get("usd")
    return float(total) if total else None


def fetch_crypto_detail(coin_id="bitcoin"):
    """Return {"market_cap": float|None, "dominance": float|None} for coin_id.

    dominance is market_cap / total_market_cap * 100 (in %).
    Returns dict with None values if CoinGecko is unavailable.
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


def consensus_price(results):
    """Median across sources; flag spread."""
    prices = [r["price"] for r in results.values() if r.get("price")]
    if not prices:
        return {"price": 0, "sources": [], "spread_pct": 0, "agreement": "no_data"}
    med = statistics.median(prices)
    if len(prices) >= 2:
        spread = (max(prices) - min(prices)) / med * 100
    else:
        spread = 0
    return {
        "price": round(med, 2),
        "sources": list(results.keys()),
        "spread_pct": round(spread, 3),
        "agreement": (
            "tight" if spread < 0.5 else
            "ok" if spread < 2 else
            "wide"
        ),
        "raw": results,
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
    detail = fetch_crypto_detail(cid)
    print(f"detail: {detail}")
    verified = fetch_with_retry(cid, sym, max_attempts=1, retry_delay=0)
    print(f"verified: {verified['price']} via {verified['sources']}")