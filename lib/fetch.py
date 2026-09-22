"""Multi-source price fetching with median + retry."""
import json
import time
import urllib.request
import urllib.error
import random
import logging
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger("fetch")

# === Throttling ===
_LAST = {}


def _throttle(name, min_delay=0.5):
    """Block until at least min_delay seconds passed since last call with this name."""
    now = time.time()
    last = _LAST.get(name, 0)
    wait = last + min_delay - now
    if wait > 0:
        time.sleep(wait)
    _LAST[name] = time.time()


# === HTTP layer ===
TIMEOUT = 15


def _http(url, headers=None, timeout=TIMEOUT, throttle_name=None, min_delay=0.5):
    """GET URL → parsed JSON or None on error. Logs WARNING on failure."""
    if throttle_name:
        _throttle(throttle_name, min_delay=min_delay)
    req = urllib.request.Request(url, headers=headers or {
        "User-Agent": "ReddingtonBot/1.0",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as e:
        log.warning("fetch failed: %s → HTTP %s: %s", url, e.code, e.reason)
        return None
    except urllib.error.URLError as e:
        log.warning("fetch failed: %s → %s", url, e.reason)
        return None
    except Exception as e:
        log.warning("fetch failed: %s → %s: %s", url, type(e).__name__, e)
        return None


# === Per-source price fetchers ===
def fetch_coingecko(coin_id="bitcoin", vs="usd"):
    url = (f"https://api.coingecko.com/api/v3/simple/price"
           f"?ids={coin_id}&vs_currencies={vs}&include_24hr_change=true&include_24hr_vol=true")
    data = _http(url, throttle_name="coingecko", min_delay=2.0)
    if data and coin_id in data:
        d = data[coin_id]
        return {
            "price": float(d.get(vs, 0) or 0),
            "change_24h": float(d.get(f"{vs}_24h_change", 0) or 0),
            "volume_24h": float(d.get(f"{vs}_24h_vol", 0) or 0),
            "source": "coingecko",
        }
    return None


def fetch_binance(symbol="BTCUSDT"):
    url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
    data = _http(url, throttle_name="binance", min_delay=0.2)
    if data and isinstance(data, dict):
        return {
            "price": float(data.get("lastPrice", 0) or 0),
            "change_24h": float(data.get("priceChangePercent", 0) or 0),
            "volume_24h": float(data.get("quoteVolume", 0) or 0),
            "source": "binance",
        }
    return None


def fetch_kraken(symbol="BTCUSDT"):
    """Fetch via Kraken. symbol like BTCUSDT → translated to Kraken pair XXBTZUSD."""
    pair_map = {
        "BTCUSDT": "XXBTZUSD",
        "ETHUSDT": "XETHZUSD",
        "SOLUSDT": "SOLUSD",
        "XRPUSDT": "XXRPZUSD",
        "ADAUSDT": "ADAUSD",
        "AVAXUSDT": "AVAXUSD",
        "DOTUSDT": "DOTUSD",
        "LINKUSDT": "LINKUSD",
    }
    pair = pair_map.get(symbol, "XXBTZUSD")
    url = f"https://api.kraken.com/0/public/Ticker?pair={pair}"
    data = _http(url, throttle_name="kraken", min_delay=1.0)
    if data and isinstance(data, dict) and "result" in data:
        for key, val in data["result"].items():
            if key != "last" and isinstance(val, dict):
                # Kraken: c = last trade (price, lot volume)
                last = val.get("c", ["0"])[0]
                # O = today's open price
                open_ = val.get("o", "0")
                try:
                    last_f = float(last)
                    open_f = float(open_)
                    change = (last_f - open_f) / open_f * 100 if open_f else 0
                except (ValueError, ZeroDivisionError):
                    return None
                return {
                    "price": last_f,
                    "change_24h": change,
                    "volume_24h": float(val.get("v", [0, 0])[1] or 0),
                    "source": "kraken",
                }
    return None


def fetch_coinbase(symbol="BTCUSDT"):
    """Fetch via Coinbase. symbol like BTCUSDT → translated to BTC-USD."""
    product_map = {
        "BTCUSDT": "BTC-USD",
        "ETHUSDT": "ETH-USD",
        "SOLUSDT": "SOL-USD",
        "XRPUSDT": "XRP-USD",
        "ADAUSDT": "ADA-USD",
        "AVAXUSDT": "AVAX-USD",
        "DOTUSDT": "DOT-USD",
        "LINKUSDT": "LINK-USD",
    }
    product = product_map.get(symbol, "BTC-USD")
    url = f"https://api.coinbase.com/v2/prices/{product}/spot"
    data = _http(url, throttle_name="coinbase", min_delay=0.5)
    if data and isinstance(data, dict) and "data" in data:
        try:
            price = float(data["data"].get("amount", 0))
        except (ValueError, TypeError):
            return None
        return {
            "price": price,
            "change_24h": 0,
            "volume_24h": 0,
            "source": "coinbase",
        }
    return None


# === OHLC ===
def fetch_ohlc(coin_id="bitcoin", vs="usd", days=7):
    """Fetch OHLC candles. Try Binance klines -> Kraken OHLC -> CoinGecko OHLC."""
    symbol_map = {
        "bitcoin": ("BTCUSDT", "XXBTZUSD"),
        "ethereum": ("ETHUSDT", "XETHZUSD"),
        "solana": ("SOLUSDT", "SOLUSD"),
        "binancecoin": ("BNBUSDT", None),
        "ripple": ("XRPUSDT", "XXRPZUSD"),
        "cardano": ("ADAUSDT", "ADAUSD"),
        "avalanche-2": ("AVAXUSDT", "AVAXUSD"),
        "polkadot": ("DOTUSDT", "DOTUSD"),
        "chainlink": ("LINKUSDT", "LINKUSD"),
        "toncoin": ("TONUSDT", None),
        "sui": ("SUIUSDT", None),
    }
    binance_sym, kraken_pair = symbol_map.get(coin_id, ("BTCUSDT", None))
    interval = "60"  # Kraken interval in minutes (60 = 1h)
    limit = days * 24 if days <= 30 else 500

    # Try 1: Binance klines (always 451 from runners - but kept for completeness)
    url = (f"https://api.binance.com/api/v3/klines"
           f"?symbol={binance_sym}&interval=1h&limit={limit}")
    data = _http(url, throttle_name="binance", min_delay=0.3)
    if data and isinstance(data, list) and len(data) > 0:
        ohlc = []
        for row in data:
            ohlc.append([row[0], float(row[1]), float(row[2]), float(row[3]), float(row[4])])
        return ohlc

    # Try 2: Kraken OHLC (works from GitHub runners, no geo-block)
    if kraken_pair:
        url = f"https://api.kraken.com/0/public/OHLC?pair={kraken_pair}&interval={interval}"
        resp = _http(url, throttle_name="kraken", min_delay=1.0)
        if resp and isinstance(resp, dict) and "result" in resp:
            candles = None
            for key, val in resp["result"].items():
                if key != "last" and isinstance(val, list) and len(val) > 0:
                    candles = val
                    break
            if candles:
                # Kraken: [time, open, high, low, close, vwap, volume, count]
                ohlc = []
                for row in candles[-limit:]:
                    ohlc.append([row[0] * 1000, float(row[1]), float(row[2]), float(row[3]), float(row[4])])
                return ohlc

    # Try 3: CoinGecko OHLC (last resort, rate-limited)
    url = (f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc"
           f"?vs_currency={vs}&days={days}")
    data = _http(url, throttle_name="coingecko", min_delay=2.0)
    if data and isinstance(data, list) and len(data) > 0:
        # CoinGecko: [timestamp, open, high, low, close]
        return [[r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4])]
                for r in data]
    return []


# === Crypto detail (market cap + dominance) ===
def _coingecko_coin(coin_id="bitcoin"):
    """Fetch CoinGecko /coins/{id} market data. Returns market_cap (USD) or None."""
    url = (f"https://api.coingecko.com/api/v3/coins/{coin_id}"
           f"?localization=false&tickers=false&community_data=false"
           f"&developer_data=false&sparkline=false")
    data = _http(url, throttle_name="coingecko", min_delay=2.0)
    if data and isinstance(data, dict):
        mcap = data.get("market_data", {}).get("market_cap", {}).get("usd")
        if isinstance(mcap, (int, float)):
            return {"market_cap": float(mcap)}
    return None


def _coingecko_global():
    """Fetch CoinGecko /global total market cap. Returns total (USD) or None."""
    data = _http("https://api.coingecko.com/api/v3/global", throttle_name="coingecko", min_delay=2.0)
    if data and isinstance(data, dict) and "data" in data:
        total = data["data"].get("total_market_cap", {}).get("usd")
        if isinstance(total, (int, float)):
            return {"total": float(total)}
    return None


def fetch_crypto_detail(coin_id="bitcoin"):
    """Return {market_cap: float, dominance: float} or {0, None} on failure."""
    coin_data = {"market_cap": None}
    total_data = {"total": None}
    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(_coingecko_coin, coin_id)
        f2 = ex.submit(_coingecko_global)
        c1 = f1.result()
        c2 = f2.result()
        if c1:
            coin_data.update(c1)
        if c2:
            total_data.update(c2)
    cap = coin_data.get("market_cap")
    total = total_data.get("total")
    # Type-guard: protect against dict/int comparison
    if (not isinstance(cap, (int, float)) or cap is None or cap == 0
            or not isinstance(total, (int, float)) or total is None or total == 0):
        dominance = None
    else:
        dominance = cap / total * 100
    return {"market_cap": cap or 0, "dominance": dominance}


# === Parallel price + consensus ===
def fetch_price_parallel(coin_id, symbol, sources=("coingecko", "kraken", "coinbase")):
    """Fetch price in parallel from N sources. Returns list of {price,change_24h,volume_24h,source}."""
    fetchers = {
        "coingecko": lambda: fetch_coingecko(coin_id, "usd"),
        "kraken": lambda: fetch_kraken(symbol),
        "coinbase": lambda: fetch_coinbase(symbol),
        "binance": lambda: fetch_binance(symbol),
    }
    results = [None] * len(sources)
    with ThreadPoolExecutor(max_workers=len(sources)) as ex:
        futs = {ex.submit(fetchers[s]): i for i, s in enumerate(sources) if s in fetchers}
        for fut in futs:
            try:
                results[futs[fut]] = fut.result()
            except Exception:
                pass
    return [r for r in results if r]


def consensus_price(coin_id, symbol, min_sources=2, max_spread_pct=2.0):
    """Fetch via parallel sources, return consensus {price, change_24h, volume_24h, sources, spread_pct, agreement, raw}."""
    import statistics
    raw = fetch_price_parallel(coin_id, symbol)
    if len(raw) < min_sources:
        raise RuntimeError(f"No fresh data from any source (got {len(raw)})")
    prices = [r["price"] for r in raw if r["price"] > 0]
    if len(prices) < min_sources:
        raise RuntimeError("Too few valid prices")
    median_p = statistics.median(prices)
    spread = (max(prices) - min(prices)) / median_p * 100 if median_p else 0
    # Median change/volume across sources
    changes = [r["change_24h"] for r in raw if r.get("change_24h") is not None]
    volumes = [r["volume_24h"] for r in raw if r.get("volume_24h")]
    median_change = statistics.median(changes) if changes else None
    median_vol = statistics.median(volumes) if volumes else None
    return {
        "price": median_p,
        "change_24h": median_change,
        "volume_24h": median_vol,
        "sources": [r["source"] for r in raw],
        "spread_pct": spread,
        "agreement": "high" if spread < 0.5 else ("medium" if spread < max_spread_pct else "low"),
        "raw": raw,
    }


# === Retry wrapper ===
def fetch_with_retry(coin_id, symbol, attempts=2, retry_delay=10):
    """Try consensus_price up to N times."""
    last_err = None
    for i in range(1, attempts + 1):
        try:
            return consensus_price(coin_id, symbol)
        except Exception as e:
            last_err = e
            log.warning("attempt %d failed: %s", i, e)
            if i < attempts:
                time.sleep(retry_delay)
    raise RuntimeError(f"All {attempts} attempts failed: {last_err}")
