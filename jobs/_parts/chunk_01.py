ttp_json(f"{COINBASE_BASE}/prices/{coin}-USD/spot")
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


def fetch_crypto_change(ticker: str) -> dict:
    """24h change % + volume. Binance → Bybit → CoinGecko."""
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
    # 2. Bybit