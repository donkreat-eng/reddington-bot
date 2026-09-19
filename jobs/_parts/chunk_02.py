 linear ticker (gives % change + turnover in USDT)
    try:
        ex = _bybit_extras(symbol)
        return {
            "change_24h": ex["change_24h"],
            "volume_24h": ex["turnover_24h"],
            "source": "bybit",
        }
    except Exception as e:
        log("market_overview",
            f"change {ticker} Bybit FAIL: {e}; trying CoinGecko")
    # 3. CoinGecko (often 429, last resort)
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


def _kraken_ohlc(symbol: str, minutes: int = 240, days: int = 7) -> list:
    pair = {"BTCUSDT": "XBTUSDT", "ETHUSDT": "ETHUSDT", "BNBUSDT": "BNBUSDT",
            "SOLUSDT": "SOLUSDT", "XRPUSDT": "XRPUSDT"}[symbol]
    data = _http_json(f"{KRAKEN_BASE}/OHLC", {"pair": pair, "interval": minutes})
    candles = data["result"][pair]
    cutoff = int(time.time()) - days * 86400
    out = []
    for c in candles:
        if c[0] >= cutoff:
            out.append((c[0] * 1000, float(c[1]), float(c[2]),
                        float(c[3]), float(c[4])))
    return out


def _coingecko_ohlc(coin_id: str, days: int = 7) -> list:
    data = _http_json(f"{COINGECKO_BASE}/coins/{coin_id}/ohlc",
                      {"vs_currency": "usd", "days": days})
    return [(c[0], float(c[1]), float(c[2]), float(c[3]), float(c[4]))
            for c in data]


def fetch_crypto_ohlc(ticker: str, days: int = 7) -> list:
    """4h OHLC for chart. Binance → Kraken → CoinGecko fallback."""
    symbol = BINANCE_SYMBOL[ticker]
    limit = days * 6  # 6 candles per day at 4h
    try:
        raw = _http_json(f"{BINANCE_BASE}/api/v3/klines",
                         {"symbol": symbol, "interval": "4h", "limit": limit})
        log("market_overview", f"ohlc {ticker} source=binance n={len(raw)}")
        return [(int(t[0]), float(t[1]), float(t[2]), float(t[3]), float(t[4]))
                for t in raw]
    except Exception as e:
        log("market_overview", f"ohlc {ticker} Binance FAIL: {e}; trying Kraken")
    try:
        candles = _kraken_ohlc(symbol, minutes=240, days=days)
        if candles:
            log("market_overview", f"ohlc {ticker} source=kraken n={len(candles)}")
            return candles
    except Exception as e:
        log("market_overview", f"ohlc {ticker} Kraken FAIL: {e}; trying CoinGecko")
    candles = _coingecko_ohlc(C