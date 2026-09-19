OINGECKO_IDS[ticker], days=days)
    log("market_overview", f"ohlc {ticker} source=coingecko n={len(candles)}")
    return candles


def fetch_crypto_positional(ticker: str) -> dict:
    """OI, funding, L/S ratio. Binance → Bybit fallback for OI+fee; L/S only Binance."""
    symbol = BINANCE_SYMBOL[ticker]
    out: dict = {}

    def _bin(path: str, params: dict, timeout: int = 10):
        return _http_json(f"{BINANCE_BASE}{path}", params, timeout=timeout)

    # ─── Open interest ───
    try:
        oi = _bin("/futures/data/openInterestHist",
                  {"symbol": symbol, "period": "1h", "limit": 1})
        out["oi_usdt"] = float(oi[0]["sumOpenInterestValue"]) if oi else 0.0
    except Exception as e:
        log("market_overview",
            f"OI {ticker} Binance FAIL: {e}; trying Bybit")
        try:
            ex = _bybit_extras(symbol)
            out["oi_usdt"] = ex["oi_usdt"]
        except Exception as e2:
            log("market_overview", f"OI {ticker} Bybit FAIL: {e2}")
            out["oi_usdt"] = None

    # ─── Funding rate ───
    try:
        fr = _bin("/fapi/v1/premiumIndex", {"symbol": symbol})
        out["funding"] = float(fr["lastFundingRate"]) * 100
    except Exception as e:
        log("market_overview",
            f"funding {ticker} Binance FAIL: {e}; trying Bybit")
        try:
            ex = _bybit_extras(symbol)
            out["funding"] = ex["funding"]
        except Exception as e2:
            log("market_overview", f"funding {ticker} Bybit FAIL: {e2}")
            out["funding"] = None

    # ─── Long/Short ratio (Binance-only — Bybit V5 requires auth for this) ───
    try:
        ls = _bin("/futures/data/globalLongShortAccountRatio",
                  {"symbol": symbol, "period": "1h", "limit": 1})
        out["long_short"] = float(ls[0]["longShortRatio"]) if ls else None
    except Exception as e:
        log("market_overview", f"L/S {ticker} Binance FAIL: {e}")
        out["long_short"] = None

    return out


def fetch_metal(name: str) -> dict:
    """Fetch metal price from Yahoo Finance (public endpoint)."""
    symbol = METAL_SYMBOLS[name]
    headers = {"User-Agent": "Mozilla/5.0"}
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    data = requests.get(url, params={"interval": "1d", "range": "5d"},
                        headers=headers, timeout=10).json()
    res = data["chart"]["result"][0]
    meta = res["meta"]
    price = meta["regularMarketPrice"]
    prev = meta.get("chartPreviousClose", meta.get("previousClose", price))
    return {
        "name": name,
        "price": float(price),
        "change_pct": (float(price) - float(prev)) / float(prev) * 100,
    }


def fmt_oi(oi: float | None) -> str:
    if oi is None:
        return "n/a"
    if oi > 1e9:
        return f"${oi / 1e9:.2f}B"
    if oi > 1e6:
        return f"${oi / 1e6:.1f}M"
    return f"${oi:,.0f}"


def fmt_funding(f: float | None) -> str:
    if f is