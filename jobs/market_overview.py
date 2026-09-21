"""13:30 YEKT market overview — BTC/ETH/BNB/SOL/XRP + Gold/Silver.

Источники (auto-fallback):
  price    Binance→CoinGecko→Kraken→Coinbase       (4 параллели, медиана + spread)
  change   Binance→Bybit→OKX→CoinGecko
  OI       Binance→Bybit→OKX (mark × qty)
  funding  Binance→Bybit→OKX
  L/S      Binance→OKX (long-short-account-ratio)
  OHLC     Binance→Kraken→CoinGecko
  metals   Yahoo → api.gold-api.com

Cron: 30 8 * * 1-5 UTC = 13:30 YEKT пн–пт
"""