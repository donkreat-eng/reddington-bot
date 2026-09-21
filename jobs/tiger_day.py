"""Tiger Day job — runs at 14:45 YEKT (token of the day with deep dive)."""
import sys
import os
import json
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.common import setup_logger, ye_now, ye_str
from lib.fetch import fetch_with_retry, fetch_ohlc
from lib.chart import generate_chart
from lib.publish import post_pair
from lib.post import tiger_caption, tiger_body

logger = setup_logger("tiger_day")

# Pool of "interesting" tokens that get deep-dive treatment
TIGER_POOL = [
    {"coin_id": "solana",        "ticker": "SOL",  "narrative": "L1 / high throughput"},
    {"coin_id": "arbitrum",      "ticker": "ARB",  "narrative": "L2 / DeFi hub"},
    {"coin_id": "sui",           "ticker": "SUI",  "narrative": "L1 / Move language"},
    {"coin_id": "render-token",  "ticker": "RNDR", "narrative": "DePIN / GPU rendering"},
    {"coin_id": "injective-protocol", "ticker": "INJ", "narrative": "DeFi / orderbook"},
    {"coin_id": "the-graph",     "ticker": "GRT",  "narrative": "infra / indexer"},
    {"coin_id": "fetch-ai",      "ticker": "FET",  "narrative": "AI / agents"},
    {"coin_id": "chainlink",     "ticker": "LINK", "narrative": "oracle / real-world data"},
]


def pick_tiger():
    """Deterministic token-of-the-day (same token all day, rotates weekly)."""
    return TIGER_POOL[datetime.now().weekday() % len(TIGER_POOL)]


def get_news(ticker):
    """Stub — returns canned headlines. Replace with real RSS in v2."""
    return [
        f"{ticker}: on-chain активность растёт второй день подряд",
        f"По {ticker} открытый интерес вырос на 7% за сутки",
        f"Аналитики отмечают усиление позиций в {ticker}",
    ]


def build_data(tiger):
    """All data for the deep-dive."""
    ye = ye_now()
    coin = fetch_with_retry(tiger["coin_id"], tiger["ticker"] + "USDT", max_attempts=2, retry_delay=10)
    btc = fetch_with_retry("bitcoin", "BTCUSDT", max_attempts=2, retry_delay=10)
    return {
        "tiger": {**tiger, **coin},
        "btc": btc,
        "date_label": ye_str(fmt="%d %b %Y"),
        "news": get_news(tiger["ticker"]),
        "levels_note": "Уровни рассчитаны по 7-дневному ATR и ключевым скоплениям",
    }


def main():
    try:
        logger.info("=== TIGER DAY START ===")
        tiger = pick_tiger()
        logger.info(f"today's tiger: {tiger['ticker']}")
        data = build_data(tiger)

        ohlc = fetch_ohlc(tiger["coin_id"], "usd", days=7)
        if not ohlc:
            logger.error("no OHLC data")
            return

        chart_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "posts")
        os.makedirs(chart_dir, exist_ok=True)
        chart_path = os.path.join(chart_dir, f"tiger_day_{tiger['ticker'].replace('/', '_')}.png")
        generate_chart(ohlc, tiger["ticker"], data["tiger"]["support"], data["tiger"]["resistance"], chart_path)

        if not data.get("tiger", {}).get("price"):
            logger.error("tiger price missing — skipping post")
            return

        caption = tiger_caption(data)
        body = tiger_body(data)
        result = post_pair(chart_path, caption, body, job_name="tiger_day", min_age_minutes=60)
        logger.info(f"posted: {result}")
        logger.info("=== TIGER DAY END ===")
    except Exception as e:
        logger.error(f"FAILED: {e}")
        raise


if __name__ == "__main__":
    main()
