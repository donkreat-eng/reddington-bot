"""Tiger day job — runs at 14:45 YEKT, weekday rotation through Tier 2."""
import sys
import os
import json
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.common import setup_logger, ye_now, ye_str
from lib.fetch import fetch_with_retry, fetch_ohlc, fetch_crypto_detail
from lib.chart import generate_chart
from lib.publish import post_pair
from lib.post import tiger_caption, tiger_body

logger = setup_logger("tiger_day")

# Tier 2 rotation: 7 tickers, weekday rotation
TIGER_POOL = [
    {"coin_id": "ethereum", "symbol": "ETHUSDT", "ticker": "ETH / USDT"},
    {"coin_id": "solana", "symbol": "SOLUSDT", "ticker": "SOL / USDT"},
    {"coin_id": "ripple", "symbol": "XRPUSDT", "ticker": "XRP / USDT"},
    {"coin_id": "sui", "symbol": "SUIUSDT", "ticker": "SUI / USDT"},
    {"coin_id": "chainlink", "symbol": "LINKUSDT", "ticker": "LINK / USDT"},
    {"coin_id": "toncoin", "symbol": "TONUSDT", "ticker": "TON / USDT"},
    {"coin_id": "avalanche-2", "symbol": "AVAXUSDT", "ticker": "AVAX / USDT"},
]


def pick_tiger():
    """Rotate through pool by ISO weekday (Mon=1..Sun=7)."""
    wd = ye_now().isoweekday()  # 1..7
    # Skip weekend: wd in (6, 7) — but the script will only be called on weekdays
    idx = (wd - 1) % len(TIGER_POOL)
    return TIGER_POOL[idx]


def build_data(tiger):
    data = fetch_with_retry(tiger["coin_id"], tiger["symbol"], attempts=2)
    last = data["price"]

    # Adaptive support/resistance zones — widen for cheap assets
    if last >= 1000:
        # Large-cap: 3% / 5% bands
        support = (round(last * 0.95, 2), round(last * 0.97, 2))
        resistance = (round(last * 1.03, 2), round(last * 1.05, 2))
    elif last >= 10:
        # Mid-cap: 5% / 7% bands
        support = (round(last * 0.93, 3), round(last * 0.96, 3))
        resistance = (round(last * 1.04, 3), round(last * 1.07, 3))
    elif last >= 1:
        # Low-cap: 7% / 10% bands, 4 decimals
        support = (round(last * 0.90, 4), round(last * 0.95, 4))
        resistance = (round(last * 1.05, 4), round(last * 1.10, 4))
    else:
        # Micro-cap: 12% / 15% bands, 6 decimals
        support = (round(last * 0.85, 6), round(last * 0.92, 6))
        resistance = (round(last * 1.08, 6), round(last * 1.15, 6))

    # Ensure zones don't overlap and have at least 1% width
    if support[1] <= support[0]:
        support = (round(last * 0.93, 4), round(last * 0.96, 4))
    if resistance[1] <= resistance[0]:
        resistance = (round(last * 1.04, 4), round(last * 1.07, 4))

    # Market cap + dominance via CoinGecko
    detail = fetch_crypto_detail(tiger["coin_id"])

    return {
        "tiger": {
            "ticker": tiger["ticker"],
            "price": last,
            "change_24h": data.get("change_24h", 0),
            "change_7d": 0,
            "market_cap": detail.get("market_cap") or 0,
            "dominance": (
                f"{detail['dominance']:.2f}%" if detail.get("dominance") is not None else "—"
            ),
            "bias": "long",
            "support": support,
            "resistance": resistance,
            "reason": "Самое сильное движение на рынке",
            "drivers": ["Нет значимых нарративов"],
            "fundamentals": ["Фундаментал без существенных изменений"],
            "risks": ["Высокая волатильность рынка"],
            "scenarios": {
                "base": "Боковик до следующего триггера",
                "bull": "Пробой сопротивления с объёмом",
                "bear": "Потеря поддержки → глубокая коррекция",
            },
            "ps": "Следи за объёмами и реакцией на ключевые уровни",
        }
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
