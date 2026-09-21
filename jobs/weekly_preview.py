"""Weekly preview job — runs at 20:00 YEKT on Sunday."""
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.common import setup_logger, ye_str
from lib.fetch import fetch_with_retry, fetch_ohlc
from lib.chart import generate_chart
from lib.publish import post_pair
from lib.post import weekly_caption, weekly_body

logger = setup_logger("weekly_preview")


def build_data():
    btc = fetch_with_retry("bitcoin", "BTCUSDT", max_attempts=2, retry_delay=10)
    eth = fetch_with_retry("ethereum", "ETHUSDT", max_attempts=2, retry_delay=10)
    return {
        "btc": btc,
        "eth": eth,
        "date_label": ye_str(fmt="%d %b %Y"),
        "week_summary": "Неделя прошла с умеренной волатильностью, рынок консолидируется.",
        "next_week_events": [
            "Заседания ФРС и ЕЦБ — ключевое событие недели",
            "Квартальные отчёты крупных технологических компаний",
            "Обновления по ключевым криптопротоколам",
        ],
        "levels_note": "Уровни рассчитаны от текущей цены",
    }


def main():
    try:
        logger.info("=== WEEKLY PREVIEW START ===")
        data = build_data()

        ohlc = fetch_ohlc("bitcoin", "usd", days=7)
        if not ohlc:
            logger.error("no OHLC data")
            return

        last = data["btc"]["price"]
        support = (round(last * 0.97, 2), round(last * 0.985, 2))
        resistance = (round(last * 1.015, 2), round(last * 1.03, 2))

        chart_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "posts")
        os.makedirs(chart_dir, exist_ok=True)
        chart_path = os.path.join(chart_dir, f"weekly_preview_{ye_str(fmt='%Y%m%d_%H%M')}.png")
        generate_chart(ohlc, "BTC / USDT", support, resistance, chart_path)

        if not data.get("btc", {}).get("price"):
            logger.error("btc price missing — skipping post")
            return

        caption = weekly_caption(data)
        body = weekly_body(data)
        result = post_pair(chart_path, caption, body, job_name="weekly_preview", min_age_minutes=60)
        logger.info(f"posted: {result}")
        logger.info("=== WEEKLY PREVIEW END ===")
    except Exception as e:
        logger.error(f"FAILED: {e}")
        raise


if __name__ == "__main__":
    main()
