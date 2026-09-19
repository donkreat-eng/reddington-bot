"""Weekly preview job — runs Sunday evening YEKT."""
import sys
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
        "week_range": ye_str(fmt="%d %b") + " – " + ye_str(fmt="%d %b"),
        "fng": 50,
        "fng_label": "Neutral",
        "btc_dominance": "—",
        "week_summary": [
            "Рынок провёл неделю в режиме коррекции",
            "BTC потерял позиции после локального максимума",
        ],
        "week_lookahead": [
            "Главное событие — макро-календарь США",
            "Ждём реакции на ключевые уровни",
        ],
        "week_events": [
            "Календарь на следующую неделю обновляется",
        ],
        "correlations": [
            "DXY: нейтрально",
            "XAU: без значимых изменений",
        ],
        "plan_base": "Торговля от уровней с короткими стопами",
        "plan_aggr": "Увеличение позиции после подтверждения тренда",
        "ps": "Главный триггер недели — заседание ФРС",
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

        chart_path = "/workspace/reddington-bot/posts/weekly.png"
        generate_chart(ohlc, "BTC / USDT", support, resistance, chart_path)

        caption = weekly_caption(data)
        body = weekly_body(data)
        result = post_pair(chart_path, caption, body)
        logger.info(f"posted: {result}")
        logger.info("=== WEEKLY PREVIEW END ===")
    except Exception as e:
        logger.error(f"FAILED: {e}")
        raise


if __name__ == "__main__":
    main()
