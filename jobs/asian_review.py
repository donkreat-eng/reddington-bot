"""Asian review job — runs at 16:15 YEKT."""
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.common import setup_logger, ye_str
from lib.fetch import fetch_with_retry
from lib.publish import send_text
from lib.post import asian_caption, asian_body

logger = setup_logger("asian_review")


def build_data():
    btc = fetch_with_retry("bitcoin", "BTCUSDT", max_attempts=2, retry_delay=10)
    eth = fetch_with_retry("ethereum", "ETHUSDT", max_attempts=2, retry_delay=10)
    ye = datetime.now(timezone(timedelta(hours=5)))
    return {
        "btc": btc,
        "eth": eth,
        "date_label": ye_str(fmt="%d %b %H:%M"),
        "asia_news": [
            "Азиатская сессия прошла спокойно без значимых движений",
            "Ликвидность в часы низкой активности",
        ],
        "flows": ["Данные обновляются"],
        "forward_focus": [
            "Европейская сессия стартует через час",
            "Следим за реакцией на уровни BTC",
        ],
    }


def main():
    try:
        logger.info("=== ASIAN REVIEW START ===")
        data = build_data()
        caption = asian_caption(data)
        body = asian_body(data)

        # Send caption as photo-less message + body as follow-up text
        from publish import send_text
        send_text(caption)
        send_text(body)
        logger.info("posted (text-only)")
        logger.info("=== ASIAN REVIEW END ===")
    except Exception as e:
        logger.error(f"FAILED: {e}")
        raise


if __name__ == "__main__":
    main()
