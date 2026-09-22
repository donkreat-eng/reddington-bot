"""One-off fix: delete broken morning_brief messages 1204/1205 (gainers=losers
overlap on CoinGecko flat movers) and republish with the dedup/noise-filter fix.

Run via bot.yml workflow_dispatch job=fix_morning_brief.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.common import setup_logger
from lib.publish import _api, CHAT_ID, send_photo, send_text
from lib.post import morning_brief_caption, morning_brief_body
from lib.chart import generate_chart
from lib.fetch import fetch_with_retry, fetch_ohlc
import morning_brief  # jobs/morning_brief.py
import textwrap

logger = setup_logger("fix_morning_brief")

OLD_MESSAGES = [1204, 1205]  # photo + text from run #200 at 10:14 YEKT


def delete_messages():
    deleted = []
    failed = []
    for mid in OLD_MESSAGES:
        r = _api("deleteMessage", chat_id=CHAT_ID, message_id=mid)
        if r.get("ok"):
            logger.info(f"  deleted: message_id={mid}")
            deleted.append(mid)
        else:
            logger.error(f"  delete failed: message_id={mid} → {r}")
            failed.append(mid)
    return deleted, failed


def republish():
    """Build a fresh brief with current data and post it."""
    data = morning_brief.build_data()
    btc = data["btc"]
    chart_path = None
    try:
        ohlc = fetch_ohlc("BTCUSDT", timeframe="1h", limit=168)
        chart_path = generate_chart(
            ticker="BTC",
            ohlc=ohlc,
            current_price=btc.get("price", 0),
            output_path="/tmp/morning_brief_chart.png",
        )
    except Exception as e:
        logger.warning(f"chart generation skipped: {e}")

    caption = morning_brief_caption(data)
    body = morning_brief_body(data)

    if chart_path:
        photo_id = send_photo(chart_path, caption, parse_mode="HTML")
    else:
        photo_id = None
    text_id = send_text(body, parse_mode="HTML")
    logger.info(f"  republished: photo_id={photo_id} text_id={text_id}")


def main():
    logger.info("=== FIX MORNING BRIEF START ===")
    logger.info(f"  old message_ids to delete: {OLD_MESSAGES}")
    deleted, failed = delete_messages()
    logger.info(f"  deleted {len(deleted)}/{len(OLD_MESSAGES)}: deleted={deleted} failed={failed}")
    republish()
    logger.info("=== FIX MORNING BRIEF END ===")


if __name__ == "__main__":
    main()