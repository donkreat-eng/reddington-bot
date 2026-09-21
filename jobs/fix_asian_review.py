"""One-off fix: delete stale asian_review posts (message_id=1178, 1179) and republish with current session context.

Reason: previous run was published at 12:38 UTC and claimed "European session starts in 1 hour",
but by 12:43 UTC the European session was already well underway (it runs 07-16 UTC),
and the American session starts in ~50 min (at 13:30 UTC = 18:30 YEKT).

Run via: workflow_dispatch with inputs.job=fix_asian_review
"""
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.common import setup_logger, ye_str
from lib.fetch import fetch_with_retry
from lib.publish import send_text, _api, CHAT_ID
from lib.post import asian_caption, asian_body

logger = setup_logger("fix_asian_review")

OLD_MESSAGES = [1178, 1179]


def _session_context_utc(now_utc=None):
    """Return forward_focus lines describing current and upcoming trading sessions."""
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    mins = now_utc.hour * 60 + now_utc.minute

    ASIA = (0, 7 * 60)
    EUROPE = (7 * 60, 16 * 60)
    US_PRE = (12 * 60, 13 * 60 + 30)
    US = (13 * 60 + 30, 20 * 60)
    US_POST = (20 * 60, 24 * 60)

    def in_range(t, lo, hi):
        return lo <= t < hi

    ye_now = now_utc.astimezone(timezone(timedelta(hours=5)))
    ye_label = ye_now.strftime("%d %b · %H:%M YEKT")

    active = []
    if in_range(mins, *ASIA):
        active.append("Азиатская сессия")
    if in_range(mins, *EUROPE):
        active.append("Европейская сессия (в разгаре)")
    if in_range(mins, *US_PRE):
        active.append("Пре-маркет США")
    if in_range(mins, *US):
        active.append("Американская сессия")
    if in_range(mins, *US_POST):
        active.append("Пост-маркет США")

    candidates = [
        ("азиатская", 24 * 60 + ASIA[0]),
        ("европейская", 24 * 60 + EUROPE[0]),
        ("американская", 24 * 60 + US[0]),
    ]
    next_session = None
    next_in_min = None
    for name, start in candidates:
        if start > mins:
            delta = start - mins
            if next_in_min is None or delta < next_in_min:
                next_in_min = delta
                next_session = name

    parts = []
    if active:
        parts.append("Сейчас активно: " + ", ".join(active) + ".")
    if next_session and next_in_min is not None:
        hh = next_in_min // 60
        mm = next_in_min % 60
        if hh > 0 and mm > 0:
            when = f"через {hh} ч {mm} мин"
        elif hh > 0:
            when = f"через {hh} ч"
        else:
            when = f"через {mm} мин"
        parts.append(f"Следующая сессия — {next_session} ({when}).")

    parts.append(f"Время публикации: {ye_label}.")
    return parts


def delete_messages(message_ids):
    deleted = []
    failed = []
    for mid in message_ids:
        result = _api("deleteMessage", chat_id=CHAT_ID, message_id=mid)
        if result.get("ok"):
            deleted.append(mid)
            logger.info(f"  deleted: message_id={mid}")
        else:
            failed.append((mid, result))
            logger.error(f"  delete failed: message_id={mid} result={result}")
    return deleted, failed


def main():
    try:
        logger.info("=== FIX ASIAN REVIEW START ===")
        logger.info(f"old message_ids to delete: {OLD_MESSAGES}")

        deleted, failed = delete_messages(OLD_MESSAGES)
        logger.info(f"deleted {len(deleted)}/{len(OLD_MESSAGES)}: deleted={deleted} failed={failed}")

        btc = fetch_with_retry("bitcoin", "BTCUSDT", max_attempts=2, retry_delay=10)
        eth = fetch_with_retry("ethereum", "ETHUSDT", max_attempts=2, retry_delay=10)
        if not btc.get("price"):
            logger.error("btc price missing — cannot republish")
            return

        ye = datetime.now(timezone(timedelta(hours=5)))
        session_lines = _session_context_utc()

        data = {
            "btc": btc,
            "eth": eth,
            "date_label": ye_str(fmt="%d %b %H:%M"),
            "asia_news": [
                "Азиатская сессия прошла спокойно без значимых движений",
                "Ликвидность в часы низкой активности",
            ],
            "flows": ["Данные обновляются"],
            "forward_focus": session_lines,
        }

        caption = asian_caption(data)
        body = asian_body(data)

        mid1 = send_text(caption)
        mid2 = send_text(body)
        logger.info(f"republished: caption_id={mid1} body_id={mid2}")
        logger.info("=== FIX ASIAN REVIEW END ===")
    except Exception as e:
        logger.error(f"FAILED: {e}")
        raise


if __name__ == "__main__":
    main()