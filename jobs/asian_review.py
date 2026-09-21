"""Asian review job — runs at 16:15 YEKT (11:15 UTC)."""
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.common import setup_logger, ye_str
from lib.fetch import fetch_with_retry
from lib.publish import send_text, was_posted_recently, mark_posted
from lib.post import asian_caption, asian_body

logger = setup_logger("asian_review")


def session_focus(now_utc=None):
    """Return forward_focus lines describing currently active and upcoming trading sessions in UTC.

    Sessions (UTC):
      Asia:     00:00 - 07:00
      Europe:   07:00 - 16:00
      US pre:   12:00 - 13:30
      US:       13:30 - 20:00
      US post:  20:00 - 24:00
    """
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

    # Compute next session boundary
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
    else:
        parts.append("Между сессиями — низкая ликвидность.")
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
    return parts


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
        "forward_focus": session_focus(),
    }


def main():
    try:
        logger.info("=== ASIAN REVIEW START ===")
        if was_posted_recently("asian_review", 60):
            logger.info("skipped: posted <60 min ago")
            return

        data = build_data()
        if not data.get("btc", {}).get("price"):
            logger.error("btc price missing — skipping post")
            return

        caption = asian_caption(data)
        body = asian_body(data)

        mid1 = send_text(caption)
        mid2 = send_text(body)
        if mid1 or mid2:
            mark_posted("asian_review")
        logger.info("posted (text-only)")
        logger.info("=== ASIAN REVIEW END ===")
    except Exception as e:
        logger.error(f"FAILED: {e}")
        raise


if __name__ == "__main__":
    main()