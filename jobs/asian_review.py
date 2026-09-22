"""Asian review job — runs at 16:15 YEKT (11:15 UTC)."""
import sys
import json
import os
from pathlib import Path
import logging
from datetime import datetime, timezone, timedelta

from lib.common import setup_logger, load_env
from lib.fetch import fetch_with_retry
from lib.post import asian_caption, asian_body
from lib.publish import post_pair, send_photo, send_text, was_posted_recently

load_env()
_logger = setup_logger("asian_review")


def log(job, msg):
    _logger.info(f"{job}: {msg}")


UTC = timezone.utc


def _now_utc():
    return datetime.now(UTC)


def _session_focus(now_utc=None):
    """Return session labels based on current UTC hour."""
    n = now_utc or _now_utc()
    wd = n.weekday()  # 0=Mon
    h = n.hour
    m = n.minute

    sessions = []  # list of strings to render as 'сейчас/следующая'
    candidates = []

    if 0 <= h < 6:
        sessions.append("Пост-азиатская")
        candidates.append(("Азиатская", 0))
    elif 6 <= h < 9:
        sessions.append("Азиатская в разгаре")
        candidates.append(("Европейская", 8))
    elif 9 <= h < 13:
        sessions.append("Европейская в разгаре")
        if h < 12:
            candidates.append(("Пре-маркет США", 12))
        else:
            candidates.append(("Американская", 14))
    elif 13 <= h < 21:
        sessions.append("Американская в разгаре")
        candidates.append(("Пост-маркет США", 21))
    else:
        sessions.append("Пост-маркет США")
        candidates.append(("Азиатская (завтра)", 24))

    next_parts = []
    for name, start in candidates[:1]:
        if start >= 24:
            start = 0
        delta = timedelta(hours=(start - h)) if start >= h else timedelta(hours=(24 - h + start), days=0)
        if start == 0 and h >= 21:
            delta = timedelta(hours=(24 - h)) if start == 0 else delta
        next_parts.append(f"Следующая — {name} (через {_fmt_delta(delta)})")
    if next_parts:
        return ", ".join(sessions) + ". " + " ".join(next_parts)
    return ", ".join(sessions)


def _fmt_delta(td):
    secs = int(td.total_seconds())
    if secs <= 0:
        return "скоро"
    h = secs // 3600
    m = (secs % 3600) // 60
    if h == 0:
        return f"{m} мин"
    if m == 0:
        return f"{h} ч"
    return f"{h} ч {m} мин"


def build_data():
    log("asian_review", "fetching BTC/ETH consensus")
    btc = fetch_with_retry("bitcoin", "BTCUSDT", attempts=2)
    eth = fetch_with_retry("ethereum", "ETHUSDT", attempts=2)
    return {
        "btc": btc,
        "eth": eth,
        "session_focus": _session_focus(),
        "generated_at": _now_utc().strftime("%Y-%m-%d %H:%M UTC"),
    }


def main():
    log("asian_review", "=== ASIAN REVIEW START ===")
    data = build_data()
    if was_posted_recently("asian_review", minutes=60):
        log("asian_review", "skipped: posted within last 60m")
        return
    cap = asian_caption(data)
    body = asian_body(data)
    result = post_pair(cap, body)
    log("asian_review", f"posted: {result}")
    log("asian_review", "=== ASIAN REVIEW END ===")


if __name__ == "__main__":
    main()
