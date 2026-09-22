"""Morning brief job — runs at 09:00 YEKT."""
import sys
import json
import os
from pathlib import Path
import logging
from datetime import datetime, timezone, timedelta

from lib.common import setup_logger, load_env
from lib.fetch import fetch_with_retry, fetch_ohlc
from lib.chart import generate_chart
from lib.post import morning_brief_caption, morning_brief_body, fmt_price, fmt_change
from lib.publish import post_pair, send_photo, send_text, was_posted_recently

load_env()
_logger = setup_logger("morning_brief")


def log(job, msg):
    _logger.info(f"{job}: {msg}")


YEKT = timezone(timedelta(hours=5))


def _now_yt():
    return datetime.now(YEKT)


def _asia_session_label():
    n = _now_yt()
    wd = n.weekday()  # 0=Mon
    h = n.hour
    # Mon-Fri only
    if wd >= 5:
        return None
    # sessions by YEKT hour
    if 0 <= h < 6:
        return None  # quiet hours — skip
    if 6 <= h < 12:
        return "Азиатская"
    if 12 <= h < 16:
        return "Европейская в разгаре"
    if 16 <= h < 22:
        return "Американская в разгаре"
    return None


def _build_levels(ohlc):
    if not ohlc or not ohlc.get("close"):
        return {"support": None, "resistance": None}
    closes = ohlc["close"]
    return {
        "support": round(min(closes[-7:]), 2),
        "resistance": round(max(closes[-7:]), 2),
    }


def _fmt_levels(lv):
    s, r = lv.get("support"), lv.get("resistance")
    if s is None and r is None:
        return ""
    parts = []
    if s is not None:
        parts.append(f"поддержка ${s:,.0f}")
    if r is not None:
        parts.append(f"сопротивление ${r:,.0f}")
    return ", ".join(parts)


def build_data():
    log("morning_brief", "fetching BTC/ETH prices + 7d OHLC")
    btc = fetch_with_retry("bitcoin", "BTCUSDT", attempts=2)
    eth = fetch_with_retry("ethereum", "ETHUSDT", attempts=2)
    btc_ohlc = fetch_ohlc("bitcoin", "usd", days=7)
    eth_ohlc = fetch_ohlc("ethereum", "usd", days=7)
    btc_lv = _build_levels(btc_ohlc)
    eth_lv = _build_levels(eth_ohlc)
    return {
        "btc": btc,
        "eth": eth,
        "btc_ohlc": btc_ohlc,
        "eth_ohlc": eth_ohlc,
        "btc_levels": btc_lv,
        "eth_levels": eth_lv,
        "session": _asia_session_label(),
        "generated_at": _now_yt().strftime("%Y-%m-%d %H:%M YEKT"),
    }


def main():
    log("morning_brief", "=== MORNING BRIEF START ===")
    data = build_data()
    cap = morning_brief_caption(data)
    body = morning_brief_body(data)
    chart_path = None
    try:
        chart_path = f"/tmp/morning_brief_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
        generate_chart(data["btc_ohlc"], "BTC", chart_path, levels=data["btc_levels"], days=7)
        log("morning_brief", f"chart saved -> {chart_path}")
    except Exception as e:
        log("morning_brief", f"chart skipped: {e}")
        chart_path = None
    if was_posted_recently("morning_brief", minutes=60):
        log("morning_brief", "skipped: posted within last 60m")
        return
    result = post_pair(cap, body, chart_path=chart_path)
    log("morning_brief", f"posted: {result}")


if __name__ == "__main__":
    main()
