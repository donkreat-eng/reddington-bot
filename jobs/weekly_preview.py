"""Weekly preview job — runs Sundays 20:00 YEKT (15:00 UTC)."""
import sys
import os
import logging
from datetime import datetime, timezone, timedelta

from lib.common import setup_logger, load_env
from lib.fetch import fetch_with_retry, fetch_ohlc
from lib.chart import generate_chart
from lib.post import weekly_caption, weekly_body
from lib.publish import post_pair, was_posted_recently

load_env()
_logger = setup_logger("weekly_preview")


def log(job, msg):
    _logger.info(f"{job}: {msg}")


UTC = timezone.utc


def _now_utc():
    return datetime.now(UTC)


def _tier_set():
    """Return a small mapping of tier -> [(symbol, coin_id)] — Tier 1 + selected Tier 2."""
    return [
        ("BTC", "bitcoin"),
        ("ETH", "ethereum"),
        ("SOL", "solana"),
        ("BNB", "binancecoin"),
        ("XRP", "ripple"),
        ("ADA", "cardano"),
        ("DOGE", "dogecoin"),
    ]


def build_data():
    log("weekly_preview", "fetching tier1+2 + 7d OHLC")
    tickers = _tier_set()
    prices = {}
    ohlc = {}
    for sym, cid in tickers:
        try:
            prices[sym] = fetch_with_retry(cid, f"{sym}USDT", attempts=2)
        except Exception as e:
            log("weekly_preview", f"price {sym} failed: {e}")
            prices[sym] = None
        try:
            ohlc[sym] = fetch_ohlc(cid, "usd", days=7)
        except Exception as e:
            log("weekly_preview", f"ohlc {sym} failed: {e}")
            ohlc[sym] = None
    return {
        "prices": prices,
        "ohlc": ohlc,
        "generated_at": _now_utc().strftime("%Y-%m-%d %H:%M UTC"),
    }


def main():
    log("weekly_preview", "=== WEEKLY PREVIEW START ===")
    if was_posted_recently("weekly_preview", minutes=60 * 24 * 6):
        log("weekly_preview", "skipped: posted within last week")
        return
    data = build_data()
    cap = weekly_caption(data)
    body = weekly_body(data)
    chart_path = None
    try:
        chart_path = f"/tmp/weekly_preview_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
        first = list(data["ohlc"].keys())[0]
        generate_chart(data["ohlc"][first], first, chart_path, days=7)
        log("weekly_preview", f"chart saved -> {chart_path}")
    except Exception as e:
        log("weekly_preview", f"chart skipped: {e}")
        chart_path = None
    result = post_pair(cap, body, chart_path=chart_path)
    log("weekly_preview", f"posted: {result}")
    log("weekly_preview", "=== WEEKLY PREVIEW END ===")


if __name__ == "__main__":
    main()
