"""Tiger Day — daily Tier-2 spotlight (1 ticker per weekday)."""
import sys
from datetime import datetime, timezone

from lib.fetch import fetch_with_retry, fetch_ohlc, fetch_crypto_detail
from lib.publish import post_pair, was_posted_recently, mark_posted
from lib.post import tiger_caption, tiger_body, generate_chart
from lib.common import setup_logger, load_env

load_env()
_logger = setup_logger("tiger_day")


def log(job, msg):
    _logger.info(f"{job}: {msg}")


# Tier-2 pool: rotate by ISO weekday (Mon=1 .. Fri=5)
TIGER_POOL = [
    {"coin_id": "ethereum", "symbol": "ETHUSDT", "ticker": "ETH"},
    {"coin_id": "solana", "symbol": "SOLUSDT", "ticker": "SOL"},
    {"coin_id": "binancecoin", "symbol": "BNBUSDT", "ticker": "BNB"},
    {"coin_id": "ripple", "symbol": "XRPUSDT", "ticker": "XRP"},
    {"coin_id": "avalanche-2", "symbol": "AVAXUSDT", "ticker": "AVAX"},
]


def ye_now():
    return datetime.now(timezone.utc)  # cron uses UTC; bot.yml converts YEKT to UTC


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
        # Mid-cap: 4% / 6% bands
        support = (round(last * 0.94, 2), round(last * 0.96, 2))
        resistance = (round(last * 1.04, 2), round(last * 1.06, 2))
    else:
        # Low-cap: 6% / 8% bands
        support = (round(last * 0.92, 2), round(last * 0.94, 2))
        resistance = (round(last * 1.06, 2), round(last * 1.08, 2))

    # 7d OHLC for the chart
    ohlc = []
    try:
        ohlc = fetch_ohlc(tiger["coin_id"], "usd", days=7) or []
    except Exception as e:
        log("tiger_day", f"ohlc fetch failed: {e}")

    # Real market cap + dominance from CoinGecko
    detail = fetch_crypto_detail(tiger["coin_id"])

    return {
        "tiger": {
            **tiger,
            "price": last,
            "support": support[1],
            "support_low": support[0],
            "resistance": resistance[0],
            "resistance_high": resistance[1],
            "market_cap": detail.get("market_cap") or 0,
            "dominance": detail.get("dominance"),
            "ohlc": ohlc,
            "spread_pct": data.get("spread_pct"),
            "sources": data.get("sources", []),
            "change_24h": data.get("change_24h", 0),
            "volume_24h": data.get("volume_24h", 0),
        },
        "now": ye_now().isoformat(),
    }


def main():
    tiger = pick_tiger()
    log("tiger_day", f"today's tiger: {tiger['ticker']}")

    data = build_data(tiger)
    caption = tiger_caption(data["tiger"])
    body = tiger_body(data["tiger"])

    if was_posted_recently("tiger_day", data["tiger"]["ticker"], within_minutes=60):
        log("tiger_day", f"skip: {data['tiger']['ticker']} already posted in last hour")
        return

    chart_path = f"posts/tiger_day_{data['tiger']['ticker'].replace('/', '_')}.png"
    try:
        generate_chart(data["tiger"]["ohlc"], data["tiger"]["ticker"], data["tiger"]["support"], data["tiger"]["resistance"], chart_path)
    except Exception as e:
        log("tiger_day", f"chart failed: {e}")
        chart_path = None

    result = post_pair(chart_path, caption, body)
    mark_posted("tiger_day")
    log("tiger_day", f"posted: {result}")


if __name__ == "__main__":
    main()
