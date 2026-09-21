"""Morning brief job — runs at 09:00 YEKT."""
import sys
import json
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.common import setup_logger, ye_str
from lib.fetch import fetch_with_retry, fetch_ohlc
from lib.chart import generate_chart
from lib.publish import post_pair
from lib.post import (
    morning_brief_caption, morning_brief_body,
    fmt_price, fmt_change
)


logger = setup_logger("morning_brief")


def get_fng():
    """Fetch Fear & Greed Index."""
    try:
        from urllib.request import urlopen
        data = json.loads(urlopen("https://api.alternative.me/fng/?limit=1", timeout=8).read())
        v = int(data["data"][0]["value"])
        label = data["data"][0]["value_classification"]
        return v, label
    except Exception as e:
        logger.warning(f"FNG fetch failed: {e}")
        return 0, ""


def get_btc_dominance():
    try:
        from urllib.request import urlopen
        data = json.loads(urlopen("https://api.coingecko.com/api/v3/global", timeout=8).read())
        dom = data["data"]["market_cap_percentage"]["btc"]
        return f"{dom:.2f}%"
    except Exception as e:
        logger.warning(f"dominance fetch failed: {e}")
        return "—"


def get_top_movers():
    """Top gainers and losers from CoinGecko (with retry on rate limit)."""
    def is_stable(m):
        s = m.get("symbol", "").upper()
        return any(st in s for st in ["USDT", "USDC", "DAI", "BUSD", "TUSD"])

    def fetch_sorted(order):
        from urllib.request import urlopen
        from urllib.error import HTTPError
        import time
        url = ("https://api.coingecko.com/api/v3/coins/markets"
               f"?vs_currency=usd&order=percent_change_24h_{order}"
               "&per_page=20&page=1&sparkline=false"
               "&price_change_percentage=24h")
        for attempt in range(2):
            try:
                data = json.loads(urlopen(url, timeout=10).read())
                return data
            except HTTPError as e:
                if e.code == 429 and attempt == 0:
                    time.sleep(5)
                else:
                    raise
        return []

    try:
        gainers_data = fetch_sorted("desc")
        losers_data = fetch_sorted("asc")
        gainers = [m for m in gainers_data if not is_stable(m)][:8]
        losers = [m for m in losers_data if not is_stable(m)][:8]
        return {
            "gainers": [{"symbol": m["symbol"], "change": m.get("price_change_percentage_24h", 0)}
                       for m in gainers],
            "losers": [{"symbol": m["symbol"], "change": m.get("price_change_percentage_24h", 0)}
                       for m in losers],
        }
    except Exception as e:
        logger.warning(f"movers fetch failed: {e}")
        return {"gainers": [], "losers": []}


def build_data():
    """Gather all data for the brief."""
    logger.info("=== MORNING BRIEF START ===")
    btc = fetch_with_retry("bitcoin", "BTCUSDT", max_attempts=2, retry_delay=10)
    eth = fetch_with_retry("ethereum", "ETHUSDT", max_attempts=2, retry_delay=10)
    fng, fng_label = get_fng()
    btc_dom = get_btc_dominance()
    movers = get_top_movers()

    return {
        "btc": btc,
        "eth": eth,
        "fng": fng,
        "fng_label": fng_label,
        "btc_dominance": btc_dom,
        "movers": movers,
        "date_label": ye_str(fmt="%d %b %Y"),
        "overnight_news": [
            "Рынок без значимых overnight-новостей",
        ],
        "today_focus": [
            "Сегодня спокойный торговый день",
            "Следим за реакцией на ключевые уровни BTC",
        ],
        "week_events": [],
    }


def main():
    try:
        data = build_data()

        # Generate chart
        ohlc = fetch_ohlc("bitcoin", "usd", days=7)
        if not ohlc:
            logger.error("no OHLC data")
            return

        chart_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "posts")
        os.makedirs(chart_dir, exist_ok=True)
        chart_path = os.path.join(chart_dir, f"morning_brief_{ye_str(fmt='%Y%m%d_%H%M')}.png")
        last_close = data["btc"]["price"]
        support = (round(last_close * 0.97, 2), round(last_close * 0.985, 2))
        resistance = (round(last_close * 1.015, 2), round(last_close * 1.03, 2))

        generate_chart(ohlc, "BTC / USDT", support, resistance, chart_path)
        logger.info(f"chart generated: {chart_path}")

        caption = morning_brief_caption(data, {"btc_dominance": data["btc_dominance"]})
        body = morning_brief_body(data)

        # Sanity check: don't post empty briefs
        if not data.get("btc", {}).get("price"):
            logger.error("btc price missing — skipping post")
            return

        result = post_pair(chart_path, caption, body, job_name="morning_brief", min_age_minutes=60)
        logger.info(f"posted: {result}")
        logger.info("=== MORNING BRIEF END ===")

    except Exception as e:
        logger.error(f"FAILED: {e}")
        # Notify owner via the chat
        import traceback
        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
