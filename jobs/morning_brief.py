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
    """Top gainers and losers from CoinGecko (with retry on rate limit).

    Filters: drop stablecoins, drop rows with None/zero/near-zero change, dedupe
    symbols between the two lists so the same ticker never appears in both
    gainers and losers (which happens when CoinGecko returns many flat movers).
    """
    def is_stable(m):
        s = m.get("symbol", "").upper()
        return any(st in s for st in ["USDT", "USDC", "DAI", "BUSD", "TUSD"])

    MIN_ABS_CHANGE = 0.5  # ignore noise < ±0.5%

    def fetch_sorted(order):
        from urllib.request import urlopen
        from urllib.error import HTTPError
        import time
        url = ("https://api.coingecko.com/api/v3/coins/markets"
               f"?vs_currency=usd&order=percent_change_24h_{order}"
               "&per_page=40&page=1&sparkline=false"
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

    def clean(rows, want_positive):
        out = []
        for m in rows:
            if is_stable(m):
                continue
            ch = m.get("price_change_percentage_24h")
            if ch is None:
                continue
            try:
                ch = float(ch)
            except (TypeError, ValueError):
                continue
            if abs(ch) < MIN_ABS_CHANGE:
                continue
            if want_positive and ch <= 0:
                continue
            if (not want_positive) and ch >= 0:
                continue
            out.append({"symbol": m["symbol"], "change": ch})
        return out

    try:
        gainers_data = fetch_sorted("desc")
        losers_data = fetch_sorted("asc")
        gainers = clean(gainers_data, want_positive=True)[:8]
        losers = clean(losers_data, want_positive=False)[:8]

        # Dedupe: if a symbol somehow appears in both, drop from the list
        # where its change is closer to zero (less of a "true" mover).
        gainer_syms = {g["symbol"] for g in gainers}
        loser_syms = {l["symbol"] for l in losers}
        overlap = gainer_syms & loser_syms
        if overlap:
            g_by_sym = {g["symbol"]: g for g in gainers}
            l_by_sym = {l["symbol"]: l for l in losers}
            for sym in overlap:
                g = g_by_sym.get(sym)
                l = l_by_sym.get(sym)
                if g and l:
                    if abs(g["change"]) < abs(l["change"]):
                        gainers = [x for x in gainers if x["symbol"] != sym]
                    else:
                        losers = [x for x in losers if x["symbol"] != sym]

        return {"gainers": gainers, "losers": losers}
    except Exception as e:
        logger.warning(f"movers fetch failed: {e}")
        return {"gainers": [], "losers": []}


def build_data():
    """Gather all data for the brief."""
    logger.info("=== MORNING BRIEF START ===")
    btc = fetch_with_retry("bitcoin", "BTCUSDT", attempts=2)
    eth = fetch_with_retry("ethereum", "ETHUSDT", attempts=2)
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
        last_close = data["btc"]["price"] if data.get("btc") else None
        generate_chart(ohlc, chart_path, last_price=last_close)

        caption = morning_brief_caption(data)
        body = morning_brief_body(data)
        result = post_pair(chart_path, caption, body)
        logger.info(f"posted: {result}")
    except Exception as e:
        logger.error(f"main failed: {e}")
        raise
    finally:
        logger.info("=== MORNING BRIEF END ===")


if __name__ == "__main__":
    main()
