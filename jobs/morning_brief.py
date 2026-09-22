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