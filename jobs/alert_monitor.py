"""Alert monitor — runs every 10 minutes, checks Tier S/A triggers."""
import sys
import time
from datetime import datetime, timezone, timedelta

sys.path.insert(0, "/workspace/reddington-bot/lib")

from common import setup_logger, ye_str
from fetch import fetch_with_retry
from publish import send_alert
from post import alert_text

logger = setup_logger("alert_monitor")

STATE_FILE = "/workspace/reddington-bot/posts/_alert_state.json"

BTC_1H_MOVE = 3.0


def load_state():
    import json
    import os
    if os.path.exists(STATE_FILE):
        try:
            return json.loads(open(STATE_FILE).read())
        except Exception:
            pass
    return {"prev_btc": None, "prev_btc_ts": None, "cooldowns": {}}


def save_state(state):
    import json
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def main():
    logger.info("=== ALERT MONITOR TICK ===")
    state = load_state()
    now = time.time()
    alerts_sent = 0
    try:
        btc = fetch_with_retry("bitcoin", "BTCUSDT", max_attempts=2, retry_delay=5)
        if btc:
            prev = state.get("prev_btc")
            prev_ts = state.get("prev_btc_ts")
            if prev and prev_ts:
                pct = (btc["price"] - prev) / prev * 100
                if abs(pct) >= BTC_1H_MOVE and (now - prev_ts) <= 7200:
                    cd = state.get("cooldowns", {}).get("btc_move", 0)
                    if now - cd > 3600:
                        title = f"BTC move: {pct:+.2f}% за час"
                        body = [
                            f"BTC: ${prev:,.0f} -> ${btc['price']:,.0f}",
                            f"Источники согласованы: {btc.get('agreement', '-')}",
                        ]
                        msg = alert_text(title, body)
                        if send_alert(msg):
                            state.setdefault("cooldowns", {})["btc_move"] = now
                            alerts_sent += 1
                            logger.warning(f"ALERT: BTC move {pct:+.2f}%")
            state["prev_btc"] = btc["price"]
            state["prev_btc_ts"] = now
    except Exception as e:
        logger.error(f"alert monitor error: {e}")
    save_state(state)
    logger.info(f"tick done, alerts sent: {alerts_sent}")
    return alerts_sent


if __name__ == "__main__":
    main()
