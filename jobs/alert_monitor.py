"""Alert monitor — runs every 5 minutes, checks Tier S/A triggers."""
import sys
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.common import setup_logger, ye_str
from lib.fetch import fetch_with_retry
from lib.publish import send_alert
from lib.post import alert_text

logger = setup_logger("alert_monitor")

# State file to remember previous price
STATE_FILE = "/workspace/reddington-bot/posts/_alert_state.json"

# Tier S thresholds
FOMC_MIN_MOVE = 1.5  # % move during FOMC is automatic alert
LIQUIDATION_THRESHOLD = 1_000_000_000  # $1B cascade
STABLE_DEPEG = 0.97  # below $0.97 = alert
BTC_1H_MOVE = 3.0    # >3% in 1h = Tier S alert


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


def check_btc_move(prev_price, current_price, prev_ts):
    if not prev_price:
        return None
    pct = (current_price - prev_price) / prev_price * 100
    elapsed = time.time() - prev_ts
    if abs(pct) >= BTC_1H_MOVE and elapsed <= 7200:  # 2h window
        return pct
    return None


def main():
    logger.info("=== ALERT MONITOR TICK ===")
    state = load_state()
    now = time.time()
    ye = datetime.now(timezone(timedelta(hours=5)))
    alerts_sent = 0

    try:
        # Check BTC for >3% moves in 1h
        btc = fetch_with_retry("bitcoin", "BTCUSDT", max_attempts=2, retry_delay=5)
        if btc:
            prev = state.get("prev_btc")
            prev_ts = state.get("prev_btc_ts")
            if prev and prev_ts:
                pct = (btc["price"] - prev) / prev * 100
                if abs(pct) >= BTC_1H_MOVE and (now - prev_ts) <= 7200:
                    cd = state.get("cooldowns", {}).get("btc_move", 0)
                    if now - cd > 3600:  # 1h cooldown
                        title = f"BTC move: {pct:+.2f}% за час"
                        body = [
                            f"BTC: ${prev:,.0f} → ${btc['price']:,.0f}",
                            f"ETH: ${0:,.0f} — следим за синхронностью",
                            f"Источники согласованы: {btc.get('agreement', '—')}",
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
