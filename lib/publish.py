"""Telegram publishing — photo with caption + separate text message."""
import os
import time
import urllib.parse
import json
from urllib.request import Request, urlopen
from urllib.error import URLError

from lib.common import load_env, setup_logger, ye_str

logger = setup_logger("publish")
ENV = load_env()
TOKEN = ENV["REDDINGTON_BOT_TOKEN"]
CHAT_ID = ENV["REDDINGTON_CHANNEL_ID"]
import hashlib as _hb
logger.info(f"token prefix={TOKEN[:12]}... len={len(TOKEN)} sha1={_hb.sha1(TOKEN.encode()).hexdigest()[:8]} chat_id={CHAT_ID!r}")


def _api(method, **fields):
    """Call Telegram Bot API. fields can include text, photo path, etc."""
    url = f"https://api.telegram.org/bot{TOKEN}/{method}"
    if "photo" in fields and isinstance(fields["photo"], str):
        boundary = "----ReddingtonBoundary"
        body = []
        for k, v in fields.items():
            if k == "photo":
                body.append(f"--{boundary}".encode())
                body.append(f'Content-Disposition: form-data; name="{k}"; filename="{os.path.basename(v)}"'.encode())
                body.append(b"Content-Type: image/png")
                body.append(b"")
                with open(v, "rb") as f:
                    body.append(f.read())
            else:
                body.append(f"--{boundary}".encode())
                body.append(f'Content-Disposition: form-data; name="{k}"'.encode())
                body.append(b"")
                body.append(str(v).encode())
        body.append(f"--{boundary}--".encode())
        body.append(b"")
        data = b"\r\n".join(body)
        req = Request(url, data=data)
        req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
    else:
        data = urllib.parse.urlencode(fields).encode()
        req = Request(url, data=data)
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except URLError as e:
        logger.error(f"API error {method}: {e}")
        return {"ok": False, "error": str(e)}


def send_photo(photo_path, caption, parse_mode="HTML", reply_markup=None):
    fields = {"chat_id": CHAT_ID, "photo": photo_path, "caption": caption, "parse_mode": parse_mode}
    if reply_markup is not None:
        fields["reply_markup"] = reply_markup
    result = _api("sendPhoto", **fields)
    if result.get("ok"):
        mid = result["result"]["message_id"]
        logger.info(f"  photo sent: message_id={mid}")
        return mid
    logger.error(f"  photo failed: {result}")
    return None


def send_text(text, parse_mode="HTML", disable_notification=False, reply_markup=None):
    fields = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
        "disable_notification": disable_notification,
    }
    if reply_markup is not None:
        fields["reply_markup"] = reply_markup
    result = _api("sendMessage", **fields)
    if result.get("ok"):
        mid = result["result"]["message_id"]
        logger.info(f"  text sent: message_id={mid}")
        return mid
    logger.error(f"  text failed: {result}")
    return None


def pin_message(message_id, disable_notification=False):
    """Pin a message in the channel."""
    result = _api("pinChatMessage",
                  chat_id=CHAT_ID,
                  message_id=message_id,
                  disable_notification=disable_notification)
    if result.get("ok"):
        logger.info(f"  message pinned: {message_id}")
        return True
    logger.error(f"  pin failed: {result}")
    return False


def url_button(text, url):
    """Build InlineKeyboardMarkup JSON string with a single URL button."""
    return json.dumps({
        "inline_keyboard": [[{"text": text, "url": url}]]
    })


def _dedup_state_path(job_name):
    """Per-job state file for dedup checks."""
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(repo_root, "posts", f"_last_post_{job_name}.ts")


def was_posted_recently(job_name, min_age_minutes):
    """Return True if a post with this job_name was sent < min_age_minutes ago."""
    if not job_name or min_age_minutes <= 0:
        return False
    p = _dedup_state_path(job_name)
    if not os.path.exists(p):
        return False
    try:
        last = float(open(p).read().strip() or 0)
    except (ValueError, OSError):
        return False
    age_min = (time.time() - last) / 60.0
    if age_min < min_age_minutes:
        logger.info(f"dedup: {job_name} posted {age_min:.1f} min ago (< {min_age_minutes}), skipping")
        return True
    return False


def mark_posted(job_name):
    """Record timestamp of successful post for this job."""
    if not job_name:
        return
    p = _dedup_state_path(job_name)
    try:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as f:
            f.write(str(time.time()))
    except OSError as e:
        logger.warning(f"could not write dedup state for {job_name}: {e}")


def post_pair(photo_path, caption, body_text, reply_markup=None, job_name=None, min_age_minutes=60):
    """Send photo+text pair to channel. If job_name is set, dedup against recent runs.

    Returns dict with photo_id, text_id, and skipped=True if dedup hit.
    """
    if was_posted_recently(job_name, min_age_minutes):
        return {"photo_id": None, "text_id": None, "skipped": True}
    logger.info(f"publishing to channel {CHAT_ID}")
    photo_mid = send_photo(photo_path, caption, reply_markup=reply_markup)
    time.sleep(1.0)
    text_mid = send_text(body_text, reply_markup=reply_markup)
    if photo_mid or text_mid:
        mark_posted(job_name)
    return {"photo_id": photo_mid, "text_id": text_mid, "skipped": False}


def send_alert(text):
    return send_text(text, disable_notification=False)


def post_donation_pinned(donate_url):
    """Post a pinned donation message with URL button."""
    body = (
        "💎 <b>Поддержать REDDINGTON</b>\n\n"
        "Канал делается для вас и за ваши донаты. "
        "Любая сумма помогает нам делать больше разборов, "
        "улучшать бот и добавлять новые фичи.\n\n"
        "🔗 Нажмите кнопку ниже — откроется безопасный "
        "инвойс от @CryptoBot (BTC, ETH, TON, USDT и др.).\n\n"
        "🙏 Спасибо за поддержку!"
    )
    markup = url_button("💸 Поддержать REDDINGTON", donate_url)
    msg_id = send_text(body, reply_markup=markup)
    if msg_id:
        pin_message(msg_id, disable_notification=False)
    return msg_id


if __name__ == "__main__":
    send_text(f"🟢 Reddington bot v1.0 — {ye_str()}\nПодключение проверено.")
