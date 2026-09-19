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


def post_pair(photo_path, caption, body_text, reply_markup=None):
    logger.info(f"publishing to channel {CHAT_ID}")
    photo_mid = send_photo(photo_path, caption, reply_markup=reply_markup)
    time.sleep(1.0)
    text_mid = send_text(body_text, reply_markup=reply_markup)
    return {"photo_id": photo_mid, "text_id": text_mid}


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
        "инвойс от @CryptoBot (BTC, ETH, TON, USDT и др.)"
    )
    markup = url_button("💸 Поддержать REDDINGTON", donate_url)
    mid = send_text(body, reply_markup=markup)
    if mid:
        pin_message(mid)
    return mid