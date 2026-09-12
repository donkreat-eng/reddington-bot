"""Telegram publishing — photo with caption + separate text message."""
import os
import time
import urllib.parse
from urllib.request import Request, urlopen
from urllib.error import URLError

from common import load_env, setup_logger, ye_str

logger = setup_logger("publish")
ENV = load_env()
TOKEN = ENV["REDDINGTON_BOT_TOKEN"]
CHAT_ID = ENV["REDDINGTON_CHANNEL_ID"]


def _api(method, **fields):
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
            import json
            return json.loads(r.read())
    except URLError as e:
        logger.error(f"API error {method}: {e}")
        return {"ok": False, "error": str(e)}


def send_photo(photo_path, caption, parse_mode="HTML"):
    result = _api("sendPhoto", chat_id=CHAT_ID, photo=photo_path, caption=caption, parse_mode=parse_mode)
    if result.get("ok"):
        mid = result["result"]["message_id"]
        logger.info(f"  photo sent: message_id={mid}")
        return mid
    logger.error(f"  photo failed: {result}")
    return None


def send_text(text, parse_mode="HTML", disable_notification=False):
    result = _api("sendMessage", chat_id=CHAT_ID, text=text, parse_mode=parse_mode,
                  disable_web_page_preview=True, disable_notification=disable_notification)
    if result.get("ok"):
        mid = result["result"]["message_id"]
        logger.info(f"  text sent: message_id={mid}")
        return mid
    logger.error(f"  text failed: {result}")
    return None


def post_pair(photo_path, caption, body_text):
    logger.info(f"publishing to channel {CHAT_ID}")
    photo_mid = send_photo(photo_path, caption)
    time.sleep(1.0)
    text_mid = send_text(body_text)
    return {"photo_id": photo_mid, "text_id": text_mid}


def send_alert(text):
    return send_text(text, disable_notification=False)


if __name__ == "__main__":
    send_text(f"🟢 Reddington bot v1.0 — {ye_str()}\nПодключение проверено.")
