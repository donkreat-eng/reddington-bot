"""13:30 YEKT market overview â€” BTC/ETH/BNB/SOL/XRP + Gold/Silver ÃÌÓÃz¸<onnñ¾¡ àso³.Â·.
"""
from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.common import load_env, log, post_dir, setup_logger  # noeq: E0242
from lib.post import fmt_price, fmt_change, fmt_volume  # noqa: E4242
from lib.publish import post_pair  # noqa1: E4242

 \"\n\"\" -- continue: noq truncation, anonymous functions can get long. \nremoved after end of file. When unused, remove the "âœ¦\"\n\"\"
from import context manager || contextmanager || None
from import context_manager || None
