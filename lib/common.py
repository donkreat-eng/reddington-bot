"""Reddington bot — common utilities, env loading, logging."""
import os
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta


def _repo_root():
    return Path(__file__).parent.parent


CONFIG_DIR = Path.home() / ".config" / "reddington"
ENV_FILE = CONFIG_DIR / ".env"

LOG_DIR = _repo_root() / "logs"
POST_DIR = _repo_root() / "posts"
CHART_SRC = _repo_root() / "channel-preview"

LOG_DIR.mkdir(parents=True, exist_ok=True)
POST_DIR.mkdir(parents=True, exist_ok=True)


def load_env():
    """Load bot token and channel id from .env file or environment variables."""
    if ENV_FILE.exists():
        env = {}
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
        return env
    env = {}
    for k in ["REDDINGTON_BOT_TOKEN", "REDDINGTON_CHANNEL_ID", "REDDINGTON_CHANNEL_NAME"]:
        v = os.environ.get(k)
        if v:
            env[k] = v
    if not env:
        raise RuntimeError(f"No .env at {ENV_FILE} and no env vars set")
    return env


def setup_logger(name):
    """Logger that writes to file and stdout."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    log_file = LOG_DIR / f"{name}.log"
    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def ye_now():
    """Current time in Yekaterinburg timezone (UTC+5)."""
    ye = timezone(timedelta(hours=5))
    return datetime.now(ye)


def ye_str(dt=None, fmt="%Y-%m-%d %H:%M:%S"):
    if dt is None:
        dt = ye_now()
    return dt.strftime(fmt)
