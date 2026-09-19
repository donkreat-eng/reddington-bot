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
    # GitHub Actions: load from environment
    env = {}
    for k in ["REDDINGTON_BOT_TOKEN", "REDDINGTON_CHANNEL_ID", "REDDINGTON_CHANNEL_NAME"]:
        v = os.environ.get(k)
        if v:
            env[k] = v
    if not env:
        raise RuntimeError(f"No .env at {ENV_FILE} and no env vars set")
    return env


_LOGGERS: dict[str, logging.Logger] = {}


def setup_logger(name="market_overview"):
    """Logger that writes to file and stdout."""
    if name in _LOGGERS:
        return _LOGGERS[name]
    logger = logging.getLogger(name)
    if logger.handlers:
        _LOGGERS[name] = logger
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
    _LOGGERS[name] = logger
    return logger


def log(name: str, msg: str) -> None:
    """Quick log helper — uses cached logger by name."""
    setup_logger(name).info(msg)


def post_dir() -> Path:
    """Return POST_DIR as Path."""
    return POST_DIR


def ye_now():
    """Current time in Yekaterinburg timezone (UTC+5)."""
    ye = timezone(timedelta(hours=5))
    return datetime.now(ye)


def ye_str(dt=None, fmt="%Y-%m-%d %H:%M:%S"):
    if dt is None:
        dt = ye_now()
    return dt.strftime(fmt)
