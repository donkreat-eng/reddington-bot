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


_REQUIRED_ENV_KEYS = ["REDDINGTON_BOT_TOKEN", "REDDINGTON_CHANNEL_ID"]
_OPTIONAL_ENV_KEYS = ["REDDINGTON_CHANNEL_NAME"]


def _load_from_file():
    """Parse .env file in KEY=VALUE format."""
    if not ENV_FILE.exists():
        return {}
    env = {}
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def _load_from_os():
    """Read REDDINGTON_* env vars (GitHub Actions path)."""
    env = {}
    for k in _REQUIRED_ENV_KEYS + _OPTIONAL_ENV_KEYS:
        v = os.environ.get(k)
        if v:
            env[k] = v
    return env


def load_env():
    """Bot config. Required: REDDINGTON_BOT_TOKEN + REDDINGTON_CHANNEL_ID.

    Source priority:
    1. Environment variables (GitHub Actions secrets — always authoritative on CI).
    2. .env file at ~/.config/reddington/.env — local dev fallback.

    Sandbox contamination fix: on CI runners, secrets passed via $GITHUB_ENV
    always win, even if a stray .env exists in the runner home. Required keys
    that are missing raise RuntimeError — never silently fall back to stale .env.
    """
    os_env = _load_from_os()
    missing = [k for k in _REQUIRED_ENV_KEYS if k not in os_env]
    if not missing:
        return os_env

    file_env = _load_from_file()
    merged = {**file_env, **os_env}
    still_missing = [k for k in _REQUIRED_ENV_KEYS if k not in merged or not merged[k]]
    if still_missing:
        raise RuntimeError(
            f"Missing required env vars: {still_missing}. "
            f"Set them in GitHub Secrets or in {ENV_FILE}"
        )
    return merged


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
