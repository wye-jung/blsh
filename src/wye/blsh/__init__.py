import logging
from logging.handlers import TimedRotatingFileHandler
from wye.blsh.common.env import LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")


def _make_file_handler(filename: str) -> TimedRotatingFileHandler:
    h = TimedRotatingFileHandler(
        LOG_DIR / filename,
        when="midnight",
        backupCount=30,
        encoding="utf-8",
    )
    h.suffix = "%Y-%m-%d"
    h.setFormatter(_fmt)
    return h


_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_fmt)

logging.basicConfig(
    level=logging.INFO,
    handlers=[_console_handler, _make_file_handler("blsh.log")],
)
