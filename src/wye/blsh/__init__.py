import logging
from logging.handlers import TimedRotatingFileHandler
from wye.blsh.common.env import LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)

# _fmt = logging.Formatter("%(asctime)s %(name)s [%(levelname)s] %(message)s")
_fmt = logging.Formatter("%(asctime)s | %(name)s [%(levelname)s] %(message)s")
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_fmt)


def _new_file_handler(filename: str) -> TimedRotatingFileHandler:
    h = TimedRotatingFileHandler(
        LOG_DIR / filename,
        when="midnight",
        backupCount=30,
        encoding="utf-8",
    )
    h.suffix = "%Y-%m-%d"
    h.setFormatter(_fmt)
    return h


def new_logger(
    filestr: str, dedicated: bool = False, propagate: bool = True
) -> logging.Logger:
    import os

    rel_path = os.path.relpath(filestr, "/home/wye/workspace/blsh/src/")

    full_name = os.path.splitext(rel_path)[0].replace(os.sep, ".")
    logger = logging.getLogger(full_name)

    if dedicated:
        logger.addHandler(_new_file_handler(f"{full_name.rsplit('.', 1)[-1]}.log"))
    if not propagate:
        logger.propagate = False  # blsh.log로 중복 전파 방지
        logger.addHandler(_console_handler)  # 콘솔 출력 유지

    return logger


logging.basicConfig(
    level=logging.INFO,
    handlers=[_console_handler, _new_file_handler("blsh.log")],
)
