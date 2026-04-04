import asyncio
import threading
from wye.blsh.common.env import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
import logging

log = logging.getLogger(__name__)

# 전용 이벤트 루프 — 데몬 스레드에서 상시 구동
# asyncio.run()은 이미 실행 중인 루프가 있는 스레드(e.g. ws_monitor)에서
# RuntimeError를 던지므로, 별도 루프 + run_coroutine_threadsafe()로 대체
_loop = asyncio.new_event_loop()
threading.Thread(target=_loop.run_forever, daemon=True, name="telegram-loop").start()

_bot = None


def _get_bot():
    global _bot
    if _bot is None:
        from telegram import Bot
        _bot = Bot(token=TELEGRAM_BOT_TOKEN)
    return _bot


def send_message(message: str, timeout: float = 10) -> None:
    try:
        future = asyncio.run_coroutine_threadsafe(
            _get_bot().send_message(chat_id=TELEGRAM_CHAT_ID, text=message),
            _loop,
        )
        future.result(timeout=timeout)
    except Exception as e:
        log.warning(e)
