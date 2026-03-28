import asyncio
from wye.blsh.common.env import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
import logging

log = logging.getLogger(__name__)


def send_message(message):
    try:
        from telegram import Bot

        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        asyncio.run(bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message))
    except Exception as e:
        log.warning(e)
