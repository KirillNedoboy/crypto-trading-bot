import asyncio
import logging
import os
import aiohttp

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config
from database import init_db, add_subscriber, remove_subscriber, get_all_subscribers
from orchestrator import scan_market_and_notify

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone="UTC")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Пользователь запускает бота и подписывается на рассылку."""
    added = await add_subscriber(message.chat.id)
    if added:
        await message.answer("✅ Вы успешно подписаны на торговые сигналы!")
        logging.info(f"Новый подписчик: {message.chat.id}")
    else:
        await message.answer("⚠️ Вы уже подписаны на рассылку.")

@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    """Пользователь отписывается от рассылки."""
    await remove_subscriber(message.chat.id)
    await message.answer("❌ Вы отписаны от торговых сигналов.")
    logging.info(f"Пользователь отписался: {message.chat.id}")

@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    """Проверка статуса."""
    subs = await get_all_subscribers()
    await message.answer(f"🤖 Бот работает.\n📈 Текущих подписчиков: {len(subs)}")

async def main():
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    async with aiohttp.ClientSession() as session:
        @dp.startup()
        async def on_startup():
            logging.info("Запуск Telegram-бота. Инициализация БД...")
            await init_db()
            
            # Добавляем админа по умолчанию
            await add_subscriber(config.ADMIN_ID)
            
            logging.info("Настройка APScheduler...")
            # Настраиваем CRON: ровно в 00, 15, 30 и 45 минут каждого часа
            scheduler.add_job(
                scan_market_and_notify,
                trigger='cron',
                minute='0,15,30,45',
                kwargs={'bot': bot, 'session': session}
            )
            scheduler.start()
            
            try:
                await bot.send_message(
                    chat_id=config.ADMIN_ID, 
                    text="✅ Бот аналитики запущен. Forward Testing инициализирован.\nИнтервал: *15 минут*."
                )
            except Exception as e:
                logging.error(f"Не удалось отправить стартовое сообщение админу: {e}")

        @dp.shutdown()
        async def on_shutdown():
            logging.warning("Получен сигнал на остановку (Graceful Shutdown)...")
            scheduler.shutdown(wait=False)
            await bot.session.close()
            logging.info("Ресурсы освобождены. Бот выключен.")

        try:
            await dp.start_polling(bot)
        except (KeyboardInterrupt, SystemExit):
            logging.info("Выход из программы...")

if __name__ == '__main__':
    asyncio.run(main())
