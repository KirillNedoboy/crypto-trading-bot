import asyncio
import logging
import os
import aiohttp

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config
from database import init_db, add_subscriber, remove_subscriber, get_all_subscribers
from orchestrator import scan_market_and_notify, scan_market_now, analyze_single_coin

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone="UTC")

# Глобальная сессия (инициализируется в main)
http_session: aiohttp.ClientSession = None


# ==========================================
# КЛАВИАТУРЫ
# ==========================================
def get_main_menu_kb() -> InlineKeyboardMarkup:
    """Главное меню бота."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Сканировать рынок", callback_data="scan_all")],
        [InlineKeyboardButton(text="📊 Анализ монеты", callback_data="pick_coin")],
        [InlineKeyboardButton(text="📈 Статус бота", callback_data="bot_status")],
    ])


def get_coins_kb() -> InlineKeyboardMarkup:
    """Сетка кнопок для выбора монеты."""
    buttons = []
    row = []
    for i, ticker in enumerate(config.TICKERS):
        short_name = ticker.split('/')[0]
        row.append(InlineKeyboardButton(text=short_name, callback_data=f"coin_{ticker}"))
        if len(row) == 4:  # 4 кнопки в ряд
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_back_kb() -> InlineKeyboardMarkup:
    """Кнопка 'Назад' к главному меню."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Меню", callback_data="main_menu"),
         InlineKeyboardButton(text="📊 Другая монета", callback_data="pick_coin")]
    ])


# ==========================================
# КОМАНДЫ
# ==========================================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Пользователь запускает бота и подписывается на рассылку."""
    added = await add_subscriber(message.chat.id)
    if added:
        text = "✅ Вы подписаны на торговые сигналы!"
        logging.info(f"Новый подписчик: {message.chat.id}")
    else:
        text = "👋 С возвращением!"
    await message.answer(text, reply_markup=get_main_menu_kb())


@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    """Пользователь отписывается от рассылки."""
    await remove_subscriber(message.chat.id)
    await message.answer("❌ Вы отписаны от торговых сигналов.")
    logging.info(f"Пользователь отписался: {message.chat.id}")


@dp.message(Command("menu"))
async def cmd_menu(message: types.Message):
    """Показать главное меню."""
    await message.answer("🤖 **Главное меню:**", reply_markup=get_main_menu_kb())


# ==========================================
# ОБРАБОТЧИКИ INLINE-КНОПОК
# ==========================================
@dp.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: CallbackQuery):
    """Возврат в главное меню."""
    await callback.message.edit_text("🤖 **Главное меню:**", reply_markup=get_main_menu_kb())
    await callback.answer()


@dp.callback_query(F.data == "scan_all")
async def cb_scan_all(callback: CallbackQuery):
    """Мгновенное сканирование всех монет."""
    await callback.message.edit_text("⏳ _Сканирую рынок..._")
    await callback.answer()

    result = await scan_market_now(http_session)
    await callback.message.edit_text(result, reply_markup=get_main_menu_kb())


@dp.callback_query(F.data == "pick_coin")
async def cb_pick_coin(callback: CallbackQuery):
    """Показать сетку монет для выбора."""
    await callback.message.edit_text("🪙 **Выберите монету:**", reply_markup=get_coins_kb())
    await callback.answer()


@dp.callback_query(F.data.startswith("coin_"))
async def cb_coin_detail(callback: CallbackQuery):
    """Анализ конкретной монеты."""
    symbol = callback.data.replace("coin_", "")
    await callback.message.edit_text(f"⏳ _Анализирую {symbol}..._")
    await callback.answer()

    result = await analyze_single_coin(http_session, symbol)
    await callback.message.edit_text(result, reply_markup=get_back_kb())


@dp.callback_query(F.data == "bot_status")
async def cb_status(callback: CallbackQuery):
    """Статус бота."""
    subs = await get_all_subscribers()
    coins = len(config.TICKERS)
    text = (
        f"🤖 **Статус бота**\n\n"
        f"📈 Монет в списке: `{coins}`\n"
        f"👥 Подписчиков: `{len(subs)}`\n"
        f"⏱ Интервал: `{config.TIMEFRAME}`\n"
        f"🎯 Мин. confluence: `{config.MIN_CONFLUENCE_SCORE}/6`"
    )
    await callback.message.edit_text(text, reply_markup=get_main_menu_kb())
    await callback.answer()


# ==========================================
# ЗАПУСК И GRACEFUL SHUTDOWN
# ==========================================
async def main():
    global http_session

    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    async with aiohttp.ClientSession() as session:
        http_session = session

        @dp.startup()
        async def on_startup():
            logging.info("Запуск Telegram-бота. Инициализация БД...")
            await init_db()
            await add_subscriber(config.ADMIN_ID)

            logging.info("Настройка APScheduler...")
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
                    text="✅ Бот запущен. Нажмите /menu для управления.",
                    reply_markup=get_main_menu_kb()
                )
            except Exception as e:
                logging.error(f"Не удалось отправить стартовое сообщение: {e}")

        @dp.shutdown()
        async def on_shutdown():
            logging.warning("Graceful Shutdown...")
            scheduler.shutdown(wait=False)
            await bot.session.close()
            logging.info("Бот выключен.")

        try:
            await dp.start_polling(bot)
        except (KeyboardInterrupt, SystemExit):
            logging.info("Выход из программы...")

if __name__ == '__main__':
    asyncio.run(main())
