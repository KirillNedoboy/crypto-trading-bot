import asyncio
import logging
import aiohttp
from aiogram import Bot

import config
from database import get_all_subscribers
from data_gateway import fetch_ohlcv_with_retry
from math_engine import calculate_indicators, evaluate_signal

async def scan_market_and_notify(bot: Bot, session: aiohttp.ClientSession):
    """Задача для планировщика: анализ и отправка всем подписчикам."""
    logging.info("Инициализирован цикл сканирования...")
    signals = []
    
    try:
        for symbol in config.TICKERS:
            df = await fetch_ohlcv_with_retry(session, symbol, config.TIMEFRAME)
            if df is not None:
                df_with_inds = calculate_indicators(df)
                signal = evaluate_signal(symbol, df_with_inds)
                
                if signal:
                    signals.append(signal)
                    logging.info(f"Найден сигнал: {symbol}")
            
            # Anti-ban sleep
            await asyncio.sleep(0.5)
            
        # Отправляем результаты
        if signals:
            subscribers = await get_all_subscribers()
            if not subscribers:
                logging.warning("Есть сигналы, но нет подписчиков для отправки.")
                return

            message_text = "⚡️ **Новые торговые сигналы:**\n\n" + "\n\n".join(signals)
            
            for chat_id in subscribers:
                try:
                    await bot.send_message(chat_id=chat_id, text=message_text)
                    await asyncio.sleep(0.1) # Защита от спам-лимитов Telegram
                except Exception as e:
                    logging.error(f"Ошибка отправки пользователю {chat_id}: {e}")
                    
            logging.info(f"Отправлено {len(signals)} сигналов {len(subscribers)} пользователям.")
        else:
            logging.info("Цикл завершен. Сигналов нет.")
            
    except Exception as e:
        logging.error(f"Критическая ошибка в потоке сканирования: {e}", exc_info=True)
