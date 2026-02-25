import asyncio
import logging
import aiohttp
from aiogram import Bot
from typing import Optional

import config
from database import get_all_subscribers
from data_gateway import fetch_ohlcv_with_retry
from math_engine import calculate_indicators, evaluate_signal


async def analyze_single_coin(session: aiohttp.ClientSession, symbol: str) -> str:
    """Анализирует одну монету и возвращает подробный отчёт."""
    df = await fetch_ohlcv_with_retry(session, symbol, config.TIMEFRAME)
    if df is None:
        return f"❌ Не удалось получить данные для {symbol}"

    df = calculate_indicators(df)
    if df.empty or len(df) < 2:
        return f"❌ Недостаточно данных для анализа {symbol}"

    last = df.iloc[-2]
    close = float(last['close'])

    # Формируем детальный отчёт по индикаторам
    rsi = float(last['RSI']) if 'RSI' in df.columns else 0
    bb_lower = float(last['BB_LOWER']) if 'BB_LOWER' in df.columns else 0
    bb_upper = float(last['BB_UPPER']) if 'BB_UPPER' in df.columns else 0
    macd_hist = float(last['MACD_HIST']) if 'MACD_HIST' in df.columns else 0
    ema_f = float(last['EMA_FAST']) if 'EMA_FAST' in df.columns else 0
    ema_s = float(last['EMA_SLOW']) if 'EMA_SLOW' in df.columns else 0
    stoch = float(last['STOCH_RSI_K']) if 'STOCH_RSI_K' in df.columns else 0

    # Определяем направление каждого индикатора
    rsi_icon = "🟢" if rsi < 30 else ("🔴" if rsi > 70 else "⚪")
    bb_icon = "🟢" if close < bb_lower else ("🔴" if close > bb_upper else "⚪")
    macd_icon = "🟢" if macd_hist > 0 else "🔴"
    ema_icon = "🟢" if ema_f > ema_s else "🔴"
    stoch_icon = "🟢" if stoch < 20 else ("🔴" if stoch > 80 else "⚪")

    # Проверяем наличие сигнала
    signal = evaluate_signal(symbol, df)
    signal_line = f"\n\n{signal}" if signal else "\n\n⚪ _Нет активного сигнала_"

    return (
        f"📊 **{symbol}** | `{close}`\n\n"
        f"{rsi_icon} RSI: `{rsi:.1f}`\n"
        f"{bb_icon} BB: `{bb_lower:.2f}` — `{bb_upper:.2f}`\n"
        f"{macd_icon} MACD Hist: `{macd_hist:.4f}`\n"
        f"{ema_icon} EMA 9/21: `{ema_f:.2f}` / `{ema_s:.2f}`\n"
        f"{stoch_icon} StochRSI: `{stoch:.1f}`"
        f"{signal_line}"
    )


async def scan_market_now(session: aiohttp.ClientSession) -> str:
    """Мгновенное сканирование всех монет. Возвращает текст результата."""
    signals = []
    no_signal_coins = []

    for symbol in config.TICKERS:
        df = await fetch_ohlcv_with_retry(session, symbol, config.TIMEFRAME)
        if df is not None:
            df_with_inds = calculate_indicators(df)
            signal = evaluate_signal(symbol, df_with_inds)
            if signal:
                signals.append(signal)
            else:
                no_signal_coins.append(symbol.split('/')[0])
        await asyncio.sleep(0.3)

    if signals:
        result = "⚡️ **Найдены сигналы:**\n\n" + "\n\n".join(signals)
    else:
        result = "⚪ **Сигналов нет**"

    result += f"\n\n_Без сигнала: {', '.join(no_signal_coins)}_"
    return result


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

            await asyncio.sleep(0.5)

        if signals:
            subscribers = await get_all_subscribers()
            if not subscribers:
                logging.warning("Есть сигналы, но нет подписчиков для отправки.")
                return

            message_text = "⚡️ **Новые торговые сигналы:**\n\n" + "\n\n".join(signals)

            for chat_id in subscribers:
                try:
                    await bot.send_message(chat_id=chat_id, text=message_text)
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logging.error(f"Ошибка отправки пользователю {chat_id}: {e}")

            logging.info(f"Отправлено {len(signals)} сигналов {len(subscribers)} пользователям.")
        else:
            logging.info("Цикл завершен. Сигналов нет.")

    except Exception as e:
        logging.error(f"Критическая ошибка в потоке сканирования: {e}", exc_info=True)
