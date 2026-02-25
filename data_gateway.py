import asyncio
import logging
from typing import Optional
import aiohttp
import pandas as pd

async def fetch_ohlcv_with_retry(
    session: aiohttp.ClientSession, symbol: str, timeframe: str, retries: int = 3
) -> Optional[pd.DataFrame]:
    """Получает свечи с Bybit API V5 с Exponential Backoff."""
    
    # Bybit API ожидает BTCUSDT, а не BTC/USDT
    api_symbol = symbol.replace('/', '')
    
    # Конвертация таймфрейма (в ССХТ '15m', в Bybit '15')
    interval = timeframe.replace('m', '')
    if 'h' in timeframe:
        interval = str(int(timeframe.replace('h', '')) * 60)
        
    url = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={api_symbol}&interval={interval}&limit=100"
    
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url) as response:
                if response.status == 429:
                    wait_time = 2 ** attempt
                    logging.warning(f"Rate limit {symbol}. Пауза {wait_time}с... ({attempt}/{retries})")
                    await asyncio.sleep(wait_time)
                    continue
                    
                response.raise_for_status()
                data = await response.json()
                
                if data.get('retCode') != 0:
                    logging.error(f"Ошибка API Bybit {symbol}: {data.get('retMsg')}")
                    break
                    
                # Данные приходят от новых спадающим к старым (descending)
                # Нужно перевернуть список
                kline_list = data['result']['list'][::-1]
                
                # Формат: [startTime, openPrice, highPrice, lowPrice, closePrice, volume, turnover]
                df = pd.DataFrame(kline_list, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
                
                # Приводим к числовым типам
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    df[col] = df[col].astype(float)
                
                return df
                
        except aiohttp.ClientError as e:
            wait_time = 2 ** attempt
            logging.warning(f"Ошибка сети {symbol}: {e}. Пауза {wait_time}с... ({attempt}/{retries})")
            await asyncio.sleep(wait_time)
        except Exception as e:
            logging.error(f"Неизвестная ошибка загрузки {symbol}: {e}", exc_info=True)
            break
            
    return None
