import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

if not BOT_TOKEN:
    raise ValueError("ОШИБКА: BOT_TOKEN не найден в .env!")

# Параметры стратегии
TICKERS = [
    'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'ADA/USDT', 
    'AVAX/USDT', 'DOGE/USDT', 'DOT/USDT', 'LINK/USDT', 'MATIC/USDT',
    'MET/USDT', 'TOWNS/USDT'
]

TIMEFRAME = '15m'          
RSI_PERIOD = 14            
BB_LENGTH = 20             
BB_STD = 2.0               

# MACD (Moving Average Convergence Divergence)
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# EMA Crossover — быстрая и медленная скользящие средние
EMA_FAST = 9
EMA_SLOW = 21

# Volume — период скользящей средней объёмов
VOLUME_SMA_PERIOD = 20

# Stochastic RSI
STOCH_RSI_PERIOD = 14
STOCH_RSI_K = 3  # %K smoothing
STOCH_RSI_D = 3  # %D smoothing

# Confluence: минимум баллов для отправки сигнала (из 5 возможных)
MIN_CONFLUENCE_SCORE = 3

STOP_LOSS_PCT = 0.015      
TAKE_PROFIT_PCT = 0.030    
