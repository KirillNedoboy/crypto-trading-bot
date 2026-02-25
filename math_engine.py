import logging
from typing import Optional
import pandas as pd
import config


def _calc_ema(series: pd.Series, period: int) -> pd.Series:
    """Вычисляет экспоненциальную скользящую среднюю (EMA)."""
    return series.ewm(span=period, adjust=False).mean()


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Вычисляет 5 технических индикаторов на чистом pandas:
    1. RSI  2. Bollinger Bands  3. MACD  4. EMA crossover  5. Volume SMA
    Дополнительно: Stochastic RSI (поверх RSI).
    """
    try:
        df = df.copy()
        close = df['close']

        # ── 1. RSI ──────────────────────────────────────
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(window=config.RSI_PERIOD).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=config.RSI_PERIOD).mean()
        rs = gain / loss.replace(0, 1e-10)
        df['RSI'] = 100 - (100 / (1 + rs))

        # ── 2. Bollinger Bands ──────────────────────────
        sma = close.rolling(window=config.BB_LENGTH).mean()
        std = close.rolling(window=config.BB_LENGTH).std()
        df['BB_LOWER'] = sma - (config.BB_STD * std)
        df['BB_UPPER'] = sma + (config.BB_STD * std)
        df['BB_MID'] = sma

        # ── 3. MACD ────────────────────────────────────
        ema_fast = _calc_ema(close, config.MACD_FAST)
        ema_slow = _calc_ema(close, config.MACD_SLOW)
        df['MACD_LINE'] = ema_fast - ema_slow
        df['MACD_SIGNAL'] = _calc_ema(df['MACD_LINE'], config.MACD_SIGNAL)
        df['MACD_HIST'] = df['MACD_LINE'] - df['MACD_SIGNAL']

        # ── 4. EMA Crossover (fast/slow) ────────────────
        df['EMA_FAST'] = _calc_ema(close, config.EMA_FAST)
        df['EMA_SLOW'] = _calc_ema(close, config.EMA_SLOW)

        # ── 5. Volume SMA ──────────────────────────────
        df['VOL_SMA'] = df['volume'].rolling(window=config.VOLUME_SMA_PERIOD).mean()

        # ── 6. Stochastic RSI ──────────────────────────
        rsi = df['RSI']
        rsi_min = rsi.rolling(window=config.STOCH_RSI_PERIOD).min()
        rsi_max = rsi.rolling(window=config.STOCH_RSI_PERIOD).max()
        stoch_rsi = (rsi - rsi_min) / (rsi_max - rsi_min).replace(0, 1e-10)
        df['STOCH_RSI_K'] = stoch_rsi.rolling(window=config.STOCH_RSI_K).mean() * 100
        df['STOCH_RSI_D'] = df['STOCH_RSI_K'].rolling(window=config.STOCH_RSI_D).mean()

        return df
    except Exception as e:
        logging.error(f"Ошибка вычисления индикаторов: {e}")
        return pd.DataFrame()


def _score_long(row: pd.Series) -> tuple[int, list[str]]:
    """
    Оценивает силу сигнала LONG по 5 критериям.
    Возвращает (score, [список сработавших индикаторов]).
    """
    score = 0
    reasons = []

    # 1. RSI < 30 → перепроданность
    if row['RSI'] < 30:
        score += 1
        reasons.append(f"RSI={row['RSI']:.1f} (<30)")

    # 2. Цена ниже нижней полосы Боллинджера
    if row['close'] < row['BB_LOWER']:
        score += 1
        reasons.append("Цена < BB Lower")

    # 3. MACD: гистограмма начинает расти (разворот вверх)
    if row['MACD_HIST'] > 0 or row['MACD_LINE'] > row['MACD_SIGNAL']:
        score += 1
        reasons.append("MACD бычий")

    # 4. EMA fast > EMA slow → бычий тренд
    if row['EMA_FAST'] > row['EMA_SLOW']:
        score += 1
        reasons.append("EMA9 > EMA21")

    # 5. Объём выше среднего → подтверждение движения
    if row['volume'] > row['VOL_SMA'] * 1.2:
        score += 1
        reasons.append("Объём ↑")

    # Бонус: Stochastic RSI в зоне перепроданности
    if row['STOCH_RSI_K'] < 20:
        score += 1
        reasons.append(f"StochRSI={row['STOCH_RSI_K']:.0f} (<20)")

    return score, reasons


def _score_short(row: pd.Series) -> tuple[int, list[str]]:
    """
    Оценивает силу сигнала SHORT по 5 критериям.
    Возвращает (score, [список сработавших индикаторов]).
    """
    score = 0
    reasons = []

    # 1. RSI > 70 → перекупленность
    if row['RSI'] > 70:
        score += 1
        reasons.append(f"RSI={row['RSI']:.1f} (>70)")

    # 2. Цена выше верхней полосы Боллинджера
    if row['close'] > row['BB_UPPER']:
        score += 1
        reasons.append("Цена > BB Upper")

    # 3. MACD: гистограмма отрицательная → медвежий импульс
    if row['MACD_HIST'] < 0 or row['MACD_LINE'] < row['MACD_SIGNAL']:
        score += 1
        reasons.append("MACD медвежий")

    # 4. EMA fast < EMA slow → медвежий тренд
    if row['EMA_FAST'] < row['EMA_SLOW']:
        score += 1
        reasons.append("EMA9 < EMA21")

    # 5. Объём выше среднего → подтверждение движения
    if row['volume'] > row['VOL_SMA'] * 1.2:
        score += 1
        reasons.append("Объём ↑")

    # Бонус: Stochastic RSI в зоне перекупленности
    if row['STOCH_RSI_K'] > 80:
        score += 1
        reasons.append(f"StochRSI={row['STOCH_RSI_K']:.0f} (>80)")

    return score, reasons


def _strength_label(score: int) -> str:
    """Человекочитаемая метка силы сигнала."""
    if score >= 5:
        return "🔥 Мощный"
    elif score >= 4:
        return "💪 Сильный"
    elif score >= 3:
        return "👍 Средний"
    return "⚠️ Слабый"


def evaluate_signal(symbol: str, df: pd.DataFrame) -> Optional[str]:
    """
    Проверяет условия для входа с системой Confluence Scoring.
    Сигнал генерируется, только если набрано >= MIN_CONFLUENCE_SCORE баллов.
    """
    if df.empty or len(df) < 2:
        return None

    last = df.iloc[-2]

    # Проверяем, что все индикаторы рассчитаны
    required_cols = ['RSI', 'BB_LOWER', 'BB_UPPER', 'MACD_HIST', 'EMA_FAST', 'EMA_SLOW', 'VOL_SMA']
    if any(col not in df.columns or pd.isna(last[col]) for col in required_cols):
        return None

    close_price = float(last['close'])

    # ── Оценка LONG ──
    long_score, long_reasons = _score_long(last)
    if long_score >= config.MIN_CONFLUENCE_SCORE:
        sl = close_price * (1 - config.STOP_LOSS_PCT)
        tp = close_price * (1 + config.TAKE_PROFIT_PCT)
        label = _strength_label(long_score)
        reasons_str = " | ".join(long_reasons)
        return (
            f"🟢 **LONG: {symbol}** {label} ({long_score}/6)\n"
            f"Вход: `{close_price}`\n"
            f"🎯 TP: `{tp:.4f}` | 🛡 SL: `{sl:.4f}`\n"
            f"📊 _{reasons_str}_"
        )

    # ── Оценка SHORT ──
    short_score, short_reasons = _score_short(last)
    if short_score >= config.MIN_CONFLUENCE_SCORE:
        sl = close_price * (1 + config.STOP_LOSS_PCT)
        tp = close_price * (1 - config.TAKE_PROFIT_PCT)
        label = _strength_label(short_score)
        reasons_str = " | ".join(short_reasons)
        return (
            f"🔴 **SHORT: {symbol}** {label} ({short_score}/6)\n"
            f"Вход: `{close_price}`\n"
            f"🎯 TP: `{tp:.4f}` | 🛡 SL: `{sl:.4f}`\n"
            f"📊 _{reasons_str}_"
        )

    return None
