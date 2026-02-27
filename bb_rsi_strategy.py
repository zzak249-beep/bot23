"""
Estrategia BB + RSI (Mean Reversion) + EMA Ribbon Scalping
"""
import numpy as np
import pandas as pd
import logging
from vwap_strategy import Signal, TradeSignal, detect_reversal_candle

logger = logging.getLogger(__name__)


def compute_bollinger_bands(df, period=20, std_mult=2.0):
    df = df.copy()
    df["bb_mid"]   = df["close"].rolling(period).mean()
    bb_std         = df["close"].rolling(period).std()
    df["bb_upper"] = df["bb_mid"] + std_mult * bb_std
    df["bb_lower"] = df["bb_mid"] - std_mult * bb_std
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]
    return df


def compute_rsi(df, period=14):
    df    = df.copy()
    delta = df["close"].diff()
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs        = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))
    return df


def compute_atr(df, period=14):
    df = df.copy()
    df["tr"] = np.maximum(
        df["high"] - df["low"],
        np.maximum(abs(df["high"] - df["close"].shift(1)),
                   abs(df["low"]  - df["close"].shift(1)))
    )
    df["atr"] = df["tr"].ewm(span=period, adjust=False).mean()
    return df


class BBRSIMeanReversionStrategy:
    def __init__(self, config: dict):
        self.bb_period      = config.get("bb_period", 20)
        self.bb_std         = config.get("bb_std", 2.0)
        self.rsi_period     = config.get("rsi_period", 14)
        self.rsi_oversold   = config.get("rsi_oversold", 30)
        self.rsi_overbought = config.get("rsi_overbought", 70)
        self.rr_ratio       = config.get("rr_ratio", 0.75)
        self.min_bb_width   = config.get("min_bb_width", 0.005)

    def analyze(self, df: pd.DataFrame) -> TradeSignal:
        if len(df) < self.bb_period + self.rsi_period:
            return TradeSignal(Signal.NONE, 0, 0, 0, 0, 0, "LOW", "Datos insuficientes")

        df   = compute_bollinger_bands(df, self.bb_period, self.bb_std)
        df   = compute_rsi(df, self.rsi_period)
        df   = compute_atr(df)
        last = df.iloc[-1]

        close    = last["close"]
        rsi      = last["rsi"]
        bb_upper = last["bb_upper"]
        bb_lower = last["bb_lower"]
        bb_mid   = last["bb_mid"]
        atr      = last["atr"]
        bb_width = last["bb_width"]

        if pd.isna(rsi) or pd.isna(bb_mid):
            return TradeSignal(Signal.NONE, 0, 0, 0, 0, 0, "LOW", "Indicadores sin calcular")
        if bb_width < self.min_bb_width:
            return TradeSignal(Signal.NONE, 0, 0, 0, round(bb_mid, 4), 0, "LOW", "BB Squeeze")

        if rsi < self.rsi_oversold and close < bb_lower:
            if detect_reversal_candle(last, "bullish"):
                gain = bb_mid - close
                sl   = max(close - gain / self.rr_ratio, last["low"] - atr * 0.3)
                rr   = gain / max(close - sl, 0.0001)
                conf = "HIGH" if rsi < 20 else "MEDIUM"
                return TradeSignal(Signal.LONG, close, round(bb_mid, 4), round(sl, 4),
                                   round(bb_mid, 4), 2, conf,
                                   f"BB Lower + RSI={round(rsi,1)}. TP=BB Mid. R:R={round(rr,2)}")

        if rsi > self.rsi_overbought and close > bb_upper:
            if detect_reversal_candle(last, "bearish"):
                gain = close - bb_mid
                sl   = min(close + gain / self.rr_ratio, last["high"] + atr * 0.3)
                rr   = gain / max(sl - close, 0.0001)
                conf = "HIGH" if rsi > 80 else "MEDIUM"
                return TradeSignal(Signal.SHORT, close, round(bb_mid, 4), round(sl, 4),
                                   round(bb_mid, 4), 2, conf,
                                   f"BB Upper + RSI={round(rsi,1)}. TP=BB Mid. R:R={round(rr,2)}")

        return TradeSignal(Signal.NONE, 0, 0, 0, round(bb_mid, 4), 0, "LOW", "Sin señal")


class EMARibbonScalpingStrategy:
    def __init__(self, config: dict):
        self.ema_fast      = config.get("ema_fast", 9)
        self.ema_slow      = config.get("ema_slow", 15)
        self.ma_macro      = config.get("ma_macro", 200)
        self.rsi_period    = config.get("rsi_period", 14)
        self.rsi_bull_min  = config.get("rsi_bull_min", 50)
        self.rsi_bear_max  = config.get("rsi_bear_max", 50)

    def analyze(self, df: pd.DataFrame) -> TradeSignal:
        if len(df) < self.ma_macro + 10:
            return TradeSignal(Signal.NONE, 0, 0, 0, 0, 0, "LOW", "Datos insuficientes para MA200")

        df = df.copy()
        df["ema_fast"] = df["close"].ewm(span=self.ema_fast, adjust=False).mean()
        df["ema_slow"] = df["close"].ewm(span=self.ema_slow, adjust=False).mean()
        df["ma200"]    = df["close"].rolling(self.ma_macro).mean()
        df = compute_rsi(df, self.rsi_period)
        df = compute_atr(df)

        last = df.iloc[-1]
        prev = df.iloc[-2]
        close    = last["close"]
        ema_fast = last["ema_fast"]
        ema_slow = last["ema_slow"]
        ma200    = last["ma200"]
        rsi      = last["rsi"]
        atr      = last["atr"]

        if pd.isna(ma200) or pd.isna(rsi):
            return TradeSignal(Signal.NONE, 0, 0, 0, 0, 0, "LOW", "Indicadores sin calcular")

        if ema_fast > ema_slow and close > ma200 and rsi > self.rsi_bull_min:
            touched = prev["low"] <= ema_fast or (prev["close"] <= ema_fast and close > ema_fast)
            if touched and detect_reversal_candle(last, "bullish"):
                sl = last["low"] - atr * 0.3
                tp = close + atr * 2.0
                rr = (tp - close) / max(close - sl, 0.0001)
                return TradeSignal(Signal.LONG, close, round(tp, 4), round(sl, 4),
                                   round(ema_slow, 4), 0,
                                   "HIGH" if rsi > 60 else "MEDIUM",
                                   f"EMA Scalp LONG | RSI={round(rsi,1)} | sobre MA200 | R:R={round(rr,2)}")

        elif ema_fast < ema_slow and close < ma200 and rsi < self.rsi_bear_max:
            touched = prev["high"] >= ema_fast or (prev["close"] >= ema_fast and close < ema_fast)
            if touched and detect_reversal_candle(last, "bearish"):
                sl = last["high"] + atr * 0.3
                tp = close - atr * 2.0
                rr = (close - tp) / max(sl - close, 0.0001)
                return TradeSignal(Signal.SHORT, close, round(tp, 4), round(sl, 4),
                                   round(ema_slow, 4), 0,
                                   "HIGH" if rsi < 40 else "MEDIUM",
                                   f"EMA Scalp SHORT | RSI={round(rsi,1)} | bajo MA200 | R:R={round(rr,2)}")

        return TradeSignal(Signal.NONE, 0, 0, 0, 0, 0, "LOW", "Sin señal de scalping")
