"""
Estrategia VWAP + Bandas de Desviación Estándar
"""
import numpy as np
import pandas as pd
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class Signal(Enum):
    LONG  = "LONG"
    SHORT = "SHORT"
    NONE  = "NONE"


@dataclass
class TradeSignal:
    signal:        Signal
    entry_price:   float
    tp_price:      float
    sl_price:      float
    vwap:          float
    deviation_band: int
    confidence:    str
    reason:        str


def compute_vwap_bands(df: pd.DataFrame, num_std: list = [1, 2, 3]) -> pd.DataFrame:
    df = df.copy()
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["tp_volume"]     = df["typical_price"] * df["volume"]
    df["date"]          = pd.to_datetime(df["timestamp"], unit="ms").dt.date
    df["cum_tp_vol"]    = df.groupby("date")["tp_volume"].cumsum()
    df["cum_vol"]       = df.groupby("date")["volume"].cumsum()
    df["vwap"]          = df["cum_tp_vol"] / df["cum_vol"]
    df["tp_vwap_diff_sq"] = (df["typical_price"] - df["vwap"]) ** 2
    df["cum_var"]       = df.groupby("date")["tp_vwap_diff_sq"].cumsum()
    df["vwap_std"]      = np.sqrt(df["cum_var"] / df.groupby("date").cumcount().add(1))
    for sd in num_std:
        df[f"vwap_upper_{sd}"] = df["vwap"] + (sd * df["vwap_std"])
        df[f"vwap_lower_{sd}"] = df["vwap"] - (sd * df["vwap_std"])
    return df


def detect_reversal_candle(row: pd.Series, direction: str) -> bool:
    body        = abs(row["close"] - row["open"])
    total_range = row["high"] - row["low"]
    if total_range == 0:
        return False
    body_ratio  = body / total_range
    upper_wick  = row["high"] - max(row["open"], row["close"])
    lower_wick  = min(row["open"], row["close"]) - row["low"]
    if direction == "bullish":
        wick_ratio = lower_wick / total_range
        return wick_ratio > 0.55 or (row["close"] > row["open"] and body_ratio > 0.5)
    elif direction == "bearish":
        wick_ratio = upper_wick / total_range
        return wick_ratio > 0.55 or (row["close"] < row["open"] and body_ratio > 0.5)
    return False


class VWAPMeanReversionStrategy:
    def __init__(self, config: dict):
        self.min_band      = config.get("min_band", 2)
        self.sl_multiplier = config.get("sl_multiplier", 1.5)
        self.min_vwap_std  = config.get("min_vwap_std", 0.001)

    def analyze(self, df: pd.DataFrame) -> TradeSignal:
        if len(df) < 50:
            return TradeSignal(Signal.NONE, 0, 0, 0, 0, 0, "LOW", "Datos insuficientes")

        df   = compute_vwap_bands(df)
        last = df.iloc[-1]
        vwap = last["vwap"]
        close= last["close"]
        vwap_std = last["vwap_std"]

        if vwap_std / vwap < self.min_vwap_std:
            return TradeSignal(Signal.NONE, 0, 0, 0, vwap, 0, "LOW", "Volatilidad insuficiente")

        atr_val = np.mean(df["high"].tail(14).values - df["low"].tail(14).values)

        for band in [3, 2]:
            upper = last[f"vwap_upper_{band}"]
            if close >= upper and band >= self.min_band:
                if detect_reversal_candle(last, "bearish"):
                    sl = last["high"] + atr_val * 0.5
                    tp = vwap
                    rr = (close - tp) / max(sl - close, 0.0001)
                    return TradeSignal(Signal.SHORT, close, round(tp, 4), round(sl, 4),
                                       round(vwap, 4), band,
                                       "HIGH" if band == 3 else "MEDIUM",
                                       f"+{band}SD VWAP con vela bajista. R:R={round(rr, 2)}")

        for band in [3, 2]:
            lower = last[f"vwap_lower_{band}"]
            if close <= lower and band >= self.min_band:
                if detect_reversal_candle(last, "bullish"):
                    sl = last["low"] - atr_val * 0.5
                    tp = vwap
                    rr = (tp - close) / max(close - sl, 0.0001)
                    return TradeSignal(Signal.LONG, close, round(tp, 4), round(sl, 4),
                                       round(vwap, 4), band,
                                       "HIGH" if band == 3 else "MEDIUM",
                                       f"-{band}SD VWAP con vela alcista. R:R={round(rr, 2)}")

        return TradeSignal(Signal.NONE, 0, 0, 0, round(vwap, 4), 0, "LOW", "Sin señal")
