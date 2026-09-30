"""Deterministic, long-only/short-only signal rules for the Binance scanner.

Signals are research candidates, not estimated probabilities or trade instructions.
All inputs are closed candles; the caller aligns higher timeframes to the 5m close.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class StrategyConfig:
    min_quote_volume_24h: float = 15_000_000.0
    max_spread_pct: float = 0.0005
    max_entry_drift_pct: float = 0.002
    breakout_lookback_5m: int = 3
    volume_lookback_5m: int = 20
    min_volume_ratio: float = 1.0
    pullback_lookback_15m: int = 3
    pullback_max_overshoot_atr: float = 0.3
    atr_period: int = 14
    rsi_period: int = 14
    stop_atr: float = 1.5
    target_r_multiples: tuple[float, float, float] = (2.0, 3.0, 4.0)
    target_fractions: tuple[float, float, float] = (0.5, 0.25, 0.25)
    max_hold_minutes: int = 240
    # Conservative per execution estimate: taker fee + half-spread + slippage.
    cost_per_fill_pct: float = 0.001
    # One score ranks candidates within a side; it is not a probability.
    score_weights: tuple[float, float, float, float] = (0.35, 0.25, 0.25, 0.15)
    stable_base_assets: frozenset[str] = frozenset(
        {"USDC", "FDUSD", "USDE", "TUSD", "USDP", "DAI", "BUSD", "UST", "USTC"}
    )


CONFIG = StrategyConfig()


def _rma(values: pd.Series, period: int) -> pd.Series:
    """Wilder moving average with the initial simple mean as its seed."""
    source = values.astype(float)
    result = pd.Series(np.nan, index=source.index, dtype=float)
    if len(source) < period:
        return result
    seed_index = source.first_valid_index()
    if seed_index is None:
        return result
    start = source.index.get_loc(seed_index)
    seed_end = start + period
    if seed_end > len(source):
        return result
    seed_values = source.iloc[start:seed_end]
    if seed_values.isna().any():
        return result
    value = float(seed_values.mean())
    result.iloc[seed_end - 1] = value
    for i in range(seed_end, len(source)):
        current = source.iloc[i]
        if pd.isna(current):
            continue
        value = ((period - 1) * value + float(current)) / period
        result.iloc[i] = value
    return result


def add_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with EMA20/50/200, Wilder RSI14 and ATR14."""
    result = frame.copy()
    close = result["close"].astype(float)
    high = result["high"].astype(float)
    low = result["low"].astype(float)
    result["ema20"] = close.ewm(span=20, adjust=False, min_periods=20).mean()
    result["ema50"] = close.ewm(span=50, adjust=False, min_periods=50).mean()
    result["ema200"] = close.ewm(span=200, adjust=False, min_periods=200).mean()

    delta = close.diff()
    avg_gain = _rma(delta.clip(lower=0), CONFIG.rsi_period)
    avg_loss = _rma((-delta).clip(lower=0), CONFIG.rsi_period)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    result["rsi"] = 100 - (100 / (1 + rs))
    result.loc[(avg_loss == 0) & (avg_gain > 0), "rsi"] = 100.0
    result.loc[(avg_gain == 0) & (avg_loss > 0), "rsi"] = 0.0
    result.loc[(avg_gain == 0) & (avg_loss == 0), "rsi"] = 50.0

    previous_close = close.shift(1)
    true_range = pd.concat(
        [high - low, (high - previous_close).abs(), (low - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    result["atr"] = _rma(true_range, CONFIG.atr_period)
    return result


def _latest_at_or_before(frame: pd.DataFrame, close_time_ms: int) -> pd.DataFrame:
    return frame.loc[frame["close_time"].astype("int64") <= int(close_time_ms)]


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def rank_score(
    side: str,
    volume_ratio: float,
    rsi: float,
    trend_separation_atr: float,
    breakout_atr: float,
) -> tuple[float, dict[str, float]]:
    """Transparent quality score 0..100; not a probability or expected return."""
    if side == "LONG":
        rsi_mid, rsi_half_width = 41.5, 6.5
        signed_separation = trend_separation_atr
    else:
        rsi_mid, rsi_half_width = 58.5, 6.5
        signed_separation = trend_separation_atr

    volume_score = _clamp01(volume_ratio / 2.0)
    rsi_score = _clamp01(1 - abs(rsi - rsi_mid) / rsi_half_width)
    # The directional EMA20/EMA50 stack is already a hard filter. Saturating
    # this component avoids rewarding arbitrarily stretched price/EMA distance.
    trend_score = _clamp01((signed_separation - 0.2) / 1.3)
    breakout_score = _clamp01(breakout_atr / 0.5)
    components = {
        "volume": round(volume_score * 100, 1),
        "rsi": round(rsi_score * 100, 1),
        "trend": round(trend_score * 100, 1),
        "breakout": round(breakout_score * 100, 1),
    }
    weighted = (
        CONFIG.score_weights[0] * volume_score
        + CONFIG.score_weights[1] * rsi_score
        + CONFIG.score_weights[2] * trend_score
        + CONFIG.score_weights[3] * breakout_score
    )
    return round(weighted * 100, 1), components


def find_signal(
    candles_5m: pd.DataFrame,
    candles_15m: pd.DataFrame,
    candles_1h: pd.DataFrame,
    *,
    base_asset: str,
) -> dict[str, Any] | None:
    """Evaluate the latest fully closed 5m candle against aligned HTF candles."""
    if base_asset.upper() in CONFIG.stable_base_assets:
        return None

    frame5 = add_indicators(candles_5m)
    if len(frame5) < max(CONFIG.volume_lookback_5m + 1, 25):
        return None
    trigger = frame5.iloc[-1]
    prior5 = frame5.iloc[-(CONFIG.volume_lookback_5m + 1):-1]
    breakout_bars = frame5.iloc[-(CONFIG.breakout_lookback_5m + 1):-1]

    frame15 = add_indicators(_latest_at_or_before(candles_15m, int(trigger.close_time)))
    frame1h = add_indicators(_latest_at_or_before(candles_1h, int(trigger.close_time)))
    if len(frame15) < 55 or len(frame1h) < 205:
        return None
    setup15 = frame15.iloc[-1]
    trend1h = frame1h.iloc[-1]
    recent15 = frame15.iloc[-CONFIG.pullback_lookback_15m:]

    needed = [
        trigger.get("atr"), setup15.get("ema20"), setup15.get("ema50"),
        setup15.get("atr"), setup15.get("rsi"), trend1h.get("ema20"),
        trend1h.get("ema50"), trend1h.get("ema200"), trend1h.get("atr"),
    ]
    if any(pd.isna(value) for value in needed):
        return None
    if prior5["volume"].mean() <= 0 or trigger.atr <= 0 or setup15.atr <= 0 or trend1h.atr <= 0:
        return None

    volume_ratio = float(trigger.volume / prior5["volume"].mean())
    previous_high = float(breakout_bars["high"].max())
    previous_low = float(breakout_bars["low"].min())
    zone_min = min(float(setup15.ema20), float(setup15.ema50))
    zone_max = max(float(setup15.ema20), float(setup15.ema50))
    touched_long = (
        float(recent15["low"].min()) <= zone_max
        and float(recent15["low"].min()) >= zone_min - CONFIG.pullback_max_overshoot_atr * float(setup15.atr)
        and float(setup15.close) > zone_min
    )
    touched_short = (
        float(recent15["high"].max()) >= zone_min
        and float(recent15["high"].max()) <= zone_max + CONFIG.pullback_max_overshoot_atr * float(setup15.atr)
        and float(setup15.close) < zone_max
    )

    long_ok = (
        float(trend1h.ema20) > float(trend1h.ema50)
        and float(trend1h.close) > float(trend1h.ema200)
        and touched_long
        and 35.0 <= float(setup15.rsi) <= 48.0
        and float(trigger.close) > previous_high
        and volume_ratio > CONFIG.min_volume_ratio
    )
    short_ok = (
        float(trend1h.ema20) < float(trend1h.ema50)
        and float(trend1h.close) < float(trend1h.ema200)
        and touched_short
        and 52.0 <= float(setup15.rsi) <= 65.0
        and float(trigger.close) < previous_low
        and volume_ratio > CONFIG.min_volume_ratio
    )
    if not long_ok and not short_ok:
        return None

    side = "LONG" if long_ok else "SHORT"
    breakout_distance = (
        float(trigger.close) - previous_high
        if side == "LONG"
        else previous_low - float(trigger.close)
    )
    trend_separation = (
        (float(trend1h.ema20) - float(trend1h.ema50)) / float(trend1h.atr)
        if side == "LONG"
        else (float(trend1h.ema50) - float(trend1h.ema20)) / float(trend1h.atr)
    )
    score, components = rank_score(
        side,
        volume_ratio,
        float(setup15.rsi),
        trend_separation,
        breakout_distance / float(setup15.atr),
    )
    risk = CONFIG.stop_atr * float(setup15.atr)
    signal_close = float(trigger.close)
    sign = 1.0 if side == "LONG" else -1.0
    stop = signal_close - sign * risk
    targets = [signal_close + sign * multiple * risk for multiple in CONFIG.target_r_multiples]
    return {
        "side": side,
        "signal_close": signal_close,
        "signal_close_time": int(trigger.close_time),
        "stop": stop,
        "targets": targets,
        "risk_distance": risk,
        "rsi": float(setup15.rsi),
        "volume_ratio": volume_ratio,
        "breakout_atr": breakout_distance / float(setup15.atr),
        "trend_separation_atr": trend_separation,
        "score": score,
        "score_components": components,
        "max_hold_minutes": CONFIG.max_hold_minutes,
    }


def levels_for_entry(signal: dict[str, Any], entry: float) -> dict[str, Any] | None:
    """Apply the 0.2% quote-drift cap and recompute RR from executable-side quote."""
    reference = float(signal["signal_close"])
    if reference <= 0 or entry <= 0:
        return None
    drift = abs(entry - reference) / reference
    if drift > CONFIG.max_entry_drift_pct:
        return None
    side = signal["side"]
    sign = 1.0 if side == "LONG" else -1.0
    risk_distance = float(signal["risk_distance"])
    stop = entry - sign * risk_distance
    targets = [entry + sign * multiple * risk_distance for multiple in CONFIG.target_r_multiples]
    first_rr = abs(targets[0] - entry) / abs(entry - stop)
    if first_rr < 2.0:
        return None
    return {
        "entry": entry,
        "stop": stop,
        "targets": targets,
        "drift_pct": drift,
        "tp1_rr": first_rr,
    }
