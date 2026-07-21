"""
Pure technical-indicator helpers (no I/O, no dependencies).

Kept dependency-free on purpose so the math is unit-testable in isolation and
reusable by both the live feature pipeline (alpha_generator) and any offline
backtest. All functions take plain lists of floats.
"""
from __future__ import annotations

from typing import List, Sequence, Tuple


def resample_closes(
    points: Sequence[Tuple[int, float]],
    bucket_ms: int,
    start_ts: int,
    end_ts: int,
) -> List[float]:
    """Turn irregular (ts_ms, price) trade points into evenly spaced closes.

    Each bucket's close = the last price seen in that bucket. Empty buckets
    carry the previous close forward (so EMAs/RSI see a continuous series).
    """
    if bucket_ms <= 0 or end_ts <= start_ts or not points:
        return []
    n_buckets = int((end_ts - start_ts) // bucket_ms) + 1
    closes: List[float | None] = [None] * n_buckets
    for ts, price in points:
        if ts < start_ts or ts > end_ts:
            continue
        idx = int((ts - start_ts) // bucket_ms)
        if 0 <= idx < n_buckets:
            closes[idx] = price
    # forward-fill
    out: List[float] = []
    last = None
    for c in closes:
        if c is not None:
            last = c
        if last is not None:
            out.append(last)
    return out


def ema(values: Sequence[float], period: int) -> float:
    """Exponential moving average of the final point. 0.0 if not enough data."""
    if period <= 0 or len(values) < period:
        return 0.0
    k = 2.0 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e


def rsi(values: Sequence[float], period: int = 14) -> float:
    """Wilder's RSI in [0, 100]. Returns 50 (neutral) if not enough data."""
    if len(values) < period + 1:
        return 50.0
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        change = values[i] - values[i - 1]
        if change >= 0:
            gains += change
        else:
            losses -= change
    avg_gain = gains / period
    avg_loss = losses / period
    for i in range(period + 1, len(values)):
        change = values[i] - values[i - 1]
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def atr_from_closes(values: Sequence[float], period: int = 14) -> float:
    """Approximate ATR from a close-only series (mean absolute change).

    True ATR needs high/low/close; on a resampled close series the mean of
    |close_t - close_{t-1}| over the last `period` bars is a robust proxy for
    the per-bar move size, which is what we need for stop sizing.
    """
    if len(values) < 2:
        return 0.0
    diffs = [abs(values[i] - values[i - 1]) for i in range(1, len(values))]
    window = diffs[-period:] if len(diffs) >= period else diffs
    return sum(window) / len(window) if window else 0.0
