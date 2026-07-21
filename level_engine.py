"""
HTF (higher-timeframe) support/resistance level engine.

Pure logic, no I/O — detects significant horizontal price ZONES (not exact
prices) from a history of OHLC candles, and classifies how live price action
reacts to a zone as one of two structurally OPPOSITE events:

  - WICK SWEEP + RECLAIM: price pierces the zone with a wick, then closes
    back on the side it came from. This is a REVERSAL setup — trading WITH
    the stop-hunt, the same logic our confluence engine already uses on the
    short-term (15-min) swing levels, now applied to zones with months of
    "memory" behind them.
  - CONFIRMED BREAK: a candle CLOSES beyond the zone (not just a wick). This
    is a structural change — a CONTINUATION/trend-flip signal, the opposite
    read from a sweep. A zone's implied role (support vs resistance) flips
    at the most recent confirmed break and holds until the next one — this
    is the "as long as price holds this area, structure stays positive; a
    confirmed break changes the read" conditional framing seen in discretionary
    technical analysis (e.g. multi-touch zones + trendlines + polarity flips).

Nothing here promises an edge by itself — these are the same measurable
building blocks a discretionary trader uses (zones, touch count, reclaim vs.
confirmed break), made deterministic and backtestable via trade_journal/learn.py.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple


@dataclass
class Candle:
    ts: int
    open: float
    high: float
    low: float
    close: float


@dataclass
class Zone:
    price_low: float
    price_high: float
    touches: int
    last_touch_ts: int

    @property
    def mid(self) -> float:
        return (self.price_low + self.price_high) / 2.0

    def contains(self, price: float) -> bool:
        return self.price_low <= price <= self.price_high

    def distance_pct(self, price: float) -> float:
        """Signed distance from price to the nearest zone edge, as a
        fraction of price. 0 if price is inside the zone."""
        if price < self.price_low:
            return (self.price_low - price) / price
        if price > self.price_high:
            return (price - self.price_high) / price
        return 0.0


def _find_pivots(candles: Sequence[Candle], window: int = 3) -> Tuple[List[int], List[int]]:
    """Return (pivot_high_indices, pivot_low_indices).

    A candle is a pivot high/low if its high/low is the extreme within a
    +/- window neighbourhood — a standard, simple swing-point detector.
    """
    highs_idx, lows_idx = [], []
    n = len(candles)
    for i in range(window, n - window):
        seg = candles[i - window: i + window + 1]
        if candles[i].high == max(c.high for c in seg):
            highs_idx.append(i)
        if candles[i].low == min(c.low for c in seg):
            lows_idx.append(i)
    return highs_idx, lows_idx


def detect_zones(
    candles: Sequence[Candle],
    pivot_window: int = 3,
    cluster_tol_pct: float = 0.015,
    min_touches: int = 2,
) -> List[Zone]:
    """Cluster pivot highs/lows into significant horizontal zones.

    cluster_tol_pct: pivots within this fraction of each other's price are
    treated as the "same" zone (default 1.5%) — zones are bands, not exact
    prices, matching how these levels are actually drawn/used.
    min_touches: a zone must be touched at least this many times to count
    as significant structure, not a random single swing point.
    """
    if len(candles) < pivot_window * 2 + 1:
        return []

    high_idx, low_idx = _find_pivots(candles, pivot_window)
    pivots = [(candles[i].high, candles[i].ts) for i in high_idx] + \
             [(candles[i].low, candles[i].ts) for i in low_idx]
    if not pivots:
        return []

    pivots.sort(key=lambda p: p[0])

    clusters: List[List[Tuple[float, int]]] = []
    for price, ts in pivots:
        placed = False
        for cluster in clusters:
            cluster_mid = sum(p for p, _ in cluster) / len(cluster)
            if abs(price - cluster_mid) / cluster_mid <= cluster_tol_pct:
                cluster.append((price, ts))
                placed = True
                break
        if not placed:
            clusters.append([(price, ts)])

    zones = []
    for cluster in clusters:
        if len(cluster) < min_touches:
            continue
        prices = [p for p, _ in cluster]
        zones.append(Zone(
            price_low=min(prices),
            price_high=max(prices),
            touches=len(cluster),
            last_touch_ts=max(ts for _, ts in cluster),
        ))
    return zones


def classify_reaction(zone: Zone, prior_close: float, candle: Candle) -> Optional[str]:
    """Classify how one candle interacted with a zone, given where price
    was (prior_close) before this candle.

    Returns one of:
      "sweep_reversal_long"  - was above the zone, wicked below it, closed
                                back above the zone's low edge (reversal buy)
      "sweep_reversal_short" - was below the zone, wicked above it, closed
                                back below the zone's high edge (reversal short)
      "confirmed_break_down" - was above the zone, closed below it (structural
                                break down — trend continuation, NOT a reversal)
      "confirmed_break_up"   - was below the zone, closed above it (structural
                                break up — trend continuation, NOT a reversal)
      None                    - no meaningful interaction this candle
    """
    was_above = prior_close > zone.price_high
    was_below = prior_close < zone.price_low

    if was_above:
        pierced_below = candle.low < zone.price_low
        if pierced_below:
            reclaimed = candle.close > zone.price_low
            return "sweep_reversal_long" if reclaimed else "confirmed_break_down"

    if was_below:
        pierced_above = candle.high > zone.price_high
        if pierced_above:
            rejected = candle.close < zone.price_high
            return "sweep_reversal_short" if rejected else "confirmed_break_up"

    return None


def infer_current_role(zone: Zone, candles: Sequence[Candle]) -> str:
    """The zone's currently-implied role: 'support' or 'resistance'.

    Derived from the most recent CONFIRMED break only (sweeps don't change
    it) — "as long as price holds this area, structure stays positive; a
    confirmed break changes the read". If never confirmed-broken, the role
    is inferred from which side price has stayed on.
    """
    if len(candles) < 2:
        return "resistance"

    role = None
    prior_close = candles[0].close
    for candle in candles[1:]:
        result = classify_reaction(zone, prior_close, candle)
        if result == "confirmed_break_up":
            role = "support"       # broken up through -> now expected to hold as support
        elif result == "confirmed_break_down":
            role = "resistance"    # broken down through -> now expected to hold as resistance
        prior_close = candle.close

    if role is None:
        role = "resistance" if prior_close < zone.price_low else "support"
    return role


def nearest_zone(zones: Sequence[Zone], price: float, max_distance_pct: float = 0.02) -> Optional[Zone]:
    """The most significant zone within max_distance_pct of price, or None.

    Ties broken by touch count (more touches = more significant), matching
    the "some zones matter more than others" intuition.
    """
    candidates = [z for z in zones if z.distance_pct(price) <= max_distance_pct]
    if not candidates:
        return None
    return max(candidates, key=lambda z: z.touches)
