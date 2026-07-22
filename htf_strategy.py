"""
HTF (higher-timeframe) breakout / trend-following strategy.

Pure logic, no I/O — fully deterministic and backtestable, same philosophy as
signal_engine.py but operating on CLOSED daily / 3-day / 4h candles instead of
the 1-minute tick stream. This is the opposite trade style from the old
15-minute sweep scalper, and the TRIGGER and the TRACKING deliberately live on
different timeframes:

  - Trigger:   a DAILY or 3-DAY candle CLOSES beyond a significant HTF level
               (a multi-touch zone built from daily/3-day pivots, or the N-bar
               Donchian extreme when price is in discovery). A wick through
               the level is NOT a trigger — only the close confirms, which is
               exactly the "teyitli kırılım" (confirmed break) read from
               discretionary chart analysis. If the daily and 3-day candles
               disagree on direction, the system stands aside.
  - Tracking:  the scan runs every 4h (see htf_agent), so the anti-chase
               extension check and the entry price use the latest known
               (4h-fresh) price rather than the possibly-stale daily close.
  - Filters:   volume expansion on the breakout candle, closing strength (no
               huge rejection wick), daily trend regime alignment, an anti-chase
               extension cap, and a minimum listing history so fresh speculative
               listings (no established structure) are never traded.
  - Stop:      structural — beyond the broken level by an ATR(1d) buffer, never
               tighter than a floor multiple of ATR. On volatile alts this lands
               in the 4–15% region, not the old 0.4% noise-stop.
  - Exit:      NO fixed take-profit. The position is TRACKED on 4h candles: a
               chandelier stop (highest close since entry minus
               TRAIL_ATR_MULT * ATR(4h)), ratcheted only in the trade's favour
               on each 4h close, moved to breakeven once the trade is
               BREAKEVEN_R multiples of risk in profit. The trend itself takes
               us out — that is what "ride it for maximum profit" means
               mechanically.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from level_engine import Candle, Zone, detect_zones


# ---------------------------------------------------------------------------
# OHLCV helpers (list-of-lists rows: [ts, open, high, low, close, volume])
# ---------------------------------------------------------------------------

@dataclass
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float


def to_bars(ohlcv: Sequence[Sequence[float]]) -> List[Bar]:
    return [Bar(int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]),
                float(r[5]) if len(r) > 5 else 0.0) for r in ohlcv]


def to_candles(bars: Sequence[Bar]) -> List[Candle]:
    """Adapt Bars to level_engine's volume-less Candle."""
    return [Candle(ts=b.ts, open=b.open, high=b.high, low=b.low, close=b.close)
            for b in bars]


def aggregate(bars: Sequence[Bar], group: int) -> List[Bar]:
    """Aggregate consecutive bars (e.g. 1d -> 3d with group=3). Trailing
    partial groups are dropped so every output bar is fully formed."""
    out: List[Bar] = []
    n = (len(bars) // group) * group
    for i in range(0, n, group):
        chunk = bars[i:i + group]
        out.append(Bar(
            ts=chunk[0].ts,
            open=chunk[0].open,
            high=max(b.high for b in chunk),
            low=min(b.low for b in chunk),
            close=chunk[-1].close,
            volume=sum(b.volume for b in chunk),
        ))
    return out


def true_atr(bars: Sequence[Bar], period: int = 14) -> float:
    """Wilder ATR from real high/low/close bars. 0.0 if not enough data."""
    if len(bars) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i].high, bars[i].low, bars[i - 1].close
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def ema_last(values: Sequence[float], period: int) -> float:
    if period <= 0 or len(values) < period:
        return 0.0
    k = 2.0 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e


def squeeze_ratio(
    bars: Sequence[Bar], recent_period: int = 10, baseline_period: int = 40
) -> Optional[float]:
    """How compressed is recent price action relative to the period right
    before it — the "sıkışma" (coiling) that precedes an HTF breakout.

    Ratio of the RECENT `recent_period`-bar high/low range to the BASELINE
    `baseline_period`-bar range immediately preceding it (the two windows do
    NOT overlap, so the ratio isn't mechanically capped at 1.0). A LOW ratio
    means the market has gone quiet relative to its own recent history — a
    genuine squeeze. A HIGH ratio means volatility is currently expanding
    (already pumping or dumping) — the opposite of what a breakout scanner
    should chase, since that move has already happened.

    Deliberately NOT based on 24h volume: volume is noisy day to day and
    biases discovery toward coins that already had a pump/dump. This looks
    at multi-week price range contraction instead, which is symbol-agnostic
    (a plain ratio, no need to normalise by price) and exactly matches what
    a discretionary trader means by "coiling under a level."
    """
    need = recent_period + baseline_period
    if len(bars) < need:
        return None
    recent = bars[-recent_period:]
    baseline = bars[-need:-recent_period]
    recent_width = max(b.high for b in recent) - min(b.low for b in recent)
    baseline_width = max(b.high for b in baseline) - min(b.low for b in baseline)
    if baseline_width <= 0:
        return None
    return recent_width / baseline_width


def build_htf_zones(
    bars_1d: Sequence[Bar], bars_3d: Sequence[Bar],
    cluster_tol_pct: float = 0.02, min_touches: int = 2,
) -> List[Zone]:
    """Merge daily + 3-day pivot zones into one HTF structure list — the same
    build used for breakout triggers (evaluate_htf_breakout) and reused for
    de-risking (next_zone_ahead), so both see identical structure."""
    zones = detect_zones(
        to_candles(bars_1d), pivot_window=3,
        cluster_tol_pct=cluster_tol_pct, min_touches=min_touches,
    )
    if len(bars_3d) >= 9:
        zones += detect_zones(
            to_candles(bars_3d), pivot_window=2,
            cluster_tol_pct=cluster_tol_pct * 1.5, min_touches=min_touches,
        )
    return zones


def next_zone_ahead(
    direction: str, current_price: float, zones: Sequence[Zone]
) -> Optional[Zone]:
    """The nearest significant HTF zone AHEAD of price in the trade's
    direction — the next overhead resistance for a LONG, the next support
    underneath for a SHORT. Used to de-risk BEFORE price reaches a level
    likely to react (Melih's "büyük dirence yaklaşırken pozisyon hafiflet"
    pattern) instead of waiting to see if it gets swept.

    Ties within 15% of the nearest distance are broken by touch count (the
    more significant zone wins) rather than picking whichever is a hair closer.
    """
    if direction == "LONG":
        candidates = [z for z in zones if z.price_low > current_price]
        if not candidates:
            return None
        nearest = min(z.price_low - current_price for z in candidates)
        near = [z for z in candidates if (z.price_low - current_price) <= nearest * 1.15]
    else:
        candidates = [z for z in zones if z.price_high < current_price]
        if not candidates:
            return None
        nearest = min(current_price - z.price_high for z in candidates)
        near = [z for z in candidates if (current_price - z.price_high) <= nearest * 1.15]
    return max(near, key=lambda z: z.touches)


# ---------------------------------------------------------------------------
# Trendline detection (bar-index space, so extrapolation to "now" is valid)
# ---------------------------------------------------------------------------

def _pivot_idx(bars: Sequence[Bar], window: int, highs: bool) -> List[int]:
    """Indices of pivot highs (or lows). The last `window` bars can never be
    pivots (they lack right-side neighbours), so there is no lookahead leak."""
    out = []
    for i in range(window, len(bars) - window):
        seg = bars[i - window: i + window + 1]
        if highs and bars[i].high == max(b.high for b in seg):
            out.append(i)
        elif not highs and bars[i].low == min(b.low for b in seg):
            out.append(i)
    return out


def trendline_break(
    bars: Sequence[Bar],
    atr: float,
    *,
    pivots_used: int = 5,
    min_pivots: int = 3,
    pivot_window: int = 2,
    max_dev_atr: float = 0.75,
):
    """Detect a confirmed CLOSE through a fitted trendline — the "trigger
    play" pattern: a descending resistance line (falling pivot highs) broken
    upward, or an ascending support line (rising pivot lows) broken downward.

    The line is least-squares fitted through the last `pivots_used` pivot
    highs/lows in BAR-INDEX space (so its value at the current bar is a valid
    extrapolation). A fit only counts when at least `min_pivots` pivots exist
    and every used pivot sits within `max_dev_atr` * ATR of the line — a
    sloppy fit is a fictional trendline, not structure.

    Returns None or a dict:
      {"direction": "LONG"|"SHORT", "level": line value at the last bar,
       "n_pivots": int, "slope": per-bar slope}
    Break condition: prior close on/behind the line, last close through it.
    """
    if atr <= 0 or len(bars) < pivot_window * 2 + 3:
        return None
    n = len(bars)

    def _fit(idx: List[int], ys: List[float]):
        k = len(idx)
        mean_x = sum(idx) / k
        mean_y = sum(ys) / k
        var = sum((x - mean_x) ** 2 for x in idx)
        if var == 0:
            return None
        slope = sum((idx[i] - mean_x) * (ys[i] - mean_y) for i in range(k)) / var
        intercept = mean_y - slope * mean_x
        if any(abs(ys[i] - (slope * idx[i] + intercept)) > max_dev_atr * atr
               for i in range(k)):
            return None  # pivots don't actually line up
        return slope, intercept

    # Descending resistance line through pivot HIGHS -> upward break = LONG.
    hi = _pivot_idx(bars, pivot_window, highs=True)[-pivots_used:]
    if len(hi) >= min_pivots:
        fit = _fit(hi, [bars[i].high for i in hi])
        if fit and fit[0] < 0:
            slope, intercept = fit
            line_now = slope * (n - 1) + intercept
            line_prev = slope * (n - 2) + intercept
            if bars[-2].close <= line_prev and bars[-1].close > line_now:
                return {"direction": "LONG", "level": line_now,
                        "n_pivots": len(hi), "slope": slope}

    # Ascending support line through pivot LOWS -> downward break = SHORT.
    lo = _pivot_idx(bars, pivot_window, highs=False)[-pivots_used:]
    if len(lo) >= min_pivots:
        fit = _fit(lo, [bars[i].low for i in lo])
        if fit and fit[0] > 0:
            slope, intercept = fit
            line_now = slope * (n - 1) + intercept
            line_prev = slope * (n - 2) + intercept
            if bars[-2].close >= line_prev and bars[-1].close < line_now:
                return {"direction": "SHORT", "level": line_now,
                        "n_pivots": len(lo), "slope": slope}

    return None


# ---------------------------------------------------------------------------
# Signal container
# ---------------------------------------------------------------------------

@dataclass
class HTFSignal:
    action: str                     # "LONG" | "SHORT" | "WAIT"
    confidence: float = 0.0         # [0, 1]
    reasoning: str = ""
    entry_price: float = 0.0
    sl_price: Optional[float] = None
    breakout_level: Optional[float] = None
    atr_1d: float = 0.0
    trigger_candle_ts: int = 0      # ts of the reference candle (latest closed track-TF bar)
    features: Dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Regime (trend) read on a higher timeframe
# ---------------------------------------------------------------------------

def regime_direction(bars: Sequence[Bar], fast: int = 20, slow: int = 50) -> float:
    """Trend regime in [-1, 1] from EMA alignment + price location.

    +1 = clean uptrend (fast > slow, close above fast), -1 = clean downtrend,
    values in between = mixed/transition. Enough history is required; with a
    short history this returns 0 (neutral), which callers treat as 'unknown'.
    """
    closes = [b.close for b in bars]
    if len(closes) < slow:
        # Not enough for the slow EMA: fall back to the fast one only.
        if len(closes) < fast:
            return 0.0
        f = ema_last(closes, fast)
        return 0.5 if closes[-1] > f else -0.5

    f = ema_last(closes, fast)
    s = ema_last(closes, slow)
    score = 0.0
    score += 0.5 if f > s else -0.5
    score += 0.5 if closes[-1] > f else -0.5
    return score


# ---------------------------------------------------------------------------
# Breakout trigger check — shared by the daily and 3-day confirmation passes
# ---------------------------------------------------------------------------

def _check_breakout(
    bars: Sequence[Bar],
    zones: Sequence[Zone],
    donchian_lookback: int,
):
    """Did the LAST closed bar in `bars` confirm a close through a zone or
    the prior Donchian channel? Returns (direction, level, zone) with
    direction in {"LONG", "SHORT", None}."""
    if len(bars) < donchian_lookback + 2:
        return None, None, None
    trigger, prior = bars[-1], bars[-2]

    long_level, long_zone = None, None
    short_level, short_zone = None, None
    for z in zones:
        if prior.close <= z.price_high and trigger.close > z.price_high:
            if long_level is None or z.price_high > long_level:
                long_level, long_zone = z.price_high, z
        if prior.close >= z.price_low and trigger.close < z.price_low:
            if short_level is None or z.price_low < short_level:
                short_level, short_zone = z.price_low, z

    window = bars[-(donchian_lookback + 1):-1]
    donchian_high = max(b.high for b in window)
    donchian_low = min(b.low for b in window)
    if long_level is None and prior.close <= donchian_high < trigger.close:
        long_level = donchian_high
    if short_level is None and prior.close >= donchian_low > trigger.close:
        short_level = donchian_low

    if long_level is not None:
        return "LONG", long_level, long_zone
    if short_level is not None:
        return "SHORT", short_level, short_zone
    return None, None, None


def _check_deviation(
    bars: Sequence[Bar],
    zones: Sequence[Zone],
    donchian_lookback: int,
    atr: float,
    min_touches: int = 3,
    min_sweep_atr: float = 0.25,
):
    """Liquidity sweep + reclaim ("deviation") on the LAST closed bar — the
    reversal pattern two independent discretionary sources both trade: the
    candle's wick pierces a significant level where stops rest (a multi-touch
    zone edge, or the prior N-bar extreme low/high), takes that liquidity,
    then CLOSES back on the original side. The wick must be a genuine sweep
    (>= min_sweep_atr ATR beyond the level), not a one-tick pierce.

    Returns (direction, level, zone, sweep_depth_atr) or (None, ...).
    """
    if atr <= 0 or len(bars) < donchian_lookback + 2:
        return None, None, None, 0.0
    trigger, prior = bars[-1], bars[-2]

    # LONG: swept below a support level, closed back above it.
    best = None  # (depth, level, zone)
    for z in zones:
        if z.touches < min_touches:
            continue
        depth = (z.price_low - trigger.low) / atr
        if prior.close >= z.price_low and depth >= min_sweep_atr \
                and trigger.close > z.price_low:
            if best is None or depth > best[0]:
                best = (depth, z.price_low, z)
    window = bars[-(donchian_lookback + 1):-1]
    donchian_low = min(b.low for b in window)
    depth = (donchian_low - trigger.low) / atr
    if depth >= min_sweep_atr and trigger.close > donchian_low:
        if best is None or depth > best[0]:
            best = (depth, donchian_low, None)
    if best is not None:
        return "LONG", best[1], best[2], best[0]

    # SHORT: swept above a resistance level, closed back below it.
    best = None
    for z in zones:
        if z.touches < min_touches:
            continue
        depth = (trigger.high - z.price_high) / atr
        if prior.close <= z.price_high and depth >= min_sweep_atr \
                and trigger.close < z.price_high:
            if best is None or depth > best[0]:
                best = (depth, z.price_high, z)
    donchian_high = max(b.high for b in window)
    depth = (trigger.high - donchian_high) / atr
    if depth >= min_sweep_atr and trigger.close < donchian_high:
        if best is None or depth > best[0]:
            best = (depth, donchian_high, None)
    if best is not None:
        return "SHORT", best[1], best[2], best[0]

    return None, None, None, 0.0


# ---------------------------------------------------------------------------
# Core evaluation — call once per scan cycle per symbol (every 4h)
#
# The TRIGGER lives on closed daily/3-day candles (that is what "confirmed
# breakout" means here); the 4h timeframe is only used for tracking the
# current price/extension and, separately, for trailing an open position
# (compute_trailing_stop below). This is the trend-follower split the old
# 15-minute scalper never had: slow, high-conviction entries, fine-grained
# exit management.
# ---------------------------------------------------------------------------

def evaluate_htf_breakout(
    bars_1d: Sequence[Bar],
    bars_3d: Sequence[Bar],
    current_price: float,
    current_ts: int,
    *,
    min_daily_candles: int = 40,
    donchian_lookback_1d: int = 20,
    donchian_lookback_3d: int = 10,
    vol_expansion_mult: float = 1.5,
    vol_avg_period: int = 20,
    max_extension_atr: float = 1.5,
    sl_zone_atr_mult: float = 1.0,
    min_sl_atr_mult: float = 1.5,
    max_sl_pct: float = 0.15,
    zone_cluster_tol_pct: float = 0.02,
    zone_min_touches: int = 2,
    min_close_strength: float = 0.55,
) -> HTFSignal:
    """Evaluate the last CLOSED daily and 3-day candles for a confirmed HTF
    trend breakout.

    bars_1d / bars_3d must contain only CLOSED candles. current_price/
    current_ts are the latest known price (typically the most recent closed
    4h close) — used for the anti-chase/extension check and as the
    approximate entry, since this function may run hours after the daily/
    3-day candle actually closed (we only scan every 4h). Returns an
    actionable LONG/SHORT with a structural stop and NO fixed TP (exits are
    managed by compute_trailing_stop on 4h closes), or WAIT with the reason.
    """
    if len(bars_1d) < min_daily_candles:
        return HTFSignal(
            "WAIT",
            reasoning=(f"Only {len(bars_1d)} daily candles (<{min_daily_candles}): "
                       "fresh/speculative listing without established structure — skipped."),
        )
    if current_price <= 0:
        return HTFSignal("WAIT", reasoning="No current price yet.")

    atr_1d = true_atr(bars_1d, 14)
    if atr_1d <= 0:
        return HTFSignal("WAIT", reasoning="Degenerate daily ATR.")

    # --- HTF structure: zones from daily AND 3-day candles, merged ---------
    zones: List[Zone] = build_htf_zones(bars_1d, bars_3d, zone_cluster_tol_pct, zone_min_touches)

    # --- Trigger candidates, in priority order ------------------------------
    # 1. zone/Donchian break on the daily close (finer, faster read)
    # 2. zone/Donchian break on the 3-day close (coarser, more significant)
    # 3. trendline break on the daily close (the "trigger play")
    # 4. liquidity sweep + reclaim ("deviation") on the daily close
    #
    # Each candidate runs the FULL filter chain below; a candidate that fails
    # a filter FALLS THROUGH to the next one instead of vetoing the whole
    # symbol. This matters most for deviations: the flush that prints a weak,
    # low-volume "breakdown" is often the very same candle whose wick swept
    # the lows and reclaimed — the breakdown read fails its filters, and the
    # deviation read then gets its own hearing.
    dir_1d, level_1d, zone_1d = _check_breakout(bars_1d, zones, donchian_lookback_1d)
    dir_3d, level_3d, zone_3d = _check_breakout(bars_3d, zones, donchian_lookback_3d)
    conflict = bool(dir_1d) and bool(dir_3d) and dir_1d != dir_3d
    both_confirm = bool(dir_1d) and bool(dir_3d) and dir_1d == dir_3d

    candidates = []
    if not conflict:
        # (a conflicting 1d-vs-3d continuation read stands aside entirely;
        #  trendline/deviation candidates below still get their hearing)
        if dir_1d:
            candidates.append({
                "type": "zone" if zone_1d is not None else "donchian",
                "tf": "1d", "direction": dir_1d, "level": level_1d,
                "zone": zone_1d, "bars": bars_1d, "tl": None, "sweep_depth": 0.0,
                "lookback": donchian_lookback_1d,
            })
        if dir_3d:
            candidates.append({
                "type": "zone" if zone_3d is not None else "donchian",
                "tf": "3d", "direction": dir_3d, "level": level_3d,
                "zone": zone_3d, "bars": bars_3d, "tl": None, "sweep_depth": 0.0,
                "lookback": donchian_lookback_3d,
            })

    tl = trendline_break(bars_1d, atr_1d)
    if tl is not None:
        candidates.append({
            "type": "trendline", "tf": "1d", "direction": tl["direction"],
            "level": tl["level"], "zone": None, "bars": bars_1d, "tl": tl,
            "sweep_depth": 0.0, "lookback": donchian_lookback_1d,
        })

    dev_dir, dev_level, dev_zone, dev_depth = _check_deviation(
        bars_1d, zones, donchian_lookback_1d, atr_1d)
    if dev_dir is not None:
        candidates.append({
            "type": "deviation", "tf": "1d", "direction": dev_dir,
            "level": dev_level, "zone": dev_zone, "bars": bars_1d, "tl": None,
            "sweep_depth": dev_depth, "lookback": donchian_lookback_1d,
        })

    if not candidates:
        if conflict:
            return HTFSignal(
                "WAIT",
                reasoning=(f"Conflicting breakout signals: daily says {dir_1d}, 3-day says "
                           f"{dir_3d} — standing aside until they agree."),
            )
        return HTFSignal("WAIT", reasoning="No confirmed daily/3-day close through an HTF level.")

    regime_1d = regime_direction(bars_1d, 20, 50)
    regime_3d = regime_direction(bars_3d, 10, 20) if len(bars_3d) >= 10 else 0.0

    first_reject: Optional[str] = None
    armable: Optional[HTFSignal] = None

    for cand in candidates:
        trigger_type = cand["type"]
        trigger_tf = cand["tf"]
        direction = cand["direction"]
        level = cand["level"]
        zone = cand["zone"]
        trigger_bars = cand["bars"]
        trigger_bar = trigger_bars[-1]
        sweep_depth = cand["sweep_depth"]
        want = 1.0 if direction == "LONG" else -1.0

        def _reject(msg: str):
            nonlocal first_reject
            if first_reject is None:
                first_reject = msg

        # --- Filter 1: trend regime must not oppose the trade ---------------
        # Deviation triggers are exempt from the hard block: a sweep-reclaim
        # is BY DEFINITION a reversal against the prior move. The regime still
        # enters its confidence sum, so a counter-regime deviation needs an
        # otherwise-excellent picture to clear the floor.
        if trigger_type != "deviation" and regime_1d * want < 0 and abs(regime_1d) >= 1.0:
            _reject(f"{direction} break at {level:.6g} rejected: daily regime "
                    f"({regime_1d:+.1f}) is firmly against it — no counter-trend trades.")
            continue

        # --- Filter 2: genuine participation (volume on the trigger TF) -----
        vol_window = trigger_bars[-(vol_avg_period + 1):-1]
        avg_vol = sum(b.volume for b in vol_window) / max(len(vol_window), 1)
        vol_ratio = (trigger_bar.volume / avg_vol) if avg_vol > 0 else 0.0
        if vol_ratio < vol_expansion_mult:
            _reject(f"{direction} {trigger_tf} {trigger_type} at {level:.6g} but volume only "
                    f"{vol_ratio:.1f}x avg (<{vol_expansion_mult}x): unconvincing, likely fakeout.")
            continue

        # --- Filter 3: closing strength (no massive rejection wick) ---------
        rng = trigger_bar.high - trigger_bar.low
        if rng <= 0:
            _reject("Flat trigger candle.")
            continue
        close_strength = ((trigger_bar.close - trigger_bar.low) / rng if direction == "LONG"
                          else (trigger_bar.high - trigger_bar.close) / rng)
        if close_strength < min_close_strength:
            _reject(f"{direction} {trigger_tf} {trigger_type} at {level:.6g} but candle closed "
                    f"weak (strength {close_strength:.2f}<{min_close_strength}): rejection wick.")
            continue

        # --- Filter 4: anti-chase vs the CURRENT price (the trigger candle
        # may have closed hours ago; we only scan every 4h) -------------------
        extension = abs(current_price - level) / atr_1d
        if extension > max_extension_atr:
            # The break itself WAS confirmed — arm it as a retest watch: if
            # price pulls back to this level and holds (evaluate_retest),
            # THAT becomes the entry ("clear retest for next leg up").
            if armable is None:
                armable = HTFSignal(
                    "WAIT",
                    reasoning=(f"{direction} {trigger_tf} {trigger_type} confirmed but price "
                               f"already {extension:.1f} ATR past the level "
                               f"(>{max_extension_atr}): too extended, wait for retest."),
                    features={
                        "armable_retest": True,
                        "direction": direction,
                        "level": level,
                        "zone_low": zone.price_low if zone else None,
                        "zone_high": zone.price_high if zone else None,
                        "atr_1d": atr_1d,
                    },
                )
            continue

        # --- Structural stop (sized off daily ATR) --------------------------
        entry = current_price
        if direction == "LONG":
            if trigger_type == "deviation":
                # The sweep wick IS the structure: below it the thesis is dead.
                sl = trigger_bar.low - 0.5 * atr_1d
            else:
                sl = level - sl_zone_atr_mult * atr_1d
                if zone is not None:
                    sl = min(sl, zone.price_low - 0.25 * atr_1d)   # below the WHOLE zone
                if trigger_type == "trendline":
                    # No zone under a broken descending line; the coil's floor
                    # is the protecting structure.
                    coil_low = min(b.low for b in bars_1d[-10:])
                    sl = min(sl, coil_low - 0.25 * atr_1d)
            sl = min(sl, entry - min_sl_atr_mult * atr_1d)     # stop floor
            sl = max(sl, entry * (1 - max_sl_pct))             # sanity cap
        else:
            if trigger_type == "deviation":
                sl = trigger_bar.high + 0.5 * atr_1d
            else:
                sl = level + sl_zone_atr_mult * atr_1d
                if zone is not None:
                    sl = max(sl, zone.price_high + 0.25 * atr_1d)
                if trigger_type == "trendline":
                    coil_high = max(b.high for b in bars_1d[-10:])
                    sl = max(sl, coil_high + 0.25 * atr_1d)
            sl = max(sl, entry + min_sl_atr_mult * atr_1d)
            sl = min(sl, entry * (1 + max_sl_pct))

        risk = abs(entry - sl)
        if risk <= 0:
            _reject("Degenerate stop distance.")
            continue

        # --- Confidence: how much of the ideal picture is present -----------
        conf = 0.45
        conf += min((vol_ratio - vol_expansion_mult) / vol_expansion_mult, 1.0) * 0.15
        conf += min(close_strength, 1.0) * 0.10
        conf += (regime_1d * want) * 0.10           # aligned daily trend
        conf += max(regime_3d * want, 0.0) * 0.05   # aligned 3d trend (bonus only)
        conf += 0.10 if both_confirm else 0.0       # daily AND 3-day agree
        if trigger_type == "deviation":
            conf += min(sweep_depth, 1.0) * 0.07    # deeper sweep = more stops taken
            if zone is not None:
                conf += min(zone.touches / 5.0, 1.0) * 0.08
        elif zone is not None:
            conf += min(zone.touches / 5.0, 1.0) * 0.10   # significant multi-touch level
        elif cand["tl"] is not None:
            conf += min(cand["tl"]["n_pivots"] / 5.0, 1.0) * 0.08  # clean trendline
        else:
            conf += 0.03                                   # Donchian-only break
        # Reversal plays cap lower than continuation: they fight the prior move.
        confidence = max(0.0, min(conf, 0.90 if trigger_type == "deviation" else 0.95))

        if trigger_type == "deviation":
            at = (f"{zone.touches}-touch zone edge" if zone is not None
                  else f"prior {cand['lookback']}-bar extreme")
            src = f"liquidity sweep of {at} (wick {sweep_depth:.1f} ATR through, reclaimed)"
        elif zone is not None:
            src = f"{zone.touches}-touch zone {zone.price_low:.6g}-{zone.price_high:.6g}"
        elif cand["tl"] is not None:
            src = (f"{cand['tl']['n_pivots']}-pivot "
                   f"{'descending resistance' if direction == 'LONG' else 'ascending support'} trendline")
        else:
            src = f"{cand['lookback']}-bar {trigger_tf} Donchian"
        agree_note = (" (3-day also confirms)" if both_confirm and trigger_tf == "1d"
                      else " (daily also confirms)" if both_confirm and trigger_tf == "3d"
                      else "")
        verb = "reversal off" if trigger_type == "deviation" else "breakout of"
        reasoning = (
            f"{direction} {trigger_tf}-close {verb} {src} at {level:.6g}{agree_note} | "
            f"vol {vol_ratio:.1f}x avg, close-strength {close_strength:.2f}, "
            f"regime 1d {regime_1d:+.1f} / 3d {regime_3d:+.1f}, ext {extension:.1f} ATR | "
            f"entry ~{entry:.6g}, SL {sl:.6g} ({risk / entry * 100:.1f}%), "
            f"no fixed TP — tracked on 4h, trailing until trend break."
        )

        return HTFSignal(
            action=direction,
            confidence=round(confidence, 4),
            reasoning=reasoning,
            entry_price=entry,
            sl_price=sl,
            breakout_level=level,
            atr_1d=atr_1d,
            trigger_candle_ts=current_ts,
            features={
                "strategy": "htf_breakout",
                "trail_mode": "structure",        # execution engine: no fixed TP
                "trigger_timeframe": trigger_tf,
                "trigger_type": trigger_type,     # "zone"|"donchian"|"trendline"|"deviation"
                "sweep_depth_atr": round(sweep_depth, 3) if trigger_type == "deviation" else None,
                "breakout_level": level,
                "atr_1d": atr_1d,
                "vol_ratio": round(vol_ratio, 3),
                "close_strength": round(close_strength, 3),
                "regime_1d": regime_1d,
                "regime_3d": regime_3d,
                "extension_atr": round(extension, 3),
                "zone_touches": zone.touches if zone else 0,
                "zone_low": zone.price_low if zone else None,
                "zone_high": zone.price_high if zone else None,
                "both_confirm": both_confirm,
                "risk_pct": round(risk / entry * 100, 3),
                "daily_candles": len(bars_1d),
                "sl_price": sl,
                "tp_price": None,
            },
        )

    # No candidate fully passed: an extension-rejected (armable) WAIT beats a
    # plain rejection, since the caller can act on it (arm the retest watch).
    if armable is not None:
        return armable
    return HTFSignal("WAIT", reasoning=first_reject or
                     "No confirmed daily/3-day close through an HTF level.")


# ---------------------------------------------------------------------------
# Retest entry — the "clear retest for next leg up" play
#
# When a confirmed breakout is rejected for being too extended (anti-chase),
# the level is ARMED instead of forgotten. If price pulls back to the broken
# level and HOLDS it on a 4h close, that pullback is the entry: same trend
# thesis, far better price, stop right under the structure. Checked every 4h
# scan by htf_agent for each armed level.
# ---------------------------------------------------------------------------

def evaluate_retest(
    direction: str,                 # "LONG" | "SHORT" (of the original break)
    level: float,                   # the broken level being retested
    zone_low: Optional[float],
    zone_high: Optional[float],
    atr_1d: float,
    armed_ts: int,                  # ms — when the level was armed
    bars_4h: Sequence[Bar],         # closed 4h candles (recent window)
    *,
    proximity_atr: float = 0.35,    # how close counts as "touched" the level
    max_reentry_ext_atr: float = 1.0,  # last close must still be near the level
    sl_zone_atr_mult: float = 1.0,
    min_sl_atr_mult: float = 1.5,
    max_sl_pct: float = 0.15,
):
    """Judge an armed level. Returns (status, signal):

      status "waiting"     — no touch-and-hold yet; keep the level armed.
      status "invalidated" — a 4h close broke back through the structure the
                             wrong way; the breakout failed, disarm.
      status "triggered"   — price touched the level area and the latest 4h
                             close is holding the right side of it: enter.
    """
    since = [b for b in bars_4h if b.ts >= armed_ts]
    if not since or atr_1d <= 0:
        return "waiting", None
    last = since[-1]
    want_long = direction == "LONG"

    # Invalidation: closed back through the far side of the structure.
    fail_level = ((zone_low - 0.25 * atr_1d) if (want_long and zone_low) else
                  (zone_high + 0.25 * atr_1d) if (not want_long and zone_high) else
                  (level - sl_zone_atr_mult * atr_1d if want_long
                   else level + sl_zone_atr_mult * atr_1d))
    if (want_long and last.close < fail_level) or (not want_long and last.close > fail_level):
        return "invalidated", None

    # Touch: any 4h bar since arming came back to the level area.
    touch_band = proximity_atr * atr_1d
    touched = any((b.low <= level + touch_band) if want_long
                  else (b.high >= level - touch_band) for b in since)
    if not touched:
        return "waiting", None

    # Hold: the LATEST close is back on the breakout side of the level,
    # and hasn't already run away again (we'd be chasing twice).
    holding = (last.close > level) if want_long else (last.close < level)
    ext = abs(last.close - level) / atr_1d
    if not holding or ext > max_reentry_ext_atr:
        return "waiting", None

    # Deviation bonus: the retest wicked THROUGH the level and got bought/sold
    # back up — a stop-sweep of the breakout entrants, typically the strongest
    # form of retest.
    swept = any((b.low < level and b.close > level) if want_long
                else (b.high > level and b.close < level) for b in since)

    entry = last.close
    if want_long:
        sl = level - sl_zone_atr_mult * atr_1d
        if zone_low:
            sl = min(sl, zone_low - 0.25 * atr_1d)
        sl = min(sl, entry - min_sl_atr_mult * atr_1d)
        sl = max(sl, entry * (1 - max_sl_pct))
    else:
        sl = level + sl_zone_atr_mult * atr_1d
        if zone_high:
            sl = max(sl, zone_high + 0.25 * atr_1d)
        sl = max(sl, entry + min_sl_atr_mult * atr_1d)
        sl = min(sl, entry * (1 + max_sl_pct))

    risk = abs(entry - sl)
    if risk <= 0:
        return "waiting", None

    conf = 0.60 + min(ext, 0.5) * 0.2 + (0.08 if swept else 0.0)
    confidence = min(conf, 0.90)
    reasoning = (
        f"{direction} retest entry at broken level {level:.6g}"
        f"{' (deviation swept & reclaimed)' if swept else ''} | "
        f"pullback held on 4h close {entry:.6g} ({ext:.2f} ATR from level) | "
        f"SL {sl:.6g} ({risk / entry * 100:.1f}%), no fixed TP — trailing until trend break."
    )
    sig = HTFSignal(
        action=direction,
        confidence=round(confidence, 4),
        reasoning=reasoning,
        entry_price=entry,
        sl_price=sl,
        breakout_level=level,
        atr_1d=atr_1d,
        trigger_candle_ts=last.ts,
        features={
            "strategy": "htf_breakout",
            "trail_mode": "structure",
            "trigger_type": "retest",
            "breakout_level": level,
            "atr_1d": atr_1d,
            "deviation_sweep": swept,
            "reentry_ext_atr": round(ext, 3),
            "zone_low": zone_low,
            "zone_high": zone_high,
            "risk_pct": round(risk / entry * 100, 3),
            "sl_price": sl,
            "tp_price": None,
        },
    )
    return "triggered", sig


# ---------------------------------------------------------------------------
# Trailing-stop management — call once per CLOSED 4h candle per open position
# ---------------------------------------------------------------------------

def compute_trailing_stop(
    side: str,                      # "LONG" | "SHORT"
    bars_4h: Sequence[Bar],         # closed candles only
    entry_price: float,
    entry_ts: int,                  # ms — candles at/after this count as "since entry"
    initial_sl: float,
    current_sl: float,
    *,
    trail_atr_mult: float = 3.0,
    breakeven_r: float = 1.5,
    breakeven_buffer_atr: float = 0.1,
) -> Optional[float]:
    """Chandelier-style ratchet. Returns a NEW stop price strictly better than
    current_sl (higher for longs, lower for shorts), or None if the stop should
    stay where it is. The stop NEVER loosens — that is the ratchet guarantee.

    - Base trail: extreme close since entry -/+ trail_atr_mult * ATR(4h).
    - Breakeven: once price has run breakeven_r * initial risk, the stop is at
      least entry +/- a small ATR buffer, so an established winner can no
      longer turn into a loss.
    """
    since = [b for b in bars_4h if b.ts >= entry_ts]
    if not since:
        return None
    atr4 = true_atr(bars_4h, 14)
    if atr4 <= 0:
        return None

    risk = abs(entry_price - initial_sl)
    last_close = since[-1].close

    if side == "LONG":
        highest_close = max(b.close for b in since)
        candidate = highest_close - trail_atr_mult * atr4
        if risk > 0 and last_close >= entry_price + breakeven_r * risk:
            candidate = max(candidate, entry_price + breakeven_buffer_atr * atr4)
        # Never trail above current price (stop must stay a stop).
        candidate = min(candidate, last_close * 0.999)
        if candidate > current_sl:
            return candidate
    else:
        lowest_close = min(b.close for b in since)
        candidate = lowest_close + trail_atr_mult * atr4
        if risk > 0 and last_close <= entry_price - breakeven_r * risk:
            candidate = min(candidate, entry_price - breakeven_buffer_atr * atr4)
        candidate = max(candidate, last_close * 1.001)
        if candidate < current_sl:
            return candidate
    return None
