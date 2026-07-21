"""
Deterministic signal engine.

Replaces the LLM as the decision-maker. Given a numerical market-state
snapshot it returns an explainable LONG / SHORT / WAIT decision plus a
confidence score in [0, 1]. Being deterministic, the exact same input always
produces the same output, which is what makes it backtestable and what lets
the learning layer (learn.py) correlate features with realised PnL and adjust
the weights over time.

Nothing here promises profit. It is an honest, transparent baseline whose
edge must be *measured* on real price data before any weight is trusted.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict


# Default feature weights. These are a starting point, NOT tuned parameters.
# learn.py reads the trade journal and proposes updated weights from real
# outcomes; you copy the proposal here (or load a weights file) once the data
# says a weight actually helps.
DEFAULT_WEIGHTS: Dict[str, float] = {
    "imbalance": 1.0,   # order-book pressure (top-N depth)
    "cvd_1m": 0.8,      # short-term aggressive-flow direction
    "cvd_5m": 0.6,      # medium-term aggressive-flow direction
}

# A signal only fires when the combined score clears this magnitude. Higher =
# fewer but higher-conviction trades (and fewer commissions paid).
DEFAULT_ENTRY_THRESHOLD = 0.45

# CVD is unbounded; squash it into [-1, 1] so no single feature dominates.
# The scale is "how much 1-minute CVD counts as a full-strength signal".
CVD_1M_SCALE = 50.0
CVD_5M_SCALE = 150.0


def _tanh_norm(value: float, scale: float) -> float:
    """Squash an unbounded value into (-1, 1)."""
    if scale <= 0:
        return 0.0
    return math.tanh(value / scale)


@dataclass
class Signal:
    action: str                       # "LONG" | "SHORT" | "WAIT"
    confidence: float                 # [0, 1]
    score: float                      # raw signed score, negative = short bias
    contributions: Dict[str, float] = field(default_factory=dict)
    reasoning: str = ""
    sl_price: float | None = None     # structure-based stop (beyond the wick)
    tp_price: float | None = None     # take-profit at R multiple of the stop

    def as_dict(self) -> Dict[str, float]:
        return {
            "action": self.action,
            "confidence": round(self.confidence, 4),
            "score": round(self.score, 4),
            "contributions": {k: round(v, 4) for k, v in self.contributions.items()},
            "reasoning": self.reasoning,
            "sl_price": self.sl_price,
            "tp_price": self.tp_price,
        }


def evaluate(
    state: Dict[str, float],
    weights: Dict[str, float] | None = None,
    entry_threshold: float = DEFAULT_ENTRY_THRESHOLD,
) -> Signal:
    """
    Turn a market-state snapshot into a trading decision.

    Expected state keys: imbalance (0..1, 0.5 = neutral), cvd_1m, cvd_5m.
    Returns a Signal. WAIT whenever conviction is below the threshold or the
    fast/slow flow disagree with the book (a cheap way to avoid chop).
    """
    w = weights or DEFAULT_WEIGHTS

    # Map each raw feature to a signed strength in [-1, 1].
    imbalance = float(state.get("imbalance", 0.5))
    imb_signal = (imbalance - 0.5) * 2.0                      # 0.75 book -> +0.5
    cvd1_signal = _tanh_norm(float(state.get("cvd_1m", 0.0)), CVD_1M_SCALE)
    cvd5_signal = _tanh_norm(float(state.get("cvd_5m", 0.0)), CVD_5M_SCALE)

    contributions = {
        "imbalance": w.get("imbalance", 0.0) * imb_signal,
        "cvd_1m": w.get("cvd_1m", 0.0) * cvd1_signal,
        "cvd_5m": w.get("cvd_5m", 0.0) * cvd5_signal,
    }

    total_weight = sum(abs(v) for v in w.values()) or 1.0
    score = sum(contributions.values()) / total_weight   # normalised to ~[-1, 1]

    # Confluence guard: require the order book and the 1-minute flow to agree.
    # If they point opposite ways, the market is contested -> stand aside.
    book_dir = math.copysign(1, imb_signal) if imb_signal != 0 else 0
    flow_dir = math.copysign(1, cvd1_signal) if cvd1_signal != 0 else 0
    agree = book_dir != 0 and book_dir == flow_dir

    confidence = min(abs(score), 1.0)

    if agree and score >= entry_threshold:
        action = "LONG"
    elif agree and score <= -entry_threshold:
        action = "SHORT"
    else:
        action = "WAIT"

    if action == "WAIT":
        if not agree:
            reason = "Book and 1m flow disagree; standing aside."
        else:
            reason = f"Conviction {confidence:.2f} below threshold {entry_threshold:.2f}."
    else:
        top = max(contributions.items(), key=lambda kv: abs(kv[1]))
        reason = (
            f"{action}: score {score:+.2f} (imb {imbalance:.2f}, "
            f"cvd1 {state.get('cvd_1m', 0.0):.1f}, cvd5 {state.get('cvd_5m', 0.0):.1f}); "
            f"driver={top[0]}."
        )

    return Signal(
        action=action,
        confidence=confidence,
        score=score,
        contributions=contributions,
        reasoning=reason,
    )


# --- Strategy 2: liquidity-sweep / stop-hunt reversal ---------------------
# Trades WITH the stop hunt instead of being its victim: after price sweeps a
# resting liquidity level and reclaims it, we enter the reversal and place the
# stop BEYOND the sweep wick (not on the obvious level everyone hunts).

def evaluate_sweep(
    state: Dict[str, float],
    wick_buffer_mult: float = 0.35,
    tp_r_multiple: float = 2.0,
    min_sl_pct: float = 0.004,
    max_sl_pct: float = 0.04,
) -> Signal:
    """
    Decide using sweep flags + order-flow confirmation, and return a
    structure-based bracket (sl_price / tp_price).

    Expected state keys (from alpha_generator features:levels + cvd/imbalance):
      sweep_low, sweep_high (bool), last_price, recent_min, recent_max,
      swing_low, swing_high, range_1m, cvd_1m, imbalance.

    Logic:
      - sweep_low  + buyers stepping in (cvd_1m >= 0, book not hostile) -> LONG
      - sweep_high + sellers stepping in (cvd_1m <= 0, book not hostile) -> SHORT
      - otherwise WAIT.
    Stop for a long sits below the swept wick (recent_min) by a buffer; the
    take-profit is tp_r_multiple times that risk distance.
    """
    last = float(state.get("last_price", 0) or 0)
    if last <= 0:
        return Signal("WAIT", 0.0, 0.0, reasoning="No price yet.")

    sweep_low = bool(state.get("sweep_low"))
    sweep_high = bool(state.get("sweep_high"))
    cvd_1m = float(state.get("cvd_1m", 0.0) or 0.0)
    imbalance = float(state.get("imbalance", 0.5) or 0.5)
    recent_min = float(state.get("recent_min", last) or last)
    recent_max = float(state.get("recent_max", last) or last)
    range_1m = float(state.get("range_1m", 0.0) or 0.0)

    # Buffer to push the stop beyond the wick; fall back to a % of price if the
    # 1m range is degenerate (thin data).
    buffer = max(range_1m * wick_buffer_mult, last * min_sl_pct * 0.5)

    def _clamp_stop(sl: float, entry: float, is_long: bool) -> float:
        dist = abs(entry - sl)
        min_d = entry * min_sl_pct
        max_d = entry * max_sl_pct
        dist = max(min_d, min(dist, max_d))
        return entry - dist if is_long else entry + dist

    if sweep_low and cvd_1m >= 0 and imbalance >= 0.40:
        entry = last
        sl = _clamp_stop(recent_min - buffer, entry, is_long=True)
        risk = entry - sl
        tp = entry + tp_r_multiple * risk
        conf = min(0.5 + min(abs(cvd_1m) / 100.0, 0.3) + (imbalance - 0.5), 1.0)
        return Signal(
            action="LONG", confidence=conf, score=conf,
            contributions={"sweep_low": 1.0, "cvd_1m": cvd_1m},
            reasoning=(f"Swept low {state.get('swing_low')} & reclaimed; "
                       f"cvd {cvd_1m:.1f}, imb {imbalance:.2f}. "
                       f"SL {sl:.6g} (beyond wick), TP {tp:.6g} @ {tp_r_multiple}R."),
            sl_price=sl, tp_price=tp,
        )

    if sweep_high and cvd_1m <= 0 and imbalance <= 0.60:
        entry = last
        sl = _clamp_stop(recent_max + buffer, entry, is_long=False)
        risk = sl - entry
        tp = entry - tp_r_multiple * risk
        conf = min(0.5 + min(abs(cvd_1m) / 100.0, 0.3) + (0.5 - imbalance), 1.0)
        return Signal(
            action="SHORT", confidence=conf, score=-conf,
            contributions={"sweep_high": 1.0, "cvd_1m": cvd_1m},
            reasoning=(f"Swept high {state.get('swing_high')} & rejected; "
                       f"cvd {cvd_1m:.1f}, imb {imbalance:.2f}. "
                       f"SL {sl:.6g} (beyond wick), TP {tp:.6g} @ {tp_r_multiple}R."),
            sl_price=sl, tp_price=tp,
        )

    reason = "No sweep+reclaim setup."
    if sweep_low and cvd_1m < 0:
        reason = "Swept low but flow still selling; no reversal confirmation."
    elif sweep_high and cvd_1m > 0:
        reason = "Swept high but flow still buying; no reversal confirmation."
    return Signal("WAIT", 0.0, 0.0, reasoning=reason)


# --- Strategy 3: multi-layer CONFLUENCE (recommended) ---------------------
# Six inputs, each a signed [-1,1] "vote", combined into one weighted score.
# A trade needs the sweep trigger PLUS at least MIN_CONFIRMATIONS other layers
# agreeing, PLUS enough volatility to be worth trading. The narrative/sector
# bonus is added on top (it never creates a trade by itself).

DEFAULT_CONFLUENCE_WEIGHTS: Dict[str, float] = {
    "structure": 1.2,    # the sweep+reclaim (the trigger)
    "orderflow": 1.0,    # CVD + book imbalance confirmation
    "trend": 0.7,        # EMA regime (buy dips in uptrends, not downtrends)
    "exhaustion": 0.6,   # RSI over-extension (mean-reversion fuel)
}
MIN_CONFIRMATIONS = 2          # non-structure layers that must agree
VOL_FLOOR_PCT = 0.0006         # ATR/price below this = dead market, skip
CONFLUENCE_THRESHOLD = 0.45    # min |score| to fire


def _clip(x: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(x, hi))


def evaluate_confluence(
    state: Dict[str, float],
    weights: Dict[str, float] | None = None,
    wick_buffer_mult: float = 0.35,
    tp_r_multiple: float = 2.0,
    min_sl_pct: float = 0.004,
    max_sl_pct: float = 0.04,
) -> Signal:
    """
    Combine structure, order flow, trend, exhaustion, volatility and narrative
    into one decision with a structure-based bracket.

    Extra expected state keys (beyond evaluate_sweep): ema_fast, ema_slow, rsi,
    atr, sector_bonus.
    """
    w = weights or DEFAULT_CONFLUENCE_WEIGHTS
    last = float(state.get("last_price", 0) or 0)
    if last <= 0:
        return Signal("WAIT", 0.0, 0.0, reasoning="No price yet.")

    # --- Layer 1: structure (the trigger) ---
    sweep_low = bool(state.get("sweep_low"))
    sweep_high = bool(state.get("sweep_high"))
    if not (sweep_low or sweep_high):
        return Signal("WAIT", 0.0, 0.0, reasoning="No liquidity sweep; no trigger.")
    structure = 1.0 if sweep_low else -1.0
    direction = "LONG" if sweep_low else "SHORT"

    # --- Layer 5: volatility gate ---
    atr = float(state.get("atr", 0.0) or 0.0)
    vol_ratio = atr / last if last else 0.0
    if vol_ratio < VOL_FLOOR_PCT:
        return Signal("WAIT", 0.0, 0.0,
                      reasoning=f"Volatility too low (ATR/price {vol_ratio:.4f}); skipping.")

    # --- Layer 3: order flow ---
    cvd_1m = float(state.get("cvd_1m", 0.0) or 0.0)
    imbalance = float(state.get("imbalance", 0.5) or 0.5)
    orderflow = _clip(0.6 * math.tanh(cvd_1m / CVD_1M_SCALE) + 0.4 * (imbalance - 0.5) * 2.0)

    # --- Layer 2: trend regime (EMA fast vs slow, scaled by ATR) ---
    ema_fast = float(state.get("ema_fast", 0.0) or 0.0)
    ema_slow = float(state.get("ema_slow", 0.0) or 0.0)
    if ema_fast and ema_slow and atr:
        trend = _clip((ema_fast - ema_slow) / (atr * 3.0))
    else:
        trend = 0.0

    # --- Layer 4: exhaustion (RSI). Oversold is bullish, overbought bearish ---
    rsi_val = float(state.get("rsi", 50.0) or 50.0)
    exhaustion = _clip((50.0 - rsi_val) / 50.0)

    # Signed layer votes (bullish positive).
    layers = {
        "structure": structure,
        "orderflow": orderflow,
        "trend": trend,
        "exhaustion": exhaustion,
    }

    # Count non-structure confirmations in the trade's direction.
    want = 1.0 if direction == "LONG" else -1.0
    confirmers = [k for k in ("orderflow", "trend", "exhaustion")
                  if layers[k] * want > 0.1]
    n_confirm = len(confirmers)

    # Weighted, normalised confluence score.
    total_w = sum(abs(v) for v in w.values()) or 1.0
    raw = sum(w.get(k, 0.0) * layers[k] for k in layers) / total_w

    # Narrative / sector bonus: nudges conviction in the sweep's direction only.
    sector_bonus = float(state.get("sector_bonus", 0.0) or 0.0)
    # HTF structure bonus: this 15-min sweep coincides with a significant
    # multi-month zone whose inferred role (support/resistance) agrees with
    # the sweep direction (e.g. a LONG sweep right at a zone that flipped to
    # support). Computed upstream (level_engine + agent_orchestrator) and
    # passed in already direction-checked, so it's simply additive here.
    structure_bonus = float(state.get("structure_bonus", 0.0) or 0.0)
    score = raw + want * sector_bonus + want * structure_bonus

    contributions = {k: round(w.get(k, 0.0) * layers[k] / total_w, 4) for k in layers}
    contributions["sector_bonus"] = round(want * sector_bonus, 4)
    contributions["structure_bonus"] = round(want * structure_bonus, 4)

    passes = (n_confirm >= MIN_CONFIRMATIONS) and (abs(score) >= CONFLUENCE_THRESHOLD) \
        and (score * want > 0)

    if not passes:
        return Signal(
            "WAIT", min(abs(score), 1.0), score, contributions=contributions,
            reasoning=(f"{direction} setup weak: {n_confirm}/{MIN_CONFIRMATIONS} confirms, "
                       f"score {score:+.2f} vs {CONFLUENCE_THRESHOLD}."),
        )

    # Structure-based bracket, stop beyond the swept wick, ATR-aware buffer.
    recent_min = float(state.get("recent_min", last) or last)
    recent_max = float(state.get("recent_max", last) or last)
    range_1m = float(state.get("range_1m", 0.0) or 0.0)
    buffer = max(range_1m * wick_buffer_mult, atr * 0.5, last * min_sl_pct * 0.5)

    if direction == "LONG":
        sl = recent_min - buffer
        dist = _clamp_dist(last - sl, last, min_sl_pct, max_sl_pct)
        sl = last - dist
        tp = last + tp_r_multiple * dist
    else:
        sl = recent_max + buffer
        dist = _clamp_dist(sl - last, last, min_sl_pct, max_sl_pct)
        sl = last + dist
        tp = last - tp_r_multiple * dist

    confidence = min(abs(score), 1.0)
    reason = (f"{direction} confluence {score:+.2f} | confirms: {'+'.join(confirmers) or 'none'} "
              f"| flow {orderflow:+.2f} trend {trend:+.2f} rsi {rsi_val:.0f} "
              f"sector {sector_bonus:.2f} structure {structure_bonus:.2f} "
              f"| SL {sl:.6g} TP {tp:.6g} @ {tp_r_multiple}R.")
    return Signal(direction, confidence, score, contributions=contributions,
                  reasoning=reason, sl_price=sl, tp_price=tp)


def _clamp_dist(dist: float, entry: float, min_pct: float, max_pct: float) -> float:
    """Clamp a stop distance to [min_pct, max_pct] of entry price."""
    return max(entry * min_pct, min(abs(dist), entry * max_pct))
