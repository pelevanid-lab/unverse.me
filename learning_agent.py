"""
Learning agent — closes the loop between journalled trade outcomes and the
HTF strategy's parameters.

HONEST CONSTRAINTS (read before "improving" this):
  - HTF trading produces FEW trades. Tuning anything off 10 trades is mostly
    noise, so every adjustment here is sample-gated (min N), shrunk toward
    zero for small samples, and hard-capped. Until enough data exists this
    agent only OBSERVES and reports — that is correct behaviour, not a bug.
  - Everything is deterministic and recomputed from the FULL journal each
    run: the same journal always produces the same adjustments, so the
    learning itself is auditable and reproducible. No incremental drift.
  - The adjustments can only NUDGE the strategy (confidence +/-0.10, trail
    mult within [2.0, 4.0]) or disable a demonstrably losing trigger type.
    They can never invent new trades or override the approval flow.

What it learns, from Supabase trade_journal (mirrored by trade_journal.py):
  1. Per-trigger-type expectancy (R multiples, net of fees) -> bounded
     confidence deltas, and disabling of trigger types with clearly negative
     expectancy (auto re-enabled if more data pulls it back up).
  2. Exit quality: MFE (max favourable excursion) per trade from exchange
     candles vs. realised R -> is the trailing stop giving back too much
     (loosen threshold not met) or cutting winners short (post-exit
     continuation)? -> +/-0.25 steps on the trailing ATR multiplier.

Outputs:
  - Redis key "learn:adjustments" (htf_agent reads it every cycle)
  - Supabase table learning_state (dashboard/history)
  - a human-readable report on "telegram:analysis" whenever adjustments change
"""
import asyncio
import json
import logging
import statistics
import time
from typing import Dict, List, Optional

import ccxt.async_support as ccxt
import orjson
import redis.asyncio as redis

import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("LearningAgent")

LEARN_INTERVAL_SEC = 6 * 3600      # full recompute every 6h (cheap, few trades)
ADJUSTMENTS_KEY = "learn:adjustments"

# Gates & caps — the "don't fool yourself" numbers.
MIN_N_CONF = 10          # trades of a trigger type before any conf delta
SHRINK_N = 20            # sample size at which shrinkage weight reaches 0.5
CONF_DELTA_CAP = 0.10
CONF_DELTA_SCALE = 0.05  # delta = clamp(expectancy_R * scale, cap) * shrink
MIN_N_DISABLE = 15
DISABLE_BELOW_R = -0.40  # expectancy worse than this -> trigger type off
REVIVE_ABOVE_R = -0.20   # ...and back on only if it recovers above this
MIN_N_TRAIL = 10         # winning trades needed before touching the trail
TRAIL_MIN, TRAIL_MAX, TRAIL_STEP = 2.0, 4.0, 0.25
POST_EXIT_BARS_4H = 30   # ~5 days of "did it keep going after we left?"


# ---------------------------------------------------------------------------
# Pure logic (unit-testable, no I/O)
# ---------------------------------------------------------------------------

def realized_r(trade: dict) -> Optional[float]:
    """Realised R multiple: net PnL over the dollars initially at risk."""
    try:
        entry = float(trade["entry_price"])
        sl = float(trade["sl_price"])
        qty = float(trade["quantity"])
        net = float(trade["net_pnl"])
    except (KeyError, TypeError, ValueError):
        return None
    risk_usd = abs(entry - sl) * qty
    if risk_usd <= 0:
        return None
    return net / risk_usd


def aggregate_by_trigger(trades: List[dict]) -> Dict[str, dict]:
    """Per-trigger-type performance from closed, R-computable trades."""
    stats: Dict[str, dict] = {}
    for t in trades:
        ttype = (t.get("features") or {}).get("trigger_type") or "unknown"
        r = realized_r(t)
        if r is None:
            continue
        s = stats.setdefault(ttype, {"n": 0, "wins": 0, "total_r": 0.0, "rs": []})
        s["n"] += 1
        s["wins"] += 1 if r > 0 else 0
        s["total_r"] += r
        s["rs"].append(round(r, 3))
    for s in stats.values():
        s["expectancy_r"] = round(s["total_r"] / s["n"], 3) if s["n"] else 0.0
        s["win_rate"] = round(s["wins"] / s["n"], 3) if s["n"] else 0.0
        s["total_r"] = round(s["total_r"], 3)
    return stats


def propose_conf_deltas(stats: Dict[str, dict]) -> Dict[str, float]:
    """Bounded, shrunk confidence deltas per trigger type.

    delta = clamp(expectancy_R * SCALE, +/-CAP) * n/(n+SHRINK_N)
    Small samples shrink toward zero; even a perfect record moves confidence
    at most +/-0.10 — enough to matter for sizing/floor, not enough to
    replace the structural filters.
    """
    deltas = {}
    for ttype, s in stats.items():
        if s["n"] < MIN_N_CONF:
            continue
        raw = max(-CONF_DELTA_CAP, min(s["expectancy_r"] * CONF_DELTA_SCALE, CONF_DELTA_CAP))
        shrink = s["n"] / (s["n"] + SHRINK_N)
        delta = round(raw * shrink, 4)
        if delta != 0.0:
            deltas[ttype] = delta
    return deltas


def propose_disabled(stats: Dict[str, dict], currently_disabled: List[str]) -> List[str]:
    """Trigger types with clearly negative expectancy get switched off;
    a disabled type comes back only when the (growing) record recovers.
    Recomputed from full history each run — deterministic either way."""
    disabled = []
    for ttype, s in stats.items():
        was_off = ttype in currently_disabled
        if s["n"] >= MIN_N_DISABLE:
            if s["expectancy_r"] < DISABLE_BELOW_R:
                disabled.append(ttype)
            elif was_off and s["expectancy_r"] < REVIVE_ABOVE_R:
                disabled.append(ttype)   # hysteresis: stay off until real recovery
    return sorted(disabled)


def exit_quality(trade: dict, bars_4h: List[list]) -> Optional[dict]:
    """MFE and post-exit continuation for one closed trade, in R multiples.

    bars_4h: raw [ts, o, h, l, c, v] rows spanning entry..exit+POST_EXIT_BARS.
    """
    r = realized_r(trade)
    if r is None or not bars_4h:
        return None
    entry = float(trade["entry_price"])
    sl = float(trade["sl_price"])
    exit_price = float(trade.get("exit_price") or 0)
    risk = abs(entry - sl)
    if risk <= 0 or exit_price <= 0:
        return None
    is_long = trade.get("action") == "LONG"

    entry_ts = trade.get("_entry_ts_ms")
    exit_ts = trade.get("_exit_ts_ms")
    if not entry_ts or not exit_ts:
        return None
    in_trade = [b for b in bars_4h if entry_ts <= b[0] <= exit_ts]
    after = [b for b in bars_4h if b[0] > exit_ts][:POST_EXIT_BARS_4H]
    if not in_trade:
        return None

    if is_long:
        mfe_r = (max(b[2] for b in in_trade) - entry) / risk
        cont_r = ((max(b[2] for b in after) - exit_price) / risk) if after else 0.0
    else:
        mfe_r = (entry - min(b[3] for b in in_trade)) / risk
        cont_r = ((exit_price - min(b[3] for b in after)) / risk) if after else 0.0

    return {
        "realized_r": round(r, 3),
        "mfe_r": round(mfe_r, 3),
        "giveback_r": round(mfe_r - r, 3),
        "post_exit_continuation_r": round(max(cont_r, 0.0), 3),
    }


def propose_trail_mult(exit_stats: List[dict], current: float) -> tuple:
    """One bounded step on the trailing ATR multiplier, from WINNING trades:

      - giving back a lot of open profit AND price not continuing after exit
        -> trail is too loose -> tighten one step;
      - price keeps running well after our exit AND giveback is small
        -> trail is too tight -> loosen one step;
      - conflicting or thin evidence -> leave it alone.

    Returns (new_mult, reason).
    """
    winners = [e for e in exit_stats if e["realized_r"] > 0]
    if len(winners) < MIN_N_TRAIL:
        return current, f"only {len(winners)} winners (<{MIN_N_TRAIL}); observing."
    med_giveback = statistics.median(e["giveback_r"] for e in winners)
    med_cont = statistics.median(e["post_exit_continuation_r"] for e in winners)

    if med_giveback > 1.2 and med_cont < 0.5:
        new = max(TRAIL_MIN, round(current - TRAIL_STEP, 2))
        return new, (f"median giveback {med_giveback:.2f}R with little continuation "
                     f"({med_cont:.2f}R): trail too loose -> {new}")
    if med_cont > 1.0 and med_giveback < 0.8:
        new = min(TRAIL_MAX, round(current + TRAIL_STEP, 2))
        return new, (f"median post-exit continuation {med_cont:.2f}R with small "
                     f"giveback ({med_giveback:.2f}R): trail too tight -> {new}")
    return current, (f"no clear signal (giveback {med_giveback:.2f}R, "
                     f"continuation {med_cont:.2f}R); unchanged.")


# ---------------------------------------------------------------------------
# Agent (I/O)
# ---------------------------------------------------------------------------

class LearningAgent:
    def __init__(self):
        self.redis_client = None
        self.exchange = ccxt.binanceusdm({'enableRateLimit': True})

    async def initialize(self):
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        await self.exchange.load_markets()

    async def cleanup(self):
        if self.redis_client:
            await self.redis_client.aclose()
        if self.exchange:
            await self.exchange.close()

    async def _fetch_closed_trades(self) -> List[dict]:
        """Closed htf_breakout trades from the Supabase journal mirror."""
        if not config.supabase:
            return []

        def _q():
            return config.supabase.table("trade_journal").select("*") \
                .eq("status", "CLOSED").order("created_at", desc=True) \
                .limit(200).execute()
        try:
            res = await asyncio.to_thread(_q)
        except Exception as e:
            logger.error(f"Journal fetch failed: {e}")
            return []

        trades = []
        for row in (res.data or []):
            feats = row.get("features") or {}
            if feats.get("strategy") != "htf_breakout":
                continue  # legacy scalper trades teach us nothing about HTF
            # entry/exit timestamps for candle-window analysis
            try:
                from datetime import datetime
                entry_ts = int(datetime.fromisoformat(
                    str(row["created_at"]).replace("Z", "+00:00")).timestamp() * 1000)
                dur_s = float(row.get("duration_s") or 0)
                row["_entry_ts_ms"] = entry_ts
                row["_exit_ts_ms"] = entry_ts + int(dur_s * 1000)
            except Exception:
                row["_entry_ts_ms"] = row["_exit_ts_ms"] = None
            trades.append(row)
        return trades

    async def _fetch_trade_bars(self, trade: dict) -> List[list]:
        symbol = trade.get("symbol", "")
        entry_ts = trade.get("_entry_ts_ms")
        if not symbol or not entry_ts:
            return []
        ccxt_symbol = None
        for sym, market in self.exchange.markets.items():
            if market.get('id', '').upper() == symbol.upper():
                ccxt_symbol = sym
                break
        if not ccxt_symbol:
            return []
        try:
            return await self.exchange.fetch_ohlcv(
                ccxt_symbol, timeframe="4h",
                since=entry_ts - 4 * 3600 * 1000, limit=500,
            )
        except Exception as e:
            logger.warning(f"[{symbol}] OHLCV fetch for exit analysis failed: {e}")
            return []

    async def _load_previous_state(self) -> dict:
        try:
            raw = await self.redis_client.get(ADJUSTMENTS_KEY)
            if raw:
                return orjson.loads(raw)
        except Exception:
            pass
        return {}

    async def run_cycle(self):
        trades = await self._fetch_closed_trades()
        prev = await self._load_previous_state()
        prev_disabled = prev.get("disabled", [])
        prev_trail = float(prev.get("trail_atr_mult") or config.HTF_TRAIL_ATR_MULT)

        stats = aggregate_by_trigger(trades)
        conf_deltas = propose_conf_deltas(stats)
        disabled = propose_disabled(stats, prev_disabled)

        # Exit-quality pass (winners drive the trail; capped fetches).
        exit_stats = []
        for t in trades[:60]:
            bars = await self._fetch_trade_bars(t)
            q = exit_quality(t, bars)
            if q:
                exit_stats.append(q)
            await asyncio.sleep(0.25)
        trail_mult, trail_reason = propose_trail_mult(exit_stats, prev_trail)

        state = {
            "updated_at": int(time.time() * 1000),
            "n_closed_trades": len(trades),
            "trigger_stats": {k: {kk: vv for kk, vv in v.items() if kk != "rs"}
                              for k, v in stats.items()},
            "conf_deltas": conf_deltas,
            "disabled": disabled,
            "trail_atr_mult": trail_mult,
            "trail_reason": trail_reason,
        }

        await self.redis_client.set(ADJUSTMENTS_KEY, orjson.dumps(state))
        if config.supabase:
            def _persist():
                config.supabase.table("learning_state").upsert(
                    {"id": 1, "state": json.loads(orjson.dumps(state)),
                     "updated_at": "now()"}
                ).execute()
            try:
                await asyncio.to_thread(_persist)
            except Exception as e:
                logger.warning(f"learning_state persist failed: {e}")

        changed = (conf_deltas != prev.get("conf_deltas", {})
                   or disabled != prev_disabled
                   or trail_mult != prev_trail)
        summary = self._format_report(state)
        logger.info(summary.replace("\n", " | "))
        config.send_log_to_dashboard("LearningAgent", "LEARN", summary)
        if changed and trades:
            await self.redis_client.publish("telegram:analysis", orjson.dumps({
                "symbol": "LEARN", "report": "🧠 Öğrenme güncellemesi\n" + summary,
            }))

    @staticmethod
    def _format_report(state: dict) -> str:
        lines = [f"Kapanmış HTF işlemi: {state['n_closed_trades']}"]
        for ttype, s in sorted(state["trigger_stats"].items()):
            d = state["conf_deltas"].get(ttype)
            off = " [DEVRE DIŞI]" if ttype in state["disabled"] else ""
            adj = f", conf {d:+.3f}" if d else ""
            lines.append(f"{ttype}: n={s['n']} win {s['win_rate']:.0%} "
                         f"beklenti {s['expectancy_r']:+.2f}R{adj}{off}")
        lines.append(f"Trailing ATR çarpanı: {state['trail_atr_mult']} "
                     f"({state['trail_reason']})")
        if not state["trigger_stats"]:
            lines.append("Henüz yeterli veri yok — sadece gözlem modunda.")
        return "\n".join(lines)

    async def run(self):
        if config.STRATEGY != "htf_breakout":
            logger.info(f"STRATEGY='{config.STRATEGY}'; learning agent idle.")
            while True:
                await asyncio.sleep(3600)
        await self.initialize()
        logger.info("Learning agent live: sample-gated parameter calibration from the trade journal.")
        try:
            while True:
                try:
                    await self.run_cycle()
                except Exception as e:
                    logger.error(f"Learning cycle error: {e}")
                await asyncio.sleep(LEARN_INTERVAL_SEC)
        finally:
            await self.cleanup()


if __name__ == "__main__":
    agent = LearningAgent()
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        logger.info("Learning Agent gracefully shut down.")
