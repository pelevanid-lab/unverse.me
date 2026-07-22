import asyncio
import logging
import os
import time
import orjson
import redis.asyncio as redis
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
from supabase import create_client, Client

import config
import signal_engine
import level_engine

# Only dispatch signals whose conviction clears this floor. This is the second
# gate on top of signal_engine's own entry threshold.
MIN_CONFIDENCE = float(os.getenv("MIN_SIGNAL_CONFIDENCE", "0.55"))

# HTF structure bonus: how close price must be to a zone to count as "at" it,
# and the max bonus a maximally-significant zone (many touches) can add.
ZONE_PROXIMITY_PCT = float(os.getenv("ZONE_PROXIMITY_PCT", "0.02"))
STRUCTURE_BONUS_MAX = float(os.getenv("STRUCTURE_BONUS_MAX", "0.30"))
STRUCTURE_BONUS_TOUCHES_NORM = float(os.getenv("STRUCTURE_BONUS_TOUCHES_NORM", "5.0"))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AgentOrchestrator")


class AgentOrchestrator:
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.pubsub = None
        
        # State: In-memory numerical state per symbol (core + dynamically added).
        self.state: Dict[str, Dict[str, float]] = {}
        # Cooldown state: last trigger timestamp per symbol
        self.last_trigger: Dict[str, float] = {}
        self.cooldown_seconds = 60.0
        for s in config.SYMBOLS:
            self._ensure_symbol(s.upper())

        # Narrative/sector bonus map {SYMBOL: bonus}, refreshed from narrative_agent.
        self.narrative_map: Dict[str, float] = {}

    def _ensure_symbol(self, sym_upper: str):
        """Register a symbol (core or dynamically discovered) for evaluation."""
        if sym_upper not in self.state:
            self.state[sym_upper] = {
                "cvd_1m": 0.0, "cvd_5m": 0.0, "imbalance": 0.5, "mark_price": 0.0,
                # liquidity-level features (from features:levels)
                "last_price": 0.0, "swing_high": 0.0, "swing_low": 0.0,
                "recent_min": 0.0, "recent_max": 0.0, "range_1m": 0.0,
                "sweep_low": False, "sweep_high": False,
                # indicator layers
                "ema_fast": 0.0, "ema_slow": 0.0, "rsi": 50.0, "atr": 0.0,
                # liquidity/squeeze awareness
                "vol_1m": 0.0, "oi_surge_15m": 0.0,
            }
            self.last_trigger[sym_upper] = 0.0

    async def initialize(self):
        logger.info(f"Connecting to Redis at {config.REDIS_URL}")
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        self.pubsub = self.redis_client.pubsub()
        await self.pubsub.psubscribe("features:*", "market:markprice:*", "narrative:sectors")
        logger.info("Subscribed to features, markprice and narrative channels.")

        # Seed the narrative bonus map from its last persisted value (if any).
        try:
            import narrative_agent
            raw = await self.redis_client.get(narrative_agent.NARRATIVE_MAP_KEY)
            if raw:
                payload = orjson.loads(raw)
                self.narrative_map = {k.upper(): float(v)
                                      for k, v in payload.get("bonus_map", {}).items()}
                logger.info(f"Seeded narrative bonus map: {self.narrative_map}")
        except Exception as e:
            logger.warning(f"Could not seed narrative map: {e}")

    async def cleanup(self):
        if self.pubsub:
            await self.pubsub.close()
        if self.redis_client:
            await self.redis_client.aclose()

    def _update_state(self, channel: str, data: dict):
        try:
            parts = channel.split(":")
            if channel.startswith("market:markprice:"):
                symbol = parts[-1]
                # Binance markPriceUpdate payload typically has 'p' for mark price
                if 'p' in data:
                    self._ensure_symbol(symbol)
                    self.state[symbol]["mark_price"] = float(data['p'])

            elif channel.startswith("features:cvd:"):
                symbol = parts[-1]
                self._ensure_symbol(symbol)
                self.state[symbol]["cvd_1m"] = float(data.get("cvd_1m", 0.0))
                self.state[symbol]["cvd_5m"] = float(data.get("cvd_5m", 0.0))
                self.state[symbol]["vol_1m"] = float(data.get("vol_1m", 0.0))

            elif channel.startswith("features:imbalance:"):
                symbol = parts[-1]
                self._ensure_symbol(symbol)
                self.state[symbol]["imbalance"] = float(data.get("imbalance", 0.5))

            elif channel.startswith("features:levels:"):
                symbol = parts[-1]
                self._ensure_symbol(symbol)
                st = self.state[symbol]
                for k in ("last_price", "swing_high", "swing_low", "recent_min",
                          "recent_max", "range_1m", "ema_fast", "ema_slow", "rsi", "atr",
                          "oi_surge_15m"):
                    if k in data:
                        st[k] = float(data[k])
                st["sweep_low"] = bool(data.get("sweep_low", False))
                st["sweep_high"] = bool(data.get("sweep_high", False))

            elif channel == "narrative:sectors":
                bm = data.get("bonus_map", {})
                self.narrative_map = {k.upper(): float(v) for k, v in bm.items()}
                logger.info(f"Narrative bonus map updated ({'LIVE' if data.get('grounded') else 'STALE'}): {self.narrative_map}")
        except (KeyError, ValueError) as e:
            logger.error(f"Error updating state for channel {channel}: {e}")

    async def _get_structure_bonus(self, symbol: str, current_state: dict):
        """Look up this symbol's HTF zones (published by level_agent) and, if
        the current sweep direction agrees with a nearby zone's inferred role
        (e.g. a LONG sweep right at a zone that flipped to support), return a
        bonus scaled by how significant (multi-touch) that zone is.

        Returns (bonus: float, zone_info: dict|None) — zone_info is logged
        into the signal's features so learn.py can later correlate HTF-zone
        alignment with realised PnL, same as the narrative sector_bonus.
        """
        if not self.redis_client:
            return 0.0, None
        try:
            raw = await self.redis_client.get(f"levels:zones:{symbol}")
        except Exception as e:
            logger.warning(f"[{symbol}] Could not read HTF zones: {e}")
            return 0.0, None
        if not raw:
            return 0.0, None

        try:
            zone_dicts = orjson.loads(raw)
        except Exception:
            return 0.0, None
        if not zone_dicts:
            return 0.0, None

        zones = [level_engine.Zone(
            price_low=z["price_low"], price_high=z["price_high"],
            touches=z["touches"], last_touch_ts=z.get("last_touch_ts", 0),
        ) for z in zone_dicts]

        price = float(current_state.get("last_price", 0) or 0)
        if price <= 0:
            return 0.0, None

        zone = level_engine.nearest_zone(zones, price, max_distance_pct=ZONE_PROXIMITY_PCT)
        if not zone:
            return 0.0, None

        role = next((z["role"] for z in zone_dicts
                     if z["price_low"] == zone.price_low and z["price_high"] == zone.price_high),
                    None)

        sweep_low = bool(current_state.get("sweep_low"))
        sweep_high = bool(current_state.get("sweep_high"))
        aligned = (sweep_low and role == "support") or (sweep_high and role == "resistance")

        zone_info = {"zone_low": zone.price_low, "zone_high": zone.price_high,
                     "touches": zone.touches, "role": role, "aligned": aligned}
        if not aligned:
            return 0.0, zone_info

        bonus = min(zone.touches / STRUCTURE_BONUS_TOUCHES_NORM, 1.0) * STRUCTURE_BONUS_MAX
        return bonus, zone_info

    async def _evaluate_symbol(self, symbol: str, current_state: dict):
        """Deterministic decision for a symbol using our own signal engine.

        No LLM: the same input always yields the same output, which is what
        makes it fast, free, backtestable, and learnable. Emits a pending
        signal (for manual approval) only when conviction clears the floor.
        """
        logger.info(f"Evaluating {symbol}. State: {current_state}")

        # Inject the current narrative/sector bonus for this symbol.
        current_state["sector_bonus"] = self.narrative_map.get(symbol, 0.0)

        # Inject the HTF structure bonus (multi-month zone alignment).
        structure_bonus, htf_zone_info = await self._get_structure_bonus(symbol, current_state)
        current_state["structure_bonus"] = structure_bonus

        # Pick the strategy (default: 5-layer confluence).
        if config.STRATEGY == "confluence":
            sig = signal_engine.evaluate_confluence(
                current_state,
                wick_buffer_mult=config.SL_WICK_BUFFER_MULT,
                tp_r_multiple=config.TP_R_MULTIPLE,
                min_sl_pct=config.MIN_SL_PCT,
                max_sl_pct=config.MAX_SL_PCT,
            )
        elif config.STRATEGY == "sweep_reversal":
            sig = signal_engine.evaluate_sweep(
                current_state,
                wick_buffer_mult=config.SL_WICK_BUFFER_MULT,
                tp_r_multiple=config.TP_R_MULTIPLE,
                min_sl_pct=config.MIN_SL_PCT,
                max_sl_pct=config.MAX_SL_PCT,
            )
        else:
            sig = signal_engine.evaluate(current_state)

        logger.info(f"[{symbol}] Decision: {sig.action} (Confidence: {sig.confidence:.2f}) | {sig.reasoning}")

        # Log every decision to the dashboard so the reasoning is visible live.
        config.send_log_to_dashboard(
            "AgentOrchestrator",
            sig.action,
            f"[{symbol}] Confidence: %{int(sig.confidence * 100)}. {sig.reasoning}"
        )

        if sig.action in ("LONG", "SHORT") and sig.confidence >= MIN_CONFIDENCE:
            logger.info(f"[{symbol}] Generating Pending Approval for {sig.action} signal.")

            if config.supabase:
                # Snapshot the exact features so the trade journal can later
                # correlate these entry conditions with the realised outcome.
                features = {
                    "mark_price": current_state.get("mark_price"),
                    "last_price": current_state.get("last_price"),
                    "cvd_1m": current_state.get("cvd_1m"),
                    "cvd_5m": current_state.get("cvd_5m"),
                    "imbalance": current_state.get("imbalance"),
                    "swing_high": current_state.get("swing_high"),
                    "swing_low": current_state.get("swing_low"),
                    "sweep_low": current_state.get("sweep_low"),
                    "sweep_high": current_state.get("sweep_high"),
                    "ema_fast": current_state.get("ema_fast"),
                    "ema_slow": current_state.get("ema_slow"),
                    "rsi": current_state.get("rsi"),
                    "atr": current_state.get("atr"),
                    "sector_bonus": current_state.get("sector_bonus"),
                    "structure_bonus": current_state.get("structure_bonus"),
                    "vol_1m": current_state.get("vol_1m"),
                    "oi_surge_15m": current_state.get("oi_surge_15m"),
                    "htf_zone": htf_zone_info,
                    "layer_contributions": sig.contributions,
                    # structure-based bracket the execution engine should honour
                    "sl_price": sig.sl_price,
                    "tp_price": sig.tp_price,
                }

                def _insert_pending_signal():
                    try:
                        res = config.supabase.table("pending_signals").insert({
                            "symbol": symbol,
                            "action": sig.action,
                            "confidence": sig.confidence,
                            "reasoning": sig.reasoning,
                            "score": sig.score,
                            "features": features,
                            "sl_price": sig.sl_price,
                            "tp_price": sig.tp_price,
                            "status": "PENDING"
                        }).execute()
                        logger.info(f"[{symbol}] Dispatched PENDING signal to Dashboard for user approval.")
                        
                        signal_id = res.data[0]['id'] if res.data else str(int(time.time() * 1000))
                        
                        # Also notify Telegram Agent
                        if self.redis_client:
                            asyncio.run_coroutine_threadsafe(
                                self.redis_client.publish("telegram:notify", orjson.dumps({
                                    "signal_id": signal_id,
                                    "symbol": symbol,
                                    "action": sig.action,
                                    "confidence": sig.confidence,
                                    "reasoning": sig.reasoning
                                })),
                                asyncio.get_running_loop()
                            )
                    except Exception as e:
                        logger.error(f"[{symbol}] Failed to insert pending signal to Supabase: {e}")

                asyncio.create_task(asyncio.to_thread(_insert_pending_signal))

    async def listen_to_channels(self):
        """Listen to incoming Redis messages and update state."""
        while True:
            try:
                message = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                if message and message['type'] == 'pmessage':
                    channel = message['channel'].decode('utf-8')
                    data = orjson.loads(message['data'])
                    self._update_state(channel, data)
                else:
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error reading message from Redis: {e}")
                await asyncio.sleep(1)

    async def evaluate_triggers(self):
        """Evaluate the state every second to check if we should trigger the agent."""
        while True:
            try:
                current_time = time.time()

                # Evaluate ALL tracked symbols (core + dynamically discovered).
                for sym_upper in list(self.state.keys()):
                    state = self.state[sym_upper]

                    # Check Cooldown
                    if current_time - self.last_trigger[sym_upper] < self.cooldown_seconds:
                        continue

                    # Trigger condition depends on the active strategy.
                    if config.STRATEGY in ("confluence", "sweep_reversal"):
                        # Fire when a liquidity sweep just printed on either side.
                        triggered = state.get("sweep_low") or state.get("sweep_high")
                    else:
                        imbalance = state["imbalance"]
                        triggered = imbalance > 0.70 or imbalance < 0.30

                    if triggered:
                        # Update last trigger time immediately to prevent spam
                        self.last_trigger[sym_upper] = current_time

                        # Snapshot the state and evaluate deterministically (cheap, local)
                        state_snapshot = state.copy()
                        await self._evaluate_symbol(sym_upper, state_snapshot)
                        
            except Exception as e:
                logger.error(f"Error in evaluate_triggers loop: {e}")
                
            await asyncio.sleep(1.0)

    async def run(self):
        await self.initialize()
        try:
            await asyncio.gather(
                self.listen_to_channels(),
                self.evaluate_triggers()
            )
        finally:
            await self.cleanup()

if __name__ == "__main__":
    orchestrator = AgentOrchestrator()
    try:
        asyncio.run(orchestrator.run())
    except KeyboardInterrupt:
        logger.info("Agent Orchestrator gracefully shut down.")
