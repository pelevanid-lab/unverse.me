"""
HTF agent — the decision layer for the "htf_breakout" strategy.

Replaces the 15-minute sweep scalper as the signal source. Scans once every
4 hours (config.HTF_CHECK_INTERVAL_SEC) instead of continuously polling:

  1. Universe: PURELY the squeeze-based discovery result (refresh_universe) —
     no fixed/core watchlist. Every liquid USDT perp is scored by
     htf_strategy.squeeze_ratio and the tightest-coiling HTF_SCAN_TOP_N make
     the cut; symbols without enough daily history (fresh speculative
     listings) are rejected inside the strategy itself. Any symbol can still
     be analyzed on demand regardless of universe membership — see (4).
  2. Each cycle, fetch CLOSED daily + 3-day candles and run
     htf_strategy.evaluate_htf_breakout per symbol — the breakout TRIGGER
     lives on those two timeframes (a candle must CLOSE beyond a level, a
     wick doesn't count), re-evaluated only when a new daily/3-day candle has
     actually closed since the last check. Confirmed breakouts become
     PENDING signals in Supabase (manual approval, same flow as before) and a
     Telegram notification.
  3. For OPEN positions, ratchet the trailing stop once per 4h close
     (htf_strategy.compute_trailing_stop) and publish the new stop on Redis
     channel "manage:stop" — execution_engine amends the exchange stop order.
     This is the "4 saatlikte takip" layer: entries are decided on 1d/3d,
     but the position is managed with 4h granularity.
  4. Two on-demand commands from Telegram (via Redis, see telegram_agent.py):
     "htf:manual_scan" wakes the cycle loop immediately instead of waiting
     for the 4h timer; "htf:analyze_request" runs the full evaluation for ONE
     named symbol right now (in or out of the current universe) and reports
     back on "telegram:analysis", dispatching for approval same as any other
     actionable signal if it clears the confidence floor.

Everything is deterministic; the LLM is nowhere in the trade path.
"""
import asyncio
import logging
import time
from typing import Dict, Optional

import ccxt.async_support as ccxt
import orjson
import redis.asyncio as redis

import config
import htf_strategy
from htf_strategy import Bar

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("HTFAgent")

FOUR_H_MS = 4 * 60 * 60 * 1000


class HTFAgent:
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.exchange = ccxt.binanceusdm({'enableRateLimit': True})

        # Scan universe: {SYMBOL: ccxt_symbol}
        self.universe: Dict[str, str] = {}
        self.last_universe_refresh = 0.0

        # Last evaluated closed-4h-candle ts per symbol (avoid re-evaluating).
        self.last_eval_candle: Dict[str, int] = {}
        # Cooldown: last actionable signal ts (ms) per symbol.
        self.last_signal_ts: Dict[str, float] = {}

        # Trailing state per open symbol:
        # {symbol: {side, entry_price, entry_ts, initial_sl, current_sl}}
        self.trail_state: Dict[str, dict] = {}

        # Armed retest levels: a confirmed breakout that was too extended to
        # chase parks its level here; every 4h we check whether price pulled
        # back and HELD it (htf_strategy.evaluate_retest) — that becomes the
        # entry ("clear retest for next leg up").
        # {symbol: {direction, level, zone_low, zone_high, atr_1d, armed_ts}}
        self.armed_retests: Dict[str, dict] = {}

        # De-risked zones per open position: {symbol: {(zone_low, zone_high), ...}}
        # so a zone only triggers a partial close ONCE, not every cycle we're
        # still near it.
        self.derisked_zones: Dict[str, set] = {}

        # Latest calibration from learning_agent (Redis "learn:adjustments"),
        # refreshed once per cycle: bounded confidence deltas per trigger
        # type, disabled trigger types, and the learned trailing multiplier.
        self.learn_state: dict = {}

    async def initialize(self):
        logger.info(f"Connecting to Redis at {config.REDIS_URL}")
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        await self.exchange.load_markets()
        await self._restore_cooldowns()

    async def _restore_cooldowns(self):
        """Restore last_signal_ts (per-symbol dispatch cooldown) from
        Supabase pending_signals on startup.

        last_eval_candle/last_signal_ts live only in memory, so EVERY
        restart (a routine redeploy included) used to wipe them — the very
        next scan then treated every symbol as "never evaluated" and
        RE-DISPATCHED whatever daily/3-day candle was still current, even
        though it had already been sent (and possibly approved/executed)
        hours earlier. This is exactly what happened on 2026-07-22: a
        redeploy restart caused XMRUSDT and LABUSDT to be dispatched a
        second time. Restoring the cooldown timestamp is enough to close
        the gap — it doesn't matter that last_eval_candle itself isn't
        restored, since HTF_SIGNAL_COOLDOWN_HOURS blocks the re-dispatch
        regardless of whether the candle gets re-evaluated.
        """
        if not config.supabase:
            return
        try:
            def _fetch():
                return config.supabase.table("pending_signals") \
                    .select("symbol,created_at") \
                    .order("created_at", desc=True).limit(500).execute()
            res = await asyncio.to_thread(_fetch)
            from datetime import datetime
            restored = 0
            for row in (res.data or []):
                symbol = row.get("symbol")
                if not symbol or symbol in self.last_signal_ts:
                    continue  # keep the most recent (rows are newest-first)
                try:
                    ts = datetime.fromisoformat(
                        str(row["created_at"]).replace("Z", "+00:00")).timestamp() * 1000
                except Exception:
                    continue
                self.last_signal_ts[symbol] = ts
                restored += 1
            logger.info(f"Restored dispatch cooldown for {restored} symbol(s) from Supabase.")
        except Exception as e:
            logger.warning(f"Could not restore cooldown state (starting cold): {e}")

    async def cleanup(self):
        if self.redis_client:
            await self.redis_client.aclose()
        if self.exchange:
            await self.exchange.close()

    # ------------------------------------------------------------------
    # Universe
    # ------------------------------------------------------------------
    async def refresh_universe(self):
        """Top-N most COMPRESSED (squeezing) liquid USDT perps. No fixed/core
        watchlist — the scan universe is PURELY this discovery result. Any
        specific symbol can still be examined regardless of universe
        membership via the on-demand analyze_symbol_on_demand() command.

        24h volume is used ONLY as a tradability floor (can we actually get
        filled and get out), never as the ranking key — ranking by recent
        volume biases discovery toward coins that already pumped/dumped,
        which is the opposite of what an HTF breakout scanner wants: we want
        coins coiling QUIETLY, before the move, not chasing ones already in
        motion. Ranking is by htf_strategy.squeeze_ratio (price-range
        contraction over ~2 months of daily candles) instead.
        """
        universe: Dict[str, str] = {}

        try:
            tickers = await self.exchange.fetch_tickers()
            floor_passing = []
            for ccxt_sym, t in tickers.items():
                try:
                    market = self.exchange.market(ccxt_sym)
                except Exception:
                    continue
                binance_id = market.get('id', '')
                if not binance_id.endswith('USDT') or market.get('linear') is False:
                    continue
                qv = float(t.get('quoteVolume', 0) or 0)
                if qv < config.HTF_MIN_QUOTE_VOLUME:
                    continue  # tradability floor only, NOT a ranking key
                floor_passing.append((binance_id, ccxt_sym))
        except Exception as e:
            logger.warning(f"Universe refresh via tickers failed ({e}); universe left empty this cycle.")
            floor_passing = []

        need_days = config.HTF_SQUEEZE_RECENT_DAYS + config.HTF_SQUEEZE_BASELINE_DAYS
        scored = []
        for binance_id, ccxt_sym in floor_passing:
            try:
                bars = await self.fetch_closed_bars(ccxt_sym, config.HTF_DAILY_TIMEFRAME, need_days + 5)
                ratio = htf_strategy.squeeze_ratio(
                    bars, config.HTF_SQUEEZE_RECENT_DAYS, config.HTF_SQUEEZE_BASELINE_DAYS
                )
                if ratio is not None:
                    scored.append((ratio, binance_id, ccxt_sym))
            except Exception as e:
                logger.warning(f"[{binance_id}] Squeeze scan failed: {e}")
            await asyncio.sleep(0.2)  # rate-limit courtesy across the full liquid list

        scored.sort(key=lambda x: x[0])  # tightest squeeze first
        chosen = scored[:config.HTF_SCAN_TOP_N]
        for ratio, binance_id, ccxt_sym in chosen:
            universe[binance_id] = ccxt_sym

        detail = ", ".join(f"{b}({r:.2f})" for r, b, _ in chosen[:10])
        logger.info(f"Squeeze scan: {len(floor_passing)} liquid candidates -> "
                    f"{len(chosen)} tightest coiling (top 10: {detail}).")
        config.send_log_to_dashboard("HTFAgent", "DISCOVERY",
                                      f"Coiling watchlist ({len(chosen)}): {detail}")

        self.universe = universe
        self.last_universe_refresh = time.time()
        logger.info(f"Scan universe: {len(universe)} symbols.")

    def _resolve_ccxt_symbol(self, symbol_upper: str) -> Optional[str]:
        for ccxt_sym, market in self.exchange.markets.items():
            if market.get('id', '').upper() == symbol_upper:
                return ccxt_sym
        return None

    # ------------------------------------------------------------------
    # Learning calibration (written by learning_agent, bounded by design)
    # ------------------------------------------------------------------
    async def refresh_learn_state(self):
        try:
            raw = await self.redis_client.get("learn:adjustments")
            self.learn_state = orjson.loads(raw) if raw else {}
        except Exception as e:
            logger.warning(f"Could not read learn:adjustments: {e}")
            self.learn_state = {}

    def _apply_learning(self, symbol: str, sig) -> Optional["htf_strategy.HTFSignal"]:
        """Apply the journal-derived calibration to a fresh signal: skip
        trigger types the record shows losing, nudge confidence by the
        bounded per-type delta. Returns None when the signal should be
        dropped."""
        if sig is None or sig.action == "WAIT":
            return sig
        ttype = sig.features.get("trigger_type", "unknown")
        if ttype in self.learn_state.get("disabled", []):
            logger.info(f"[{symbol}] {sig.action} [{ttype}] skipped: trigger type "
                        f"disabled by learning (negative expectancy in journal).")
            return None
        delta = float(self.learn_state.get("conf_deltas", {}).get(ttype, 0.0) or 0.0)
        if delta:
            sig.confidence = round(max(0.0, min(sig.confidence + delta, 0.95)), 4)
            sig.features["learn_conf_delta"] = delta
            sig.reasoning += f" | learn adj {delta:+.3f}"
        return sig

    @property
    def trail_atr_mult(self) -> float:
        """Trailing multiplier: journal-calibrated when available (bounded
        [2.0, 4.0] by learning_agent), else the configured default."""
        v = self.learn_state.get("trail_atr_mult")
        return float(v) if v else config.HTF_TRAIL_ATR_MULT

    # ------------------------------------------------------------------
    # Candle fetching
    # ------------------------------------------------------------------
    async def fetch_closed_bars(self, ccxt_symbol: str, timeframe: str, limit: int):
        """Fetch OHLCV and DROP the still-forming last candle."""
        ohlcv = await self.exchange.fetch_ohlcv(ccxt_symbol, timeframe=timeframe, limit=limit)
        if not ohlcv:
            return []
        bars = htf_strategy.to_bars(ohlcv)
        # The last row is the live (unclosed) candle whenever its open time
        # is the current period — drop it so decisions use closed data only.
        tf_ms = self.exchange.parse_timeframe(timeframe) * 1000
        now_ms = int(time.time() * 1000)
        if bars and bars[-1].ts + tf_ms > now_ms:
            bars = bars[:-1]
        return bars

    # ------------------------------------------------------------------
    # Signal dispatch (Supabase pending approval + Telegram)
    # ------------------------------------------------------------------
    async def dispatch_signal(self, symbol: str, sig: htf_strategy.HTFSignal):
        config.send_log_to_dashboard(
            "HTFAgent", sig.action,
            f"[{symbol}] Confidence: %{int(sig.confidence * 100)}. {sig.reasoning}"
        )
        if not config.supabase:
            logger.warning(f"[{symbol}] No Supabase configured; signal not persisted.")
            return

        row = {
            "symbol": symbol,
            "action": sig.action,
            "confidence": sig.confidence,
            "reasoning": sig.reasoning,
            "score": sig.confidence if sig.action == "LONG" else -sig.confidence,
            "features": sig.features,
            "sl_price": sig.sl_price,
            "tp_price": None,           # no fixed TP: trailing exit
            "status": "PENDING",
        }

        def _insert():
            return config.supabase.table("pending_signals").insert(row).execute()

        try:
            res = await asyncio.to_thread(_insert)
            signal_id = res.data[0]['id'] if res.data else str(int(time.time() * 1000))
            logger.info(f"[{symbol}] PENDING signal dispatched for approval (id={signal_id}).")
        except Exception as e:
            logger.error(f"[{symbol}] Failed to insert pending signal: {e}")
            return

        # Telegram notification — awaited in THIS loop (the old orchestrator
        # published from inside a worker thread with no event loop, which
        # raised and silently killed every notification).
        try:
            await self.redis_client.publish("telegram:notify", orjson.dumps({
                "signal_id": signal_id,
                "symbol": symbol,
                "action": sig.action,
                "confidence": sig.confidence,
                "reasoning": sig.reasoning,
                "entry_price": sig.entry_price,
                "sl_price": sig.sl_price,
                "risk_pct": sig.features.get("risk_pct"),
            }))
            logger.info(f"[{symbol}] Telegram notification published.")
        except Exception as e:
            logger.error(f"[{symbol}] Failed to publish Telegram notification: {e}")

    # ------------------------------------------------------------------
    # Breakout scan
    # ------------------------------------------------------------------
    async def scan_symbol(self, symbol: str, ccxt_symbol: str):
        try:
            # Trigger candles: daily (fine) + 3-day (coarse) confirmation.
            bars_1d = await self.fetch_closed_bars(ccxt_symbol, config.HTF_DAILY_TIMEFRAME, 200)
            if not bars_1d:
                return
            bars_3d = await self.fetch_closed_bars(ccxt_symbol, config.HTF_3D_TIMEFRAME, 200)

            # Only re-evaluate once a NEW daily or 3-day candle has actually
            # closed since the last check — avoids re-firing on the same
            # confirmed break every 4h scan.
            eval_key = (bars_1d[-1].ts, bars_3d[-1].ts if bars_3d else 0)
            if self.last_eval_candle.get(symbol) == eval_key:
                return

            # Current price: latest closed 4h candle. The daily/3-day candle
            # may have closed hours ago; scanning every 4h keeps the
            # anti-chase/extension check and entry price fresh.
            bars_4h = await self.fetch_closed_bars(ccxt_symbol, config.HTF_TRACK_TIMEFRAME, 10)
            if not bars_4h:
                return
            current_price = bars_4h[-1].close
            current_ts = bars_4h[-1].ts

            sig = htf_strategy.evaluate_htf_breakout(
                bars_1d, bars_3d, current_price, current_ts,
                min_daily_candles=config.HTF_MIN_DAILY_CANDLES,
                donchian_lookback_1d=config.HTF_DONCHIAN_LOOKBACK_1D,
                donchian_lookback_3d=config.HTF_DONCHIAN_LOOKBACK_3D,
                vol_expansion_mult=config.HTF_VOL_EXPANSION_MULT,
                max_extension_atr=config.HTF_MAX_EXTENSION_ATR,
                sl_zone_atr_mult=config.HTF_SL_ZONE_ATR_MULT,
                min_sl_atr_mult=config.HTF_MIN_SL_ATR_MULT,
                max_sl_pct=config.HTF_MAX_SL_PCT,
                max_atr_pct=config.HTF_MAX_ATR_PCT,
                spent_lookback_days=config.HTF_SPENT_LOOKBACK_DAYS,
                spent_drop_pct=config.HTF_SPENT_DROP_PCT,
                spent_rise_mult=config.HTF_SPENT_RISE_MULT,
            )
            # Mark AFTER a successful evaluation so a transient fetch error
            # above gets retried on the next scan instead of this daily/3-day
            # candle being silently skipped forever.
            self.last_eval_candle[symbol] = eval_key

            if sig.action == "WAIT":
                # A confirmed-but-too-extended break arms a retest watch: if
                # price pulls back to the level and holds, we enter THERE.
                if sig.features.get("armable_retest") and symbol not in self.armed_retests:
                    self.armed_retests[symbol] = {
                        "direction": sig.features["direction"],
                        "level": float(sig.features["level"]),
                        "zone_low": sig.features.get("zone_low"),
                        "zone_high": sig.features.get("zone_high"),
                        "atr_1d": float(sig.features["atr_1d"]),
                        "armed_ts": int(time.time() * 1000),
                    }
                    logger.info(f"[{symbol}] Retest ARMED: {sig.features['direction']} "
                                f"level {sig.features['level']:.6g} (was too extended to chase).")
                    config.send_log_to_dashboard(
                        "HTFAgent", "RETEST_ARMED",
                        f"[{symbol}] {sig.features['direction']} kırılım teyitli ama fiyat "
                        f"uzaklaşmış; {sig.features['level']:.6g} seviyesine retest bekleniyor."
                    )
                # Only log interesting rejections (an actual level interaction).
                elif "break" in sig.reasoning or "listing" in sig.reasoning or "Conflicting" in sig.reasoning:
                    logger.info(f"[{symbol}] WAIT: {sig.reasoning}")
                return

            # Journal-derived calibration: skip disabled trigger types, apply
            # the bounded per-type confidence delta BEFORE the floor check.
            sig = self._apply_learning(symbol, sig)
            if sig is None:
                return

            if sig.confidence < config.HTF_MIN_CONFIDENCE:
                logger.info(f"[{symbol}] {sig.action} below confidence floor "
                            f"({sig.confidence:.2f} < {config.HTF_MIN_CONFIDENCE}); not dispatched.")
                return

            # Per-symbol cooldown so one trending coin doesn't spam approvals.
            now_ms = time.time() * 1000
            cooldown_ms = config.HTF_SIGNAL_COOLDOWN_HOURS * 3600 * 1000
            if now_ms - self.last_signal_ts.get(symbol, 0) < cooldown_ms:
                logger.info(f"[{symbol}] {sig.action} suppressed by cooldown.")
                return
            self.last_signal_ts[symbol] = now_ms

            logger.info(f"[{symbol}] ACTIONABLE {sig.action} ({sig.confidence:.2f}): {sig.reasoning}")
            await self.dispatch_signal(symbol, sig)
        except Exception as e:
            logger.error(f"[{symbol}] Scan failed: {e}")

    async def scan_cycle(self):
        # Scan cadence is now 4h (config.HTF_CHECK_INTERVAL_SEC), so this
        # simply refreshes the liquid-symbol universe once per cycle.
        if time.time() - self.last_universe_refresh > 900:
            await self.refresh_universe()
        for symbol, ccxt_symbol in list(self.universe.items()):
            await self.scan_symbol(symbol, ccxt_symbol)
            await asyncio.sleep(0.25)   # rate-limit courtesy

    # ------------------------------------------------------------------
    # Armed-retest management — checked every 4h cycle (NOT gated on a new
    # daily candle: the touch-and-hold plays out on 4h closes)
    # ------------------------------------------------------------------
    async def manage_retests(self):
        now_ms = time.time() * 1000
        valid_ms = config.HTF_RETEST_VALID_DAYS * 86_400_000
        for symbol in list(self.armed_retests.keys()):
            st = self.armed_retests[symbol]

            if now_ms - st["armed_ts"] > valid_ms:
                logger.info(f"[{symbol}] Armed retest expired "
                            f"({config.HTF_RETEST_VALID_DAYS}d) without a pullback.")
                del self.armed_retests[symbol]
                continue

            ccxt_symbol = self.universe.get(symbol) or self._resolve_ccxt_symbol(symbol)
            if not ccxt_symbol:
                continue
            try:
                bars_4h = await self.fetch_closed_bars(
                    ccxt_symbol, config.HTF_TRACK_TIMEFRAME, 80)
                status, sig = htf_strategy.evaluate_retest(
                    st["direction"], st["level"], st.get("zone_low"),
                    st.get("zone_high"), st["atr_1d"], st["armed_ts"], bars_4h,
                    proximity_atr=config.HTF_RETEST_PROXIMITY_ATR,
                    sl_zone_atr_mult=config.HTF_SL_ZONE_ATR_MULT,
                    min_sl_atr_mult=config.HTF_MIN_SL_ATR_MULT,
                    max_sl_pct=config.HTF_MAX_SL_PCT,
                )
                if status == "invalidated":
                    logger.info(f"[{symbol}] Armed retest INVALIDATED: breakout failed.")
                    del self.armed_retests[symbol]
                elif status == "triggered" and sig is not None:
                    del self.armed_retests[symbol]
                    sig = self._apply_learning(symbol, sig)
                    if sig is None or sig.confidence < config.HTF_MIN_CONFIDENCE:
                        continue
                    cooldown_ms = config.HTF_SIGNAL_COOLDOWN_HOURS * 3600 * 1000
                    if now_ms - self.last_signal_ts.get(symbol, 0) < cooldown_ms:
                        logger.info(f"[{symbol}] Retest {sig.action} suppressed by cooldown.")
                        continue
                    self.last_signal_ts[symbol] = now_ms
                    logger.info(f"[{symbol}] RETEST TRIGGERED {sig.action} "
                                f"({sig.confidence:.2f}): {sig.reasoning}")
                    await self.dispatch_signal(symbol, sig)
            except Exception as e:
                logger.error(f"[{symbol}] Retest check failed: {e}")
            await asyncio.sleep(0.2)

    # ------------------------------------------------------------------
    # Trailing-stop management for open positions
    # ------------------------------------------------------------------
    async def _load_open_positions(self) -> Dict[str, dict]:
        """OPEN rows from Supabase active_trades: {symbol: row}."""
        if not config.supabase:
            return {}

        def _fetch():
            return config.supabase.table("active_trades").select("*") \
                .eq("status", "OPEN").execute()
        try:
            res = await asyncio.to_thread(_fetch)
            return {row["symbol"]: row for row in (res.data or [])}
        except Exception as e:
            logger.error(f"Could not load open positions: {e}")
            return {}

    async def _seed_trail_state(self, symbol: str, row: dict):
        """Initialise trail tracking for a position we haven't seen yet."""
        entry_price = float(row.get("entry_price", 0) or 0)
        side = row.get("side", "LONG")

        # Best source for the initial stop: the most recent EXECUTED signal.
        initial_sl = None
        entry_ts = None
        if config.supabase:
            def _fetch_sig():
                return config.supabase.table("pending_signals").select("*") \
                    .eq("symbol", symbol).eq("status", "EXECUTED") \
                    .order("created_at", desc=True).limit(1).execute()
            try:
                res = await asyncio.to_thread(_fetch_sig)
                if res.data:
                    initial_sl = res.data[0].get("sl_price")
            except Exception:
                pass

        ccxt_symbol = self.universe.get(symbol) or self._resolve_ccxt_symbol(symbol)
        if initial_sl is None and ccxt_symbol and entry_price > 0:
            bars_4h = await self.fetch_closed_bars(ccxt_symbol, config.HTF_TRACK_TIMEFRAME, 60)
            atr4 = htf_strategy.true_atr(bars_4h, 14) if bars_4h else 0.0
            if atr4 > 0:
                initial_sl = (entry_price - 2 * atr4 if side == "LONG"
                              else entry_price + 2 * atr4)
        if initial_sl is None:
            return  # can't manage what we can't anchor

        created = row.get("created_at")
        if created:
            try:
                from datetime import datetime
                entry_ts = int(datetime.fromisoformat(
                    str(created).replace("Z", "+00:00")).timestamp() * 1000)
            except Exception:
                entry_ts = None
        if entry_ts is None:
            entry_ts = int(time.time() * 1000)

        self.trail_state[symbol] = {
            "side": side,
            "entry_price": entry_price,
            "entry_ts": entry_ts,
            "initial_sl": float(initial_sl),
            "current_sl": float(initial_sl),
            "last_trail_candle": 0,
        }
        logger.info(f"[{symbol}] Trail state seeded: {side} from {entry_price} "
                    f"initial SL {initial_sl}.")

    async def manage_derisk(self):
        """Reduce part of an open position BEFORE price reaches the next
        significant HTF zone in its path — Melih's "büyük dirence yaklaşırken
        pozisyon hafiflet" pattern. Autonomous: this only ever shrinks
        exposure, never opens anything, so it needs no Telegram approval,
        same principle as the trailing stop.
        """
        open_rows = await self._load_open_positions()

        for sym in list(self.derisked_zones.keys()):
            if sym not in open_rows:
                del self.derisked_zones[sym]

        for symbol, row in open_rows.items():
            st = self.trail_state.get(symbol)
            if not st:
                continue  # not yet seeded (manage_trailing seeds it first)

            ccxt_symbol = self.universe.get(symbol) or self._resolve_ccxt_symbol(symbol)
            if not ccxt_symbol:
                continue
            try:
                bars_1d = await self.fetch_closed_bars(ccxt_symbol, config.HTF_DAILY_TIMEFRAME, 200)
                bars_3d = await self.fetch_closed_bars(ccxt_symbol, config.HTF_3D_TIMEFRAME, 200)
                bars_4h = await self.fetch_closed_bars(ccxt_symbol, config.HTF_TRACK_TIMEFRAME, 20)
                if not bars_1d or not bars_4h:
                    continue

                current_price = bars_4h[-1].close
                atr_1d = htf_strategy.true_atr(bars_1d, 14)
                if atr_1d <= 0:
                    continue

                zones = htf_strategy.build_htf_zones(
                    bars_1d, bars_3d, min_touches=config.HTF_DERISK_MIN_ZONE_TOUCHES)
                zone = htf_strategy.next_zone_ahead(st["side"], current_price, zones)
                if not zone:
                    continue

                zone_key = (round(zone.price_low, 8), round(zone.price_high, 8))
                done = self.derisked_zones.setdefault(symbol, set())
                if zone_key in done:
                    continue

                edge = zone.price_low if st["side"] == "LONG" else zone.price_high
                dist_atr = abs(edge - current_price) / atr_1d
                if dist_atr > config.HTF_DERISK_PROXIMITY_ATR:
                    continue

                done.add(zone_key)
                await self.redis_client.publish("manage:derisk", orjson.dumps({
                    "symbol": symbol,
                    "side": st["side"],
                    "fraction": config.HTF_DERISK_FRACTION,
                }))
                logger.info(f"[{symbol}] De-risk triggered: reducing {config.HTF_DERISK_FRACTION:.0%} "
                            f"ahead of {zone.touches}-touch zone {zone.price_low:.6g}-"
                            f"{zone.price_high:.6g} ({dist_atr:.2f} ATR away).")
                config.send_log_to_dashboard(
                    "HTFAgent", "DERISK",
                    f"[{symbol}] Önemli bölgeye ({zone.price_low:.6g}-{zone.price_high:.6g}, "
                    f"{zone.touches} dokunuş, {dist_atr:.2f} ATR uzaklık) yaklaşıldı; "
                    f"pozisyonun %{config.HTF_DERISK_FRACTION*100:.0f}'i azaltılıyor."
                )
            except Exception as e:
                logger.error(f"[{symbol}] De-risk check failed: {e}")
            await asyncio.sleep(0.2)

    async def manage_trailing(self):
        open_rows = await self._load_open_positions()

        # Drop state for closed positions.
        for sym in list(self.trail_state.keys()):
            if sym not in open_rows:
                logger.info(f"[{sym}] Position closed; trail state cleared.")
                del self.trail_state[sym]

        for symbol, row in open_rows.items():
            if symbol not in self.trail_state:
                await self._seed_trail_state(symbol, row)
            st = self.trail_state.get(symbol)
            if not st:
                continue

            ccxt_symbol = self.universe.get(symbol) or self._resolve_ccxt_symbol(symbol)
            if not ccxt_symbol:
                continue
            try:
                bars_4h = await self.fetch_closed_bars(ccxt_symbol, config.HTF_TRACK_TIMEFRAME, 200)
                if not bars_4h:
                    continue
                if st["last_trail_candle"] == bars_4h[-1].ts:
                    continue   # already ratcheted on this candle close
                st["last_trail_candle"] = bars_4h[-1].ts

                new_sl = htf_strategy.compute_trailing_stop(
                    st["side"], bars_4h,
                    entry_price=st["entry_price"],
                    entry_ts=st["entry_ts"],
                    initial_sl=st["initial_sl"],
                    current_sl=st["current_sl"],
                    trail_atr_mult=self.trail_atr_mult,   # journal-calibrated
                    breakeven_r=config.HTF_BREAKEVEN_R,
                )
                if new_sl is None:
                    continue

                st["current_sl"] = new_sl
                await self.redis_client.publish("manage:stop", orjson.dumps({
                    "symbol": symbol,
                    "side": st["side"],
                    "new_sl": new_sl,
                }))
                logger.info(f"[{symbol}] Trailing stop ratcheted to {new_sl:.6g}.")
                config.send_log_to_dashboard(
                    "HTFAgent", "TRAIL",
                    f"[{symbol}] {st['side']} trailing stop -> {new_sl:.6g}."
                )
            except Exception as e:
                logger.error(f"[{symbol}] Trailing management failed: {e}")

    # ------------------------------------------------------------------
    async def run(self):
        if config.STRATEGY != "htf_breakout":
            logger.info(f"STRATEGY='{config.STRATEGY}' (not htf_breakout); HTF agent idle.")
            while True:
                await asyncio.sleep(3600)

        await self.initialize()
        logger.info("HTF breakout agent live: 1d/3d confirmed breakouts + "
                    "trendline triggers + retest entries, tracked on 4h.")
        self.manual_trigger_event = asyncio.Event()
        try:
            await asyncio.gather(
                self.command_listener(),
                self.poll_dashboard_commands(),
                self.cycle_loop(),
            )
        finally:
            await self.cleanup()

    async def run_full_cycle(self):
        """One full pass: discovery, entry evaluation, retest/derisk/trailing
        management. Called by the 4h timer AND by a manual scan trigger — the
        exact same routine either way, just fired early on demand."""
        await self.refresh_learn_state()   # pick up the latest calibration first
        await self.scan_cycle()
        await self.manage_retests()
        await self.manage_trailing()   # seeds trail_state before de-risk needs it
        await self.manage_derisk()

    async def cycle_loop(self):
        while True:
            try:
                await self.run_full_cycle()
            except Exception as e:
                logger.error(f"HTF cycle error: {e}")

            self.manual_trigger_event.clear()
            try:
                await asyncio.wait_for(
                    self.manual_trigger_event.wait(), timeout=config.HTF_CHECK_INTERVAL_SEC
                )
                logger.info("Manual scan trigger received — running the cycle now.")
            except asyncio.TimeoutError:
                pass  # normal 4h tick

    # ------------------------------------------------------------------
    # On-demand commands from Telegram (via Redis) — manual scan trigger and
    # single-symbol analysis, independent of the current discovered universe.
    # ------------------------------------------------------------------
    async def command_listener(self):
        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe("htf:manual_scan", "htf:analyze_request")
        logger.info("Listening for manual-scan / analyze-symbol commands...")
        while True:
            try:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message['type'] == 'message':
                    channel = message['channel'].decode('utf-8')
                    raw = message['data']
                    data = orjson.loads(raw) if raw else {}
                    if channel == "htf:manual_scan":
                        logger.info("Manual scan requested via Telegram.")
                        self.manual_trigger_event.set()
                    elif channel == "htf:analyze_request":
                        symbol = str(data.get("symbol", "")).strip().upper()
                        if symbol:
                            asyncio.create_task(self.analyze_symbol_on_demand(symbol))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Command listener error: {e}")
            await asyncio.sleep(0.1)

    async def poll_dashboard_commands(self):
        """Same two on-demand commands as command_listener, but sourced from
        the web dashboard via Supabase instead of Telegram via Redis (Redis
        isn't reachable from the dashboard's Vercel-hosted serverless
        functions without exposing it to the public internet, which the
        unauthenticated instance in docker-compose should never be)."""
        if not config.supabase:
            return
        logger.info("Polling dashboard_commands (web dashboard manual scan / analyze)...")
        while True:
            try:
                def _fetch():
                    return config.supabase.table("dashboard_commands").select("*") \
                        .eq("status", "PENDING").order("created_at").execute()
                res = await asyncio.to_thread(_fetch)
                for cmd in (res.data or []):
                    cmd_id = cmd["id"]

                    def _mark_processed(_id=cmd_id):
                        config.supabase.table("dashboard_commands") \
                            .update({"status": "PROCESSED"}).eq("id", _id).execute()
                    await asyncio.to_thread(_mark_processed)

                    if cmd.get("type") == "manual_scan":
                        logger.info("Manual scan requested via dashboard.")
                        self.manual_trigger_event.set()
                    elif cmd.get("type") == "analyze_symbol":
                        symbol = str(cmd.get("symbol", "")).strip().upper()
                        if symbol:
                            asyncio.create_task(self.analyze_symbol_on_demand(symbol))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Dashboard command poll error: {e}")
            await asyncio.sleep(3.0)

    async def analyze_symbol_on_demand(self, symbol: str):
        """Full evaluation for ONE named symbol, right now — regardless of
        whether it's in the current discovered universe. Reports back on
        "telegram:analysis"; if the result is actionable it goes through the
        same dispatch_signal() approval flow as any other signal.
        """
        symbol = symbol.lstrip("$").upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"

        async def _report(text: str):
            # Telegram (for the /tara-style on-demand flow) AND agent_logs,
            # so the SAME report shows up in the dashboard's "Live AI Stream"
            # panel whether the request came from Telegram or the dashboard.
            config.send_log_to_dashboard("HTFAgent", "ANALYSIS", text)
            await self.redis_client.publish("telegram:analysis", orjson.dumps({
                "symbol": symbol, "report": text,
            }))

        ccxt_symbol = self.universe.get(symbol) or self._resolve_ccxt_symbol(symbol)
        if not ccxt_symbol:
            await _report(f"{symbol} bulunamadı (Binance Futures'ta yok).")
            return
        try:
            bars_1d = await self.fetch_closed_bars(ccxt_symbol, config.HTF_DAILY_TIMEFRAME, 200)
            bars_3d = await self.fetch_closed_bars(ccxt_symbol, config.HTF_3D_TIMEFRAME, 200)
            bars_4h = await self.fetch_closed_bars(ccxt_symbol, config.HTF_TRACK_TIMEFRAME, 10)
            if not bars_1d or not bars_4h:
                await _report(f"{symbol} için yeterli mum verisi yok.")
                return

            current_price = bars_4h[-1].close
            current_ts = bars_4h[-1].ts
            sig = htf_strategy.evaluate_htf_breakout(
                bars_1d, bars_3d, current_price, current_ts,
                min_daily_candles=config.HTF_MIN_DAILY_CANDLES,
                donchian_lookback_1d=config.HTF_DONCHIAN_LOOKBACK_1D,
                donchian_lookback_3d=config.HTF_DONCHIAN_LOOKBACK_3D,
                vol_expansion_mult=config.HTF_VOL_EXPANSION_MULT,
                max_extension_atr=config.HTF_MAX_EXTENSION_ATR,
                sl_zone_atr_mult=config.HTF_SL_ZONE_ATR_MULT,
                min_sl_atr_mult=config.HTF_MIN_SL_ATR_MULT,
                max_sl_pct=config.HTF_MAX_SL_PCT,
                max_atr_pct=config.HTF_MAX_ATR_PCT,
                spent_lookback_days=config.HTF_SPENT_LOOKBACK_DAYS,
                spent_drop_pct=config.HTF_SPENT_DROP_PCT,
                spent_rise_mult=config.HTF_SPENT_RISE_MULT,
            )
            squeeze = htf_strategy.squeeze_ratio(
                bars_1d, config.HTF_SQUEEZE_RECENT_DAYS, config.HTF_SQUEEZE_BASELINE_DAYS)
            armed = self.armed_retests.get(symbol)
            if sig.action in ("LONG", "SHORT"):
                sig = self._apply_learning(symbol, sig) or htf_strategy.HTFSignal(
                    "WAIT", reasoning="Tetik tipi, işlem geçmişindeki negatif "
                                      "beklenti nedeniyle öğrenme katmanınca devre dışı.")

            lines = [f"📊 {symbol} anlık analiz", f"Fiyat: {current_price:.6g}"]
            if squeeze is not None:
                lines.append(f"Sıkışma oranı: {squeeze:.2f} "
                             f"({'sıkışmış' if squeeze < 1 else 'genişliyor'})")
            if armed:
                lines.append(f"⏳ Bekleyen retest: {armed['direction']} @ {armed['level']:.6g}")
            lines.append(f"Karar: {sig.action} (güven %{int(sig.confidence * 100)})")
            lines.append(sig.reasoning)
            await _report("\n".join(lines))

            if sig.action in ("LONG", "SHORT") and sig.confidence >= config.HTF_MIN_CONFIDENCE:
                logger.info(f"[{symbol}] On-demand analysis is ACTIONABLE; dispatching for approval.")
                await self.dispatch_signal(symbol, sig)
        except Exception as e:
            logger.error(f"[{symbol}] On-demand analysis failed: {e}")
            await _report(f"{symbol} analiz hatası: {e}")


if __name__ == "__main__":
    agent = HTFAgent()
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        logger.info("HTF Agent gracefully shut down.")
