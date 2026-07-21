"""
Level agent — periodically fetches long-history candles per tracked symbol,
detects significant HTF support/resistance zones (level_engine.py), and
publishes them for agent_orchestrator to use as a structure_bonus on top of
its existing 15-min sweep trigger.

Runs far less often than the live pipeline: zones built from 4h candles
don't meaningfully change minute to minute, so a slow refresh (a few times a
day) is both sufficient and considerate of exchange rate limits.

Scope note: v1 only builds zones for the CORE static watchlist (config.SYMBOLS).
Dynamic satellite coins (from macro_agent's discovery) rotate every 15 minutes
and don't have a stable identity long enough to justify the cost of a fresh
months-long history fetch each time; this can be extended later if needed.
"""
import asyncio
import logging
import os
import orjson
import ccxt.async_support as ccxt
import redis.asyncio as redis

import config
from level_engine import Candle, detect_zones, infer_current_role

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("LevelAgent")

REFRESH_INTERVAL_SEC = float(os.getenv("LEVEL_REFRESH_INTERVAL_SEC", "14400"))  # 4h
CANDLE_TIMEFRAME = os.getenv("LEVEL_CANDLE_TIMEFRAME", "4h")
CANDLE_LIMIT = int(os.getenv("LEVEL_CANDLE_LIMIT", "1000"))  # ~5.5 months of 4h candles
PIVOT_WINDOW = int(os.getenv("LEVEL_PIVOT_WINDOW", "3"))
CLUSTER_TOL_PCT = float(os.getenv("LEVEL_CLUSTER_TOL_PCT", "0.015"))
MIN_TOUCHES = int(os.getenv("LEVEL_MIN_TOUCHES", "2"))

LEVELS_KEY_PREFIX = "levels:zones:"  # + SYMBOL -> json list of zones+role


class LevelAgent:
    def __init__(self):
        self.redis_client = None
        self.exchange = ccxt.binanceusdm({'enableRateLimit': True})

    async def initialize(self):
        logger.info(f"Connecting to Redis at {config.REDIS_URL}")
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        await self.exchange.load_markets()

    async def cleanup(self):
        if self.redis_client:
            await self.redis_client.aclose()
        if self.exchange:
            await self.exchange.close()

    def _resolve_ccxt_symbol(self, symbol_upper: str):
        """Plain Binance id (e.g. 'BTCUSDT') -> ccxt unified symbol."""
        for ccxt_sym, market in self.exchange.markets.items():
            if market.get('id', '').upper() == symbol_upper:
                return ccxt_sym
        return None

    async def build_zones_for_symbol(self, symbol_upper: str):
        ccxt_symbol = self._resolve_ccxt_symbol(symbol_upper)
        if not ccxt_symbol:
            logger.warning(f"Could not resolve ccxt symbol for {symbol_upper}")
            return
        try:
            ohlcv = await self.exchange.fetch_ohlcv(
                ccxt_symbol, timeframe=CANDLE_TIMEFRAME, limit=CANDLE_LIMIT
            )
        except Exception as e:
            logger.error(f"Failed to fetch OHLCV for {symbol_upper}: {e}")
            return
        if not ohlcv:
            return

        candles = [Candle(ts=row[0], open=row[1], high=row[2], low=row[3], close=row[4])
                   for row in ohlcv]
        zones = detect_zones(
            candles, pivot_window=PIVOT_WINDOW,
            cluster_tol_pct=CLUSTER_TOL_PCT, min_touches=MIN_TOUCHES,
        )

        payload = []
        for z in zones:
            role = infer_current_role(z, candles)
            payload.append({
                "price_low": z.price_low,
                "price_high": z.price_high,
                "touches": z.touches,
                "last_touch_ts": z.last_touch_ts,
                "role": role,
            })

        await self.redis_client.set(f"{LEVELS_KEY_PREFIX}{symbol_upper}", orjson.dumps(payload))
        logger.info(f"[{symbol_upper}] Published {len(payload)} HTF zone(s) "
                    f"from {len(candles)} {CANDLE_TIMEFRAME} candles.")

        if config.supabase:
            def _persist(sym, zones_payload):
                try:
                    config.supabase.table("htf_levels").upsert({
                        "symbol": sym,
                        "zones": zones_payload,
                        "updated_at": "now()",
                    }, on_conflict="symbol").execute()
                except Exception as e:
                    logger.error(f"[{sym}] Failed to persist HTF zones: {e}")
            asyncio.create_task(asyncio.to_thread(_persist, symbol_upper, payload))

    async def run_cycle(self):
        symbols = {s.upper() for s in config.SYMBOLS}
        logger.info(f"Building HTF zones for {len(symbols)} core symbol(s): {symbols}")
        for sym in symbols:
            await self.build_zones_for_symbol(sym)
            await asyncio.sleep(0.5)  # rate-limit courtesy between symbols

    async def run(self):
        await self.initialize()
        try:
            while True:
                await self.run_cycle()
                logger.info(f"Sleeping {REFRESH_INTERVAL_SEC}s until next zone refresh.")
                await asyncio.sleep(REFRESH_INTERVAL_SEC)
        finally:
            await self.cleanup()


if __name__ == "__main__":
    agent = LevelAgent()
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        logger.info("Level Agent gracefully shut down.")
