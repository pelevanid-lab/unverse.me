import asyncio
import logging
import os
import orjson
import aiohttp
import ccxt.async_support as ccxt
import redis.asyncio as redis
from supabase import create_client, Client

import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MacroAgent")

# How many satellite coins to promote to the live decision layer.
MAX_DYNAMIC_SYMBOLS = int(os.getenv("MAX_DYNAMIC_SYMBOLS", "3"))
# Ranking weights (deterministic — no LLM). The narrative bonus is added on top.
W_OI_SURGE = float(os.getenv("W_OI_SURGE", "1.0"))
W_VOLUME = float(os.getenv("W_VOLUME", "0.6"))
W_MOMENTUM = float(os.getenv("W_MOMENTUM", "0.5"))
# Minimum 24h quote volume for a candidate. Keeps thin / exotic listings
# (tokenised equities, commodities, brand-new pairs) out of the watchlist:
# they surge on OI but are illiquid and can lack the streams we subscribe to.
MIN_QUOTE_VOLUME = float(os.getenv("MIN_QUOTE_VOLUME", "50000000"))


class MacroAgent:
    def __init__(self):
        self.redis_client = None
        self.exchange = ccxt.binanceusdm({'enableRateLimit': True})
        self.session = None
        self.run_interval = 900  # 15 minutes

    async def initialize(self):
        logger.info(f"Connecting to Redis at {config.REDIS_URL}")
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        self.session = aiohttp.ClientSession()
        await self.exchange.load_markets()

    async def cleanup(self):
        if self.redis_client:
            await self.redis_client.aclose()
        if self.exchange:
            await self.exchange.close()
        if self.session:
            await self.session.close()

    async def fetch_oi_surge(self, symbol: str) -> float:
        """Fetch 15m historical OI and return the % surge."""
        url = "https://fapi.binance.com/futures/data/openInterestHist"
        params = {
            "symbol": symbol,
            "period": "15m",
            "limit": 2
        }
        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if len(data) >= 2:
                        old_oi = float(data[0]['sumOpenInterest'])
                        new_oi = float(data[-1]['sumOpenInterest'])
                        if old_oi > 0:
                            return (new_oi - old_oi) / old_oi
                elif resp.status == 429:
                    logger.warning("Rate limit hit fetching OI. Sleeping.")
                    await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Error fetching OI for {symbol}: {e}")
        return 0.0

    async def _get_narrative_bonus(self) -> dict:
        """Read the narrative agent's {SYMBOL: bonus} map from Redis (if any)."""
        try:
            raw = await self.redis_client.get("narrative:bonus_map")
            if raw:
                payload = orjson.loads(raw)
                return {k.upper(): float(v) for k, v in payload.get("bonus_map", {}).items()}
        except Exception as e:
            logger.warning(f"Could not read narrative bonus map: {e}")
        return {}

    @staticmethod
    def _minmax(values):
        lo, hi = min(values), max(values)
        span = (hi - lo) or 1.0
        return lambda x: (x - lo) / span

    async def run_discovery_cycle(self):
        """Find the best satellite pairs deterministically (no LLM) and publish.

        Score = w_oi*OI_surge + w_vol*volume + w_mom*momentum + narrative_bonus.
        Our own quantitative math finds the candidates; being in a hot sector
        (from the Gemini narrative agent) only adds points on top.
        """
        logger.info("Starting Macro Discovery Cycle (deterministic)...")

        try:
            # 1. Fetch 24h tickers for all futures pairs
            tickers = await self.exchange.fetch_tickers()

            # 2. Keep liquid USDT perps. We no longer hard-filter by price move;
            #    momentum is scored, not excluded (the sweep strategy needs
            #    coins that actually move).
            candidates = []
            for ccxt_sym, ticker in tickers.items():
                try:
                    binance_id = self.exchange.market(ccxt_sym)['id']
                except Exception:
                    continue
                if not binance_id.endswith('USDT'):
                    continue
                quote_vol = float(ticker.get('quoteVolume', 0) or 0)
                if quote_vol < MIN_QUOTE_VOLUME:
                    continue
                candidates.append({
                    "symbol": binance_id,
                    "quote_volume": quote_vol,
                    "momentum": abs(float(ticker.get('percentage', 0) or 0)),
                    "last_price": ticker.get('last', 0),
                })

            # Take the top 20 by liquidity, then enrich with OI surge.
            candidates.sort(key=lambda x: x["quote_volume"], reverse=True)
            top_20 = candidates[:20]
            logger.info("Top 20 liquidity candidates selected. Fetching OI history...")

            for cand in top_20:
                surge_pct = await self.fetch_oi_surge(cand["symbol"])
                cand["oi_surge_pct"] = surge_pct * 100.0
                await asyncio.sleep(0.5)  # respect REST rate limits

            # 3. Deterministic composite score (+ narrative bonus).
            bonus_map = await self._get_narrative_bonus()
            import math
            oi_norm = self._minmax([c["oi_surge_pct"] for c in top_20])
            vol_norm = self._minmax([math.log10(c["quote_volume"] + 1) for c in top_20])
            mom_norm = self._minmax([c["momentum"] for c in top_20])

            for c in top_20:
                bonus = bonus_map.get(c["symbol"].upper(), 0.0)
                c["bonus"] = bonus
                c["score"] = (
                    W_OI_SURGE * oi_norm(c["oi_surge_pct"])
                    + W_VOLUME * vol_norm(math.log10(c["quote_volume"] + 1))
                    + W_MOMENTUM * mom_norm(c["momentum"])
                    + bonus  # narrative/sector boost
                )

            top_20.sort(key=lambda x: x["score"], reverse=True)
            chosen = top_20[:MAX_DYNAMIC_SYMBOLS]
            chosen_pairs = [c["symbol"].lower() for c in chosen]

            detail = ", ".join(
                f"{c['symbol']}(s={c['score']:.2f},oi={c['oi_surge_pct']:.0f}%,b={c['bonus']:.2f})"
                for c in chosen
            )
            logger.info(f"Selected satellites: {detail}")

            # 4. Publish via the existing dynamic-symbols plumbing.
            await self.redis_client.publish(
                "control:dynamic_symbols",
                orjson.dumps(chosen_pairs)
            )
            logger.info(f"Published dynamic symbols: {chosen_pairs}")

            config.send_log_to_dashboard("MacroAgent", "DISCOVERY", f"Satellites: {detail}")

        except Exception as e:
            logger.error(f"Error in Macro Discovery Cycle: {e}")

    async def run(self):
        await self.initialize()
        try:
            while True:
                await self.run_discovery_cycle()
                logger.info(f"Sleeping for {self.run_interval} seconds...")
                await asyncio.sleep(self.run_interval)
        finally:
            await self.cleanup()

if __name__ == "__main__":
    agent = MacroAgent()
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        logger.info("Macro Agent gracefully shut down.")
