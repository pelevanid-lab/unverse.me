import asyncio
import logging
import orjson
import aiohttp
import ccxt.async_support as ccxt
import google.generativeai as genai
import redis.asyncio as redis
from pydantic import BaseModel, Field
from supabase import create_client, Client

import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MacroAgent")

if not config.GEMINI_API_KEY:
    logger.warning("GEMINI_API_KEY is not set.")
genai.configure(api_key=config.GEMINI_API_KEY)

class SatelliteList(BaseModel):
    pairs: list[str] = Field(description="List of top 3 pair symbols chosen (e.g. ['BTCUSDT', 'SOLUSDT', 'PEPEUSDT'])")

class MacroAgent:
    def __init__(self):
        self.redis_client = None
        self.exchange = ccxt.binanceusdm({'enableRateLimit': True})
        self.session = None
        self.run_interval = 900 # 15 minutes
        
        if config.SUPABASE_URL and config.SUPABASE_KEY:
            self.supabase: Client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
        else:
            self.supabase = None
        
        
        self.model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json",
                response_schema=SatelliteList,
                temperature=0.2,
            )
        )

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

    async def run_discovery_cycle(self):
        """Find the best satellite pairs and publish them."""
        logger.info("Starting Macro Discovery Cycle...")
        
        try:
            # 1. Fetch 24h tickers for all futures pairs
            tickers = await self.exchange.fetch_tickers()
            
            # 2. Filter & Sort
            # Keep USDT pairs, price change < 10%
            candidates = []
            for ccxt_sym, ticker in tickers.items():
                market = self.exchange.market(ccxt_sym)
                binance_id = market['id'] # e.g. 'BTCUSDT'
                
                if not binance_id.endswith('USDT'):
                    continue
                
                # Check price change (Binance usually provides percentage)
                pct_change = abs(float(ticker.get('percentage', 0) or 0))
                quote_vol = float(ticker.get('quoteVolume', 0) or 0)
                
                if pct_change < 10.0 and quote_vol > 0:
                    candidates.append({
                        "symbol": binance_id,
                        "quote_volume": quote_vol,
                        "price_change": pct_change,
                        "last_price": ticker.get('last', 0)
                    })
                    
            # Sort by quote volume descending (highest liquidity/activity)
            candidates.sort(key=lambda x: x["quote_volume"], reverse=True)
            top_20 = candidates[:20]
            
            logger.info(f"Selected top 20 liquidity candidates. Fetching OI history...")
            
            # 3. Fetch OI History sequentially to respect rate limits
            enriched_candidates = []
            for cand in top_20:
                surge_pct = await self.fetch_oi_surge(cand["symbol"])
                cand["oi_surge_pct"] = surge_pct * 100 # Convert to percentage
                enriched_candidates.append(cand)
                await asyncio.sleep(0.5) # Avoid spamming the REST API
                
            # Sort by OI Surge descending and take Top 10
            enriched_candidates.sort(key=lambda x: x["oi_surge_pct"], reverse=True)
            top_10 = enriched_candidates[:10]
            
            logger.info(f"Top 10 candidates by OI Surge ready. Querying Gemini...")
            
            prompt = f"""
            You are a Macro Quantitative Analyst. Review the following top 10 Binance Futures pairs which are experiencing the highest Open Interest surges in the last 15 minutes, but their price has moved less than 10% in the last 24h.
            
            This data pattern often indicates accumulation or heavy positioning before a massive explosive breakout.
            
            Candidates (Format: Symbol | 24h Volume (USDT) | Price Change % | 15m OI Surge %):
            """
            for c in top_10:
                prompt += f"\n- {c['symbol']} | ${c['quote_volume']:,.0f} | {c['price_change']:.2f}% | {c['oi_surge_pct']:.2f}%"
                
            prompt += """
            Select exactly 3 pairs that you believe have the most explosive potential. Prioritize extreme OI surges on high volume.
            Return the exact JSON structure with the 3 symbols (e.g. BTCUSDT) in lowercase.
            """
            
            # 4. Call Gemini
            response = await self.model.generate_content_async(prompt)
            decision_dict = orjson.loads(response.text)
            satellite_list = SatelliteList(**decision_dict)
            
            # Convert to lowercase
            chosen_pairs = [p.lower() for p in satellite_list.pairs]
            logger.info(f"Gemini Selected Satellite Pairs: {chosen_pairs}")
            
            # 5. Publish to Redis
            await self.redis_client.publish(
                "control:dynamic_symbols",
                orjson.dumps(chosen_pairs)
            )
            logger.info("Published dynamic symbols to control channel.")
            
            # 6. Supabase Persistence
            if self.supabase:
                try:
                    def _insert_macro():
                        self.supabase.table("macro_trends").insert({"pairs": chosen_pairs}).execute()
                    asyncio.create_task(asyncio.to_thread(_insert_macro))
                    logger.info("Dispatched Supabase insert for macro_trends.")
                except Exception as e:
                    logger.error(f"Failed to dispatch Supabase insert: {e}")
            
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
