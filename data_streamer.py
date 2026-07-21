import asyncio
import logging
import orjson
import websockets
import redis.asyncio as redis
import aiohttp
from typing import List, Optional

import config

# Setup basic asynchronous-friendly logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BinanceDataStreamer")

class BinanceDataStreamer:
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.symbols: List[str] = [s.lower() for s in config.SYMBOLS]
        self.ws_url = config.BINANCE_WS_URL
        self.rest_url = config.BINANCE_REST_URL
        self.oi_poll_interval = config.OI_POLL_INTERVAL_SECONDS
        
        # Rate limiting state for REST API
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def initialize(self):
        """Initialize Redis connection and HTTP session."""
        logger.info(f"Connecting to Redis at {config.REDIS_URL}")
        # Note: decode_responses=False to keep orjson bytes intact for faster I/O
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        # Test connection
        await self.redis_client.ping()
        logger.info("Connected to Redis successfully.")
        
        self._session = aiohttp.ClientSession()

    async def cleanup(self):
        """Cleanup resources."""
        if self.redis_client:
            await self.redis_client.aclose()
        if self._session:
            await self._session.close()

    def _get_streams(self) -> List[str]:
        """Generate list of stream names for subscriptions."""
        streams = []
        for symbol in self.symbols:
            streams.append(f"{symbol}@aggTrade")
            streams.append(f"{symbol}@depth10@100ms")
            streams.append(f"{symbol}@markPrice")
        return streams

    async def handle_message(self, message: str):
        """Parse WebSocket message and route to correct Redis channel."""
        try:
            # Using orjson for high performance parsing
            data = orjson.loads(message)
            
            # Binance event types: 'aggTrade', 'depthUpdate', 'markPriceUpdate'
            # We ignore responses to subscribe events which lack an 'e' key
            if 'e' not in data:
                return

            event_type = data['e']
            symbol = data.get('s', '').upper()
            
            if event_type == 'aggTrade':
                channel = f"market:trades:{symbol}"
            elif event_type == 'depthUpdate':
                channel = f"market:depth:{symbol}"
            elif event_type == 'markPriceUpdate':
                channel = f"market:markprice:{symbol}"
            else:
                return
            
            # Publish raw orjson serialized data directly to Redis (as bytes)
            await self.redis_client.publish(channel, orjson.dumps(data))
            
        except orjson.JSONDecodeError:
            logger.error(f"Failed to decode JSON: {message}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def connect_and_stream(self):
        """Connect to Binance Futures WebSocket and stream data with auto-reconnect."""
        streams = self._get_streams()
        subscribe_payload = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        }
        
        backoff = 1
        max_backoff = 60

        while True:
            try:
                logger.info(f"Connecting to Binance WS: {self.ws_url}")
                async with websockets.connect(self.ws_url) as ws:
                    logger.info("WebSocket connected. Subscribing to streams...")
                    await ws.send(orjson.dumps(subscribe_payload).decode('utf-8'))
                    
                    # Reset backoff on successful connection
                    backoff = 1
                    
                    async for message in ws:
                        # Process message asynchronously
                        asyncio.create_task(self.handle_message(message))
                        
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}. Reconnecting in {backoff} seconds...")
            except Exception as e:
                logger.error(f"WebSocket error: {e}. Reconnecting in {backoff} seconds...")
            
            # Exponential backoff
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

    async def poll_open_interest(self):
        """Poll Open Interest periodically via REST API."""
        endpoint = f"{self.rest_url}/fapi/v1/openInterest"
        
        while True:
            for symbol in self.symbols:
                try:
                    params = {"symbol": symbol.upper()}
                    async with self._session.get(endpoint, params=params) as response:
                        if response.status == 200:
                            text = await response.read()
                            data = orjson.loads(text)
                            channel = f"market:oi:{symbol.upper()}"
                            # Publish to redis as bytes
                            await self.redis_client.publish(channel, orjson.dumps(data))
                        elif response.status == 429:
                            logger.warning("Rate limit exceeded! Backing off for 60 seconds.")
                            await asyncio.sleep(60)
                        elif response.status == 418:
                            logger.error("IP BANNED by Binance! Backing off for 5 minutes.")
                            await asyncio.sleep(300)
                        else:
                            logger.error(f"Failed to fetch OI for {symbol}. Status: {response.status}")
                except Exception as e:
                    logger.error(f"Error polling OI for {symbol}: {e}")
                
                # Small delay between symbols to avoid bursting REST API
                await asyncio.sleep(0.1)
                
            # Wait for next polling cycle
            await asyncio.sleep(self.oi_poll_interval)

    async def run(self):
        """Run all tasks."""
        await self.initialize()
        try:
            # Gather WS streaming and REST polling tasks
            await asyncio.gather(
                self.connect_and_stream(),
                self.poll_open_interest()
            )
        finally:
            await self.cleanup()

if __name__ == "__main__":
    streamer = BinanceDataStreamer()
    try:
        asyncio.run(streamer.run())
    except KeyboardInterrupt:
        logger.info("Streamer gracefully shut down.")
