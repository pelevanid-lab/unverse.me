import asyncio
import logging
import orjson
import websockets
import redis.asyncio as redis
import aiohttp
from typing import List, Optional, Set

import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BinanceDataStreamer")

class BinanceDataStreamer:
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.pubsub = None
        
        # Hybrid Setup: Core (static) + Dynamic (satellite)
        self.core_symbols: Set[str] = {s.lower() for s in config.SYMBOLS}
        self.dynamic_symbols: Set[str] = set()
        self.active_symbols: Set[str] = self.core_symbols.copy()
        
        self.ws_url = config.BINANCE_WS_URL
        self.rest_url = config.BINANCE_REST_URL
        self.oi_poll_interval = config.OI_POLL_INTERVAL_SECONDS
        
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Task Tracking for graceful restarts
        self.ws_task: Optional[asyncio.Task] = None
        self.oi_task: Optional[asyncio.Task] = None
    
    async def initialize(self):
        logger.info(f"Connecting to Redis at {config.REDIS_URL}")
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        self.pubsub = self.redis_client.pubsub()
        await self.pubsub.subscribe("control:dynamic_symbols")
        
        self._session = aiohttp.ClientSession()

    async def cleanup(self):
        if self.pubsub:
            await self.pubsub.close()
        if self.redis_client:
            await self.redis_client.aclose()
        if self._session:
            await self._session.close()

    def _get_streams(self) -> List[str]:
        streams = []
        for symbol in self.active_symbols:
            streams.append(f"{symbol}@aggTrade")
            streams.append(f"{symbol}@depth10@100ms")
            streams.append(f"{symbol}@markPrice")
        return streams

    async def handle_message(self, message: str):
        try:
            data = orjson.loads(message)
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
            
            await self.redis_client.publish(channel, orjson.dumps(data))
            
        except orjson.JSONDecodeError:
            logger.error(f"Failed to decode JSON: {message}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def connect_and_stream(self):
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
                logger.info(f"Connecting to Binance WS for {len(self.active_symbols)} symbols...")
                async with websockets.connect(self.ws_url) as ws:
                    logger.info("WebSocket connected. Subscribing to streams...")
                    await ws.send(orjson.dumps(subscribe_payload).decode('utf-8'))
                    
                    backoff = 1
                    
                    async for message in ws:
                        asyncio.create_task(self.handle_message(message))
                        
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket closed: {e}. Reconnecting in {backoff}s...")
            except asyncio.CancelledError:
                logger.info("WebSocket stream cancelled (likely for a dynamic restart).")
                raise # Re-raise to cleanly exit the task
            except Exception as e:
                logger.error(f"WebSocket error: {e}. Reconnecting in {backoff}s...")
            
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

    async def poll_open_interest(self):
        endpoint = f"{self.rest_url}/fapi/v1/openInterest"
        while True:
            try:
                # Iterate over a snapshot of active_symbols to avoid runtime modification errors
                for symbol in list(self.active_symbols):
                    params = {"symbol": symbol.upper()}
                    async with self._session.get(endpoint, params=params) as response:
                        if response.status == 200:
                            text = await response.read()
                            data = orjson.loads(text)
                            channel = f"market:oi:{symbol.upper()}"
                            await self.redis_client.publish(channel, orjson.dumps(data))
                        elif response.status == 429:
                            logger.warning("Rate limit exceeded! Backing off for 60 seconds.")
                            await asyncio.sleep(60)
                        elif response.status == 418:
                            logger.error("IP BANNED by Binance! Backing off for 5 minutes.")
                            await asyncio.sleep(300)
                    
                    await asyncio.sleep(0.1)
                
                await asyncio.sleep(self.oi_poll_interval)
            except asyncio.CancelledError:
                logger.info("OI Poller cancelled.")
                raise
            except Exception as e:
                logger.error(f"Error in OI poller loop: {e}")
                await asyncio.sleep(1)

    def _start_worker_tasks(self):
        """Launch or restart the streaming and polling tasks."""
        if self.ws_task and not self.ws_task.done():
            self.ws_task.cancel()
        if self.oi_task and not self.oi_task.done():
            self.oi_task.cancel()
            
        logger.info(f"Starting workers for Active Symbols: {self.active_symbols}")
        self.ws_task = asyncio.create_task(self.connect_and_stream())
        self.oi_task = asyncio.create_task(self.poll_open_interest())

    async def listen_for_control_signals(self):
        """Listen to dynamic satellite symbols updates and restart streams if changed."""
        while True:
            try:
                message = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                if message and message['type'] == 'message':
                    channel = message['channel'].decode('utf-8')
                    if channel == "control:dynamic_symbols":
                        new_dynamic = set(orjson.loads(message['data']))
                        new_active = self.core_symbols | new_dynamic
                        
                        if new_active != self.active_symbols:
                            logger.info(f"Received new Dynamic Symbols. Core: {len(self.core_symbols)} | Dynamic: {len(new_dynamic)}")
                            self.dynamic_symbols = new_dynamic
                            self.active_symbols = new_active
                            self._start_worker_tasks()
                else:
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in control listener: {e}")
                await asyncio.sleep(1)

    async def run(self):
        await self.initialize()
        try:
            # Start the initial streams
            self._start_worker_tasks()
            
            # Start listening for dynamic changes (this blocks the main run loop)
            await self.listen_for_control_signals()
        finally:
            await self.cleanup()

if __name__ == "__main__":
    streamer = BinanceDataStreamer()
    try:
        asyncio.run(streamer.run())
    except KeyboardInterrupt:
        logger.info("Streamer gracefully shut down.")
