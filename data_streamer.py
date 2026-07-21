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

        # Per-event-type counters so a dead stream is immediately visible.
        self.event_counts: dict = {}
        self.heartbeat_task: Optional[asyncio.Task] = None

        # Symbols Binance actually lists as tradable USD-M perpetuals.
        self.valid_symbols: Set[str] = set()

    async def log_heartbeat(self):
        """Every 60s report how many of each event type arrived.

        A zero (or missing) count for aggTrade means the trade stream is dead,
        which silently starves CVD, swing levels and every indicator downstream.
        """
        while True:
            await asyncio.sleep(60)
            snapshot = dict(self.event_counts)
            self.event_counts = {}
            if snapshot:
                summary = ", ".join(f"{k}={v}" for k, v in sorted(snapshot.items()))
                logger.info(f"[heartbeat] last 60s: {summary} | symbols={len(self.active_symbols)}")
            else:
                logger.error("[heartbeat] NO market events received in the last 60s! "
                             "WebSocket is connected but delivering nothing.")
    
    async def initialize(self):
        logger.info(f"Connecting to Redis at {config.REDIS_URL}")
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        self.pubsub = self.redis_client.pubsub()
        await self.pubsub.subscribe("control:dynamic_symbols")
        
        self._session = aiohttp.ClientSession()
        await self._load_valid_symbols()

    async def _load_valid_symbols(self):
        """Fetch the real tradable USD-M perpetual list from Binance.

        Binance's SUBSCRIBE is all-or-nothing: ONE invalid stream name makes it
        reject the entire request, so every stream goes silent. The macro agent
        can surface thin/exotic listings (tokenised equities etc.) that do not
        have the streams we ask for, so we validate before subscribing.
        """
        url = f"{self.rest_url}/fapi/v1/exchangeInfo"
        try:
            async with self._session.get(url) as resp:
                if resp.status != 200:
                    logger.error(f"exchangeInfo failed ({resp.status}); skipping validation.")
                    return
                data = orjson.loads(await resp.read())
            self.valid_symbols = {
                s['symbol'].lower()
                for s in data.get('symbols', [])
                if s.get('status') == 'TRADING' and s.get('contractType') == 'PERPETUAL'
            }
            logger.info(f"Loaded {len(self.valid_symbols)} tradable perpetual symbols.")
        except Exception as e:
            logger.error(f"Could not load exchangeInfo: {e}")

    async def cleanup(self):
        if self.pubsub:
            await self.pubsub.close()
        if self.redis_client:
            await self.redis_client.aclose()
        if self._session:
            await self._session.close()

    def _get_streams(self) -> List[str]:
        """Build the stream list, dropping symbols Binance does not actually list.

        Without this filter a single bad symbol kills the whole subscription.
        """
        streams = []
        dropped = []
        for symbol in self.active_symbols:
            if self.valid_symbols and symbol not in self.valid_symbols:
                dropped.append(symbol)
                continue
            streams.append(f"{symbol}@aggTrade")
            streams.append(f"{symbol}@depth10@100ms")
            streams.append(f"{symbol}@markPrice")
        if dropped:
            logger.warning(f"Dropped {len(dropped)} symbol(s) not tradable as "
                           f"USD-M perpetuals: {dropped}")
        return streams

    async def handle_message(self, message: str):
        try:
            data = orjson.loads(message)
            # Combined-stream endpoint wraps every event: {"stream":.., "data":..}
            if 'stream' in data and 'data' in data:
                data = data['data']
            if 'e' not in data:
                # Not a market event. This is where Binance returns SUBSCRIBE
                # acks and ERRORS - previously swallowed silently, which made a
                # failed subscription look identical to a quiet market.
                if 'error' in data:
                    logger.error(f"Binance subscription ERROR: {data['error']}")
                elif 'result' in data:
                    logger.info(f"Binance subscribe ack (id={data.get('id')}): "
                                f"result={data['result']}")
                else:
                    logger.debug(f"Non-event message: {str(data)[:200]}")
                return

            event_type = data['e']
            self.event_counts[event_type] = self.event_counts.get(event_type, 0) + 1
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
        if not streams:
            logger.error("No valid streams to connect to. Waiting for symbols...")
            return

        # Use the COMBINED stream endpoint (/stream?streams=a/b/c) instead of
        # connecting to /ws and sending a SUBSCRIBE message. The SUBSCRIBE path
        # was acknowledged by Binance but silently delivered ONLY depthUpdate --
        # aggTrade and markPriceUpdate never arrived, starving CVD and every
        # price-based indicator. The combined endpoint delivers all of them.
        base = self.ws_url.rsplit("/ws", 1)[0]  # wss://fstream.binance.com
        combined_url = f"{base}/stream?streams=" + "/".join(streams)

        backoff = 1
        max_backoff = 60

        while True:
            try:
                logger.info(f"Connecting to Binance COMBINED WS for "
                            f"{len(self.active_symbols)} symbols ({len(streams)} streams).")
                async with websockets.connect(combined_url, max_size=2 ** 23) as ws:
                    logger.info("Combined WebSocket connected; events flowing.")
                    backoff = 1
                    async for message in ws:
                        asyncio.create_task(self.handle_message(message))

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket closed: {e}. Reconnecting in {backoff}s...")
            except asyncio.CancelledError:
                logger.info("WebSocket stream cancelled (likely for a dynamic restart).")
                raise  # Re-raise to cleanly exit the task
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
        if not self.heartbeat_task or self.heartbeat_task.done():
            self.heartbeat_task = asyncio.create_task(self.log_heartbeat())

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
