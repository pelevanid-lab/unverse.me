import asyncio
import logging
import time
import orjson
import redis.asyncio as redis
from collections import deque
from typing import Dict, Any

import config

# Setup basic asynchronous-friendly logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AlphaGenerator")

class AlphaGenerator:
    def __init__(self):
        self.redis_client = None
        self.pubsub = None
        
        # State: CVD rolling queues for each symbol
        # { "BTCUSDT": deque([(timestamp, buy_vol, sell_vol), ...]) }
        self.cvd_state: Dict[str, deque] = {s.upper(): deque() for s in config.SYMBOLS}
        
        # State: Latest orderbook imbalance for each symbol
        # { "BTCUSDT": imbalance_ratio }
        self.ob_state: Dict[str, float] = {s.upper(): 0.5 for s in config.SYMBOLS}
        
        self.window_1m = 60 * 1000  # ms
        self.window_5m = 5 * 60 * 1000  # ms

    async def initialize(self):
        """Initialize Redis connection and pubsub."""
        logger.info(f"Connecting to Redis at {config.REDIS_URL}")
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        self.pubsub = self.redis_client.pubsub()
        await self.pubsub.psubscribe("market:trades:*", "market:depth:*")
        logger.info("Subscribed to market data channels successfully.")

    async def cleanup(self):
        """Cleanup resources."""
        if self.pubsub:
            await self.pubsub.close()
        if self.redis_client:
            await self.redis_client.aclose()

    def _process_trade(self, symbol: str, data: dict):
        """Process an aggTrade event and update the CVD state."""
        # Binance aggTrade fields:
        # p: price, q: quantity, T: timestamp, m: is buyer market maker
        # If 'm' is True, it's a seller-initiated trade (sell order matched maker bid).
        # If 'm' is False, it's a buyer-initiated trade (buy order matched maker ask).
        try:
            qty = float(data['q'])
            ts = int(data['T'])
            is_buyer_maker = bool(data['m'])
            
            buy_vol = 0.0
            sell_vol = 0.0
            
            if is_buyer_maker:
                sell_vol = qty
            else:
                buy_vol = qty
                
            if symbol in self.cvd_state:
                self.cvd_state[symbol].append((ts, buy_vol, sell_vol))
        except (KeyError, ValueError) as e:
            logger.error(f"Error processing trade data for {symbol}: {e}")

    def _process_depth(self, symbol: str, data: dict):
        """Process a top-10 depth snapshot and calculate imbalance."""
        # Binance partial depth stream fields:
        # b: bids [[price, qty], ...], a: asks [[price, qty], ...]
        try:
            bids = data.get('b', [])
            asks = data.get('a', [])
            
            total_bid_vol = sum(float(lvl[1]) for lvl in bids)
            total_ask_vol = sum(float(lvl[1]) for lvl in asks)
            
            total_vol = total_bid_vol + total_ask_vol
            if total_vol > 0:
                imbalance = total_bid_vol / total_vol
                self.ob_state[symbol] = imbalance
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error processing depth data for {symbol}: {e}")

    async def listen_to_market_data(self):
        """Listen to incoming Redis messages and process them."""
        while True:
            try:
                # Use a small timeout so we can gracefully shutdown if cancelled
                message = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                if message and message['type'] == 'pmessage':
                    channel = message['channel'].decode('utf-8')
                    data = orjson.loads(message['data'])
                    
                    if channel.startswith("market:trades:"):
                        symbol = channel.split(":")[-1]
                        self._process_trade(symbol, data)
                        
                    elif channel.startswith("market:depth:"):
                        symbol = channel.split(":")[-1]
                        self._process_depth(symbol, data)
                else:
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error reading message from Redis: {e}")
                await asyncio.sleep(1)

    def _calculate_cvd(self, symbol: str, current_ts: int) -> tuple[float, float]:
        """Calculate 1m and 5m CVD and clean up old data."""
        if symbol not in self.cvd_state:
            return 0.0, 0.0
            
        dq = self.cvd_state[symbol]
        
        # Remove elements older than 5m
        while dq and current_ts - dq[0][0] > self.window_5m:
            dq.popleft()
            
        cvd_1m = 0.0
        cvd_5m = 0.0
        
        # Calculate from right to left (newest to oldest)
        for ts, buy_vol, sell_vol in reversed(dq):
            delta = buy_vol - sell_vol
            cvd_5m += delta
            if current_ts - ts <= self.window_1m:
                cvd_1m += delta
                
        return cvd_1m, cvd_5m

    async def publish_features(self):
        """Periodically calculate and publish features to Redis."""
        while True:
            try:
                current_ts = int(time.time() * 1000)
                
                for symbol in config.SYMBOLS:
                    sym_upper = symbol.upper()
                    
                    # 1. Calculate and publish CVD
                    cvd_1m, cvd_5m = self._calculate_cvd(sym_upper, current_ts)
                    cvd_payload = {
                        "symbol": sym_upper,
                        "cvd_1m": round(cvd_1m, 4),
                        "cvd_5m": round(cvd_5m, 4),
                        "timestamp": current_ts
                    }
                    await self.redis_client.publish(
                        f"features:cvd:{sym_upper}",
                        orjson.dumps(cvd_payload)
                    )
                    
                    # 2. Publish Orderbook Imbalance
                    imbalance = self.ob_state.get(sym_upper, 0.5)
                    imb_payload = {
                        "symbol": sym_upper,
                        "imbalance": round(imbalance, 4),
                        "timestamp": current_ts
                    }
                    await self.redis_client.publish(
                        f"features:imbalance:{sym_upper}",
                        orjson.dumps(imb_payload)
                    )
                    
            except Exception as e:
                logger.error(f"Error publishing features: {e}")
                
            await asyncio.sleep(1.0)

    async def run(self):
        """Run all tasks."""
        await self.initialize()
        try:
            # Gather listening and publishing tasks
            await asyncio.gather(
                self.listen_to_market_data(),
                self.publish_features()
            )
        finally:
            await self.cleanup()

if __name__ == "__main__":
    generator = AlphaGenerator()
    try:
        asyncio.run(generator.run())
    except KeyboardInterrupt:
        logger.info("Alpha Generator gracefully shut down.")
