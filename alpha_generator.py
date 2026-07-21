import asyncio
import logging
import time
import orjson
import redis.asyncio as redis
from collections import deque
from typing import Dict, Any

import config
import indicators

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

        # State: rolling (timestamp_ms, price) per symbol for liquidity levels
        # { "BTCUSDT": deque([(ts, price), ...]) }
        self.price_state: Dict[str, deque] = {s.upper(): deque() for s in config.SYMBOLS}

        self.window_1m = 60 * 1000  # ms
        self.window_5m = 5 * 60 * 1000  # ms

        # Liquidity-level windows (ms)
        self.swing_lookback = int(config.SWING_LOOKBACK_SEC * 1000)
        self.sweep_reaction = int(config.SWEEP_REACTION_SEC * 1000)
        self.range_window = int(config.RANGE_WINDOW_SEC * 1000)

    async def initialize(self):
        """Initialize Redis connection and pubsub."""
        logger.info(f"Connecting to Redis at {config.REDIS_URL}")
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        self.pubsub = self.redis_client.pubsub()
        await self.pubsub.psubscribe("market:trades:*", "market:depth:*", "control:dynamic_symbols")
        logger.info("Subscribed to market data + dynamic-symbol channels.")

    def _ensure_symbol(self, sym_upper: str):
        """Start tracking a newly discovered symbol so its features get computed."""
        if sym_upper not in self.cvd_state:
            self.cvd_state[sym_upper] = deque()
            self.ob_state[sym_upper] = 0.5
            self.price_state[sym_upper] = deque()
            logger.info(f"Now tracking dynamic symbol: {sym_upper}")

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
            price = float(data['p'])
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
            if symbol in self.price_state:
                self.price_state[symbol].append((ts, price))
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

                    elif channel == "control:dynamic_symbols":
                        # Macro agent discovered new satellite coins -> track them.
                        for s in data:
                            self._ensure_symbol(str(s).upper())
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

    def _compute_levels(self, symbol: str, current_ts: int) -> dict:
        """Track liquidity levels and detect stop-hunt sweeps.

        A "sweep low" = price pierced BELOW the established swing low (where
        stops rest) within the recent reaction window, then reclaimed back
        above it. That is the footprint of a stop hunt; a reversal long often
        follows. "sweep high" is the mirror image for shorts.

        Returns the levels plus a 1-minute range used to size the stop beyond
        the sweep wick (so our stop is NOT sitting on the hunted level).
        """
        dq = self.price_state.get(symbol)
        if not dq:
            return {}

        # Drop anything older than the lookback window.
        while dq and current_ts - dq[0][0] > self.swing_lookback:
            dq.popleft()
        if len(dq) < 5:
            return {}

        reaction_cutoff = current_ts - self.sweep_reaction
        range_cutoff = current_ts - self.range_window

        established_prices = [p for ts, p in dq if ts < reaction_cutoff]
        recent_prices = [p for ts, p in dq if ts >= reaction_cutoff]
        range_prices = [p for ts, p in dq if ts >= range_cutoff]

        if not established_prices or not recent_prices:
            return {}

        last_price = dq[-1][1]
        swing_high = max(established_prices)   # liquidity resting above
        swing_low = min(established_prices)    # liquidity resting below
        recent_min = min(recent_prices)
        recent_max = max(recent_prices)
        range_1m = (max(range_prices) - min(range_prices)) if range_prices else 0.0

        # Sweep detection: pierced the level, then closed back inside it.
        sweep_low = recent_min < swing_low and last_price > swing_low
        sweep_high = recent_max > swing_high and last_price < swing_high

        # Indicator layers: resample the trade stream to 5s closes, then derive
        # trend (EMA fast/slow), exhaustion (RSI) and volatility (ATR).
        start_ts = current_ts - self.swing_lookback
        closes = indicators.resample_closes(list(dq), 5000, start_ts, current_ts)
        ema_fast = indicators.ema(closes, 12)
        ema_slow = indicators.ema(closes, 26)
        rsi_val = indicators.rsi(closes, 14)
        atr_val = indicators.atr_from_closes(closes, 14)

        return {
            "symbol": symbol,
            "last_price": round(last_price, 8),
            "swing_high": round(swing_high, 8),
            "swing_low": round(swing_low, 8),
            "recent_min": round(recent_min, 8),
            "recent_max": round(recent_max, 8),
            "range_1m": round(range_1m, 8),
            "sweep_low": sweep_low,
            "sweep_high": sweep_high,
            "ema_fast": round(ema_fast, 8),
            "ema_slow": round(ema_slow, 8),
            "rsi": round(rsi_val, 2),
            "atr": round(atr_val, 8),
        }

    async def publish_features(self):
        """Periodically calculate and publish features to Redis."""
        while True:
            try:
                current_ts = int(time.time() * 1000)

                # Loop over ALL tracked symbols (core + dynamic), not just config.
                for sym_upper in list(self.cvd_state.keys()):

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

                    # 3. Compute & publish liquidity levels + sweep flags
                    levels = self._compute_levels(sym_upper, current_ts)
                    if levels:
                        levels["timestamp"] = current_ts
                        await self.redis_client.publish(
                            f"features:levels:{sym_upper}",
                            orjson.dumps(levels)
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
