import asyncio
import logging
import orjson
import time
import ccxt.async_support as ccxt
import redis.asyncio as redis
from typing import Set

import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ExecutionEngine")

class ExecutionEngine:
    def __init__(self):
        self.redis_client = None
        self.pubsub = None
        
        # Local tracker to prevent duplicate entries
        self.active_positions: Set[str] = set()
        
        # Initialize CCXT Async Client
        self.exchange = ccxt.binanceusdm({
            'apiKey': config.BINANCE_API_KEY,
            'secret': config.BINANCE_SECRET_KEY,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future'
            }
        })
        
        if config.BINANCE_USE_TESTNET:
            self.exchange.set_sandbox_mode(True)
            logger.info("Initializing Execution Engine on Binance TESTNET.")
        else:
            logger.warning("Initializing Execution Engine on Binance MAINNET.")

    async def initialize(self):
        logger.info(f"Connecting to Redis at {config.REDIS_URL}")
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        self.pubsub = self.redis_client.pubsub()
        await self.pubsub.psubscribe("signals:master:*")
        logger.info("Subscribed to signals:master:* channels.")
        
        # Load markets for precision formatting
        await self.exchange.load_markets()
        
        # Sync local tracker with existing positions
        try:
            positions = await self.exchange.fetch_positions()
            for pos in positions:
                if float(pos.get('contracts', 0)) > 0:
                    symbol = pos['symbol'].replace("/", "").replace(":", "")
                    self.active_positions.add(symbol)
                    logger.info(f"Found active position for {symbol}. Tracking.")
        except Exception as e:
            logger.error(f"Failed to fetch initial positions: {e}")

    async def cleanup(self):
        if self.pubsub:
            await self.pubsub.close()
        if self.redis_client:
            await self.redis_client.aclose()
        if self.exchange:
            await self.exchange.close()

    async def execute_trade(self, symbol: str, action: str):
        """Execute the bracket order logic based on strictly hard-coded risk rules."""
        ccxt_symbol = None
        # Try to find the CCXT formatted symbol (e.g. BTC/USDT:USDT)
        for sym in self.exchange.markets:
            if sym.replace("/", "").replace(":", "").startswith(symbol):
                ccxt_symbol = sym
                break
                
        if not ccxt_symbol:
            logger.error(f"Could not resolve CCXT symbol for {symbol}")
            return
            
        try:
            # 1. Set Leverage
            try:
                await self.exchange.set_leverage(config.DEFAULT_LEVERAGE, ccxt_symbol)
                logger.info(f"[{symbol}] Leverage set to {config.DEFAULT_LEVERAGE}x")
            except Exception as e:
                # Some testnets or accounts might fail leverage updates if already set
                logger.warning(f"[{symbol}] Could not set leverage (might already be {config.DEFAULT_LEVERAGE}x): {e}")
            
            # 2. Fetch Margin & Price
            balance = await self.exchange.fetch_balance()
            free_margin = float(balance.get('USDT', {}).get('free', 0))
            if free_margin <= 0:
                logger.error(f"[{symbol}] Insufficient free margin: {free_margin}")
                return
                
            ticker = await self.exchange.fetch_ticker(ccxt_symbol)
            current_price = float(ticker['last'])
            
            # 3. Risk Math
            risk_amount = free_margin * config.RISK_PER_TRADE_PCT
            
            if action == "LONG":
                sl_price = current_price * (1 - config.SL_PCT)
                tp_price = current_price * (1 + config.TP_PCT)
                side = 'buy'
                sl_side = 'sell'
            else: # SHORT
                sl_price = current_price * (1 + config.SL_PCT)
                tp_price = current_price * (1 - config.TP_PCT)
                side = 'sell'
                sl_side = 'buy'
                
            price_delta = abs(current_price - sl_price)
            if price_delta <= 0:
                logger.error(f"[{symbol}] Invalid price delta for SL calculation.")
                return
                
            raw_quantity = risk_amount / price_delta
            
            # 4. Precision Formatting (CRITICAL)
            qty_str = self.exchange.amount_to_precision(ccxt_symbol, raw_quantity)
            qty = float(qty_str)
            
            formatted_sl_price = float(self.exchange.price_to_precision(ccxt_symbol, sl_price))
            formatted_tp_price = float(self.exchange.price_to_precision(ccxt_symbol, tp_price))
            
            if qty <= 0:
                logger.error(f"[{symbol}] Calculated quantity is 0 after precision formatting. Risk amount too small?")
                return
                
            logger.info(f"[{symbol}] Preparing {action} Order. Risk Amount: ${risk_amount:.2f}, Qty: {qty}, Entry: ~{current_price}, SL: {formatted_sl_price}, TP: {formatted_tp_price}")
            
            # 5. Execution Sequence
            # Entry Market Order
            entry_order = await self.exchange.create_order(
                symbol=ccxt_symbol,
                type='market',
                side=side,
                amount=qty
            )
            logger.info(f"[{symbol}] ENTRY EXECUTED: {entry_order['id']} at {entry_order.get('average', current_price)}")
            
            # Stop Loss (Reduce Only / Close Position)
            sl_order = await self.exchange.create_order(
                symbol=ccxt_symbol,
                type='STOP_MARKET',
                side=sl_side,
                amount=qty,
                params={
                    'stopPrice': formatted_sl_price,
                    'closePosition': True
                }
            )
            logger.info(f"[{symbol}] SL EXECUTED: {sl_order['id']} at Stop Price {formatted_sl_price}")
            
            # Take Profit (Reduce Only / Close Position)
            tp_order = await self.exchange.create_order(
                symbol=ccxt_symbol,
                type='TAKE_PROFIT_MARKET',
                side=sl_side,
                amount=qty,
                params={
                    'stopPrice': formatted_tp_price,
                    'closePosition': True
                }
            )
            logger.info(f"[{symbol}] TP EXECUTED: {tp_order['id']} at Stop Price {formatted_tp_price}")
            
            # 6. Update local tracker & publish telemetry
            self.active_positions.add(symbol)
            
            telemetry = {
                "symbol": symbol,
                "status": "executed",
                "action": action,
                "entry_price": current_price,
                "quantity": qty,
                "sl_price": formatted_sl_price,
                "tp_price": formatted_tp_price,
                "timestamp": int(time.time() * 1000)
            }
            await self.redis_client.publish(
                f"execution:status:{symbol}",
                orjson.dumps(telemetry)
            )
            
        except Exception as e:
            logger.error(f"[{symbol}] Execution Pipeline Failed: {e}")

    async def listen_to_signals(self):
        """Listen to incoming Redis AI signals and act on them."""
        while True:
            try:
                message = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                if message and message['type'] == 'pmessage':
                    channel = message['channel'].decode('utf-8')
                    symbol = channel.split(":")[-1]
                    data = orjson.loads(message['data'])
                    
                    action = data.get("action", "WAIT")
                    confidence = float(data.get("confidence_score", 0.0))
                    
                    # 1. Filter Check
                    if action in ["LONG", "SHORT"] and confidence >= 0.75:
                        # 2. Position Check
                        if symbol in self.active_positions:
                            logger.info(f"[{symbol}] Ignoring valid {action} signal. Active position already exists.")
                            continue
                            
                        logger.info(f"[{symbol}] Received valid master signal: {action} (Confidence: {confidence}). Initiating execution.")
                        
                        # 3. Fire Execution Task (Background)
                        asyncio.create_task(self.execute_trade(symbol, action))
                else:
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error reading message from Redis: {e}")
                await asyncio.sleep(1)

    async def run(self):
        await self.initialize()
        try:
            await self.listen_to_signals()
        finally:
            await self.cleanup()

if __name__ == "__main__":
    engine = ExecutionEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logger.info("Execution Engine gracefully shut down.")
