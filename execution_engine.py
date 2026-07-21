import asyncio
import logging
import orjson
import time
import ccxt.async_support as ccxt
import redis.asyncio as redis
from typing import Set
from supabase import create_client, Client

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
        # We no longer subscribe to signals:master:* because we rely on Supabase manual approval
        
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
                    
                    # Ensure it is in Supabase active_trades
                    if config.supabase:
                        def _sync_to_supabase(sym, position_data):
                            try:
                                # Check if already in Supabase
                                res = config.supabase.table("active_trades").select("*").eq("symbol", sym).eq("status", "OPEN").execute()
                                if not res.data:
                                    entry_price = float(position_data.get('entryPrice', 0))
                                    qty = float(position_data.get('contracts', 0))
                                    
                                    # Try to determine side from positionAmt (positive = long, negative = short)
                                    pos_amt = float(position_data.get('info', {}).get('positionAmt', 0))
                                    side = "LONG" if pos_amt > 0 else "SHORT"
                                    
                                    # Fallback for leverage if it's missing or None in the API response
                                    leverage_val = position_data.get('leverage')
                                    leverage = int(leverage_val) if leverage_val is not None else config.DEFAULT_LEVERAGE
                                    
                                    config.supabase.table("active_trades").insert({
                                        "symbol": sym,
                                        "side": side,
                                        "entry_price": entry_price,
                                        "leverage": leverage,
                                        "quantity": qty,
                                        "status": "OPEN"
                                    }).execute()
                                    logger.info(f"[{sym}] Synced pre-existing position to Dashboard.")
                            except Exception as e:
                                logger.error(f"[{sym}] Failed to sync pre-existing position: {e}")
                                
                        asyncio.create_task(asyncio.to_thread(_sync_to_supabase, symbol, pos))
                        
            # Start background wallet sync
            asyncio.create_task(self.monitor_wallets())
            
            # Start background open position monitor
            asyncio.create_task(self.monitor_open_positions())
            
        except Exception as e:
            logger.error(f"Failed to fetch initial positions: {e}")
            
    async def monitor_wallets(self):
        """Periodically fetches Binance wallet balance and updates Supabase."""
        if not config.supabase:
            return
            
        while True:
            try:
                balance = await self.exchange.fetch_balance()
                total_margin = float(balance.get('USDT', {}).get('total', 0))
                
                def _update_wallet():
                    try:
                        # Upsert wallet data
                        config.supabase.table("wallets").upsert({
                            "id": "00000000-0000-0000-0000-000000000001", # Fixed UUID for main wallet
                            "wallet_name": "Binance Futures (Main)",
                            "network": "Binance USD-M",
                            "balance": total_margin,
                            "updated_at": "now()"
                        }).execute()
                    except Exception as e:
                        logger.error(f"Failed to sync wallet to dashboard: {e}")
                        
                asyncio.create_task(asyncio.to_thread(_update_wallet))
            except Exception as e:
                logger.error(f"Wallet monitor encountered an error: {e}")
                
            await asyncio.sleep(60.0) # Update every 60 seconds
            
    async def monitor_open_positions(self):
        """Periodically checks if tracked positions are closed on Binance and updates Trade History."""
        while True:
            try:
                if not self.active_positions:
                    await asyncio.sleep(10.0)
                    continue
                    
                # Fetch all current positions
                positions = await self.exchange.fetch_positions()
                open_symbols = set()
                
                for pos in positions:
                    if float(pos.get('contracts', 0)) > 0:
                        sym = pos['symbol'].replace("/", "").replace(":", "")
                        open_symbols.add(sym)
                        
                # Check for closed positions
                closed_positions = self.active_positions - open_symbols
                
                for closed_sym in closed_positions:
                    logger.info(f"[{closed_sym}] Position closed on exchange. Updating history...")
                    self.active_positions.remove(closed_sym)
                    
                    if config.supabase:
                        def _update_history(sym):
                            try:
                                # Mark as closed in active_trades
                                config.supabase.table("active_trades").update({"status": "CLOSED"}).eq("symbol", sym).eq("status", "OPEN").execute()
                                
                                # We would ideally fetch the exact PnL from trades/orders, but as a basic implementation:
                                # We just insert a generic closed record into trade_history
                                config.supabase.table("trade_history").insert({
                                    "symbol": sym,
                                    "side": "CLOSED",
                                    "entry_price": 0, # Requires complex order fetching to get exact exit
                                    "exit_price": 0,
                                    "pnl": 0, 
                                    "date": "now()"
                                }).execute()
                                logger.info(f"[{sym}] Moved to Trade History on Dashboard.")
                            except Exception as e:
                                logger.error(f"[{sym}] Failed to update trade history: {e}")
                                
                        asyncio.create_task(asyncio.to_thread(_update_history, closed_sym))
                        
            except Exception as e:
                logger.error(f"Error monitoring open positions: {e}")
                
            await asyncio.sleep(15.0) # Check every 15 seconds

    async def cleanup(self):
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
            
            # 7. Supabase Persistence
            if config.supabase:
                def _insert_trade():
                    try:
                        config.supabase.table("active_trades").insert({
                            "symbol": symbol,
                            "side": action,
                            "entry_price": current_price,
                            "leverage": config.DEFAULT_LEVERAGE,
                            "quantity": calculated_qty,
                            "status": "OPEN"
                        }).execute()
                    except Exception as e:
                        logger.error(f"[{symbol}] Failed to dispatch Supabase insert: {e}")
                asyncio.create_task(asyncio.to_thread(_insert_trade))
                
            config.send_log_to_dashboard("ExecutionEngine", "TRADE_OPENED", f"[{symbol}] Opened {action} with {calculated_qty} qty at {current_price}.")
            
        except Exception as e:
            logger.error(f"[{symbol}] Execution Pipeline Failed: {e}")

    async def monitor_pending_approvals(self):
        """Poll Supabase for signals that have been manually approved by the user."""
        logger.info("Starting Pending Approvals monitor loop...")
        while True:
            try:
                if config.supabase:
                    def _fetch_approved():
                        return config.supabase.table("pending_signals").select("*").eq("status", "APPROVED").execute()
                        
                    res = await asyncio.to_thread(_fetch_approved)
                    if res.data:
                        for signal in res.data:
                            symbol = signal['symbol']
                            action = signal['action']
                            signal_id = signal['id']
                            
                            logger.info(f"[{symbol}] Found USER APPROVED signal for {action}! Initiating execution.")
                            
                            # 1. Update status to EXECUTING to prevent double-execution
                            def _mark_executing():
                                config.supabase.table("pending_signals").update({"status": "EXECUTED"}).eq("id", signal_id).execute()
                            await asyncio.to_thread(_mark_executing)
                            
                            # 2. Fire Execution Task (Background)
                            if symbol not in self.active_positions:
                                asyncio.create_task(self.execute_trade(symbol, action))
                            else:
                                logger.info(f"[{symbol}] Ignoring approved {action} signal. Active position already exists.")
                                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error checking pending approvals: {e}")
                
            await asyncio.sleep(2.0) # Check every 2 seconds

    async def run(self):
        await self.initialize()
        try:
            await self.monitor_pending_approvals()
        finally:
            await self.cleanup()

if __name__ == "__main__":
    engine = ExecutionEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logger.info("Execution Engine gracefully shut down.")
