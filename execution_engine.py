import asyncio
import logging
import orjson
import time
import uuid
import ccxt.async_support as ccxt
import redis.asyncio as redis
from typing import Set, Dict
from supabase import create_client, Client

import config
import trade_journal

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ExecutionEngine")


def _bool_str(value: bool) -> str:
    return "true" if value else "false"


class ExecutionEngine:
    def __init__(self):
        self.redis_client = None
        self.pubsub = None
        
        # Local tracker to prevent duplicate entries
        self.active_positions: Set[str] = set()
        # Per-symbol context for open trades: trade_id, entry_ts, features, signal.
        # Needed to compute real PnL and to close out the trade journal on exit.
        self.position_context: Dict[str, dict] = {}
        
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
                    symbol = self._normalize_symbol(pos['symbol'])
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
            
    def _normalize_symbol(self, ccxt_symbol: str) -> str:
        """CCXT symbol -> Binance id.  'CRV/USDT:USDT' -> 'CRVUSDT'.

        The old code did .replace('/','').replace(':','') which produced
        'CRVUSDTUSDT' (the settlement suffix was kept), breaking symbol lookup,
        PnL computation and journal closure for those positions.
        """
        try:
            return self.exchange.market(ccxt_symbol)['id']
        except Exception:
            # Fallback: drop the ':SETTLE' suffix, then remove the slash.
            return ccxt_symbol.split(':')[0].replace('/', '')

    def _resolve_ccxt_symbol(self, symbol: str):
        """Map a plain symbol (e.g. BTCUSDT) to the CCXT symbol (BTC/USDT:USDT)."""
        for sym in self.exchange.markets:
            if sym.replace("/", "").replace(":", "").startswith(symbol):
                return sym

    # ------------------------------------------------------------------
    # Binance's Algo Order API (conditional orders) — since 2025-12-09
    # Binance migrated ALL conditional futures order types (STOP_MARKET,
    # TAKE_PROFIT_MARKET, TRAILING_STOP_MARKET, ...) off the standard
    # POST/DELETE /fapi/v1/order endpoints entirely; they now reject those
    # types outright with error -4120 ("use the Algo Order API instead"),
    # regardless of reduceOnly/closePosition. This is what left the
    # XMRUSDT and LABUSDT positions with NO stop-loss on 2026-07-22 despite
    # two different parameter combinations being tried — the endpoint
    # itself was the problem, not the params.
    #
    # ccxt's unified create_order()/cancel_order() don't wrap the new
    # /fapi/v1/algoOrder endpoint yet, so these call ccxt's generic signed
    # request() method directly — this works regardless of whether the
    # installed ccxt release has a pre-registered named alias for it.
    # Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Algo-Order
    # ------------------------------------------------------------------
    async def _create_algo_conditional(
        self, ccxt_symbol: str, side: str, order_type: str,
        amount: float = None, stop_price: float = None,
        activate_price: float = None, callback_rate: float = None,
        reduce_only: bool = True,
    ) -> dict:
        market = self.exchange.market(ccxt_symbol)
        params = {
            "algoType": "CONDITIONAL",
            "symbol": market["id"],
            "side": side.upper(),
            "type": order_type,
            "reduceOnly": _bool_str(reduce_only),
        }
        if amount is not None:
            params["quantity"] = self.exchange.amount_to_precision(ccxt_symbol, amount)
        if stop_price is not None:
            params["stopPrice"] = self.exchange.price_to_precision(ccxt_symbol, stop_price)
        if activate_price is not None:
            params["activationPrice"] = self.exchange.price_to_precision(ccxt_symbol, activate_price)
        if callback_rate is not None:
            params["callbackRate"] = str(callback_rate)
        return await self.exchange.request("algoOrder", "fapiPrivate", "POST", params)

    async def _cancel_algo_order(self, ccxt_symbol: str, algo_id) -> dict:
        market = self.exchange.market(ccxt_symbol)
        return await self.exchange.request(
            "algoOrder", "fapiPrivate", "DELETE",
            {"symbol": market["id"], "algoId": algo_id},
        )

    async def _cancel_all_algo_orders(self, ccxt_symbol: str) -> dict:
        market = self.exchange.market(ccxt_symbol)
        return await self.exchange.request(
            "algoOpenOrders", "fapiPrivate", "DELETE", {"symbol": market["id"]},
        )
        return None

    async def _compute_realized_result(self, symbol: str, since_ts):
        """Fetch the real closing result from the exchange fills.

        Returns (exit_price, realized_pnl, commission). Falls back to zeros only
        if the exchange query fails, so a bad fetch never fabricates a fake win.
        """
        ccxt_symbol = self._resolve_ccxt_symbol(symbol)
        if not ccxt_symbol:
            return 0.0, 0.0, 0.0
        try:
            # Look back a little before entry to be safe; None -> recent window.
            since = (since_ts - 1000) if since_ts else None
            fills = await self.exchange.fetch_my_trades(ccxt_symbol, since=since, limit=100)
            realized = 0.0
            commission = 0.0
            last_exit_price = 0.0
            for f in fills:
                info = f.get('info', {})
                realized += float(info.get('realizedPnl', 0) or 0)
                # commission is charged on every fill; treat as a positive cost
                fee = f.get('fee') or {}
                commission += abs(float(fee.get('cost', info.get('commission', 0)) or 0))
                # closing fills carry realizedPnl != 0; use their price as exit
                if float(info.get('realizedPnl', 0) or 0) != 0:
                    last_exit_price = float(f.get('price', 0) or 0)
            return last_exit_price, realized, commission
        except Exception as e:
            logger.error(f"[{symbol}] Failed to compute realized result: {e}")
            return 0.0, 0.0, 0.0

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
                        sym = self._normalize_symbol(pos['symbol'])
                        open_symbols.add(sym)

                # Check for closed positions
                closed_positions = self.active_positions - open_symbols

                for closed_sym in closed_positions:
                    logger.info(f"[{closed_sym}] Position closed on exchange. Computing real PnL...")
                    self.active_positions.remove(closed_sym)

                    # Cancel any leftover bracket orders (SL / TP1 / trailing) so
                    # a closed position never leaves dangling reduceOnly orders.
                    # Algo (conditional) orders are a separate order book from
                    # regular orders since Binance's Algo Order API migration,
                    # so both cancel calls are needed.
                    ccxt_symbol = self._resolve_ccxt_symbol(closed_sym)
                    if ccxt_symbol:
                        try:
                            await self.exchange.cancel_all_orders(ccxt_symbol)
                        except Exception as e:
                            logger.warning(f"[{closed_sym}] Could not cancel leftover orders: {e}")
                        try:
                            await self._cancel_all_algo_orders(ccxt_symbol)
                        except Exception as e:
                            logger.warning(f"[{closed_sym}] Could not cancel leftover algo orders: {e}")
                        logger.info(f"[{closed_sym}] Cancelled leftover bracket orders.")

                    ctx = self.position_context.pop(closed_sym, {})
                    entry_ts = ctx.get("entry_ts")

                    # Query the exchange for the REAL closing result (net of fees).
                    exit_price, realized_pnl, commission = await self._compute_realized_result(
                        closed_sym, entry_ts
                    )

                    # Close out the learning journal with the true outcome.
                    if ctx.get("trade_id"):
                        outcome = trade_journal.record_exit(
                            trade_id=ctx["trade_id"],
                            symbol=closed_sym,
                            exit_price=exit_price,
                            realized_pnl=realized_pnl,
                            commission=commission,
                            entry_ts=entry_ts,
                        )
                        net = outcome["net_pnl"]
                        logger.info(f"[{closed_sym}] Journalled {outcome['outcome']} net PnL {net:+.4f} USDT.")
                        config.send_log_to_dashboard(
                            "ExecutionEngine", "TRADE_CLOSED",
                            f"[{closed_sym}] {outcome['outcome']} net {net:+.2f} USDT (fees {outcome['commission']:.2f})."
                        )
                    else:
                        net = realized_pnl - abs(commission)

                    if config.supabase:
                        entry_price = ctx.get("entry_price", 0)
                        side = ctx.get("side", "CLOSED")
                        def _update_history(sym, e_price, x_price, pnl, side_):
                            try:
                                config.supabase.table("active_trades").update({"status": "CLOSED"}).eq("symbol", sym).eq("status", "OPEN").execute()
                                config.supabase.table("trade_history").insert({
                                    "symbol": sym,
                                    "side": side_,
                                    "entry_price": e_price,
                                    "exit_price": x_price,
                                    "pnl": round(pnl, 4),
                                    "closed_at": "now()"
                                }).execute()
                                logger.info(f"[{sym}] Moved to Trade History with real PnL {pnl:+.4f}.")
                            except Exception as e:
                                logger.error(f"[{sym}] Failed to update trade history: {e}")

                        asyncio.create_task(asyncio.to_thread(
                            _update_history, closed_sym, entry_price, exit_price, net, side
                        ))

            except Exception as e:
                logger.error(f"Error monitoring open positions: {e}")

            await asyncio.sleep(15.0) # Check every 15 seconds

    async def cleanup(self):
        if self.redis_client:
            await self.redis_client.aclose()
        if self.exchange:
            await self.exchange.close()

    async def execute_trade(self, symbol: str, action: str, signal_meta: dict = None):
        """Execute the bracket order logic based on strictly hard-coded risk rules.

        signal_meta may carry the feature snapshot and signal that produced this
        trade; it is journalled so the learning layer can later correlate the
        entry conditions with the realised outcome.
        """
        signal_meta = signal_meta or {}
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

            # 3. Risk Math — confidence-scaled ("low conviction, small size").
            # RISK_PER_TRADE_PCT is the MAXIMUM; a signal at the confidence
            # floor (0.55) risks half of it, scaling linearly to full size at
            # 0.85+. Deterministic and journalled, so learn.py can verify the
            # scaling actually helps.
            confidence = float((signal_meta.get("signal") or {}).get("confidence") or 0.0)
            if confidence > 0:
                risk_mult = max(0.5, min(1.0, 0.5 + 0.5 * (confidence - 0.55) / 0.30))
            else:
                risk_mult = 1.0   # legacy/manual signals without a confidence
            risk_amount = free_margin * config.RISK_PER_TRADE_PCT * risk_mult
            logger.info(f"[{symbol}] Risk sizing: conf {confidence:.2f} -> "
                        f"{risk_mult:.2f}x of {config.RISK_PER_TRADE_PCT:.0%} = "
                        f"${risk_amount:.2f}")

            # Prefer the structure-based bracket from the strategy (stop placed
            # beyond the swept wick). Fall back to fixed-percent only if absent.
            features = signal_meta.get("features") or {}
            sig_sl = features.get("sl_price")
            sig_tp = features.get("tp_price")
            # HTF trend trades carry trail_mode="structure": NO fixed TP at all;
            # the exit is the trailing stop ratcheted by htf_agent on each 4h
            # close (via the manage:stop channel).
            trail_managed = features.get("trail_mode") == "structure"

            if action == "LONG":
                side = 'buy'
                sl_side = 'sell'
                sl_price = float(sig_sl) if sig_sl else current_price * (1 - config.SL_PCT)
                tp_price = float(sig_tp) if sig_tp else current_price * (1 + config.TP_PCT)
            else:  # SHORT
                side = 'sell'
                sl_side = 'buy'
                sl_price = float(sig_sl) if sig_sl else current_price * (1 + config.SL_PCT)
                tp_price = float(sig_tp) if sig_tp else current_price * (1 - config.TP_PCT)

            # Safety: make sure SL is on the correct side of price (a stale
            # structure stop could otherwise be nonsensical after a fast move).
            if action == "LONG" and sl_price >= current_price:
                sl_price = current_price * (1 - config.SL_PCT)
            if action == "SHORT" and sl_price <= current_price:
                sl_price = current_price * (1 + config.SL_PCT)

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

            # Disaster Stop Loss. reduceOnly + explicit quantity, NOT
            # closePosition=True: Binance now rejects closePosition STOP_MARKET
            # on the standard order endpoint (-4120 "use the Algo Order API
            # instead"), which left a live position with NO stop order at all
            # on 2026-07-22 (XMRUSDT). reduceOnly caps the fill at whatever
            # position remains open (never reverses it), so a stale qty after
            # a later partial de-risk still closes the position fully.
            sl_algo = await self._create_algo_conditional(
                ccxt_symbol, side=sl_side, order_type='STOP_MARKET',
                amount=qty, stop_price=formatted_sl_price, reduce_only=True,
            )
            sl_algo_id = sl_algo.get('algoId')
            logger.info(f"[{symbol}] SL PLACED (algo): {sl_algo_id} at {formatted_sl_price}")

            # Take-profit / runner management.
            partial_qty = 0.0
            runner_qty = 0.0
            if trail_managed:
                # HTF trend trade: stop-loss only. No TP order caps the winner;
                # htf_agent ratchets the stop upward/downward on every 4h close
                # until the trend breaks. This is the "stay in until the move
                # is over" exit.
                logger.info(f"[{symbol}] Trail-managed position: SL only at "
                            f"{formatted_sl_price}, no fixed TP (trend exit).")
            elif config.USE_TRAILING:
                # TP1 banks part of the winner; the rest trails the trend.
                raw_partial = qty * config.PARTIAL_TP_PCT
                partial_qty = float(self.exchange.amount_to_precision(ccxt_symbol, raw_partial))
                runner_qty = float(self.exchange.amount_to_precision(ccxt_symbol, qty - partial_qty))

                # First target = TP1_R multiple of risk (nearer than the full TP).
                if action == "LONG":
                    tp1_price = current_price + config.TP1_R_MULTIPLE * price_delta
                else:
                    tp1_price = current_price - config.TP1_R_MULTIPLE * price_delta
                formatted_tp1 = float(self.exchange.price_to_precision(ccxt_symbol, tp1_price))

            # Only use the managed exit if BOTH slices survive precision/min-size.
            use_managed = (not trail_managed) and config.USE_TRAILING \
                and partial_qty > 0 and runner_qty > 0

            if trail_managed:
                pass  # no TP orders of any kind; exit is the ratcheting stop
            elif use_managed:
                # Partial TP1 (reduceOnly, banks profit at the near target)
                await self._create_algo_conditional(
                    ccxt_symbol, side=sl_side, order_type='TAKE_PROFIT_MARKET',
                    amount=partial_qty, stop_price=formatted_tp1, reduce_only=True,
                )
                logger.info(f"[{symbol}] TP1 (partial {partial_qty}) at {formatted_tp1}")

                # Runner: native trailing stop that activates once TP1 is reached,
                # then follows the trend by callbackRate. This is what keeps us in
                # the trade when the coin "flies" after the first target.
                try:
                    await self._create_algo_conditional(
                        ccxt_symbol, side=sl_side, order_type='TRAILING_STOP_MARKET',
                        amount=runner_qty, activate_price=formatted_tp1,
                        callback_rate=config.TRAIL_CALLBACK_PCT, reduce_only=True,
                    )
                    logger.info(f"[{symbol}] RUNNER trailing {runner_qty} activates @ {formatted_tp1}, callback {config.TRAIL_CALLBACK_PCT}%")
                except Exception as e:
                    # If trailing is rejected, fall back to a fixed TP for the runner.
                    logger.warning(f"[{symbol}] Trailing stop rejected ({e}); using fixed TP for runner.")
                    await self._create_algo_conditional(
                        ccxt_symbol, side=sl_side, order_type='TAKE_PROFIT_MARKET',
                        amount=runner_qty, stop_price=formatted_tp_price, reduce_only=True,
                    )
            else:
                # Fallback: single full-size take-profit (original behaviour).
                tp_order = await self._create_algo_conditional(
                    ccxt_symbol, side=sl_side, order_type='TAKE_PROFIT_MARKET',
                    amount=qty, stop_price=formatted_tp_price, reduce_only=True,
                )
                logger.info(f"[{symbol}] TP (full) at {formatted_tp_price}")

            # Use the real average fill price when the exchange reports it.
            actual_entry = float(entry_order.get('average') or current_price)

            # 6. Update local tracker & publish telemetry
            self.active_positions.add(symbol)

            # Record context so we can compute real PnL and close the journal on exit.
            trade_id = str(uuid.uuid4())
            self.position_context[symbol] = {
                "trade_id": trade_id,
                "entry_ts": int(time.time() * 1000),
                "entry_price": actual_entry,
                "side": action,
                "quantity": qty,
                "sl_price": formatted_sl_price,
                "sl_order_id": sl_algo_id,
                "trail_managed": trail_managed,
            }
            trade_journal.record_entry(
                trade_id=trade_id,
                symbol=symbol,
                action=action,
                entry_price=actual_entry,
                quantity=qty,
                leverage=config.DEFAULT_LEVERAGE,
                sl_price=formatted_sl_price,
                tp_price=formatted_tp_price,
                features=signal_meta.get("features"),
                signal=signal_meta.get("signal"),
            )

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
                            "entry_price": actual_entry,
                            "leverage": config.DEFAULT_LEVERAGE,
                            "quantity": qty,
                            "status": "OPEN"
                        }).execute()
                    except Exception as e:
                        logger.error(f"[{symbol}] Failed to dispatch Supabase insert: {e}")
                asyncio.create_task(asyncio.to_thread(_insert_trade))

            config.send_log_to_dashboard("ExecutionEngine", "TRADE_OPENED", f"[{symbol}] Opened {action} with {qty} qty at {actual_entry}.")
            
        except Exception as e:
            logger.error(f"[{symbol}] Execution Pipeline Failed: {e}")

    async def apply_stop_update(self, symbol: str, side: str, new_sl: float):
        """Move the exchange stop order for an open position (trailing ratchet).

        Only ever tightens: a long's stop moves UP, a short's moves DOWN. The
        old STOP_MARKET is cancelled and a fresh reduceOnly stop is placed at
        the new level, sized to the LIVE exchange position (not the locally
        tracked quantity, which can go stale after a partial de-risk close).
        """
        if symbol not in self.active_positions:
            logger.info(f"[{symbol}] Stop update ignored: no active position.")
            return
        ctx = self.position_context.get(symbol, {})
        current_sl = ctx.get("sl_price")
        if current_sl is not None:
            improves = (new_sl > current_sl) if side == "LONG" else (new_sl < current_sl)
            if not improves:
                logger.info(f"[{symbol}] Stop update {new_sl} does not improve on "
                            f"{current_sl}; ignored (ratchet only).")
                return

        ccxt_symbol = self._resolve_ccxt_symbol(symbol)
        if not ccxt_symbol:
            logger.error(f"[{symbol}] Cannot resolve ccxt symbol for stop update.")
            return
        try:
            formatted_sl = float(self.exchange.price_to_precision(ccxt_symbol, new_sl))
            sl_side = 'sell' if side == "LONG" else 'buy'

            # Cancel the existing algo stop first (by algoId when known,
            # else all algo orders — trail-managed positions only ever hold
            # the one stop). This is a SEPARATE order book from regular
            # orders (Binance's Algo Order API), so the old cancel_order/
            # cancel_all_orders calls silently missed it entirely.
            old_id = ctx.get("sl_order_id")
            if old_id:
                try:
                    await self._cancel_algo_order(ccxt_symbol, old_id)
                except Exception as e:
                    logger.warning(f"[{symbol}] Could not cancel old algo SL {old_id}: {e}")
            else:
                try:
                    await self._cancel_all_algo_orders(ccxt_symbol)
                except Exception as e:
                    logger.warning(f"[{symbol}] cancel_all_algo before stop move failed: {e}")

            live_qty = ctx.get("quantity") or 0
            try:
                positions = await self.exchange.fetch_positions([ccxt_symbol])
                for pos in positions:
                    if self._normalize_symbol(pos['symbol']) == symbol:
                        live_qty = abs(float(pos.get('contracts', 0) or 0)) or live_qty
                        break
            except Exception as e:
                logger.warning(f"[{symbol}] Could not fetch live position size for stop "
                                f"move; using locally tracked quantity: {e}")

            sl_algo = await self._create_algo_conditional(
                ccxt_symbol, side=sl_side, order_type='STOP_MARKET',
                amount=live_qty, stop_price=formatted_sl, reduce_only=True,
            )
            ctx["sl_price"] = formatted_sl
            ctx["sl_order_id"] = sl_algo.get('algoId')
            self.position_context[symbol] = ctx
            logger.info(f"[{symbol}] Trailing stop moved to {formatted_sl}.")
            config.send_log_to_dashboard(
                "ExecutionEngine", "STOP_MOVED",
                f"[{symbol}] {side} stop trailed to {formatted_sl}."
            )
        except Exception as e:
            logger.error(f"[{symbol}] Failed to move stop to {new_sl}: {e}")

    async def listen_stop_updates(self):
        """Subscribe to manage:stop and apply trailing-stop ratchets."""
        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe("manage:stop")
        logger.info("Listening for trailing-stop updates on manage:stop...")
        while True:
            try:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message['type'] == 'message':
                    data = orjson.loads(message['data'])
                    await self.apply_stop_update(
                        str(data['symbol']), str(data.get('side', 'LONG')),
                        float(data['new_sl'])
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stop-update listener: {e}")
            await asyncio.sleep(0.2)

    async def apply_derisk(self, symbol: str, side: str, fraction: float):
        """Partially close a position ahead of a major HTF zone (Melih's
        "büyük dirence yaklaşırken pozisyon hafiflet" pattern). This only
        ever REDUCES exposure — never opens or reverses anything — so like
        the trailing-stop ratchet it runs autonomously, no approval needed.

        Reads the LIVE position size from the exchange rather than trusting
        locally-tracked quantity, since a prior de-risk (or partial fill)
        may already have shrunk it.
        """
        if symbol not in self.active_positions:
            logger.info(f"[{symbol}] De-risk ignored: no active position.")
            return
        ccxt_symbol = self._resolve_ccxt_symbol(symbol)
        if not ccxt_symbol:
            logger.error(f"[{symbol}] Cannot resolve ccxt symbol for de-risk.")
            return
        try:
            positions = await self.exchange.fetch_positions([ccxt_symbol])
            live_qty = 0.0
            for pos in positions:
                if self._normalize_symbol(pos['symbol']) == symbol:
                    live_qty = abs(float(pos.get('contracts', 0) or 0))
                    break
            if live_qty <= 0:
                logger.info(f"[{symbol}] De-risk skipped: no live position quantity found.")
                return

            close_side = 'sell' if side == "LONG" else 'buy'
            qty = float(self.exchange.amount_to_precision(ccxt_symbol, live_qty * fraction))
            if qty <= 0:
                logger.info(f"[{symbol}] De-risk quantity rounds to 0; skipping.")
                return

            await self.exchange.create_order(
                symbol=ccxt_symbol, type='market', side=close_side,
                amount=qty, params={'reduceOnly': True}
            )
            logger.info(f"[{symbol}] De-risked {qty} ({fraction:.0%} of {live_qty}) at market. "
                        f"The reduceOnly stop still fully closes whatever remains when it "
                        f"triggers (reduceOnly caps the fill at the live position size), and "
                        f"apply_stop_update refreshes the stop's quantity from the live "
                        f"position on every trailing ratchet.")

            ctx = self.position_context.get(symbol)
            if ctx:
                ctx["quantity"] = max(live_qty - qty, 0.0)

            config.send_log_to_dashboard(
                "ExecutionEngine", "DERISK",
                f"[{symbol}] {side} pozisyonun %{fraction*100:.0f}'i majör bölge öncesi "
                f"piyasa fiyatından kapatıldı ({qty} adet)."
            )
        except Exception as e:
            logger.error(f"[{symbol}] Failed to de-risk: {e}")

    async def listen_derisk_updates(self):
        """Subscribe to manage:derisk and apply partial position reductions."""
        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe("manage:derisk")
        logger.info("Listening for de-risk triggers on manage:derisk...")
        while True:
            try:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message['type'] == 'message':
                    data = orjson.loads(message['data'])
                    await self.apply_derisk(
                        str(data['symbol']), str(data.get('side', 'LONG')),
                        float(data['fraction'])
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in de-risk listener: {e}")
            await asyncio.sleep(0.2)

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
                                signal_meta = {
                                    "features": signal.get("features"),
                                    "signal": {
                                        "confidence": signal.get("confidence"),
                                        "reasoning": signal.get("reasoning"),
                                        "score": signal.get("score"),
                                    },
                                }
                                asyncio.create_task(self.execute_trade(symbol, action, signal_meta))
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
            await asyncio.gather(
                self.monitor_pending_approvals(),
                self.listen_stop_updates(),
                self.listen_derisk_updates(),
            )
        finally:
            await self.cleanup()

if __name__ == "__main__":
    engine = ExecutionEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        logger.info("Execution Engine gracefully shut down.")
