import asyncio
import logging
import time
import orjson
import redis.asyncio as redis
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
import google.generativeai as genai
from supabase import create_client, Client

import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AgentOrchestrator")

# Configure Gemini
if not config.GEMINI_API_KEY:
    logger.warning("GEMINI_API_KEY is not set in config/env. Gemini API calls will fail.")
genai.configure(api_key=config.GEMINI_API_KEY)

# Define the expected JSON output schema for Gemini
class TradingDecision(BaseModel):
    action: str = Field(description="The trading action to take: LONG, SHORT, or WAIT")
    confidence_score: float = Field(description="Confidence score between 0.0 and 1.0")
    reasoning: str = Field(description="A short explanation of why this decision was made based on the provided metrics")

class AgentOrchestrator:
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.pubsub = None
        
        # State: In-memory numerical state per symbol
        # { "BTCUSDT": {"cvd_1m": 0, "cvd_5m": 0, "imbalance": 0.5, "mark_price": 0.0} }
        self.state: Dict[str, Dict[str, float]] = {
            s.upper(): {"cvd_1m": 0.0, "cvd_5m": 0.0, "imbalance": 0.5, "mark_price": 0.0}
            for s in config.SYMBOLS
        }
        
        # Cooldown state: last trigger timestamp per symbol
        self.last_trigger: Dict[str, float] = {s.upper(): 0.0 for s in config.SYMBOLS}
        self.cooldown_seconds = 60.0
        
        # Initialize Gemini Model
        self.model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json",
                response_schema=TradingDecision,
                temperature=0.2, # Low temperature for more deterministic outputs
            )
        )

    async def initialize(self):
        logger.info(f"Connecting to Redis at {config.REDIS_URL}")
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        self.pubsub = self.redis_client.pubsub()
        await self.pubsub.psubscribe("features:*", "market:markprice:*")
        logger.info("Subscribed to features and markprice channels.")

    async def cleanup(self):
        if self.pubsub:
            await self.pubsub.close()
        if self.redis_client:
            await self.redis_client.aclose()

    def _update_state(self, channel: str, data: dict):
        try:
            parts = channel.split(":")
            if channel.startswith("market:markprice:"):
                symbol = parts[-1]
                # Binance markPriceUpdate payload typically has 'p' for mark price
                if symbol in self.state and 'p' in data:
                    self.state[symbol]["mark_price"] = float(data['p'])
                    
            elif channel.startswith("features:cvd:"):
                symbol = parts[-1]
                if symbol in self.state:
                    self.state[symbol]["cvd_1m"] = float(data.get("cvd_1m", 0.0))
                    self.state[symbol]["cvd_5m"] = float(data.get("cvd_5m", 0.0))
                    
            elif channel.startswith("features:imbalance:"):
                symbol = parts[-1]
                if symbol in self.state:
                    self.state[symbol]["imbalance"] = float(data.get("imbalance", 0.5))
        except (KeyError, ValueError) as e:
            logger.error(f"Error updating state for channel {channel}: {e}")

    async def _call_gemini_agent(self, symbol: str, current_state: dict):
        """Non-blocking background task to call Gemini and publish the decision."""
        logger.info(f"Triggering Gemini Agent for {symbol}. State: {current_state}")
        
        prompt = f"""
        You are an elite quantitative trading AI. Analyze the following real-time orderflow and market data for {symbol}.
        
        Market Data:
        - Mark Price: {current_state['mark_price']}
        - Cumulative Volume Delta (1 min): {current_state['cvd_1m']}
        - Cumulative Volume Delta (5 min): {current_state['cvd_5m']}
        - Orderbook Imbalance (Top 10 Levels): {current_state['imbalance']} 
          (Note: Imbalance > 0.5 indicates more Bid volume, < 0.5 indicates more Ask volume)
          
        Rules:
        1. If Imbalance is heavily skewed (> 0.70) and CVD is positive, it might indicate strong buying pressure. Consider LONG.
        2. If Imbalance is heavily skewed (< 0.30) and CVD is negative, it might indicate strong selling pressure. Consider SHORT.
        3. If signals are mixed, action should be WAIT.
        
        Provide your response exactly matching the requested JSON schema.
        """
        
        try:
            response = await self.model.generate_content_async(prompt)
            
            # The response text should be valid JSON matching the schema
            decision_text = response.text
            decision_dict = orjson.loads(decision_text)
            
            # Validate with Pydantic
            decision = TradingDecision(**decision_dict)
            logger.info(f"[{symbol}] Gemini Decision: {decision.action} (Confidence: {decision.confidence_score}) | Reason: {decision.reasoning}")
            
            # Always log the AI's thought process to the Dashboard so the user can see it live!
            config.send_log_to_dashboard(
                "AgentOrchestrator", 
                decision.action, 
                f"[{symbol}] Confidence: %{int(decision.confidence_score * 100)}. Reason: {decision.reasoning}"
            )
            
            if decision.action in ["LONG", "SHORT"] and decision.confidence_score > 0.75:
                signal_payload = {
                    "symbol": symbol,
                    "action": decision.action,
                    "confidence_score": decision.confidence_score,
                    "reasoning": decision.reasoning,
                    "trigger_state": current_state,
                    "timestamp": int(time.time() * 1000)
                }
                
                await self.redis_client.publish(
                    f"signals:master:{symbol}",
                    orjson.dumps(signal_payload)
                )
                logger.info(f"[{symbol}] Published trade signal to signals:master:{symbol}")
                
        except Exception as e:
            logger.error(f"Error calling Gemini or processing response for {symbol}: {e}")

    async def listen_to_channels(self):
        """Listen to incoming Redis messages and update state."""
        while True:
            try:
                message = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                if message and message['type'] == 'pmessage':
                    channel = message['channel'].decode('utf-8')
                    data = orjson.loads(message['data'])
                    self._update_state(channel, data)
                else:
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error reading message from Redis: {e}")
                await asyncio.sleep(1)

    async def evaluate_triggers(self):
        """Evaluate the state every second to check if we should trigger the agent."""
        while True:
            try:
                current_time = time.time()
                
                for symbol in config.SYMBOLS:
                    sym_upper = symbol.upper()
                    state = self.state[sym_upper]
                    
                    # Check Cooldown
                    if current_time - self.last_trigger[sym_upper] < self.cooldown_seconds:
                        continue
                    
                    imbalance = state["imbalance"]
                    
                    # Trigger Condition: Heavy Buy Wall or Heavy Sell Wall
                    if imbalance > 0.70 or imbalance < 0.30:
                        # Update last trigger time immediately to prevent spam
                        self.last_trigger[sym_upper] = current_time
                        
                        # Snapshot the state to pass to the agent
                        state_snapshot = state.copy()
                        
                        # Spawn background task so evaluation loop doesn't block
                        asyncio.create_task(self._call_gemini_agent(sym_upper, state_snapshot))
                        
            except Exception as e:
                logger.error(f"Error in evaluate_triggers loop: {e}")
                
            await asyncio.sleep(1.0)

    async def run(self):
        await self.initialize()
        try:
            await asyncio.gather(
                self.listen_to_channels(),
                self.evaluate_triggers()
            )
        finally:
            await self.cleanup()

if __name__ == "__main__":
    orchestrator = AgentOrchestrator()
    try:
        asyncio.run(orchestrator.run())
    except KeyboardInterrupt:
        logger.info("Agent Orchestrator gracefully shut down.")
