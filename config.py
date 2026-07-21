import os
from typing import List
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Redis Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Binance API Configuration
BINANCE_WS_URL = os.getenv("BINANCE_WS_URL", "wss://fstream.binance.com/ws")
BINANCE_REST_URL = os.getenv("BINANCE_REST_URL", "https://fapi.binance.com")

# Gemini API Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# Target Pairs (lowercase for WebSocket streams)
# Defaults to BTCUSDT, ETHUSDT, FETUSDT
SYMBOLS: List[str] = os.getenv("SYMBOLS", "btcusdt,ethusdt,fetusdt").split(",")

# Polling Configurations
OI_POLL_INTERVAL_SECONDS = float(os.getenv("OI_POLL_INTERVAL_SECONDS", "5.0"))

# Execution & Risk Management Configuration
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY", "")
BINANCE_USE_TESTNET = os.getenv("BINANCE_USE_TESTNET", "True").lower() == "true"

DEFAULT_LEVERAGE = int(os.getenv("DEFAULT_LEVERAGE", "3"))
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.015"))
SL_PCT = float(os.getenv("SL_PCT", "0.015"))
TP_PCT = float(os.getenv("TP_PCT", "0.03"))

# Supabase Configuration
import asyncio
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

supabase: Client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"Supabase init error: {e}")

def send_log_to_dashboard(agent_name: str, action: str, message: str):
    if supabase:
        def _insert():
            try:
                supabase.table("agent_logs").insert({
                    "agent_name": agent_name,
                    "action": action,
                    "message": message
                }).execute()
            except Exception as e:
                pass
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(asyncio.to_thread(_insert))
        except RuntimeError:
            pass # No running event loop

