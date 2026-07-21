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
# Legacy fixed-percent bracket (fallback when the strategy gives no structure stop).
SL_PCT = float(os.getenv("SL_PCT", "0.015"))
TP_PCT = float(os.getenv("TP_PCT", "0.03"))

# ---- Strategy selection & parameters -------------------------------------
# "confluence"     = 5-layer confluence + narrative bonus (default, recommended)
# "sweep_reversal" = liquidity-sweep / stop-hunt reversal only
# "flow"           = the simpler order-flow score (signal_engine.evaluate)
STRATEGY = os.getenv("STRATEGY", "confluence")

# Liquidity-level tracking (used by alpha_generator to find swept levels)
SWING_LOOKBACK_SEC = float(os.getenv("SWING_LOOKBACK_SEC", "900"))   # 15m liquidity pool
SWEEP_REACTION_SEC = float(os.getenv("SWEEP_REACTION_SEC", "25"))    # recent "wick" window
RANGE_WINDOW_SEC = float(os.getenv("RANGE_WINDOW_SEC", "60"))        # 1m range for stop sizing

# Structure-based stop: place it beyond the sweep wick by this fraction of the
# 1-minute range, so the stop does NOT sit on the obvious hunted level.
SL_WICK_BUFFER_MULT = float(os.getenv("SL_WICK_BUFFER_MULT", "0.35"))
# Take-profit as a multiple of the (dynamic) risk distance.
TP_R_MULTIPLE = float(os.getenv("TP_R_MULTIPLE", "2.0"))
# Guard rails so a bad tick can't create an absurd stop distance.
MIN_SL_PCT = float(os.getenv("MIN_SL_PCT", "0.004"))   # never tighter than 0.4%
MAX_SL_PCT = float(os.getenv("MAX_SL_PCT", "0.04"))    # never wider than 4%

# ---- Exit management (partial TP + trailing runner) ----------------------
# Fixes the "sold at +7% then it flew" problem: bank part of the winner at a
# near target, then let the rest ride a trailing stop so trends run.
USE_TRAILING = os.getenv("USE_TRAILING", "True").lower() == "true"
PARTIAL_TP_PCT = float(os.getenv("PARTIAL_TP_PCT", "0.5"))     # fraction closed at TP1
TP1_R_MULTIPLE = float(os.getenv("TP1_R_MULTIPLE", "1.0"))     # first target (banked)
TRAIL_CALLBACK_PCT = float(os.getenv("TRAIL_CALLBACK_PCT", "1.2"))  # Binance callbackRate %

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


# ---- UI-editable runtime config (bot_config table) -----------------------
# The dashboard writes to the bot_config key-value table; every agent reads
# these overrides at startup so the watchlist / strategy / risk can be changed
# from the UI without editing .env or redeploying. .env stays the fallback.
def _apply_bot_config():
    global SYMBOLS, STRATEGY, RISK_PER_TRADE_PCT, TP_R_MULTIPLE, DEFAULT_LEVERAGE
    if not supabase:
        return
    try:
        res = supabase.table("bot_config").select("*").execute()
        cfg = {row["key"]: row["value"] for row in (res.data or [])}
        if isinstance(cfg.get("symbols"), list) and cfg["symbols"]:
            SYMBOLS = [str(s).lower() for s in cfg["symbols"]]
        if cfg.get("strategy"):
            STRATEGY = str(cfg["strategy"])
        if cfg.get("risk_per_trade_pct") is not None:
            RISK_PER_TRADE_PCT = float(cfg["risk_per_trade_pct"])
        if cfg.get("tp_r_multiple") is not None:
            TP_R_MULTIPLE = float(cfg["tp_r_multiple"])
        if cfg.get("leverage") is not None:
            DEFAULT_LEVERAGE = int(cfg["leverage"])
        print(f"[config] bot_config overrides applied: symbols={SYMBOLS}, "
              f"strategy={STRATEGY}, risk={RISK_PER_TRADE_PCT}, lev={DEFAULT_LEVERAGE}")
    except Exception as e:
        print(f"[config] bot_config load skipped ({e}); using .env defaults.")


_apply_bot_config()

