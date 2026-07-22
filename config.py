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

# Hardcoded, not env-configurable: risk sizing and leverage are trading-strategy
# decisions, not deployment config, so they live in code where they're
# reviewed like any other logic change instead of silently drifting via .env.
DEFAULT_LEVERAGE = 20
RISK_PER_TRADE_PCT = 0.10   # max 10% of free margin risked per trade
# Legacy fixed-percent bracket (fallback when the strategy gives no structure stop).
SL_PCT = float(os.getenv("SL_PCT", "0.015"))
TP_PCT = float(os.getenv("TP_PCT", "0.03"))

# ---- Strategy selection & parameters -------------------------------------
# Hardcoded, not env-configurable: which strategy runs is a code decision.
# "htf_breakout"   = 4h confirmed-breakout trend follower with 1d/3d structure
#                    and trailing exits (active). Signals come from htf_agent;
#                    the legacy 15-min pipeline below stays silent.
# "confluence"     = legacy 15-min 5-layer confluence + narrative bonus
# "sweep_reversal" = legacy liquidity-sweep / stop-hunt reversal only
# "flow"           = legacy simple order-flow score (signal_engine.evaluate)
STRATEGY = "htf_breakout"

# ---- HTF breakout strategy parameters (htf_agent / htf_strategy) ----------
# Trigger lives on daily/3-day CLOSES (confirmed breakout); the 4h timeframe
# is only used to track the current price and to trail an open position's
# stop. Scanning runs every 4h too — there is no value in polling more often
# than the finest candle actually used, and it matches "4 saatte bir tarama,
# günlük/3 günlükte kırılım ara, 4 saatlikte takip et".
HTF_TRACK_TIMEFRAME = os.getenv("HTF_TRACK_TIMEFRAME", "4h")     # tracking/trailing TF
HTF_DAILY_TIMEFRAME = os.getenv("HTF_DAILY_TIMEFRAME", "1d")     # fine breakout confirmation
HTF_3D_TIMEFRAME = os.getenv("HTF_3D_TIMEFRAME", "3d")           # coarse breakout confirmation
HTF_CHECK_INTERVAL_SEC = float(os.getenv("HTF_CHECK_INTERVAL_SEC", "14400"))  # 4h cadence
HTF_SCAN_TOP_N = int(os.getenv("HTF_SCAN_TOP_N", "30"))          # tightest-squeeze extras added
HTF_MIN_QUOTE_VOLUME = float(os.getenv("HTF_MIN_QUOTE_VOLUME", "30000000"))  # tradability FLOOR only, not a ranking key
# Squeeze/coiling discovery: rank candidates by price-range contraction over
# daily candles (recent window vs. the baseline window right before it), NOT
# by 24h volume — that biases toward coins already pumping/dumping instead of
# the quiet compression that precedes an HTF breakout.
HTF_SQUEEZE_RECENT_DAYS = int(os.getenv("HTF_SQUEEZE_RECENT_DAYS", "10"))
HTF_SQUEEZE_BASELINE_DAYS = int(os.getenv("HTF_SQUEEZE_BASELINE_DAYS", "40"))
HTF_MIN_DAILY_CANDLES = int(os.getenv("HTF_MIN_DAILY_CANDLES", "40"))  # no fresh listings
HTF_DONCHIAN_LOOKBACK_1D = int(os.getenv("HTF_DONCHIAN_LOOKBACK_1D", "20"))  # ~1 month
HTF_DONCHIAN_LOOKBACK_3D = int(os.getenv("HTF_DONCHIAN_LOOKBACK_3D", "10"))  # ~1 month
HTF_VOL_EXPANSION_MULT = float(os.getenv("HTF_VOL_EXPANSION_MULT", "1.5"))
HTF_MAX_EXTENSION_ATR = float(os.getenv("HTF_MAX_EXTENSION_ATR", "1.5"))  # anti-chase
HTF_SL_ZONE_ATR_MULT = float(os.getenv("HTF_SL_ZONE_ATR_MULT", "1.0"))
HTF_MIN_SL_ATR_MULT = float(os.getenv("HTF_MIN_SL_ATR_MULT", "1.5"))   # stop floor
HTF_MAX_SL_PCT = float(os.getenv("HTF_MAX_SL_PCT", "0.15"))            # sanity cap 15%
HTF_TRAIL_ATR_MULT = float(os.getenv("HTF_TRAIL_ATR_MULT", "3.0"))     # chandelier (4h ATR)
HTF_BREAKEVEN_R = float(os.getenv("HTF_BREAKEVEN_R", "1.5"))
HTF_SIGNAL_COOLDOWN_HOURS = float(os.getenv("HTF_SIGNAL_COOLDOWN_HOURS", "24"))
HTF_MIN_CONFIDENCE = float(os.getenv("HTF_MIN_CONFIDENCE", "0.55"))
# Retest entries ("clear retest for next leg up"): a confirmed break that was
# too extended to chase arms its level; if price pulls back to within
# PROXIMITY ATR of it and holds on a 4h close within VALID_DAYS, we enter.
HTF_RETEST_PROXIMITY_ATR = float(os.getenv("HTF_RETEST_PROXIMITY_ATR", "0.35"))
HTF_RETEST_VALID_DAYS = float(os.getenv("HTF_RETEST_VALID_DAYS", "10"))
# De-risking ahead of the next significant HTF zone (Melih's "büyük dirence
# yaklaşırken pozisyon hafiflet" pattern): reduce part of an open position
# BEFORE price reaches a major zone in its path, anticipating a stop-hunt/
# reaction there instead of waiting to see if it gets swept. Autonomous, like
# the trailing stop — it only ever reduces risk, never opens anything new, so
# it needs no Telegram approval.
HTF_DERISK_PROXIMITY_ATR = float(os.getenv("HTF_DERISK_PROXIMITY_ATR", "0.5"))
HTF_DERISK_FRACTION = float(os.getenv("HTF_DERISK_FRACTION", "0.3"))
HTF_DERISK_MIN_ZONE_TOUCHES = int(os.getenv("HTF_DERISK_MIN_ZONE_TOUCHES", "3"))

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


