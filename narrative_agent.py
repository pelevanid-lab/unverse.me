"""
Narrative agent — the "which sector is hot right now" oracle.

Uses Gemini as an analyst (the same instinct as asking Grok on X) to name the
currently-running crypto narratives and their leading Binance-listed tokens.
The output is NOT a trade signal and NOT a hard filter: it produces a per-symbol
BONUS that the deterministic discovery layer adds to a coin's own quantitative
score. Our candidates are found by our own math; belonging to a hot sector just
earns extra points.

IMPORTANT HONESTY NOTE
----------------------
By default the Gemini API answers from its training data, which is stale for a
"what is trending *today*" question. For this to reflect the real current
market we try to attach Google Search grounding. If grounding is unavailable
the agent still runs but logs a clear warning and marks the result as
"ungrounded" so you know not to over-trust it.
"""
from __future__ import annotations

import asyncio
import logging
import os
import orjson
import redis.asyncio as redis
import google.generativeai as genai
from pydantic import BaseModel, Field

import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("NarrativeAgent")

if not config.GEMINI_API_KEY:
    logger.warning("GEMINI_API_KEY is not set; narrative agent will not run.")
genai.configure(api_key=config.GEMINI_API_KEY)

# Redis key the discovery/decision layers read for the bonus map.
NARRATIVE_MAP_KEY = "narrative:bonus_map"
# Max bonus a coin can earn purely from being in the hottest sector.
SECTOR_BONUS_MAX = float(os.getenv("SECTOR_BONUS_MAX", "0.25"))
# How often to refresh the narrative view (seconds). Default 4h.
NARRATIVE_INTERVAL = float(os.getenv("NARRATIVE_INTERVAL_SEC", "14400"))


class Sector(BaseModel):
    sector: str = Field(description="Short narrative/sector name, e.g. 'AI agents', 'RWA'")
    heat: float = Field(description="How hot right now, 0.0 (cold) to 1.0 (dominant)")
    tokens: list[str] = Field(description="Leading Binance USDT-perp base symbols, e.g. ['ONDO','FET']")


class NarrativeView(BaseModel):
    sectors: list[Sector]


class NarrativeAgent:
    def __init__(self):
        self.redis_client = None
        self.grounded = False
        self.model = self._build_model()

    def _build_model(self):
        """Build a Gemini model, attaching Google Search grounding if we can."""
        gen_cfg = genai.GenerationConfig(
            response_mime_type="application/json",
            response_schema=NarrativeView,
            temperature=0.3,
        )
        # Try to enable live search grounding. The exact tool name has changed
        # across Gemini versions, so we attempt the modern one and fall back.
        for tool in ("google_search", "google_search_retrieval"):
            try:
                model = genai.GenerativeModel(
                    model_name="gemini-2.5-flash",
                    generation_config=gen_cfg,
                    tools=tool,
                )
                self.grounded = True
                logger.info(f"Narrative model using live grounding via '{tool}'.")
                return model
            except Exception as e:
                logger.debug(f"Grounding tool '{tool}' unavailable: {e}")
        # Fallback: no grounding. Structured output can't be combined with the
        # search tool on every version, so this path answers from training data.
        logger.warning("Google Search grounding NOT available. Narrative results "
                       "will come from Gemini's training data and may be STALE. "
                       "Treat the sector bonus as a weak prior, not live truth.")
        self.grounded = False
        return genai.GenerativeModel(model_name="gemini-2.5-flash", generation_config=gen_cfg)

    async def initialize(self):
        self.redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)

    async def cleanup(self):
        if self.redis_client:
            await self.redis_client.aclose()

    def _build_bonus_map(self, view: NarrativeView) -> dict:
        """Turn sectors into a {SYMBOL: bonus} map (symbol = e.g. ONDOUSDT)."""
        bonus_map = {}
        for sec in view.sectors:
            bonus = round(max(0.0, min(sec.heat, 1.0)) * SECTOR_BONUS_MAX, 4)
            for tok in sec.tokens:
                sym = tok.strip().upper()
                if not sym:
                    continue
                if not sym.endswith("USDT"):
                    sym = sym + "USDT"
                # Keep the highest bonus if a token appears in two sectors.
                bonus_map[sym] = max(bonus_map.get(sym, 0.0), bonus)
        return bonus_map

    async def run_cycle(self):
        prompt = (
            "You are a crypto market analyst. Identify the 3 to 5 HOTTEST crypto "
            "narratives / sectors that capital is rotating into RIGHT NOW "
            "(e.g. AI agents, RWA, DePIN, memecoins, L2s, etc.). For each sector "
            "give a heat score 0..1 and the leading Binance-listed tokens (base "
            "symbols only, e.g. ONDO, FET). Prefer liquid perpetual-futures tokens. "
            "Return the exact JSON schema requested."
        )
        try:
            response = await self.model.generate_content_async(prompt)
            view = NarrativeView(**orjson.loads(response.text))
            bonus_map = self._build_bonus_map(view)

            payload = {
                "grounded": self.grounded,
                "bonus_map": bonus_map,
                "sectors": [s.model_dump() for s in view.sectors],
            }
            # Publish for consumers + persist last value for late subscribers.
            await self.redis_client.set(NARRATIVE_MAP_KEY, orjson.dumps(payload))
            await self.redis_client.publish("narrative:sectors", orjson.dumps(payload))

            top = ", ".join(f"{s.sector}({s.heat:.1f})" for s in view.sectors)
            logger.info(f"Narrative view [{'LIVE' if self.grounded else 'STALE'}]: {top}")
            logger.info(f"Bonus map: {bonus_map}")

            # Mirror to the dashboard (and it's editable there — see bot_config).
            config.send_log_to_dashboard(
                "NarrativeAgent",
                "GROUNDED" if self.grounded else "UNGROUNDED",
                f"Hot sectors: {top}"
            )
            if config.supabase:
                def _persist():
                    try:
                        config.supabase.table("narrative_trends").upsert({
                            "id": "00000000-0000-0000-0000-000000000002",
                            "grounded": self.grounded,
                            "sectors": payload["sectors"],
                            "bonus_map": bonus_map,
                            "updated_at": "now()",
                        }).execute()
                    except Exception as e:
                        logger.error(f"Failed to persist narrative_trends: {e}")
                asyncio.create_task(asyncio.to_thread(_persist))

        except Exception as e:
            logger.error(f"Narrative cycle failed: {e}")

    async def run(self):
        await self.initialize()
        try:
            while True:
                await self.run_cycle()
                logger.info(f"Narrative agent sleeping {NARRATIVE_INTERVAL}s.")
                await asyncio.sleep(NARRATIVE_INTERVAL)
        finally:
            await self.cleanup()


if __name__ == "__main__":
    agent = NarrativeAgent()
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        logger.info("Narrative Agent gracefully shut down.")
