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


def _extract_json(text: str) -> dict:
    """Pull a JSON object out of a model reply that may wrap it in prose/fences."""
    t = (text or "").strip()
    if t.startswith("```"):
        t = t.split("```")[1] if "```" in t[3:] else t.strip("`")
        if t.lstrip().startswith("json"):
            t = t.lstrip()[4:]
    start, end = t.find("{"), t.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"No JSON object found in model reply: {text[:200]}")
    return orjson.loads(t[start:end + 1])


class NarrativeAgent:
    def __init__(self):
        self.redis_client = None
        self.grounded = False

    def _attempts(self):
        """Ordered generation strategies: grounded first, ungrounded last.

        IMPORTANT: Gemini does not allow the Google Search tool together with
        structured JSON output (response_schema). So the grounded attempts ask
        for JSON *in the prompt* and we parse it ourselves; only the final,
        ungrounded fallback can use the strict schema.
        Failures are only visible at generate() time, not construction, so each
        attempt is fully exercised inside run_cycle.
        """
        return [
            ("google_search", [{"google_search": {}}], True),
            ("google_search_retrieval", [{"google_search_retrieval": {}}], True),
            (None, None, False),  # ungrounded fallback
        ]

    def _build_model(self, tools, grounded: bool):
        if grounded:
            # Free-form text + our own JSON parsing (schema is not allowed here).
            return genai.GenerativeModel(
                model_name="gemini-2.5-flash",
                generation_config=genai.GenerationConfig(temperature=0.3),
                tools=tools,
            )
        return genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json",
                response_schema=NarrativeView,
                temperature=0.3,
            ),
        )

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
        base_prompt = (
            "You are a crypto market analyst. Identify the 3 to 5 HOTTEST crypto "
            "narratives / sectors that capital is rotating into RIGHT NOW "
            "(e.g. AI agents, RWA, DePIN, memecoins, L2s, etc.). For each sector "
            "give a heat score 0..1 and the leading Binance-listed tokens (base "
            "symbols only, e.g. ONDO, FET). Prefer liquid perpetual-futures tokens."
        )
        json_instruction = (
            "\n\nReply with ONLY a JSON object, no prose, no markdown fences, "
            'in exactly this shape: {"sectors":[{"sector":"AI agents","heat":0.9,'
            '"tokens":["FET","TAO"]}]}'
        )

        view = None
        # Try grounded first; fall back through to ungrounded so a tool-name
        # change on Google's side can never take the whole agent down.
        for label, tools, grounded in self._attempts():
            try:
                model = self._build_model(tools, grounded)
                prompt = base_prompt + (json_instruction if grounded else
                                        "\nReturn the exact JSON schema requested.")
                response = await model.generate_content_async(prompt)
                view = NarrativeView(**_extract_json(response.text)) if grounded \
                    else NarrativeView(**orjson.loads(response.text))
                self.grounded = grounded
                logger.info(f"Narrative generated via "
                            f"{'grounded:' + label if grounded else 'UNGROUNDED fallback'}.")
                break
            except Exception as e:
                logger.warning(f"Narrative attempt "
                               f"'{label or 'ungrounded'}' failed: {e}")

        if view is None:
            logger.error("All narrative attempts failed; no bonus map published.")
            return False

        if not self.grounded:
            logger.warning("Narrative is UNGROUNDED (from training data) and may be "
                           "STALE. Treat the sector bonus as a weak prior.")

        try:
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

            # Mirror to the dashboard log feed.
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

            return True

        except Exception as e:
            logger.error(f"Narrative publish failed: {e}")
            return False

    async def run(self):
        await self.initialize()
        try:
            while True:
                ok = await self.run_cycle()
                # Don't sit idle for 4h after a failure — retry soon instead.
                delay = NARRATIVE_INTERVAL if ok else 300
                logger.info(f"Narrative agent sleeping {delay}s "
                            f"({'ok' if ok else 'retry after failure'}).")
                await asyncio.sleep(delay)
        finally:
            await self.cleanup()


if __name__ == "__main__":
    agent = NarrativeAgent()
    try:
        asyncio.run(agent.run())
    except KeyboardInterrupt:
        logger.info("Narrative Agent gracefully shut down.")
