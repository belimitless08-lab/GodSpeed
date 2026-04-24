"""
strategy_brain/ai_pipeline/groq_sentiment.py
=============================================
Groq LLM sentiment analysis — pre-market and per-symbol alignment.

Models used
-----------
    llama-3.3-70b-versatile  (Groq hosted)

System prompts explicitly demand ONLY valid JSON — no markdown,
no backticks, no preamble.  All responses are validated.

Redis keys written
------------------
    ai:premarket              → JSON (market-level sentiment)
    ai:alignment:{symbol}     → JSON (symbol-level alignment)

Usage
-----
    from strategy_brain.ai_pipeline.groq_sentiment import (
        analyze_premarket_sentiment,
        analyze_stock_alignment,
    )
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from groq import AsyncGroq, RateLimitError, APIStatusError

from core.config import cfg
from core.redis_client import get_redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_IST = timezone(timedelta(hours=5, minutes=30))

_MODEL             = "llama-3.3-70b-versatile"
_MAX_TOKENS        = 1024
_TEMPERATURE       = 0.1       # low temperature for deterministic JSON output
_PREMARKET_TTL     = 3600      # 1 hour cache
_ALIGNMENT_TTL     = 1200      # 20 minutes cache

_PREMARKET_SYSTEM_PROMPT = """\
You are a financial market analyst assistant.
Return ONLY valid JSON, no markdown, no backticks, no explanation, no preamble.
Strictly follow the exact schema provided. Do not add extra keys.
"""

_ALIGNMENT_SYSTEM_PROMPT = """\
You are a financial market analyst for NSE Indian equities.
Return ONLY valid JSON, no markdown, no backticks, no explanation, no preamble.
Strictly follow the exact schema provided. Do not add extra keys.
"""


# ---------------------------------------------------------------------------
# Groq client factory
# ---------------------------------------------------------------------------

def _get_client() -> AsyncGroq:
    return AsyncGroq(api_key=cfg.GROQ_API_KEY)


# ---------------------------------------------------------------------------
# JSON response validator
# ---------------------------------------------------------------------------

def _strip_and_parse(raw: str) -> Optional[dict]:
    """
    Strip any accidental markdown fences and parse JSON.
    Returns None on failure.
    """
    text = raw.strip()
    # Remove ```json ... ``` or ``` ... ``` if Groq disobeys
    if text.startswith("```"):
        lines = text.splitlines()
        # Drop first line (```json or ```) and last line (```)
        inner = [ln for ln in lines[1:] if ln.strip() != "```"]
        text = "\n".join(inner).strip()

    try:
        result = json.loads(text)
        if not isinstance(result, dict):
            raise ValueError("Top-level JSON must be an object")
        return result
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning("[groq_sentiment] JSON parse failure: %s | raw=%r", exc, raw[:200])
        return None


# ---------------------------------------------------------------------------
# Public API — pre-market
# ---------------------------------------------------------------------------

async def analyze_premarket_sentiment(headlines: list[dict]) -> dict:
    """
    Analyze a list of headlines for broad market sentiment.

    Parameters
    ----------
    headlines : list from news_scraper.scrape_premarket_news()
        Each element: {headline, source, url, scraped_at}

    Returns
    -------
    {
        "market_sentiment": "BULLISH" | "BEARISH" | "NEUTRAL",
        "sentiment_score":  float (-1.0 to 1.0),
        "top_positive": [{"symbol": str, "reason": str}],  # top 10
        "top_negative": [{"symbol": str, "reason": str}],  # top 10
        "macro_context": str,   # 1 sentence on global macro
        "key_themes":    [str], # max 3 themes
        "analyzed_at":   str,   # ISO timestamp
    }

    Also writes to Redis: ai:premarket
    """
    if not headlines:
        logger.warning("[groq_sentiment] No headlines — skipping pre-market analysis")
        return _empty_premarket_result()

    # Build prompt
    headline_block = "\n".join(
        f"- [{h['source']}] {h['headline']}" for h in headlines[:25]
    )

    user_prompt = f"""\
Analyze these NSE/Indian market headlines and return JSON matching this exact schema:
{{
  "market_sentiment": "BULLISH",
  "sentiment_score": 0.4,
  "top_positive": [{{"symbol": "RELIANCE", "reason": "strong Q4 results"}}],
  "top_negative": [{{"symbol": "INFY", "reason": "guidance cut"}}],
  "macro_context": "Global markets steady; US Fed holds rates.",
  "key_themes": ["earnings season", "rate hold", "FII inflows"]
}}

Headlines:
{headline_block}
"""

    client = _get_client()

    for attempt in range(3):
        try:
            response = await client.chat.completions.create(
                model=_MODEL,
                max_tokens=_MAX_TOKENS,
                temperature=_TEMPERATURE,
                messages=[
                    {"role": "system", "content": _PREMARKET_SYSTEM_PROMPT},
                    {"role": "user",   "content": user_prompt},
                ],
            )
            raw = response.choices[0].message.content or ""
            parsed = _strip_and_parse(raw)

            if parsed is None:
                logger.warning("[groq_sentiment] premarket attempt %d — bad JSON", attempt + 1)
                continue

            # Validate required keys
            required = {"market_sentiment", "sentiment_score", "top_positive",
                        "top_negative", "macro_context", "key_themes"}
            if not required.issubset(parsed.keys()):
                missing = required - parsed.keys()
                logger.warning("[groq_sentiment] premarket missing keys: %s", missing)
                continue

            parsed["analyzed_at"] = datetime.now(_IST).isoformat()

            # Write to Redis.
            # Keep both keys during migration so older dashboards/endpoints
            # that still read ai:premarket:summary continue to work.
            redis = await get_redis()
            await redis.set("ai:premarket", json.dumps(parsed), ex=_PREMARKET_TTL)
            await redis.set("ai:premarket:summary", json.dumps(parsed), ex=_PREMARKET_TTL)

            logger.info(
                "[groq_sentiment] premarket sentiment=%s score=%.2f themes=%s",
                parsed.get("market_sentiment"),
                float(parsed.get("sentiment_score", 0)),
                parsed.get("key_themes"),
            )
            return parsed

        except RateLimitError as exc:
            logger.warning("[groq_sentiment] rate limit on attempt %d: %s", attempt + 1, exc)
            if attempt < 2:
                import asyncio
                await asyncio.sleep(2 ** (attempt + 1))
            continue
        except APIStatusError as exc:
            logger.error("[groq_sentiment] Groq API error: %s", exc)
            break
        except Exception as exc:  # noqa: BLE001
            logger.error("[groq_sentiment] Unexpected error: %s", exc, exc_info=True)
            break

    logger.error("[groq_sentiment] premarket analysis failed after retries")
    return _empty_premarket_result()


# ---------------------------------------------------------------------------
# Public API — per-symbol alignment
# ---------------------------------------------------------------------------

async def analyze_stock_alignment(
    symbol: str,
    news: list[dict],
    snapshot: dict,
) -> dict:
    """
    Assess news-technical alignment for a specific stock.

    Parameters
    ----------
    symbol   : NSE symbol
    news     : list from news_scraper.scrape_stock_news()
    snapshot : raw snapshot dict (hgetall result, values are strings)

    Returns
    -------
    {
        "symbol":               str,
        "news_sentiment":       "BULLISH" | "BEARISH" | "NEUTRAL",
        "technical_alignment":  "CONFIRMING" | "CONFLICTING" | "NEUTRAL",
        "confidence":           float (0–1),
        "summary":              str (1 sentence),
        "analyzed_at":          str (ISO),
    }

    Also writes to Redis: ai:alignment:{symbol}
    """
    if not news:
        logger.debug("[groq_sentiment] No news for %s — returning NEUTRAL alignment", symbol)
        result = _neutral_alignment(symbol)
        await _cache_alignment(symbol, result)
        return result

    # Build concise technical context
    def _f(k: str) -> str:
        v = snapshot.get(k, "")
        try:
            return f"{float(v):.2f}" if v else "N/A"
        except (ValueError, TypeError):
            return str(v) or "N/A"

    technical_ctx = (
        f"LTP={_f('ltp')} | VWAP={_f('vwap')} | RSI={_f('rsi14')} | "
        f"Supertrend={snapshot.get('supertrend_dir', 'N/A')} | "
        f"RVOL={_f('rvol')} | Choppiness={_f('choppiness14')}"
    )

    news_block = "\n".join(f"- {n['headline']}" for n in news[:5])

    user_prompt = f"""\
Symbol: {symbol}
Technical snapshot: {technical_ctx}

Recent news:
{news_block}

Return JSON matching this exact schema (no extra keys):
{{
  "symbol": "{symbol}",
  "news_sentiment": "BULLISH",
  "technical_alignment": "CONFIRMING",
  "confidence": 0.75,
  "summary": "Strong Q4 results align with bullish technical momentum."
}}

technical_alignment values:
- CONFIRMING  = news and technicals agree
- CONFLICTING = news and technicals disagree
- NEUTRAL     = insufficient signal from news

confidence: 0.0–1.0 (how confident you are in this assessment)
"""

    client = _get_client()

    for attempt in range(3):
        try:
            response = await client.chat.completions.create(
                model=_MODEL,
                max_tokens=256,
                temperature=_TEMPERATURE,
                messages=[
                    {"role": "system", "content": _ALIGNMENT_SYSTEM_PROMPT},
                    {"role": "user",   "content": user_prompt},
                ],
            )
            raw = response.choices[0].message.content or ""
            parsed = _strip_and_parse(raw)

            if parsed is None:
                logger.warning("[groq_sentiment] alignment %s attempt %d — bad JSON",
                               symbol, attempt + 1)
                continue

            required = {"symbol", "news_sentiment", "technical_alignment",
                        "confidence", "summary"}
            if not required.issubset(parsed.keys()):
                continue

            # Sanitise
            parsed["symbol"]       = symbol
            parsed["confidence"]   = max(0.0, min(1.0, float(parsed.get("confidence", 0))))
            parsed["analyzed_at"]  = datetime.now(_IST).isoformat()

            await _cache_alignment(symbol, parsed)

            logger.info(
                "[groq_sentiment] %s news=%s alignment=%s conf=%.2f",
                symbol,
                parsed.get("news_sentiment"),
                parsed.get("technical_alignment"),
                parsed.get("confidence", 0),
            )
            return parsed

        except RateLimitError as exc:
            logger.warning("[groq_sentiment] rate limit on alignment %s attempt %d: %s",
                           symbol, attempt + 1, exc)
            if attempt < 2:
                import asyncio
                await asyncio.sleep(2 ** (attempt + 1))
            continue
        except APIStatusError as exc:
            logger.error("[groq_sentiment] API error for %s: %s", symbol, exc)
            break
        except Exception as exc:  # noqa: BLE001
            logger.error("[groq_sentiment] Unexpected error for %s: %s", symbol, exc, exc_info=True)
            break

    logger.warning("[groq_sentiment] alignment analysis failed for %s — using NEUTRAL", symbol)
    result = _neutral_alignment(symbol)
    await _cache_alignment(symbol, result)
    return result


# ---------------------------------------------------------------------------
# Redis cache helper
# ---------------------------------------------------------------------------

async def _cache_alignment(symbol: str, result: dict) -> None:
    try:
        redis = await get_redis()
        await redis.set(f"ai:alignment:{symbol}", json.dumps(result), ex=_ALIGNMENT_TTL)
    except Exception as exc:  # noqa: BLE001
        logger.error("[groq_sentiment] Failed to cache alignment for %s: %s", symbol, exc)


async def get_cached_alignment(symbol: str) -> Optional[dict]:
    """Read cached alignment from Redis. Returns None if missing/expired."""
    try:
        redis = await get_redis()
        raw = await redis.get(f"ai:alignment:{symbol}")
        if raw:
            return json.loads(raw)
    except Exception:  # noqa: BLE001
        pass
    return None


# ---------------------------------------------------------------------------
# Fallback constructors
# ---------------------------------------------------------------------------

def _empty_premarket_result() -> dict:
    return {
        "market_sentiment": "NEUTRAL",
        "sentiment_score":   0.0,
        "top_positive":      [],
        "top_negative":      [],
        "macro_context":     "Data unavailable.",
        "key_themes":        [],
        "analyzed_at":       datetime.now(_IST).isoformat(),
    }


def _neutral_alignment(symbol: str) -> dict:
    return {
        "symbol":              symbol,
        "news_sentiment":      "NEUTRAL",
        "technical_alignment": "NEUTRAL",
        "confidence":          0.0,
        "summary":             "Insufficient news data for alignment assessment.",
        "analyzed_at":         datetime.now(_IST).isoformat(),
    }
