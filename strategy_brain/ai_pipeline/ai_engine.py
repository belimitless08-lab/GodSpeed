"""
strategy_brain/ai_pipeline/ai_engine.py
=========================================
AI Intelligence Pipeline for Market Pulse Pro v2.
Redis-native. No file I/O. All state in Redis.

Pipeline (runs 8:10 AM after scraper finishes):
  Stage 1 — Context Engine    (1 llama call)   → ai:context
  Stage 2 — Sentiment Engine  (~60-80 calls)   → ai:sentiment:{symbol}
  Stage 3 — Filter             (no LLM calls)  → in-memory
  Stage 4 — Decision Engine   (~15-20 calls)   → in-memory + ai:trade_list
  Stage 5 — Ranking            (no LLM calls)  → ai:trade_list

Output: ai:trade_list → top 10 bullish + top 10 bearish with:
  - sentiment score
  - final decision score  
  - action (BUY CE / BUY PE / AVOID)
  - conviction (high / medium / low)
  - reason + risk note
  - technical snapshot (supertrend, RSI, choppiness from seeder)

During live market:
  - conviction_scorer reads ai:sentiment:{symbol} for Pillar 3 ICI scoring
  - Technical scoring happens live (from snapshot:{symbol})

Called by:
  scripts/morning_seeder.py → run_ai_pipeline(redis_client)
  execution/api_server.py   → _scheduler at 08:30 (fires after seeder)
  GET /api/ai/trade-list    → reads ai:trade_list from Redis
  GET /api/ai/sentiment/{symbol} → reads ai:sentiment:{symbol}
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

from core.redis_client import get_redis
from strategy_brain.ai_pipeline.ai_config import (
    OPENROUTER_API_BASE, MODELS, RATE_LIMITS, REDIS_KEYS, REDIS_TTL,
    FILTER_THRESHOLDS, RANKING_CONFIG,
    UNIVERSE, get_sector, get_company_names, format_prompt,
)
from strategy_brain.ai_pipeline.ai_scraper import (
    get_market_headlines, get_stock_news, get_stocks_with_news,
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

# Pipeline run status (in-memory, also written to Redis)
_pipeline_status: dict = {
    "active":       False,
    "last_run_at":  None,
    "last_run_duration_s": None,
    "stage_timings": {},
    "stock_counts":  {},
    "error":         None,
}


# ===========================================================================
# SECTION 1 — LLM API Client (OpenRouter)
# ===========================================================================

async def _call_llm(model_key: str, prompt: str, retries: int = 3) -> dict:
    """
    Call OpenRouter API with the specified model.
    Handles 429 rate limiting with exponential backoff.
    Returns parsed JSON dict or {"error": "..."} on failure.
    """
    import os
    model_cfg = MODELS.get(model_key)
    if not model_cfg:
        return {"error": f"Unknown model_key: {model_key}"}

    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        logger.error("[ai_engine] OPENROUTER_API_KEY not set")
        return {"error": "OPENROUTER_API_KEY not configured"}

    headers = {
        "Authorization":  f"Bearer {api_key}",
        "Content-Type":   "application/json",
        "HTTP-Referer":   "https://marketpulsepro.app",
        "X-Title":        "Market Pulse Pro",
    }

    payload: dict = {
        "model":       model_cfg["id"],
        "messages":    [{"role": "user", "content": prompt}],
        "max_tokens":  model_cfg["max_tokens"],
        "temperature": model_cfg["temperature"],
    }

    # response_format: json_object for models that support it
    model_id = model_cfg["id"].lower()
    if "gpt" in model_id or "llama" in model_id:
        payload["response_format"] = {"type": "json_object"}

    url = f"{OPENROUTER_API_BASE}/chat/completions"
    retry_delay = RATE_LIMITS["retry_delay"]

    for attempt in range(retries + 1):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(url, headers=headers, json=payload)

            if resp.status_code == 429:
                wait = retry_delay * (attempt + 1)
                logger.warning(
                    "[ai_engine] 429 rate limit (attempt %d/%d) — waiting %ds...",
                    attempt + 1, retries, wait,
                )
                await asyncio.sleep(wait)
                continue

            if resp.status_code != 200:
                return {"error": f"HTTP {resp.status_code}: {resp.text[:200]}"}

            data      = resp.json()
            raw_text  = data["choices"][0]["message"]["content"]
            return _parse_llm_json(raw_text)

        except httpx.TimeoutException:
            logger.warning("[ai_engine] Timeout on attempt %d", attempt + 1)
            await asyncio.sleep(5)
        except Exception as exc:
            logger.error("[ai_engine] LLM call error: %s", exc)
            return {"error": str(exc)}

    return {"error": f"LLM call failed after {retries} retries"}


def _parse_llm_json(raw: str) -> dict:
    """
    Parse JSON from LLM response. Strips:
    - <think>...</think> blocks (reasoning models)
    - ```json ... ``` markdown fences
    Falls back to regex extraction on parse failure.
    """
    if not raw:
        return {"error": "empty_response"}

    # Strip reasoning blocks
    text = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL)
    # Strip markdown fences
    text = re.sub(r"```(?:json)?\s*(.*?)\s*```", r"\1", text, flags=re.DOTALL)
    text = text.strip()

    try:
        result = json.loads(text)
        if isinstance(result, dict):
            return result
    except json.JSONDecodeError:
        pass

    # Regex fallback — grab outermost { }
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass

    logger.warning("[ai_engine] JSON parse failed. Raw[:200]: %s", raw[:200])
    return {"error": "parse_failed", "raw": raw[:300]}


# ===========================================================================
# SECTION 2 — Context Engine (Stage 1)
# ===========================================================================

async def run_context_engine(redis_client) -> dict:
    """
    Stage 1: Read market headlines → 1 llama call → macro context.

    Output written to Redis: ai:context
    {
        "market_bias": "bullish/bearish/neutral",
        "volatility_expectation": "low/medium/high",
        "themes": [...],
        "sector_bias": {"BANKING": "bullish", ...},
        "key_events": [...],
        "global_macro": "..."
    }
    """
    _neutral = {
        "market_bias": "neutral", "volatility_expectation": "medium",
        "themes": [], "sector_bias": {}, "key_events": [],
        "global_macro": "No macro data available.",
    }

    import os, redis as _redis_sync
    _sync_redis = _redis_sync.from_url(os.environ["REDIS_URL"])

    headlines = get_market_headlines(_sync_redis)
    if not headlines:
        logger.warning("[ai_engine] Context: no market headlines — using neutral default")
        await _cache_context(redis_client, _neutral)
        _sync_redis.close()
        return _neutral

    hl_text = "\n".join(
        f"{i+1}. {h['headline']}" for i, h in enumerate(headlines[:25])
    )

    prompt = format_prompt("context", headlines=hl_text)
    result = await _call_llm("context", prompt)

    if "error" in result:
        logger.error("[ai_engine] Context Engine failed: %s", result["error"])
        await _cache_context(redis_client, _neutral)
        _sync_redis.close()
        return _neutral

    # Ensure required keys exist
    for key, default in _neutral.items():
        result.setdefault(key, default)

    result["analyzed_at"] = datetime.now(IST).isoformat()
    await _cache_context(redis_client, result)

    logger.info(
        "[ai_engine] Context: bias=%s volatility=%s themes=%s",
        result.get("market_bias"),
        result.get("volatility_expectation"),
        result.get("themes"),
    )
    _sync_redis.close()
    return result


async def _cache_context(redis_client, context: dict) -> None:
    try:
        await redis_client.setex(
            REDIS_KEYS["context"],
            REDIS_TTL["engine"],
            json.dumps(context),
        )
    except Exception as e:
        logger.error("[ai_engine] Failed to cache context: %s", e)


# ===========================================================================
# SECTION 3 — Sentiment Engine (Stage 2)
# ===========================================================================

async def run_sentiment_engine(redis_client, context: dict) -> dict[str, dict]:
    """
    Stage 2: For each stock WITH news, call gpt-oss for sentiment.
    Stocks without news auto-score 0 (no LLM call).

    Returns {symbol: {score, confidence, driver, sector_alignment, news_quality}}
    Also writes each result to Redis: ai:sentiment:{symbol}
    """
    _no_news_default = {
        "score": 0.0, "confidence": "low",
        "driver": "No news available",
        "sector_alignment": "neutral", "news_quality": "none",
    }

    results: dict[str, dict] = {}

    market_bias     = context.get("market_bias", "neutral")
    themes          = str(context.get("themes", []))
    global_macro    = context.get("global_macro", "No data")
    sector_bias_map = context.get("sector_bias", {})

    import os, redis as _redis_sync
    _sync_redis = _redis_sync.from_url(os.environ["REDIS_URL"])

    stocks_with_news = get_stocks_with_news(_sync_redis)
    stocks_no_news   = [s for s in UNIVERSE if s not in stocks_with_news]

    # Auto-score no-news stocks (no LLM call)
    for symbol in stocks_no_news:
        results[symbol] = dict(_no_news_default)

    total = len(stocks_with_news)
    logger.info(
        "[ai_engine] Sentiment: %d stocks with news, %d auto-scored 0",
        total, len(stocks_no_news)
    )

    delay = RATE_LIMITS["delay_between_sentiment_calls"]

    for idx, symbol in enumerate(stocks_with_news):
        try:
            headlines = get_stock_news(_sync_redis, symbol)
            if not headlines:
                results[symbol] = dict(_no_news_default)
                continue

            news_text = "\n".join(
                f"- {h['headline']}" for h in headlines[:5]
            )

            sector      = get_sector(symbol)
            sector_bias = sector_bias_map.get(sector, "neutral")
            company     = ", ".join(get_company_names(symbol)[:2])

            prompt = format_prompt(
                "sentiment",
                symbol=symbol,
                company_name=company,
                sector=sector,
                stock_news=news_text,
                market_bias=market_bias,
                sector_bias=sector_bias,
                themes=themes,
                global_macro=global_macro,
            )

            call_result = await _call_llm("sentiment", prompt)

            if "error" in call_result:
                logger.warning(
                    "[ai_engine] Sentiment failed for %s: %s",
                    symbol, call_result["error"]
                )
                results[symbol] = dict(_no_news_default)
            else:
                # Clamp score to [-5, +5]
                score = max(-5.0, min(5.0, float(call_result.get("score", 0))))
                call_result["score"] = round(score, 2)
                call_result["analyzed_at"] = datetime.now(IST).isoformat()
                results[symbol] = call_result

                # Cache individual sentiment
                _cache_sentiment(redis_client, symbol, call_result)

            if (idx + 1) % 20 == 0:
                logger.info(
                    "[ai_engine] Sentiment: %d/%d processed", idx + 1, total
                )

            await asyncio.sleep(delay)

        except Exception as exc:
            logger.error("[ai_engine] Sentiment error for %s: %s", symbol, exc)
            results[symbol] = dict(_no_news_default)

    logger.info("[ai_engine] Sentiment: %d/%d done", total, total)
    _sync_redis.close()
    return results


def _cache_sentiment(redis_client, symbol: str, data: dict) -> None:
    try:
        key = REDIS_KEYS["sentiment"].format(symbol=symbol)
        redis_client.setex(key, REDIS_TTL["engine"], json.dumps(data))
    except Exception as e:
        logger.error("[ai_engine] Failed to cache sentiment for %s: %s", symbol, e)


# ===========================================================================
# SECTION 4 — Filter (Stage 3)
# ===========================================================================

def run_filter(sentiment_results: dict) -> dict:
    """
    Stage 3: Filter stocks by sentiment score + confidence + news quality.
    Returns {bullish: [...], bearish: [...], stats: {...}}
    """
    thresholds   = FILTER_THRESHOLDS
    bull_min     = thresholds["bullish_min_score"]
    bear_min     = thresholds["bearish_min_score"]
    max_filtered = thresholds["max_filtered_stocks"]

    bullish:  list[dict] = []
    bearish:  list[dict] = []
    skipped_no_news = 0
    skipped_neutral = 0

    for symbol, data in sentiment_results.items():
        score       = float(data.get("score", 0))
        news_quality = data.get("news_quality", "none")

        # Skip stocks with no news
        if news_quality == "none":
            skipped_no_news += 1
            continue

        entry = {"symbol": symbol, **data}

        if score >= bull_min:
            bullish.append(entry)
        elif score <= bear_min:
            bearish.append(entry)
        else:
            skipped_neutral += 1

    # Sort by absolute score
    bullish.sort(key=lambda x: float(x.get("score", 0)), reverse=True)
    bearish.sort(key=lambda x: float(x.get("score", 0)))

    # Cap total at max_filtered (proportional trim)
    total = len(bullish) + len(bearish)
    if total > max_filtered:
        half = max_filtered // 2
        bullish = bullish[:half]
        bearish = bearish[:half]

    logger.info(
        "[ai_engine] Filter: %d bullish, %d bearish | "
        "skipped: %d no-news, %d neutral",
        len(bullish), len(bearish), skipped_no_news, skipped_neutral
    )

    return {
        "bullish": bullish,
        "bearish": bearish,
        "stats": {
            "bullish_count":    len(bullish),
            "bearish_count":    len(bearish),
            "skipped_no_news":  skipped_no_news,
            "skipped_neutral":  skipped_neutral,
        },
    }


# ===========================================================================
# SECTION 5 — Decision Engine (Stage 4)
# ===========================================================================

async def run_decision_engine(
    filtered: dict,
    context: dict,
    sentiment_results: dict,
    redis_client,
) -> dict[str, dict]:
    """
    Stage 4: For each filtered stock, call gpt-oss (reasoning) for final decision.
    Injects REAL technical context from Redis snapshot:{symbol} (seeder data).

    Returns {symbol: {final_score, action, conviction, reason, risk_note}}
    """
    results: dict[str, dict] = {}

    market_bias     = context.get("market_bias", "neutral")
    volatility      = context.get("volatility_expectation", "medium")
    themes          = str(context.get("themes", []))
    sector_bias_map = context.get("sector_bias", {})

    candidates = filtered.get("bullish", []) + filtered.get("bearish", [])
    total = len(candidates)
    delay = RATE_LIMITS["delay_between_decision_calls"]

    logger.info("[ai_engine] Decision Engine: %d stocks to process", total)

    for idx, stock_entry in enumerate(candidates):
        symbol = stock_entry.get("symbol", "")
        if not symbol:
            continue

        try:
            sentiment   = sentiment_results.get(symbol, {})
            sector      = get_sector(symbol)
            sector_bias = sector_bias_map.get(sector, "neutral")

            # ── Pull technical context from seeder snapshot ───────────────
            tech = await _get_technical_context(symbol, redis_client)

            prompt = format_prompt(
                "decision",
                symbol=symbol,
                sector=sector,
                sentiment_score=sentiment.get("score", 0),
                sentiment_driver=sentiment.get("driver", "N/A"),
                sentiment_confidence=sentiment.get("confidence", "low"),
                news_quality=sentiment.get("news_quality", "none"),
                market_bias=market_bias,
                volatility=volatility,
                sector_bias=sector_bias,
                themes=themes,
                # Technical fields from snapshot
                supertrend_dir=tech["supertrend_dir"],
                rsi14=tech["rsi14"],
                ema9_position=tech["ema9_position"],
                ema200_position=tech["ema200_position"],
                choppiness_class=tech["choppiness_class"],
                st_band_dist=tech["st_band_dist"],
            )

            call_result = await _call_llm("decision", prompt)

            if "error" in call_result:
                logger.warning(
                    "[ai_engine] Decision failed for %s: %s",
                    symbol, call_result["error"]
                )
                results[symbol] = _default_decision(sentiment)
            else:
                final_score = max(-5.0, min(5.0, float(call_result.get("final_score", 0))))
                call_result["final_score"] = round(final_score, 2)
                call_result["analyzed_at"] = datetime.now(IST).isoformat()
                results[symbol] = call_result

            if (idx + 1) % 10 == 0:
                logger.info(
                    "[ai_engine] Decision: %d/%d processed", idx + 1, total
                )

            await asyncio.sleep(delay)

        except Exception as exc:
            logger.error("[ai_engine] Decision error for %s: %s", symbol, exc)
            results[symbol] = _default_decision(sentiment_results.get(symbol, {}))

    logger.info("[ai_engine] Decision: %d/%d done", len(results), total)
    return results


async def _get_technical_context(symbol: str, redis_client) -> dict:
    """
    Read technical snapshot from Redis (written by morning_seeder).
    Returns a dict with formatted technical strings for the decision prompt.
    Falls back to 'N/A (pre-market)' if snapshot not available.
    """
    try:
        snap = await redis_client.hgetall(f"snapshot:{symbol}")
        if not snap:
            return _no_technicals()

        def sf(k: str, default: float = 0.0) -> float:
            try:
                return float(snap.get(k, default) or default)
            except (TypeError, ValueError):
                return default

        ltp             = sf("ltp") or sf("prev_close")
        ema9            = sf("ema9")
        ema200          = sf("ema200")
        rsi14           = sf("rsi14", 50.0)
        supertrend_dir  = (snap.get("supertrend_dir") or b"BULL")
        supertrend_dir  = supertrend_dir.decode() if isinstance(supertrend_dir, bytes) else supertrend_dir
        supertrend_band = sf("supertrend_band")
        choppiness_class = (snap.get("choppiness_class") or b"NEUTRAL")
        choppiness_class = choppiness_class.decode() if isinstance(choppiness_class, bytes) else choppiness_class

        # Format EMA positions
        if ltp > 0 and ema9 > 0:
            ema9_pct = round((ltp - ema9) / ema9 * 100, 2)
            ema9_pos = f"{'above' if ltp > ema9 else 'below'} EMA9 by {abs(ema9_pct):.1f}%"
        else:
            ema9_pos = "N/A"

        if ltp > 0 and ema200 > 0:
            ema200_pct = round((ltp - ema200) / ema200 * 100, 2)
            ema200_pos = f"{'above' if ltp > ema200 else 'below'} EMA200 by {abs(ema200_pct):.1f}%"
        else:
            ema200_pos = "N/A"

        # Supertrend band distance
        if ltp > 0 and supertrend_band > 0:
            dist = abs(ltp - supertrend_band)
            dist_pct = round(dist / ltp * 100, 2)
            st_band_dist = f"{dist_pct:.1f}% from band ({supertrend_dir})"
        else:
            st_band_dist = "N/A"

        return {
            "supertrend_dir":  supertrend_dir,
            "rsi14":           f"{rsi14:.1f}",
            "ema9_position":   ema9_pos,
            "ema200_position": ema200_pos,
            "choppiness_class": choppiness_class,
            "st_band_dist":    st_band_dist,
        }

    except Exception as e:
        logger.debug("[ai_engine] Technical context failed for %s: %s", symbol, e)
        return _no_technicals()


def _no_technicals() -> dict:
    return {
        "supertrend_dir":  "N/A",
        "rsi14":           "N/A",
        "ema9_position":   "N/A (pre-market)",
        "ema200_position": "N/A (pre-market)",
        "choppiness_class": "N/A",
        "st_band_dist":    "N/A",
    }


def _default_decision(sentiment: dict) -> dict:
    score = float(sentiment.get("score", 0))
    return {
        "final_score": score,
        "action":      "AVOID",
        "conviction":  "low",
        "reason":      "Decision engine unavailable — using sentiment score only",
        "risk_note":   "No decision context",
        "analyzed_at": datetime.now(IST).isoformat(),
    }


# ===========================================================================
# SECTION 6 — Ranking (Stage 5)
# ===========================================================================

def run_ranking(
    decision_results: dict,
    filtered: dict,
    sentiment_results: dict,
    context: dict,
) -> dict:
    """
    Stage 5: Rank by final_score → top 10 bullish + top 10 bearish.
    Returns the final trade list dict.
    """
    cfg        = RANKING_CONFIG
    top_bull   = cfg["top_bullish"]
    top_bear   = cfg["top_bearish"]

    bull_syms = [e["symbol"] for e in filtered.get("bullish", [])]
    bear_syms = [e["symbol"] for e in filtered.get("bearish", [])]

    def _build_entry(symbol: str, rank: int) -> dict:
        decision    = decision_results.get(symbol, {})
        sentiment   = sentiment_results.get(symbol, {})
        sector      = get_sector(symbol)
        sent_score  = float(sentiment.get("score", 0))
        final_score = float(decision.get("final_score", sent_score))
        return {
            "rank":            rank,
            "symbol":          symbol,
            "sector":          sector,
            "sentiment_score": round(sent_score, 2),
            "final_score":     round(final_score, 2),
            "action":          decision.get("action", "AVOID"),
            "conviction":      decision.get("conviction", "low"),
            "reason":          decision.get("reason", ""),
            "risk_note":       decision.get("risk_note", ""),
            "news_driver":     sentiment.get("driver", ""),
            "confidence":      sentiment.get("confidence", "low"),
            "news_quality":    sentiment.get("news_quality", "none"),
        }

    # Sort by final_score
    bullish_ranked = sorted(
        [s for s in bull_syms if s in decision_results],
        key=lambda s: float(decision_results[s].get("final_score", 0)),
        reverse=True,
    )
    bearish_ranked = sorted(
        [s for s in bear_syms if s in decision_results],
        key=lambda s: float(decision_results[s].get("final_score", 0)),
    )

    top_bullish = [_build_entry(s, i + 1) for i, s in enumerate(bullish_ranked[:top_bull])]
    top_bearish = [_build_entry(s, i + 1) for i, s in enumerate(bearish_ranked[:top_bear])]

    trade_list = {
        "generated_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S+05:30"),
        "market_context": {
            "market_bias":            context.get("market_bias", "neutral"),
            "volatility":             context.get("volatility_expectation", "medium"),
            "themes":                 context.get("themes", []),
            "key_events":             context.get("key_events", []),
            "global_macro":           context.get("global_macro", ""),
        },
        "top_bullish":          top_bullish,
        "top_bearish":          top_bearish,
        "total_universe":       len(UNIVERSE),
        "total_with_news":      len([s for s in UNIVERSE if sentiment_results.get(s, {}).get("news_quality") != "none"]),
        "total_filtered":       len(bull_syms) + len(bear_syms),
        "total_decisions":      len(decision_results),
    }

    return trade_list


# ===========================================================================
# SECTION 7 — Full Pipeline Orchestrator
# ===========================================================================

async def run_ai_pipeline() -> dict:
    """
    Run the complete AI pipeline end-to-end.
    Called by morning_seeder.py at ~8:10 AM (after scraping completes).

    Returns the final trade list dict.
    """
    global _pipeline_status

    redis = await get_redis()
    _pipeline_status["active"] = True
    _pipeline_status["error"]  = None
    pipeline_start = time.time()
    stage_timings:  dict = {}

    logger.info("[ai_engine] ═══ AI Pipeline Starting ═══")

    # ── Stage 1: Context Engine ───────────────────────────────────────────
    t0 = time.time()
    logger.info("[ai_engine] Stage 1: Context Engine (1 llama call)...")
    try:
        context = await run_context_engine(redis)
    except Exception as exc:
        logger.error("[ai_engine] Stage 1 crashed: %s", exc)
        context = {
            "market_bias": "neutral", "volatility_expectation": "medium",
            "themes": [], "sector_bias": {}, "key_events": [],
            "global_macro": "Context engine unavailable.",
        }
    stage_timings["context"] = round(time.time() - t0, 1)
    logger.info(
        "[ai_engine] Stage 1 done (%.1fs) — bias=%s",
        stage_timings["context"], context.get("market_bias")
    )

    # ── Stage 2: Sentiment Engine ─────────────────────────────────────────
    t0 = time.time()
    import os, redis as _redis_sync
    _sync_r = _redis_sync.from_url(os.environ["REDIS_URL"])
    stocks_with_news = get_stocks_with_news(_sync_r)
    _sync_r.close()
    logger.info(
        "[ai_engine] Stage 2: Sentiment Engine (%d stocks with news)...",
        len(stocks_with_news)
    )
    try:
        sentiment_results = await run_sentiment_engine(redis, context)
    except Exception as exc:
        logger.error("[ai_engine] Stage 2 crashed: %s", exc)
        sentiment_results = {}
    stage_timings["sentiment"] = round(time.time() - t0, 1)
    s_min = int(stage_timings["sentiment"] // 60)
    s_sec = int(stage_timings["sentiment"] % 60)
    logger.info(
        "[ai_engine] Stage 2 done (%dm%ds) — %d stocks scored",
        s_min, s_sec, len(sentiment_results)
    )

    # ── Stage 3: Filter ───────────────────────────────────────────────────
    t0 = time.time()
    try:
        filtered = run_filter(sentiment_results)
    except Exception as exc:
        logger.error("[ai_engine] Stage 3 crashed: %s", exc)
        filtered = {"bullish": [], "bearish": [], "stats": {}}
    stage_timings["filter"] = round(time.time() - t0, 1)
    fstats = filtered.get("stats", {})
    logger.info(
        "[ai_engine] Stage 3 done — %d bullish, %d bearish passed filter",
        fstats.get("bullish_count", 0), fstats.get("bearish_count", 0)
    )

    # ── Stage 4: Decision Engine ──────────────────────────────────────────
    t0 = time.time()
    total_filtered = fstats.get("bullish_count", 0) + fstats.get("bearish_count", 0)
    logger.info(
        "[ai_engine] Stage 4: Decision Engine (%d stocks)...", total_filtered
    )
    try:
        decision_results = await run_decision_engine(
            filtered, context, sentiment_results, redis
        )
    except Exception as exc:
        logger.error("[ai_engine] Stage 4 crashed: %s", exc)
        decision_results = {}
    stage_timings["decision"] = round(time.time() - t0, 1)
    d_min = int(stage_timings["decision"] // 60)
    d_sec = int(stage_timings["decision"] % 60)
    logger.info(
        "[ai_engine] Stage 4 done (%dm%ds) — %d decisions made",
        d_min, d_sec, len(decision_results)
    )

    # ── Stage 5: Ranking ──────────────────────────────────────────────────
    t0 = time.time()
    logger.info("[ai_engine] Stage 5: Ranking — Top 10 bull + Top 10 bear...")
    try:
        trade_list = run_ranking(decision_results, filtered, sentiment_results, context)
    except Exception as exc:
        logger.error("[ai_engine] Stage 5 crashed: %s", exc)
        trade_list = {}
    stage_timings["ranking"] = round(time.time() - t0, 1)

    # ── Write final trade list to Redis ───────────────────────────────────
    try:
        await redis.setex(
            REDIS_KEYS["trade_list"],
            REDIS_TTL["engine"],
            json.dumps(trade_list),
        )
        logger.info("[ai_engine] Trade list written to Redis (ai:trade_list)")
    except Exception as e:
        logger.error("[ai_engine] Failed to write trade list to Redis: %s", e)

    # ── Final status ──────────────────────────────────────────────────────
    total_duration = round(time.time() - pipeline_start)
    total_str = f"{total_duration // 60}m {total_duration % 60}s"

    _pipeline_status.update({
        "active":              False,
        "last_run_at":         datetime.now(IST).isoformat(),
        "last_run_duration_s": total_duration,
        "stage_timings":       stage_timings,
        "stock_counts": {
            "universe":       len(UNIVERSE),
            "with_news":      len(stocks_with_news),
            "sentiment_scored": len([s for s in sentiment_results if sentiment_results[s].get("news_quality") != "none"]),
            "filtered":       total_filtered,
            "decisions":      len(decision_results),
            "top_bullish":    len(trade_list.get("top_bullish", [])),
            "top_bearish":    len(trade_list.get("top_bearish", [])),
        },
    })

    try:
        await redis.setex(
            REDIS_KEYS["pipeline_status"],
            REDIS_TTL["pipeline"],
            json.dumps(_pipeline_status),
        )
    except Exception:
        pass

    logger.info(
        "[ai_engine] ═══ Pipeline complete in %s ═══ | "
        "bull=%d bear=%d",
        total_str,
        len(trade_list.get("top_bullish", [])),
        len(trade_list.get("top_bearish", [])),
    )

    return trade_list


# ===========================================================================
# SECTION 8 — Live Market: Conviction Scorer (Pillar 3)
# ===========================================================================

async def get_ai_sentiment_score(symbol: str) -> Optional[float]:
    """
    Read ai:sentiment:{symbol} from Redis for ICI Pillar 3 scoring.
    Called by conviction_scorer.py during live market.

    Returns:
        float: sentiment score (-5 to +5)
        None: if no data available (treat as neutral in scorer)
    """
    try:
        redis = await get_redis()
        key = REDIS_KEYS["sentiment"].format(symbol=symbol)
        raw = await redis.get(key)
        if not raw:
            return None
        data = json.loads(raw if isinstance(raw, str) else raw.decode())
        return float(data.get("score", 0))
    except Exception:
        return None


# ===========================================================================
# SECTION 9 — API Helpers (read-only, called by api_server.py endpoints)
# ===========================================================================

async def get_trade_list() -> Optional[dict]:
    """Read ai:trade_list from Redis."""
    try:
        redis = await get_redis()
        raw = await redis.get(REDIS_KEYS["trade_list"])
        if not raw:
            return None
        return json.loads(raw if isinstance(raw, str) else raw.decode())
    except Exception:
        return None


async def get_context() -> Optional[dict]:
    """Read ai:context from Redis."""
    try:
        redis = await get_redis()
        raw = await redis.get(REDIS_KEYS["context"])
        if not raw:
            return None
        return json.loads(raw if isinstance(raw, str) else raw.decode())
    except Exception:
        return None


def get_pipeline_status() -> dict:
    """Return in-memory pipeline status."""
    return dict(_pipeline_status)
