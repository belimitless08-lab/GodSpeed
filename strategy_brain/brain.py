"""
strategy_brain/brain.py
=========================
Strategy Brain — Node 3 main entry point.

Event-driven decision engine that:
  1. Subscribes to Redis candles:1m and candles:5m pub/sub channels.
  2. On every 1m candle → scans signals + checks retest watchlist.
  3. On every 5m candle → runs ICI scorer on pending signals + market breadth.
  4. Publishes execution payloads to "trade_execution" Redis channel.
  5. Runs pre-market news scrape at 7:30 AM and stock news every 20 min.
  6. Runs market breadth every 60 seconds independently.

No raw indicator math here — all math is done by candle_builder.

Architecture
------------
  candles:1m pub/sub → on_1m_candle()
    → scan_all_signals()
    → check_retest_triggers()
    → check_macro_gates()
    → queue pending scores to Redis set: brain:pending_score

  candles:5m pub/sub → on_5m_candle()
    → process brain:pending_score set
    → calculate_ici_score()
    → if score actionable: evaluate_options + publish trade_execution
    → compute_market_breadth()

Run
---
    python -m strategy_brain.brain
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.config import cfg, validate
from core.redis_client import get_redis, close as close_redis
from core.universe_builder import get_symbols
from execution.order_manager import (
    run_execution_listener,
    monitor_stop_losses_event_driven,
    monitor_trigger_orders,
)

from strategy_brain.macro_gatekeeper  import check_macro_gates
from strategy_brain.conviction_scorer  import score_signal
from strategy_brain.signal_engines     import scan_all_signals
from strategy_brain.retest_watchlist   import (
    add_to_retest, check_retest_triggers, get_watchlist_snapshot
)
from strategy_brain.options_tracker    import (
    evaluate_options_for_signal, compute_tradability_badge
)
from strategy_brain.market_breadth     import compute_market_breadth
try:
    from strategy_brain.ai_pipeline.news_scraper import (
        scrape_premarket_news, scrape_stock_news
    )
    from strategy_brain.ai_pipeline.groq_sentiment import (
        analyze_premarket_sentiment, analyze_stock_alignment
    )
except ModuleNotFoundError:
    async def scrape_premarket_news(): return []
    async def scrape_stock_news(symbol): return []
    async def analyze_premarket_sentiment(h): return {}
    async def analyze_stock_alignment(s, n, snap): return {}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_IST = timezone(timedelta(hours=5, minutes=30))

_MARKET_OPEN  = (9,  15)   # HH, MM
_MARKET_CLOSE = (15, 30)   # HH, MM

_PREMARKET_NEWS_HOUR   = 7
_PREMARKET_NEWS_MINUTE = 30

_STOCK_NEWS_INTERVAL_MIN = 20     # minutes between per-symbol news scrapes
_BREADTH_INTERVAL_SEC    = 60     # seconds between breadth recomputes
_SCORE_EXPIRY_MINUTES    = 10     # ICI score cache duration

_CH_1M  = "candles:1m"
_CH_5M  = "candles:5m"
_CH_15M = "candles:15m"

_PENDING_SCORE_KEY = "brain:pending_score"

# Limits how many on_1m_candle / on_5m_candle coroutines run concurrently.
# 213 symbols all close at the same second — without this, they stampede Redis.
# 30 concurrent = ~30 Redis connections in flight at once, well within pool.
_SCAN_SEMAPHORE = asyncio.Semaphore(30)
background_tasks: set[asyncio.Task] = set()
_execution_engine_started = False


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
_last_stock_news_scrape: dict[str, float] = {}   # symbol → monotonic timestamp
_premarket_news_done = False
_vix_cache: Optional[float] = None
_vix_cache_ts: float = 0.0
_VIX_CACHE_TTL_SEC = 60


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ist() -> datetime:
    return datetime.now(_IST)


def _current_time_ist() -> str:
    return _now_ist().strftime("%H:%M")


def _within_market_hours() -> bool:
    now = _now_ist()
    h, m = now.hour, now.minute
    open_mins  = _MARKET_OPEN[0]  * 60 + _MARKET_OPEN[1]
    close_mins = _MARKET_CLOSE[0] * 60 + _MARKET_CLOSE[1]
    now_mins   = h * 60 + m
    return open_mins <= now_mins < close_mins


async def _get_vix() -> float:
    """Read VIX from snapshot:INDIA_VIX with 60-second in-process cache."""
    global _vix_cache, _vix_cache_ts
    now = time.monotonic()
    if _vix_cache is not None and (now - _vix_cache_ts) < _VIX_CACHE_TTL_SEC:
        return _vix_cache

    try:
        redis = await get_redis()
        raw   = await redis.hgetall("snapshot:INDIA_VIX")
        vix   = float(raw.get("ltp", 15.0)) if raw else 15.0
    except Exception:
        vix = 15.0

    _vix_cache    = vix
    _vix_cache_ts = now
    return vix


async def _load_snapshot(symbol: str) -> dict:
    redis = await get_redis()
    return await redis.hgetall(f"snapshot:{symbol}") or {}


async def _is_score_expired(item: dict) -> bool:
    """Check if a queued pending score has exceeded its expiry."""
    try:
        expires_at_str = item.get("expires_at", "")
        if not expires_at_str:
            return True
        expires_at = datetime.fromisoformat(expires_at_str)
        return _now_ist() > expires_at
    except (ValueError, TypeError):
        return True


# ---------------------------------------------------------------------------
# Stock news scraper (rate-limited, per-symbol)
# ---------------------------------------------------------------------------

async def _maybe_scrape_stock_news(symbol: str) -> list[dict]:
    """Scrape stock news if it hasn't been done in the last 20 minutes."""
    now = time.monotonic()
    last = _last_stock_news_scrape.get(symbol, 0.0)
    if (now - last) < _STOCK_NEWS_INTERVAL_MIN * 60:
        # Read from cache
        redis = await get_redis()
        raw = await redis.get(f"news:stock:{symbol}")
        if raw:
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                pass
        return []

    _last_stock_news_scrape[symbol] = now
    return await scrape_stock_news(symbol)


# ---------------------------------------------------------------------------
# Core event handlers
# ---------------------------------------------------------------------------

async def on_1m_candle(symbol: str, candle: dict) -> None:
    """
    Called on every 1m candle close for *symbol*.

    1. Scan all signal detectors.
    2. Check retest watchlist.
    3. Run macro gates on detected signals.
    4. Queue passing signals for ICI scoring on next 5m close.
    """
    if not _within_market_hours():
        return

    snapshot = await _load_snapshot(symbol)
    if not snapshot:
        return

    # ── Signal scan ────────────────────────────────────────────────────
    try:
        # Pass the already-loaded snapshot to avoid a redundant Redis read
        raw_signals = await scan_all_signals(symbol, snapshot=snapshot)
    except Exception as exc:
        logger.warning("[brain] signal scan failed for %s: %s", symbol, exc)
        raw_signals = []

    # ── Retest watchlist check ─────────────────────────────────────────
    try:
        retest_signals = await check_retest_triggers({symbol: snapshot})
    except Exception as exc:
        logger.warning("[brain] retest check failed for %s: %s", symbol, exc)
        retest_signals = []

    all_signals = raw_signals + retest_signals

    # Volume surge signals have no direction — exclude from gating
    directional = [s for s in all_signals if "direction" in s]
    auxiliary   = [s for s in all_signals if "direction" not in s]

    for signal in auxiliary:
        try:
            payload = {
                "symbol": symbol,
                "signal": signal,
                "type": "AUXILIARY",
                "published_at": _now_ist().isoformat(),
            }

            redis = await get_redis()

            # store for frontend
            signal_id = uuid.uuid4().hex[:8]
            key = f"signal:aux:{symbol}:{signal_id}"

            await redis.setex(key, 120, json.dumps(payload))

            # publish to websocket
            await redis.publish("signals_aux", json.dumps(payload))

            logger.info(f"[brain] AUX SIGNAL → {symbol} {signal['type']}")

        except Exception as e:
            logger.error(f"[brain] failed to publish aux signal: {e}")

    active_signal_types = [s["type"] for s in all_signals]

    for signal in directional:
        await asyncio.sleep(0)
        direction = signal.get("direction", "LONG")

        try:
            passed, failed_gates = await check_macro_gates(symbol, direction)
        except Exception as exc:
            logger.warning("[brain] gate check error for %s: %s", symbol, exc)
            continue

        if not passed:
            logger.info("[brain] %s %s → BLOCKED gates=%s", symbol, signal["type"], failed_gates)
            continue

        # Gates passed — queue for ICI scoring on next 5m candle.
        # Use a Redis hash keyed by "symbol:signal_type:direction" so a
        # repeated signal at the next 1m candle overwrites the previous entry
        # instead of stacking — prevents double-execution on the same signal.
        now = _now_ist()
        expires_at = now + timedelta(minutes=_SCORE_EXPIRY_MINUTES)

        pending_item = json.dumps({
            "symbol":             symbol,
            "signal":             signal,
            "active_signal_types": active_signal_types,
            "queued_at":          now.isoformat(),
            "expires_at":         expires_at.isoformat(),
        })

        field_key = f"{symbol}:{signal['type']}:{signal.get('direction', 'NONE')}"

        try:
            redis = await get_redis()
            await redis.hset(_PENDING_SCORE_KEY, field_key, pending_item)
            logger.info("[brain] %s %s → queued for scoring (key=%s)", symbol, signal["type"], field_key)
        except Exception as exc:
            logger.error("[brain] failed to queue score for %s: %s", symbol, exc)


async def on_5m_candle(symbol: str, candle: dict) -> None:
    """
    Called on every 5m candle close for *symbol*.

    1. Process all pending scores for this symbol.
    2. Calculate ICI score; publish execution payload if actionable.
    3. Trigger market breadth recompute (amortised — only one caller wins).
    """
    if not _within_market_hours():
        return

    redis = await get_redis()
    vix   = await _get_vix()
    mtime = _current_time_ist()

    # ── Process pending scores for this symbol ──────────────────────────
    # _PENDING_SCORE_KEY is a Redis HASH: field = "symbol:type:direction",
    # value = JSON item.  Using a hash prevents duplicate entries for the same
    # signal — a later 1m candle simply overwrites the earlier one.
    try:
        # Fetch only fields that belong to this symbol
        all_fields = await redis.hkeys(_PENDING_SCORE_KEY)
    except Exception as exc:
        logger.error("[brain] hkeys failed: %s", exc)
        return

    symbol_fields = [f for f in all_fields if f.startswith(f"{symbol}:")]

    for field_key in symbol_fields:
        await asyncio.sleep(0)
        raw_item = await redis.hget(_PENDING_SCORE_KEY, field_key)
        if not raw_item:
            continue

        try:
            item = json.loads(raw_item)
        except (json.JSONDecodeError, TypeError):
            await redis.hdel(_PENDING_SCORE_KEY, field_key)
            continue

        # Drop stale items
        if await _is_score_expired(item):
            logger.debug("[brain] dropping expired pending score for %s", symbol)
            await redis.hdel(_PENDING_SCORE_KEY, field_key)
            continue

        signal              = item["signal"]
        active_signal_types = item.get("active_signal_types", [signal["type"]])

        # ── ICI Score ──────────────────────────────────────────────────
        try:
            snapshot_for_score = await _load_snapshot(symbol)
            signal = await score_signal(signal, snapshot_for_score)
        except Exception as exc:
            logger.error("[brain] scorer error for %s: %s", symbol, exc, exc_info=True)
            await redis.hdel(_PENDING_SCORE_KEY, field_key)
            continue
        grade  = signal.get("ici_grade", "IGNORE")
        action = "EXECUTE_MARKET" if grade == "EXECUTE" else ("WATCHLIST" if grade == "WATCHLIST" else "IGNORE")
        logger.info(
            "[brain] %s %s → score=%.1f grade=%s action=%s",
            symbol, signal["type"],
            score_result.get("score", 0),
            score_result.get("grade", "?"),
            action,
        )

        if action in ("EXECUTE_MARKET", "EXECUTE_LIMIT", "WATCHLIST"):
            # ── Options evaluation ─────────────────────────────────────
            try:
                options = await evaluate_options_for_signal(symbol, signal.get("direction", "LONG"))
            except Exception as exc:
                logger.warning("[brain] options eval failed for %s: %s", symbol, exc)
                options = {}

            try:
                badge = await compute_tradability_badge(symbol, signal.get("direction", "LONG"))
            except Exception as exc:
                logger.warning("[brain] tradability badge failed for %s: %s", symbol, exc)
                badge = {}

            # ── AI alignment (non-blocking best-effort) ────────────────
            ai_alignment = None
            snapshot = {}
            try:
                snapshot = await _load_snapshot(symbol)
                news     = await _maybe_scrape_stock_news(symbol)
                if news:
                    ai_alignment = await analyze_stock_alignment(symbol, news, snapshot)
            except Exception as exc:
                logger.warning("[brain] AI alignment failed for %s: %s", symbol, exc)

            # ── Publish execution payload ──────────────────────────────
            # Enrich payload with key snapshot fields for frontend display
            ltp        = float(snapshot.get("ltp") or 0)
            prev_close = float(snapshot.get("prev_close") or 0)
            change_pct = round((ltp - prev_close) / prev_close * 100, 2) if prev_close else 0.0

            payload = {
                "symbol":       symbol,
                "signal":       signal,
                "score":        score_result,
                "options":      options,
                "badge":        badge,
                "ai_alignment": ai_alignment,
                "published_at": _now_ist().isoformat(),
                # Snapshot fields for frontend card display
                "ltp":              ltp,
                "change_pct":       change_pct,
                "vwap":             float(snapshot.get("vwap") or 0),
                "rsi14":            float(snapshot.get("rsi14") or 0),
                "supertrend_dir":   snapshot.get("supertrend_dir", ""),
                "choppiness_class": snapshot.get("choppiness_class", "NEUTRAL"),
                "sector":           snapshot.get("sector", ""),
                "atr14":            float(snapshot.get("atr14") or 0),
            }

            try:
                signal_id = uuid.uuid4().hex[:8]
                signal_key = f"signal:active:{symbol}:{signal_id}"
                await redis.setex(signal_key, 300, json.dumps(payload))
                logger.debug(
                    "[brain] Stored signal %s for %s (TTL 300s)",
                    signal_id, symbol
                )
                await redis.publish("trade_execution", json.dumps(payload))
                logger.info(
                    "[brain] ★ EXECUTION PUBLISHED — %s %s grade=%s action=%s",
                    symbol, signal["type"],
                    score_result.get("grade"),
                    action,
                )
            except Exception as exc:
                logger.error("[brain] publish failed for %s: %s", symbol, exc)

        # Always remove processed item from the hash
        await redis.hdel(_PENDING_SCORE_KEY, field_key)

    # ── Market breadth (amortised per 5m cycle) ─────────────────────────
    # Use a Redis lock-like flag so only one symbol's 5m close triggers it
    try:
        redis2 = await get_redis()
        acquired = await redis2.set(
            "brain:breadth_lock",
            "1",
            ex=280,          # 4m 40s — ensures only one per 5m cycle
            nx=True,         # only set if not exists
        )
        if acquired:
            asyncio.create_task(_run_breadth_safe())
    except Exception as exc:
        logger.warning("[brain] breadth lock check failed: %s", exc)


async def _run_breadth_safe() -> None:
    try:
        await asyncio.sleep(0)
        await compute_market_breadth()
    except Exception as exc:
        logger.error("[brain] market breadth error: %s", exc, exc_info=True)


async def _guarded(coro) -> None:
    """Run *coro* under the scan semaphore to cap concurrent Redis load."""
    async with _SCAN_SEMAPHORE:
        await coro


# ---------------------------------------------------------------------------
# Redis pub/sub listener
# ---------------------------------------------------------------------------

async def _subscribe_candles() -> None:
    """
    Subscribe to candles:1m and candles:5m channels.
    Dispatches candle events to on_1m_candle / on_5m_candle.
    """
    while True:
        try:
            redis  = await get_redis()
            pubsub = redis.pubsub()
            await pubsub.subscribe(_CH_1M, _CH_5M)
            logger.info("[brain] Subscribed to %s and %s channels.", _CH_1M, _CH_5M)

            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                await asyncio.sleep(0)

                channel = message["channel"]

                try:
                    data = json.loads(message["data"])
                except (json.JSONDecodeError, TypeError):
                    continue

                try:
                    if isinstance(channel, bytes):
                        channel = channel.decode()
                    symbol = data.get("symbol")
                    candle = data  # cruncher publishes flat candle payload

                    if not symbol:
                        continue

                    if channel == _CH_1M:
                        asyncio.create_task(_guarded(on_1m_candle(symbol, candle)))
                    elif channel == _CH_5M:
                        asyncio.create_task(_guarded(on_5m_candle(symbol, candle)))
                except Exception as e:
                    logger.error("[brain] Message processing error: %s", e)
        except Exception as e:
            logger.warning("[brain] Pub/sub connection dropped, reconnecting in 2s: %s", e)
            await asyncio.sleep(2)


# ---------------------------------------------------------------------------
# Scheduled tasks
# ---------------------------------------------------------------------------

async def _premarket_news_task() -> None:
    """
    Fire at 7:30 AM IST — scrape broad market news + run Groq sentiment.
    Keeps re-running every 5 seconds until the scrape fires, then exits.
    """
    global _premarket_news_done

    while True:
        now = _now_ist()
        if (now.hour, now.minute) >= (_PREMARKET_NEWS_HOUR, _PREMARKET_NEWS_MINUTE):
            if not _premarket_news_done:
                _premarket_news_done = True
                logger.info("[brain] Pre-market news scrape firing at %s", _current_time_ist())
                try:
                    headlines = await scrape_premarket_news()
                    if headlines:
                        await analyze_premarket_sentiment(headlines)
                except Exception as exc:
                    logger.error("[brain] Pre-market pipeline error: %s", exc, exc_info=True)
            # Reset flag at midnight for the next trading day
            if now.hour == 0 and now.minute == 0:
                _premarket_news_done = False

        await asyncio.sleep(30)


async def _breadth_background_task() -> None:
    """
    Independent breadth recompute loop — every 60 seconds.
    Complements the 5m-candle triggered breadth (ensures freshness even
    if no 5m candle fires for a while — e.g. halted stocks).
    """
    while True:
        await asyncio.sleep(_BREADTH_INTERVAL_SEC)
        if _within_market_hours():
            await _run_breadth_safe()


async def start_execution_engine() -> None:
    global _execution_engine_started
    if _execution_engine_started:
        logger.info("[brain] execution engine already started; skipping duplicate start")
        return

    tasks = [
        asyncio.create_task(run_execution_listener(), name="execution_listener"),
        asyncio.create_task(monitor_stop_losses_event_driven(), name="sl_monitor"),
        asyncio.create_task(monitor_trigger_orders(), name="trigger_monitor"),
    ]

    for t in tasks:
        background_tasks.add(t)
        t.add_done_callback(background_tasks.discard)

    _execution_engine_started = True


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def run_brain() -> None:
    """
    Main brain coroutine.

    Starts all background tasks and enters the candle subscription loop.
    Never returns under normal operation.
    """
    logger.info("[brain] ═══ Strategy Brain v2 starting ═══")
    print("REDIS URL:", cfg.REDIS_URL)

    # Validate config early
    validate()

    # Warm up universe
    symbols = await get_symbols()
    logger.info("[brain] Universe loaded — %d symbols", len(symbols))

    # Launch background tasks
    await start_execution_engine()
    premarket_task = asyncio.create_task(_premarket_news_task(), name="premarket_news")
    breadth_task = asyncio.create_task(_breadth_background_task(), name="breadth_bg")
    background_tasks.add(premarket_task)
    background_tasks.add(breadth_task)
    premarket_task.add_done_callback(background_tasks.discard)
    breadth_task.add_done_callback(background_tasks.discard)

    # Prime market breadth once on startup
    asyncio.create_task(_run_breadth_safe())

    # Enter pub/sub loop (blocks forever)
    await _subscribe_candles()


# ---------------------------------------------------------------------------
# __main__ block
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    async def _main() -> None:
        try:
            await run_brain()
        except KeyboardInterrupt:
            logger.info("[brain] Shutting down…")
        finally:
            for task in list(background_tasks):
                task.cancel()
            if background_tasks:
                await asyncio.gather(*background_tasks, return_exceptions=True)
            await close_redis()

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        sys.exit(0)
