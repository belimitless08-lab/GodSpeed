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
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.config import cfg, validate
from core.market_data import write_market_intelligence as _write_market_intel
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
_SEM = asyncio.Semaphore(8)  # max 8 symbols processed concurrently
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
    # ⚠️ REPLAY_TEST_ONLY — NEVER leave this set in production.
    # REPLAY_MODE=1 bypasses all market-hours checks.
    # Remove this env var from Railway brain service immediately after testing.
    if os.environ.get("REPLAY_MODE") == "1":
        return True

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
        async with _SEM:
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
        async with _SEM:
            await asyncio.sleep(0)
            raw_item = await redis.hget(_PENDING_SCORE_KEY, field_key)
            
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
            "[brain] %s %s → score=%s grade=%s action=%s",
            symbol, signal["type"],
            signal.get("ici_score", 0),
            grade,
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
                "score": {
                    "score": signal.get("ici_score", 0),
                    "grade": grade,
                },
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
                    grade,
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


async def _run_pcr_vix_update() -> None:
    """
    Fetch PCR (NIFTY + BANKNIFTY) and India VIX every 60 minutes.
    Fetches immediately on startup if market is open, then waits 60 min.
    Uses core.market_data.write_market_intelligence (shared with seeder).
    Brain writes include 7200s TTL; seeder writes have no TTL.
    """
    while True:
        if _within_market_hours():
            try:
                redis = await get_redis()
                data  = await _write_market_intel(redis)
                # Brain overwrites with TTL so keys expire if brain stops
                await redis.expire("market:vix",           7200)
                await redis.expire("market:pcr_sentiment", 7200)
                await redis.expire("market:intelligence",  7200)
                logger.info(
                    "[brain] PCR NIFTY=%.3f BN=%.3f VIX=%.1f → %s",
                    data["nifty_pcr"], data["banknifty_pcr"],
                    data["vix"], data["sentiment"],
                )
            except Exception as exc:
                logger.warning("[brain] PCR/VIX update error: %s", exc)
        await asyncio.sleep(3600)


async def _run_volume_ranking() -> None:
    """
    Rank all universe symbols by composite volume score every 5 minutes.
    Fires immediately on startup if within market hours, then every 5 minutes.

    Enhancements:
    - Session phase normalisation via elif+mins phase multiplier
    - Liquidity tiering (avg_volume_5d < 200K requires cum_rvol >= 2.5)
    - Expiry day handling (Thursday — all thresholds raised 30%)
    - first5m breakout filter
    - vol_state included in each ranked entry
    """
    def _phase_multiplier() -> float:
        """
        Returns RVOL divisor by session phase.
        Higher value = harder to qualify (e.g. Close Surge always high vol).
        Lower value  = easier to qualify (e.g. Dead Zone naturally quiet).
        """
        now  = _now_ist()
        mins = now.hour * 60 + now.minute
        if   mins < 600:  return 1.0   # 09:15–10:00 Opening Drive
        elif mins < 690:  return 1.0   # 10:00–11:30 Morning Core
        elif mins < 780:  return 0.7   # 11:30–13:00 Dead Zone
        elif mins < 810:  return 0.75  # 13:00–13:30 Post-lunch
        elif mins < 900:  return 0.85  # 13:30–15:00 Afternoon Core
        else:             return 1.3   # 15:00–15:30 Close Surge

    def _sf(snap, k, d=0.0):
        try:
            return float(snap.get(k, d) or d)
        except Exception:
            return d

    while True:
        if _within_market_hours():
            try:
                redis   = await get_redis()
                symbols = await get_symbols()

                # Expiry day — raise effective thresholds 30% on Thursdays
                try:
                    is_expiry = (await redis.get("market:is_expiry") or b"0") in (b"1", "1")
                except Exception:
                    is_expiry = False
                expiry_mult  = 1.3 if is_expiry else 1.0
                rvol_divisor = _phase_multiplier() * expiry_mult

                rows = []
                for sym in symbols:
                    try:
                        snap = await redis.hgetall(f"snapshot:{sym}")
                        if not snap:
                            continue

                        cum_rvol     = _sf(snap, "cum_rvol")
                        vol_accel    = _sf(snap, "vol_accel")
                        consec_rvol  = _sf(snap, "consec_rvol")
                        cum_volume   = _sf(snap, "cum_volume")
                        avg_vol_5d   = _sf(snap, "avg_volume_5d")
                        ltp          = _sf(snap, "ltp")
                        prev_close   = _sf(snap, "prev_close")
                        change_pct   = round((ltp - prev_close) / max(prev_close, 1) * 100, 2) if prev_close > 0 else 0.0
                        first5m_high = _sf(snap, "first5m_high")
                        first5m_low  = _sf(snap, "first5m_low")

                        # Liquidity guard: small-cap needs stronger RVOL signal
                        if 0 < avg_vol_5d < 200_000 and cum_rvol < 2.5:
                            continue

                        # cum_rvol is already time-normalised (vs same-slot
                        # historical average). No phase divisor needed.
                        adj_rvol = cum_rvol

                        # First 5m candle breakout filter
                        if first5m_high > 0 and first5m_low > 0:
                            if ltp > first5m_high:
                                breakout_dir = "LONG"
                            elif ltp < first5m_low:
                                breakout_dir = "SHORT"
                            else:
                                breakout_dir = None
                        else:
                            breakout_dir = None

                        rows.append({
                            "symbol":       sym,
                            "cum_rvol":     cum_rvol,
                            "prev_high":    _sf(snap, "prev_high"),
                            "prev_low":     _sf(snap, "prev_low"),
                            "r1":           _sf(snap, "r1"),
                            "s1":           _sf(snap, "s1"),
                            "r2":           _sf(snap, "r2"),
                            "s2":           _sf(snap, "s2"),
                            "vwap":         _sf(snap, "vwap"),
                            "supertrend_dir": str(
                                snap.get(b"supertrend_dir") or
                                snap.get("supertrend_dir") or "BEAR"
                            ).replace("b'","").replace("'",""),
                            "change_pct":   change_pct,
                            "adj_rvol":     round(adj_rvol, 3),
                            "vol_accel":    vol_accel,
                            "consec_rvol":  consec_rvol,
                            "cum_volume":   cum_volume,
                            "ltp":          ltp,
                            "first5m_high": first5m_high,
                            "first5m_low":  first5m_low,
                            "breakout_dir": breakout_dir,
                            "vol_state":    (snap.get("vol_state") or b"DRY").decode() if isinstance(snap.get("vol_state"), bytes) else (snap.get("vol_state") or "DRY"),
                        })
                    except Exception as sym_exc:
                        logger.debug("[brain] vol rank skip %s: %s", sym, sym_exc)
                        continue

                if rows:
                    _STATE_PTS = {
                        "BURST": 8, "CLIMAX": 6, "BUILDING": 4,
                        "FADE": 1,  "DRY": 0,
                    }

                    for r in rows:
                        cr  = r["cum_rvol"]
                        va  = r["vol_accel"]
                        con = r["consec_rvol"]
                        st  = r.get("vol_state", "DRY")

                        # Smooth base (0–60): linear, 60 pts at 4.0x RVOL
                        base    = min(cr * 15.0, 60.0)
                        # Acceleration bonus (0–20): 20 pts at 4.0x
                        accel   = min(va * 5.0, 20.0)
                        # Persistence bonus (0–12): 2.4 pts per elevated candle
                        persist = min(con * 2.4, 12.0)
                        # Discrete state bonus (0–8)
                        state_b = _STATE_PTS.get(st, 0)

                        r["vol_leader_score"] = round(
                            min(base + accel + persist + state_b, 100.0), 1
                        )

                    # Strength percentile — where does this stock rank
                    # among ALL 209 stocks by volume quality today?
                    _all_scores = [r["vol_leader_score"] for r in rows]
                    _n          = len(_all_scores)
                    for r in rows:
                        r["strength_pct"] = round(
                            sum(1 for s in _all_scores
                                if s < r["vol_leader_score"]) / _n * 100, 1
                        )

                    rows.sort(key=lambda x: x["vol_leader_score"], reverse=True)
                    top50 = rows[:50]

                    await redis.set("volume_leaders:ranked", json.dumps(top50), ex=360)
                    logger.info(
                        "[brain] Vol ranking — leader=%s score=%.1f phase_div=%.2f",
                        top50[0]["symbol"] if top50 else "none",
                        top50[0]["vol_leader_score"] if top50 else 0,
                        rvol_divisor,
                    )

            except Exception as exc:
                logger.warning("[brain] volume ranking error: %s", exc)

        await asyncio.sleep(300)


async def _run_options_volume_ranking() -> None:
    """
    Rank all F&O symbols by ATM option turnover RVOL every 5 minutes.
    Writes options_leaders:ranked (top 20, JSON, TTL 360s).

    Flow direction: BULL if ce_today > pe_today*1.5,
                    BEAR if pe_today > ce_today*1.5, else FLAT.
    FLAT dominant side: whichever of CE/PE has higher turnover.
    RVOL states: <0.7 CALM / 0.7-1.4 ACTIVE / 1.4-2.2 HOT / >2.2 EXTREME
    atm_strike stored as float to handle fractional strikes (e.g. 222.5).
    """
    while True:
        if _within_market_hours():
            try:
                redis   = await get_redis()
                symbols = await get_symbols()
                rows    = []

                for sym in symbols:
                    try:
                        today_raw   = await redis.get(f"options:atm_turnover_today:{sym}")
                        profile_raw = await redis.get(f"options:atm_profile:cum:{sym}")
                        if not today_raw or not profile_raw:
                            continue

                        today   = float(today_raw)
                        profile = json.loads(profile_raw)

                        # Slot-based RVOL
                        _now    = _now_ist()
                        _slot_m = (_now.minute // 5) * 5
                        _slot   = f"{_now.hour:02d}{_slot_m:02d}"
                        _ref    = profile.get(_slot, 0.0)
                        if _ref <= 0:
                            _pm = _slot_m - 5
                            _ph = _now.hour
                            if _pm < 0:
                                _pm = 55
                                _ph -= 1
                            _ref = profile.get(f"{_ph:02d}{_pm:02d}", 0.0)
                        if _ref <= 0:
                            continue

                        atm_rvol = round(today / _ref, 3)

                        # CE / PE separate turnover
                        ce_today = float(
                            await redis.get(f"options:atm_ce_turnover_today:{sym}") or 0
                        )
                        pe_today = float(
                            await redis.get(f"options:atm_pe_turnover_today:{sym}") or 0
                        )

                        # Flow direction — simple ratio only, no dead scoring variables
                        if ce_today > pe_today * 1.5:
                            flow_dir = "BULL"
                        elif pe_today > ce_today * 1.5:
                            flow_dir = "BEAR"
                        else:
                            flow_dir = "FLAT"

                        # Dominant side: BULL→CE, BEAR→PE, FLAT→higher turnover side
                        dom_side = (
                            "CE" if flow_dir == "BULL"
                            else "PE" if flow_dir == "BEAR"
                            else ("CE" if ce_today >= pe_today else "PE")
                        )

                        # Options vol state
                        if atm_rvol < 0.7:
                            opt_vol_state = "CALM"
                        elif atm_rvol < 1.4:
                            opt_vol_state = "ACTIVE"
                        elif atm_rvol < 2.2:
                            opt_vol_state = "HOT"
                        else:
                            opt_vol_state = "EXTREME"

                        # ATM strike — kept as float for fractional strikes (e.g. 222.5)
                        atm_strike = 0.0
                        try:
                            prev_raw = await redis.get(f"options:prev:{sym}")
                            if prev_raw:
                                atm_strike = float(
                                    json.loads(prev_raw).get("atm_strike") or 0
                                )
                        except Exception:
                            pass

                        # Live CE / PE LTP — use float strike for key construction
                        ce_ltp, pe_ltp = 0.0, 0.0
                        if atm_strike:
                            # Strike in key: use int if whole number, else keep decimal
                            _sk = (
                                int(atm_strike)
                                if atm_strike == int(atm_strike)
                                else atm_strike
                            )
                            _ce_tick = await redis.hgetall(f"options:tick:{sym}:{_sk}CE")
                            _pe_tick = await redis.hgetall(f"options:tick:{sym}:{_sk}PE")
                            try:
                                ce_ltp = float(_ce_tick.get("ltp", 0) or 0)
                            except Exception:
                                pass
                            try:
                                pe_ltp = float(_pe_tick.get("ltp", 0) or 0)
                            except Exception:
                                pass

                        # Prev OHLC
                        ce_close = ce_high = ce_low = 0.0
                        pe_close = pe_high = pe_low = 0.0
                        try:
                            _ohlc_raw = await redis.get(f"options:prev_ohlc:{sym}")
                            if _ohlc_raw:
                                _ohlc    = json.loads(_ohlc_raw)
                                ce_high  = float(_ohlc.get("ce_high",  0))
                                ce_low   = float(_ohlc.get("ce_low",   0))
                                ce_close = float(_ohlc.get("ce_close", 0))
                                pe_high  = float(_ohlc.get("pe_high",  0))
                                pe_low   = float(_ohlc.get("pe_low",   0))
                                pe_close = float(_ohlc.get("pe_close", 0))
                        except Exception:
                            pass

                        # Change % from prev day close
                        def _chg(ltp, close):
                            if close <= 0 or ltp <= 0:
                                return 0.0
                            return round((ltp - close) / close * 100, 2)

                        # Option pivot R1/S1
                        def _pivot_levels(high, low, close):
                            if high <= 0 or low <= 0 or close <= 0:
                                return 0.0, 0.0
                            p  = (high + low + close) / 3
                            return round(2 * p - low, 2), round(2 * p - high, 2)

                        ce_r1, ce_s1 = _pivot_levels(ce_high, ce_low, ce_close)
                        pe_r1, pe_s1 = _pivot_levels(pe_high, pe_low, pe_close)

                        # Stock snapshot
                        snap = await redis.hgetall(f"snapshot:{sym}")
                        def _sf(k, d=0.0):
                            try:
                                return float(snap.get(k, d) or d)
                            except Exception:
                                return d

                        stock_ltp  = _sf("ltp")
                        stock_r1   = _sf("r1")
                        stock_s1   = _sf("s1")
                        stock_pdh  = _sf("pdh")
                        stock_pdl  = _sf("pdl")

                        # Dominant side values — consistent with dom_side
                        if dom_side == "CE":
                            dom_ltp        = ce_ltp
                            dom_change_pct = _chg(ce_ltp, ce_close)
                            dom_above_pdh  = ce_high  > 0 and ce_ltp > ce_high
                            dom_below_pdl  = ce_low   > 0 and ce_ltp < ce_low
                            dom_above_r1   = ce_r1    > 0 and ce_ltp > ce_r1
                            dom_below_s1   = ce_s1    > 0 and ce_ltp < ce_s1
                        else:
                            dom_ltp        = pe_ltp
                            dom_change_pct = _chg(pe_ltp, pe_close)
                            dom_above_pdh  = pe_high  > 0 and pe_ltp > pe_high
                            dom_below_pdl  = pe_low   > 0 and pe_ltp < pe_low
                            dom_above_r1   = pe_r1    > 0 and pe_ltp > pe_r1
                            dom_below_s1   = pe_s1    > 0 and pe_ltp < pe_s1

                        rows.append({
                            "symbol":          sym,
                            "atm_rvol":        atm_rvol,
                            "opt_vol_state":   opt_vol_state,
                            "flow_dir":        flow_dir,
                            "dom_side":        dom_side,
                            "today_turnover":  round(today),
                            "prev_turnover":   round(_ref),
                            "ce_today":        round(ce_today),
                            "pe_today":        round(pe_today),
                            "atm_strike":      atm_strike,
                            "ce_ltp":          ce_ltp,
                            "pe_ltp":          pe_ltp,
                            "dom_ltp":         dom_ltp,
                            "dom_change_pct":  dom_change_pct,
                            "dom_above_pdh":   dom_above_pdh,
                            "dom_below_pdl":   dom_below_pdl,
                            "dom_above_r1":    dom_above_r1,
                            "dom_below_s1":    dom_below_s1,
                            "stock_above_r1":  stock_r1 > 0 and stock_ltp > stock_r1,
                            "stock_below_s1":  stock_s1 > 0 and stock_ltp < stock_s1,
                            "stock_above_pdh": stock_pdh > 0 and stock_ltp > stock_pdh,
                            "stock_below_pdl": stock_pdl > 0 and stock_ltp < stock_pdl,
                        })

                    except Exception as sym_exc:
                        logger.debug("[brain] options rank skip %s: %s", sym, sym_exc)
                        continue

                if rows:
                    rows.sort(key=lambda x: x["atm_rvol"], reverse=True)
                    top20 = rows[:20]
                    await redis.set(
                        "options_leaders:ranked",
                        json.dumps(top20),
                        ex=360,
                    )
                    logger.info(
                        "[brain] Options ranking — leader=%s rvol=%.2f "
                        "flow=%s side=%s state=%s",
                        top20[0]["symbol"],
                        top20[0]["atm_rvol"],
                        top20[0]["flow_dir"],
                        top20[0]["dom_side"],
                        top20[0]["opt_vol_state"],
                    )

            except Exception as exc:
                logger.warning("[brain] options ranking error: %s", exc)

        await asyncio.sleep(300)


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
    pcr_task = asyncio.create_task(_run_pcr_vix_update(), name="pcr_vix_update")
    vol_rank_task = asyncio.create_task(_run_volume_ranking(), name="volume_ranking")
    background_tasks.add(vol_rank_task)
    vol_rank_task.add_done_callback(background_tasks.discard)
    opt_rank_task = asyncio.create_task(
        _run_options_volume_ranking(), name="options_volume_ranking"
    )
    background_tasks.add(opt_rank_task)
    opt_rank_task.add_done_callback(background_tasks.discard)
    background_tasks.add(premarket_task)
    background_tasks.add(breadth_task)
    background_tasks.add(pcr_task)
    premarket_task.add_done_callback(background_tasks.discard)
    breadth_task.add_done_callback(background_tasks.discard)
    pcr_task.add_done_callback(background_tasks.discard)

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
