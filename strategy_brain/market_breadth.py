"""
strategy_brain/market_breadth.py
==================================
Market breadth scanner — runs every 60 seconds.

Scans all symbol snapshots to compute:
  - Advances / Declines / Unchanged counts
  - A/D ratio
  - % stocks above EMA-200
  - Per-sector average % change

Writes results to:
    Redis key  : market:breadth           (full JSON)
    Redis keys : market:breadth:sector:{name}  (per-sector avg, float string)

These are consumed by:
    - conviction_scorer.py (Pillar 2 — double RS)
    - Frontend dashboard (breadth gauge widget)

Usage
-----
    from strategy_brain.market_breadth import compute_market_breadth

    breadth = await compute_market_breadth()
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.redis_client import get_redis
from core.universe_builder import get_symbols

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_IST = timezone(timedelta(hours=5, minutes=30))

_ADVANCE_THRESHOLD = 0.1   # % change > 0.1  → advance
_DECLINE_THRESHOLD = -0.1  # % change < -0.1 → decline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Optional[str], default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _safe_divide(num: float, den: float, default: float = 0.0) -> float:
    if den == 0:
        return default
    return num / den


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def compute_market_breadth() -> dict:
    """
    Compute full market breadth and publish to Redis.

    Returns
    -------
    breadth dict (same structure as what's written to Redis).

    Raises — never.  All errors are caught and logged; returns a minimal
    skeleton breadth dict on catastrophic failure.
    """
    try:
        return await _compute()
    except Exception as exc:  # noqa: BLE001
        logger.error("[market_breadth] Unhandled error in compute: %s", exc, exc_info=True)
        return _empty_breadth()


async def _compute() -> dict:
    redis   = await get_redis()
    symbols = await get_symbols()

    advances  = 0
    declines  = 0
    unchanged = 0
    above_ema200 = 0

    sector_changes: dict[str, list[float]] = {}

    for symbol in symbols:
        try:
            snap = await redis.hgetall(f"snapshot:{symbol}")
            if not snap:
                continue

            last_close = _safe_float(snap.get("last_close") or snap.get("ltp"))
            prev_close = _safe_float(snap.get("prev_close"), 1.0)
            ema200     = _safe_float(snap.get("ema200"))

            change_pct = _safe_divide(last_close - prev_close, prev_close) * 100

            if change_pct > _ADVANCE_THRESHOLD:
                advances += 1
            elif change_pct < _DECLINE_THRESHOLD:
                declines += 1
            else:
                unchanged += 1

            if ema200 > 0 and last_close > ema200:
                above_ema200 += 1

            sector = snap.get("sector", "UNKNOWN") or "UNKNOWN"
            sector_changes.setdefault(sector, []).append(change_pct)

        except Exception as exc:  # noqa: BLE001
            logger.debug("[market_breadth] Error processing %s: %s", symbol, exc)
            continue

    total = max(len(symbols), 1)

    # Per-sector averages
    sector_avg: dict[str, float] = {
        s: round(sum(v) / len(v), 2)
        for s, v in sector_changes.items()
        if v
    }

    breadth = {
        "advances":        advances,
        "declines":        declines,
        "unchanged":       unchanged,
        "total":           total,
        "ad_ratio":        round(_safe_divide(advances, max(declines, 1)), 3),
        "above_ema200":    above_ema200,
        "above_ema200_pct": round(above_ema200 / total * 100, 1),
        "sector_performance": sector_avg,
        "computed_at":     datetime.now(_IST).isoformat(),
    }

    # ── Write to Redis ──────────────────────────────────────────────────
    async with redis.pipeline(transaction=False) as pipe:
        pipe.set("market:breadth", json.dumps(breadth))
        for sector, avg in sector_avg.items():
            pipe.set(f"market:breadth:sector:{sector}", str(avg))
        await pipe.execute()

    logger.info(
        "[market_breadth] A=%d D=%d U=%d ad_ratio=%.2f ema200_pct=%.1f%% sectors=%d",
        advances, declines, unchanged,
        breadth["ad_ratio"],
        breadth["above_ema200_pct"],
        len(sector_avg),
    )

    return breadth


# ---------------------------------------------------------------------------
# Public reader (for API endpoints)
# ---------------------------------------------------------------------------

async def get_latest_breadth() -> Optional[dict]:
    """
    Read the last breadth result from Redis.
    Returns None if not yet computed.
    """
    redis = await get_redis()
    raw = await redis.get("market:breadth")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


async def get_sector_avg(sector: str) -> float:
    """
    Return the latest average % change for a sector.
    Returns 0.0 if not available.
    """
    redis = await get_redis()
    raw = await redis.get(f"market:breadth:sector:{sector}")
    return _safe_float(raw)


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------

def _empty_breadth() -> dict:
    return {
        "advances":           0,
        "declines":           0,
        "unchanged":          0,
        "total":              0,
        "ad_ratio":           0.0,
        "above_ema200":       0,
        "above_ema200_pct":   0.0,
        "sector_performance": {},
        "computed_at":        datetime.now(_IST).isoformat(),
    }
