"""
strategy_brain/conviction_scorer.py
=====================================
ICI (Intraday Conviction Index) scorer — Node 3b.

Runs after all macro gates pass.  Reads pre-calculated indicator
snapshots from Redis and returns a scored, graded, time-bounded
conviction payload.

Snapshot field contract (written by candle builder, all guaranteed):
    vwap              float  — current VWAP
    vwap_slope        float  — % slope over last 5 candles
    rsi14             float  — current RSI-14
    choppiness_class  str    — "TRENDING" | "NEUTRAL" | "CHOPPY"

Usage
-----
    from strategy_brain.conviction_scorer import calculate_ici_score

    result = await calculate_ici_score(
        symbol="RELIANCE",
        signal_direction="LONG",
        signal_type="ORB_BREAKOUT",
        active_signals=["ORB_BREAKOUT", "VOLUME_SURGE_STRONG"],
        vix=16.5,
        market_time="09:45",
    )
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone, date
from typing import Optional

from core.redis_client import get_redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_IST = timezone(timedelta(hours=5, minutes=30))

_SCORE_EXPIRY_MINUTES = 10

# Signal bonus table
_SIGNAL_BONUSES: dict[str, int] = {
    "OPENING_DRIVE":                    15,
    "ORB_BREAKOUT":                     15,
    "HOURLY_BREAKOUT":                  10,
    "CHOPPINESS_BREAKOUT_CONFIRMED":    15,
    "CHOPPINESS_BREAKOUT_UNCONFIRMED":   8,
    "SUPERTREND_FLIP":                   8,
    "VOLUME_SURGE_STRONG":              10,
    "VOLUME_SURGE_MODERATE":             5,
}

# Grade / action thresholds
_GRADE_TABLE = [
    (90, "GOD",    "EXECUTE_MARKET"),
    (75, "A",      "EXECUTE_LIMIT"),
    (50, "B",      "WATCHLIST"),
    (0,  "IGNORE", "IGNORE"),
]

# ---------------------------------------------------------------------------
# Internal helpers
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
# Regime-aware weight calculator
# ---------------------------------------------------------------------------

def _get_weights(vix: float, market_time: str) -> dict[str, float]:
    """
    Return pillar weights that adapt to VIX regime and session time.

    Pillars: rvol | rs | options | vwap | chop
    """
    try:
        hour, minute = map(int, market_time.split(":"))
    except (ValueError, AttributeError):
        hour, minute = 10, 30  # safe default — mid-session

    is_opening = hour == 9 or (hour == 10 and minute == 0)  # 9:15–10:00

    if is_opening and vix > 18:
        return {"rvol": 35, "rs": 25, "options": 25, "vwap":  5, "chop": 10}
    elif vix > 20:
        return {"rvol": 25, "rs": 20, "options": 35, "vwap": 10, "chop": 10}
    else:
        return {"rvol": 30, "rs": 25, "options": 20, "vwap": 15, "chop": 10}


# ---------------------------------------------------------------------------
# Pillar scorers
# ---------------------------------------------------------------------------

def _score_rvol(snap: dict, weight: float, direction: str) -> float:
    """Pillar 1 — Time-weighted relative volume."""
    rvol = _safe_float(snap.get("live_volume_ratio"), _safe_float(snap.get("rvol")))

    if rvol >= 3.0:
        return weight * 1.0
    elif rvol >= 2.0:
        return weight * 0.67
    elif rvol >= 1.5:
        return weight * 0.33
    return 0.0


def _score_relative_strength(
    snap: dict,
    weight: float,
    direction: str,
    nifty_change: float,
    sector_avg_change: float,
) -> float:
    """Pillar 2 — Double relative strength vs Nifty + sector."""
    prev_close = _safe_float(snap.get("prev_close"), 1.0)
    ltp        = _safe_float(snap.get("ltp"), _safe_float(snap.get("last_close")))

    if prev_close <= 0:
        return 0.0

    stock_change = _safe_divide(ltp - prev_close, prev_close) * 100

    # For SHORT signals, invert the comparison
    if direction == "SHORT":
        stock_change = -stock_change
        nifty_change = -nifty_change
        sector_avg_change = -sector_avg_change

    rs_vs_nifty  = stock_change - nifty_change
    rs_vs_sector = stock_change - sector_avg_change

    if rs_vs_nifty > 1.0 and rs_vs_sector > 0:
        return weight * 1.0
    elif rs_vs_nifty > 0.5:
        return weight * 0.6
    elif rs_vs_nifty > 0:
        return weight * 0.2
    return 0.0


def _score_options_flow_raw(
    prev: dict,
    live_ce: dict,
    live_pe: dict,
    weight: float,
    direction: str,
) -> float:
    """Pillar 3 — Options OI flow imbalance (raw component)."""
    prev_ce_oi = _safe_float(prev.get("ce_oi_prev"))
    prev_pe_oi = _safe_float(prev.get("pe_oi_prev"))

    live_ce_oi = _safe_float(live_ce.get("oi"))
    live_pe_oi = _safe_float(live_pe.get("oi"))

    ce_oi_change = live_ce_oi - prev_ce_oi
    pe_oi_change = live_pe_oi - prev_pe_oi

    # For LONG: PE writing (pe_oi_change > 0) + CE unwinding (ce_oi_change < 0) = bullish
    # For SHORT: CE writing (ce_oi_change > 0) + PE unwinding (pe_oi_change < 0) = bearish
    if direction == "LONG":
        oi_flow = _safe_divide(
            pe_oi_change - ce_oi_change,
            max(abs(pe_oi_change + ce_oi_change), 1)
        )
    else:  # SHORT
        oi_flow = _safe_divide(
            ce_oi_change - pe_oi_change,
            max(abs(pe_oi_change + ce_oi_change), 1)
        )

    if oi_flow >= 0.2:
        return weight * 1.0
    elif oi_flow >= 0.05:
        return weight * 0.5
    elif oi_flow < -0.2:
        return weight * -1.0   # opposing flow penalty
    return 0.0


def _score_options_flow(
    prev: dict,
    live_ce: dict,
    live_pe: dict,
    ai_sentiment: Optional[dict],
    weight: float,
    direction: str,
) -> float:
    """
    Pillar 3 — Blended options OI flow + AI sentiment.

    Combines:
      • 60% weight  → raw OI flow imbalance  (_score_options_flow_raw)
      • 40% weight  → AI sentiment from Redis key ``ai:sentiment:{symbol}``

    If the AI sentiment key is absent (no-news day / pipeline not run),
    ``ai_contribution`` is 0 and the raw options score passes through
    unchanged (i.e. the blend simply returns ``options_flow_score * 0.6``
    for the options portion — total weight is preserved because the caller
    already sized ``weight`` to cover the full pillar).

    AI sentiment JSON contract
    --------------------------
    Key  : ``ai:sentiment:{symbol}``
    Value: ``{"score": <float -5 … +5>, ...}``   (extra keys ignored)
    """
    options_flow_score = _score_options_flow_raw(prev, live_ce, live_pe, weight, direction)

    ai_contribution = 0.0
    if ai_sentiment:
        try:
            ai_score_raw = float(ai_sentiment.get("score", 0))  # -5 to +5

            if direction == "LONG":
                if ai_score_raw >= 2.0:
                    ai_contribution = weight * 0.4    # strong bullish AI → +40% of weight
                elif ai_score_raw >= 0.5:
                    ai_contribution = weight * 0.2
                elif ai_score_raw <= -2.0:
                    ai_contribution = weight * -0.3   # AI contradicts → penalty
                else:
                    ai_contribution = 0.0
            else:  # SHORT
                if ai_score_raw <= -2.0:
                    ai_contribution = weight * 0.4
                elif ai_score_raw <= -0.5:
                    ai_contribution = weight * 0.2
                elif ai_score_raw >= 2.0:
                    ai_contribution = weight * -0.3
                else:
                    ai_contribution = 0.0

        except (json.JSONDecodeError, TypeError, ValueError):
            ai_contribution = 0.0

    # ── Blend: 60% options flow + 40% AI sentiment ──────────────────────
    options_contribution = options_flow_score * 0.6
    return options_contribution + ai_contribution


def _score_vwap_trend(snap: dict, weight: float, direction: str) -> float:
    """
    Pillar 4 — VWAP position and slope.

    Keys consumed (guaranteed by candle builder):
        vwap        float  — snap["vwap"]
        vwap_slope  float  — snap["vwap_slope"]
    """
    ltp        = _safe_float(snap.get("ltp"))
    vwap       = _safe_float(snap.get("vwap"), 1.0)       # guaranteed
    vwap_slope = _safe_float(snap.get("vwap_slope"))       # guaranteed

    if direction == "LONG":
        above    = ltp > vwap
        slope_ok = vwap_slope > 0.15
    else:
        above    = ltp < vwap
        slope_ok = vwap_slope < -0.15

    if above and slope_ok:
        return weight * 1.0
    elif above:
        return weight * 0.33
    return 0.0


def _score_choppiness(snap: dict, weight: float) -> float:
    """
    Pillar 5 — Choppiness cleanliness.

    Reads snapshot["choppiness_class"] (guaranteed by candle builder)
    directly instead of re-deriving from raw chop values.
    Falls back to inline computation only if the key is absent
    (e.g. during local testing against old snapshots).
    """
    chop_class = snap.get("choppiness_class", "")          # guaranteed

    if not chop_class:
        # Fallback: re-derive from raw values (backward compat only)
        chop14   = _safe_float(snap.get("choppiness14"),     50.0)
        chop_avg = _safe_float(snap.get("choppiness_5d_avg"), 50.0)
        chop_std = _safe_float(snap.get("choppiness_5d_std"),  5.0)
        upper = chop_avg + 0.5 * chop_std
        lower = chop_avg - 0.5 * chop_std
        if chop14 < lower:
            chop_class = "TRENDING"
        elif chop14 > upper:
            chop_class = "CHOPPY"
        else:
            chop_class = "NEUTRAL"

    if chop_class == "TRENDING":
        return weight * 1.0
    elif chop_class == "NEUTRAL":
        return weight * 0.5
    return 0.0   # CHOPPY


def _resolve_grade_action(total_score: float) -> tuple[str, str]:
    for threshold, grade, action in _GRADE_TABLE:
        if total_score >= threshold:
            return grade, action
    return "IGNORE", "IGNORE"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fetch_scoring_inputs(
    symbol: str,
    signal_direction: str,
    vix: float,
    market_time: str,
) -> dict:
    """Async Redis/network reads required by compute_ici_score."""
    redis = await get_redis()

    snap_raw = await redis.hgetall(f"snapshot:{symbol}")
    snap = snap_raw or {}
    nifty_snap_raw = await redis.hgetall("snapshot:NIFTY")
    nifty_snap = nifty_snap_raw or {}
    nifty_prev  = _safe_float(nifty_snap.get("prev_close"), 1.0)
    nifty_ltp   = _safe_float(nifty_snap.get("ltp"), nifty_prev)
    nifty_change = _safe_divide(nifty_ltp - nifty_prev, nifty_prev) * 100

    sector = snap.get("sector", "UNKNOWN")
    sector_avg_raw = await redis.get(f"market:breadth:sector:{sector}")
    sector_avg_change = float(sector_avg_raw) if sector_avg_raw else 0.0

    prev: dict = {}
    live_ce: dict = {}
    live_pe: dict = {}
    ai_sentiment: Optional[dict] = None

    prev_raw = await redis.get(f"options:prev:{symbol}")
    if prev_raw:
        try:
            prev = json.loads(prev_raw)
            atm = prev.get("atm_strike") or prev.get("atm")
            if atm:
                live_ce = await redis.hgetall(f"options:tick:{symbol}:{atm}CE") or {}
                live_pe = await redis.hgetall(f"options:tick:{symbol}:{atm}PE") or {}
        except json.JSONDecodeError:
            prev = {}

    ai_raw_bytes = await redis.get(f"ai:sentiment:{symbol}")
    if ai_raw_bytes:
        try:
            ai_sentiment = json.loads(ai_raw_bytes)
        except json.JSONDecodeError:
            ai_sentiment = None

    return {
        "snap": snap,
        "nifty_change": nifty_change,
        "sector_avg_change": sector_avg_change,
        "options_prev": prev,
        "options_live_ce": live_ce,
        "options_live_pe": live_pe,
        "ai_sentiment": ai_sentiment,
        "weights": _get_weights(vix, market_time),
    }


def compute_ici_score(
    data: dict,
    symbol: str,
    signal_direction: str,
    signal_type: str,
    active_signals: list[str],
    vix: float,
    market_time: str,
) -> dict:
    """Pure synchronous score computation. No await, no Redis I/O."""
    snap = data.get("snap", {})
    if not snap:
        logger.warning("[scorer] No snapshot for %s — score=0", symbol)
        _now = datetime.now(_IST)
        return _zero_result(symbol, signal_type, _now)

    weights = data.get("weights") or _get_weights(vix, market_time)
    options_score = _score_options_flow(
        data.get("options_prev", {}),
        data.get("options_live_ce", {}),
        data.get("options_live_pe", {}),
        data.get("ai_sentiment"),
        weights["options"],
        signal_direction,
    )

    return _calculate_ici_score_core(
        symbol,
        signal_direction,
        signal_type,
        active_signals,
        weights,
        snap,
        data.get("nifty_change", 0.0),
        data.get("sector_avg_change", 0.0),
        options_score,
    )


async def calculate_ici_score(
    symbol: str,
    signal_direction: str,
    signal_type: str,
    active_signals: list[str],
    vix: float,
    market_time: str,
) -> dict:
    """
    Backward-compatible async wrapper around fetch + pure compute.
    """
    data = await fetch_scoring_inputs(
        symbol=symbol,
        signal_direction=signal_direction,
        vix=vix,
        market_time=market_time,
    )
    return await asyncio.to_thread(
        compute_ici_score,
        data,
        symbol,
        signal_direction,
        signal_type,
        active_signals,
        vix,
        market_time,
    )


def _calculate_ici_score_core(
    symbol: str,
    signal_direction: str,
    signal_type: str,
    active_signals: list[str],
    weights: dict[str, float],
    snap: dict,
    nifty_change: float,
    sector_avg_change: float,
    options_score: float,
) -> dict:
    """
    Compute the Intraday Conviction Index score for a signal.

    Parameters
    ----------
    symbol           : NSE underlying symbol
    signal_direction : "LONG" | "SHORT"
    signal_type      : Primary signal type (e.g. "ORB_BREAKOUT")
    active_signals   : All detected signals for this symbol (for bonus calc)
    vix              : Current India VIX value
    market_time      : IST time string "HH:MM"

    Returns
    -------
    {
        "score":      float (0–100),
        "grade":      str   (GOD | A | B | IGNORE),
        "breakdown":  dict  (per-pillar raw scores),
        "action":     str   (EXECUTE_MARKET | EXECUTE_LIMIT | WATCHLIST | IGNORE),
        "scored_at":  str   (ISO-8601),
        "expires_at": str   (ISO-8601, scored_at + 10 min),
        "symbol":     str,
        "signal_type": str,
    }
    """
    # ── Score each pillar ────────────────────────────────────────────────
    p1 = _score_rvol(snap, weights["rvol"], signal_direction)
    p2 = _score_relative_strength(snap, weights["rs"], signal_direction,
                                   nifty_change, sector_avg_change)
    p3 = options_score
    p4 = _score_vwap_trend(snap, weights["vwap"], signal_direction)
    p5 = _score_choppiness(snap, weights["chop"])

    base_score = p1 + p2 + p3 + p4 + p5

    # ── Signal bonuses ──────────────────────────────────────────────────
    bonus = sum(_SIGNAL_BONUSES.get(sig, 0) for sig in active_signals)
    total_score = min(base_score + bonus, 100.0)
    total_score = max(total_score, 0.0)

    grade, action = _resolve_grade_action(total_score)

    now        = datetime.now(_IST)
    expires_at = now + timedelta(minutes=_SCORE_EXPIRY_MINUTES)

    result = {
        "symbol":       symbol,
        "signal_type":  signal_type,
        "score":        round(total_score, 2),
        "grade":        grade,
        "action":       action,
        "breakdown": {
            "rvol":    round(p1, 2),
            "rs":      round(p2, 2),
            "options": round(p3, 2),
            "vwap":    round(p4, 2),
            "chop":    round(p5, 2),
            "bonus":   bonus,
        },
        "weights":      weights,
        "scored_at":    now.isoformat(),
        "expires_at":   expires_at.isoformat(),
    }

    logger.info(
        "[scorer] %s %s | score=%.1f grade=%s action=%s | "
        "rvol=%.1f rs=%.1f opts=%.1f vwap=%.1f chop=%.1f bonus=%d",
        symbol, signal_direction, total_score, grade, action,
        p1, p2, p3, p4, p5, bonus,
    )

    return result


def sync_calculate_ici_score(
    symbol: str,
    signal_direction: str,
    signal_type: str,
    active_signals: list[str],
    vix: float,
    market_time: str,
    snap: dict,
    nifty_change: float,
    sector_avg_change: float,
    options_score: float,
) -> dict:
    """
    Backward-compatible sync entrypoint retained for existing internal callers.
    """
    weights = _get_weights(vix, market_time)
    return _calculate_ici_score_core(
        symbol,
        signal_direction,
        signal_type,
        active_signals,
        weights,
        snap,
        nifty_change,
        sector_avg_change,
        options_score,
    )


def _zero_result(symbol: str, signal_type: str, now: datetime) -> dict:
    return {
        "symbol":      symbol,
        "signal_type": signal_type,
        "score":       0.0,
        "grade":       "IGNORE",
        "action":      "IGNORE",
        "breakdown":   {"rvol": 0, "rs": 0, "options": 0, "vwap": 0, "chop": 0, "bonus": 0},
        "weights":     {},
        "scored_at":   now.isoformat(),
        "expires_at":  (now + timedelta(minutes=_SCORE_EXPIRY_MINUTES)).isoformat(),
    }
