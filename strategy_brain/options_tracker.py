"""
strategy_brain/options_tracker.py
===================================
Options intelligence layer — called by Brain when a signal passes gates.

Responsibilities
----------------
1. Evaluate live CE/PE data against 5d baselines → explosion ratios.
2. Compute tradability badges (liquidity scoring) per contract.
3. Publish options:subscribe commands to the options WS feed.

Uses
----
    options:prev:{symbol}         — baseline from nightly seeder
    options:tick:{symbol}:{atm}CE — live tick hash (written by options WS)
    options:tick:{symbol}:{atm}PE — live tick hash

Usage
-----
    from strategy_brain.options_tracker import (
        evaluate_options_for_signal,
        compute_tradability_badge,
    )
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from core.redis_client import get_redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_EXPLOSION_VOLUME_THRESHOLD = 3.0   # volume ratio > 3x = explosion
_TRADABILITY_GREEN_THRESHOLD = 70
_TRADABILITY_AMBER_THRESHOLD = 40


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _safe_divide(num: float, den: float, default: float = 0.0) -> float:
    if den == 0:
        return default
    return round(num / den, 4)


async def _load_prev(symbol: str) -> dict:
    redis = await get_redis()
    raw = await redis.get(f"options:prev:{symbol}")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


async def _load_tick(symbol: str, atm: int, opt_type: str) -> dict:
    redis = await get_redis()
    return await redis.hgetall(f"options:tick:{symbol}:{atm}{opt_type}") or {}


async def _load_snapshot(symbol: str) -> dict:
    redis = await get_redis()
    return await redis.hgetall(f"snapshot:{symbol}") or {}


# ---------------------------------------------------------------------------
# Subscribe command publisher
# ---------------------------------------------------------------------------

async def _publish_subscribe(symbol: str, atm: int, prev: dict) -> None:
    """
    Publish options:subscribe command so the options WS feed subscribes
    the CE/PE contracts for this symbol.
    """
    redis = await get_redis()

    # Build contract list — ATM CE + PE.  Token must come from prev baseline.
    ce_token = str(prev.get("ce_token", ""))
    pe_token = str(prev.get("pe_token", ""))

    contracts = []
    if ce_token:
        contracts.append({"token": ce_token, "strike": atm, "type": "CE"})
    if pe_token:
        contracts.append({"token": pe_token, "strike": atm, "type": "PE"})

    if not contracts:
        logger.debug("[options_tracker] No tokens in prev baseline for %s — skipping subscribe", symbol)
        return

    payload = json.dumps({"symbol": symbol, "contracts": contracts})
    await redis.publish("options:subscribe", payload)
    logger.info("[options_tracker] Published options:subscribe for %s atm=%d", symbol, atm)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def evaluate_options_for_signal(symbol: str, direction: str) -> dict:
    """
    Evaluate live options data for a signal that has passed all gates.

    1. Publishes options:subscribe to trigger WS subscription if needed.
    2. Returns current snapshot with explosion ratios.

    Parameters
    ----------
    symbol    : NSE underlying symbol
    direction : "LONG" | "SHORT"

    Returns
    -------
    {
        "atm_strike":      int,
        "ce_ltp":          float,
        "pe_ltp":          float,
        "ce_volume_ratio": float,
        "pe_volume_ratio": float,
        "ce_oi_ratio":     float,
        "pe_oi_ratio":     float,
        "options_explosion": bool,
    }
    """
    prev = await _load_prev(symbol)

    atm = prev.get("atm_strike") or prev.get("atm")
    if not atm:
        logger.warning("[options_tracker] No ATM strike in prev baseline for %s", symbol)
        return _empty_options_result()

    atm = int(atm)

    # Ensure subscription is live
    await _publish_subscribe(symbol, atm, prev)

    live_ce = await _load_tick(symbol, atm, "CE")
    live_pe = await _load_tick(symbol, atm, "PE")

    ce_avg_vol = _safe_float(prev.get("ce_avg_volume_5d"), 1.0)
    pe_avg_vol = _safe_float(prev.get("pe_avg_volume_5d"), 1.0)
    ce_avg_oi  = _safe_float(prev.get("ce_avg_oi_5d"),     1.0)
    pe_avg_oi  = _safe_float(prev.get("pe_avg_oi_5d"),     1.0)

    ce_volume = _safe_float(live_ce.get("volume"))
    pe_volume = _safe_float(live_pe.get("volume"))
    ce_oi     = _safe_float(live_ce.get("oi"))
    pe_oi     = _safe_float(live_pe.get("oi"))

    ce_vol_ratio = _safe_divide(ce_volume, ce_avg_vol)
    pe_vol_ratio = _safe_divide(pe_volume, pe_avg_vol)
    ce_oi_ratio  = _safe_divide(ce_oi,     ce_avg_oi)
    pe_oi_ratio  = _safe_divide(pe_oi,     pe_avg_oi)

    explosion = (
        (direction == "LONG"  and pe_vol_ratio > _EXPLOSION_VOLUME_THRESHOLD) or
        (direction == "SHORT" and ce_vol_ratio > _EXPLOSION_VOLUME_THRESHOLD)
    )

    result = {
        "atm_strike":      atm,
        "ce_ltp":          _safe_float(live_ce.get("ltp")),
        "pe_ltp":          _safe_float(live_pe.get("ltp")),
        "ce_volume_ratio": ce_vol_ratio,
        "pe_volume_ratio": pe_vol_ratio,
        "ce_oi_ratio":     ce_oi_ratio,
        "pe_oi_ratio":     pe_oi_ratio,
        "options_explosion": explosion,
    }

    logger.info(
        "[options_tracker] %s %s | atm=%d ce_vol_ratio=%.2f pe_vol_ratio=%.2f explosion=%s",
        symbol, direction, atm, ce_vol_ratio, pe_vol_ratio, explosion,
    )
    return result


async def compute_tradability_badge(symbol: str, direction: str) -> dict:
    """
    Compute separate liquidity badges for CE and PE contracts.

    Scores are purely liquidity-based — no directional bias.
    Separate CE and PE scores are NEVER averaged.

    Parameters
    ----------
    symbol    : NSE underlying symbol
    direction : "LONG" | "SHORT" — used to determine which side is "primary"

    Returns
    -------
    {
        "ce": {"score": int, "badge": str, "spread_pct": float, "abs_slippage": float},
        "pe": {"score": int, "badge": str, "spread_pct": float, "abs_slippage": float},
        "primary_side": "CE" | "PE",
    }
    """
    prev = await _load_prev(symbol)
    snap = await _load_snapshot(symbol)

    atm = prev.get("atm_strike") or prev.get("atm")
    if not atm:
        return {"ce": _illiquid_result(), "pe": _illiquid_result(), "primary_side": "CE"}

    atm      = int(atm)
    lot_size = int(_safe_float(prev.get("lot_size"), 1))

    ce_avg_vol = _safe_float(prev.get("ce_avg_volume_5d"), 1.0)
    pe_avg_vol = _safe_float(prev.get("pe_avg_volume_5d"), 1.0)

    async def _score_contract(opt_type: str, avg_vol: float) -> dict:
        data = await _load_tick(symbol, atm, opt_type)

        bid = _safe_float(data.get("bid"))
        ask = _safe_float(data.get("ask"))
        mid = (bid + ask) / 2

        if mid <= 0:
            return _illiquid_result()

        spread_pct   = (ask - bid) / mid * 100
        abs_slippage = (ask - bid) * lot_size
        tick_rate    = _safe_float(data.get("ticks_per_min"))
        volume       = _safe_float(data.get("volume"))
        vol_ratio    = _safe_divide(volume, avg_vol)

        score = 100

        # Spread penalty
        if spread_pct > 3.0:
            score -= 40
        elif spread_pct > 1.5:
            score -= 20

        # Slippage penalty
        if abs_slippage > 600:
            score -= 30
        elif abs_slippage > 300:
            score -= 15

        # Tick rate penalty
        if tick_rate < 2:
            score -= 20
        elif tick_rate < 5:
            score -= 10

        # Volume ratio penalty
        if vol_ratio < 0.4:
            score -= 20
        elif vol_ratio < 0.8:
            score -= 10

        score = max(0, score)

        if score >= _TRADABILITY_GREEN_THRESHOLD:
            badge = "GREEN"
        elif score >= _TRADABILITY_AMBER_THRESHOLD:
            badge = "AMBER"
        else:
            badge = "RED"

        return {
            "score":        score,
            "badge":        badge,
            "spread_pct":   round(spread_pct,   2),
            "abs_slippage": round(abs_slippage, 2),
            "tick_rate":    round(tick_rate,     2),
            "vol_ratio":    round(vol_ratio,     2),
        }

    ce_result = await _score_contract("CE", ce_avg_vol)
    pe_result = await _score_contract("PE", pe_avg_vol)

    # Primary = the side you'd trade (CE for LONG, PE for SHORT)
    # But also respect supertrend direction as a secondary signal
    st_dir = snap.get("supertrend_dir", "BULL")
    if direction == "LONG":
        primary = "CE"
    elif direction == "SHORT":
        primary = "PE"
    else:
        primary = "CE" if st_dir == "BULL" else "PE"

    logger.info(
        "[options_tracker] %s tradability | CE: %s(%d) PE: %s(%d) primary=%s",
        symbol,
        ce_result["badge"], ce_result["score"],
        pe_result["badge"], pe_result["score"],
        primary,
    )

    return {"ce": ce_result, "pe": pe_result, "primary_side": primary}


# ---------------------------------------------------------------------------
# Fallback constructors
# ---------------------------------------------------------------------------

def _empty_options_result() -> dict:
    return {
        "atm_strike":      None,
        "ce_ltp":          0.0,
        "pe_ltp":          0.0,
        "ce_volume_ratio": 0.0,
        "pe_volume_ratio": 0.0,
        "ce_oi_ratio":     0.0,
        "pe_oi_ratio":     0.0,
        "options_explosion": False,
    }


def _illiquid_result() -> dict:
    return {
        "score":        0,
        "badge":        "ILLIQUID",
        "spread_pct":   None,
        "abs_slippage": None,
        "tick_rate":    None,
        "vol_ratio":    None,
    }
