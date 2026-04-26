"""
strategy_brain/macro_gatekeeper.py
====================================
Gate 0 — Kill switch layer.

Every signal must pass all six gates before being forwarded to the
conviction scorer.  If any gate fails, score = 0 and the signal is
either dropped or (for WAIT_RETEST) parked in the retest watchlist.

Usage
-----
    from strategy_brain.macro_gatekeeper import check_macro_gates

    passed, failed_gates = await check_macro_gates("RELIANCE", "LONG")
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from core.redis_client import get_redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Optional[str], default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


async def _load_snapshot(symbol: str) -> dict:
    redis = await get_redis()
    raw = await redis.hgetall(f"snapshot:{symbol}")
    return raw or {}


async def _load_options_prev(symbol: str) -> dict:
    redis = await get_redis()
    raw = await redis.get(f"options:prev:{symbol}")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


async def _load_options_tick(symbol: str, strike: int, opt_type: str) -> dict:
    """Load live options tick hash from Redis."""
    redis = await get_redis()
    key = f"options:tick:{symbol}:{strike}{opt_type}"
    return await redis.hgetall(key) or {}


# ---------------------------------------------------------------------------
# Gate implementations
# ---------------------------------------------------------------------------

async def _gate1_choppiness(snap: dict) -> tuple[bool, str]:
    """Gate 1 — Choppiness veto: reads pre-classified choppiness_class written by candle_builder."""
    chop_class = snap.get("choppiness_class", "NEUTRAL")
    if chop_class == "CHOPPY":
        logger.debug(
            "[gate1] CHOPPY_MARKET chop_class=%s", chop_class
        )
        return False, "CHOPPY_MARKET"
    return True, ""


async def _gate2_vwap(snap: dict, direction: str) -> tuple[bool, str]:
    """Gate 2 — VWAP veto: long signals must be above VWAP."""
    if direction != "LONG":
        return True, ""

    ltp  = _safe_float(snap.get("ltp"))
    vwap = _safe_float(snap.get("vwap"), 1.0)

    if ltp < vwap:
        logger.debug("[gate2] BELOW_VWAP ltp=%.2f vwap=%.2f", ltp, vwap)
        return False, "BELOW_VWAP"
    return True, ""


async def _gate3_nifty_trend(direction: str) -> tuple[bool, str]:
    """Gate 3 — Nifty trend veto: don't fight the index supertrend."""
    nifty_snap = await _load_snapshot("NIFTY")
    st_dir = nifty_snap.get("supertrend_dir", "BULL")

    if st_dir == "BEAR" and direction == "LONG":
        logger.debug("[gate3] NIFTY_BEARISH — blocking LONG")
        return False, "NIFTY_BEARISH"
    if st_dir == "BULL" and direction == "SHORT":
        logger.debug("[gate3] NIFTY_BULLISH — blocking SHORT")
        return False, "NIFTY_BULLISH"
    return True, ""


def _rsi_gate(
    rsi: float,
    choppiness_class: str,
    signal_direction: str,
) -> tuple[bool, str]:
    """
    Regime-aware RSI gate (pure, sync — called from async _gate4 wrapper).

    Levels by regime
    ----------------
    TRENDING       : exhaustion at 78/22, momentum floor at 50 (LONG) / ceiling at 50 (SHORT)
    NEUTRAL/CHOPPY : exhaustion at 70/30, momentum floor at 45 (LONG) / ceiling at 55 (SHORT)
    Hard ceiling   : 85/15 — vetoed regardless of regime
    """
    # Hard ceiling — no exceptions
    if signal_direction == "LONG" and rsi > 85:
        return False, "RSI_EXTREME_EXHAUSTION"
    if signal_direction == "SHORT" and rsi < 15:
        return False, "RSI_EXTREME_EXHAUSTION"

    if choppiness_class == "TRENDING":
        # Trending — RSI can run higher
        if signal_direction == "LONG" and rsi > 78:
            return False, "RSI_EXHAUSTED"
        if signal_direction == "SHORT" and rsi < 22:
            return False, "RSI_EXHAUSTED"
        if signal_direction == "LONG" and rsi < 50:
            return False, "RSI_NO_MOMENTUM"
        if signal_direction == "SHORT" and rsi > 50:
            return False, "RSI_NO_MOMENTUM"
    else:
        # NEUTRAL or CHOPPY — tighter ceiling
        if signal_direction == "LONG" and rsi > 70:
            return False, "RSI_EXHAUSTED"
        if signal_direction == "SHORT" and rsi < 30:
            return False, "RSI_EXHAUSTED"
        if signal_direction == "LONG" and rsi < 45:
            return False, "RSI_NO_MOMENTUM"
        if signal_direction == "SHORT" and rsi > 55:
            return False, "RSI_NO_MOMENTUM"

    return True, ""


async def _gate4_rsi_exhaustion(snap: dict, direction: str) -> tuple[bool, str]:
    """Gate 4 — RSI exhaustion veto: don't chase overbought/oversold."""
    rsi14            = _safe_float(snap.get("rsi14"), 50.0)
    choppiness_class = snap.get("choppiness_class", "NEUTRAL")

    passed, label = _rsi_gate(rsi14, choppiness_class, direction)
    if not passed:
        logger.debug(
            "[gate4] %s rsi14=%.1f choppiness_class=%s direction=%s",
            label, rsi14, choppiness_class, direction,
        )
        return False, label
    return True, ""


async def _gate5_max_extension(snap: dict, direction: str) -> tuple[bool, str]:
    """
    Gate 5 — Max extension veto.

    Returns:
        (False, "OVEREXTENDED_IGNORE")  — hard veto, drop signal
        (False, "WAIT_RETEST")          — soft veto, park in retest watchlist
        (True,  "")                     — passes
    """
    ltp  = _safe_float(snap.get("ltp"),   1.0)
    ema9 = _safe_float(snap.get("ema9"),  ltp)
    vwap = _safe_float(snap.get("vwap"),  ltp)
    atr  = _safe_float(snap.get("atr14"), 1.0)

    if ema9 == 0:
        ema9 = ltp
    if vwap == 0:
        vwap = ltp

    if direction == "LONG":
        ema_dist  = (ltp - ema9) / ema9 * 100  if ema9 else 0.0
        vwap_dist = (ltp - vwap) / vwap * 100  if vwap else 0.0
    else:
        ema_dist  = (ema9 - ltp) / ema9 * 100  if ema9 else 0.0
        vwap_dist = (vwap - ltp) / vwap * 100  if vwap else 0.0

    if ema_dist > 3.0:
        logger.debug("[gate5] OVEREXTENDED_IGNORE ema_dist=%.2f%%", ema_dist)
        return False, "OVEREXTENDED_IGNORE"

    if 1.5 <= ema_dist <= 3.0:
        logger.debug("[gate5] WAIT_RETEST ema_dist=%.2f%% — parking signal", ema_dist)
        return False, "WAIT_RETEST"

    return True, ""


async def _gate6_opposing_options_flow(
    symbol: str, snap: dict, direction: str
) -> tuple[bool, str]:
    """Gate 6 — Opposing options flow veto."""
    prev = await _load_options_prev(symbol)
    if not prev:
        # No baseline — can't evaluate; pass through (benefit of the doubt)
        logger.debug("[gate6] No options:prev for %s — skipping gate", symbol)
        return True, ""

    atm_strike = prev.get("atm_strike") or prev.get("atm")
    if not atm_strike:
        return True, ""

    live_ce = await _load_options_tick(symbol, atm_strike, "CE")
    live_pe = await _load_options_tick(symbol, atm_strike, "PE")

    prev_ce_oi = _safe_float(prev.get("ce_oi_prev"))
    prev_pe_oi = _safe_float(prev.get("pe_oi_prev"))

    live_ce_oi = _safe_float(live_ce.get("oi"))
    live_pe_oi = _safe_float(live_pe.get("oi"))

    ce_oi_change = live_ce_oi - prev_ce_oi
    pe_oi_change = live_pe_oi - prev_pe_oi

    if direction == "LONG" and ce_oi_change > pe_oi_change * 2:
        logger.debug(
            "[gate6] OPPOSING_OPTIONS_FLOW ce_chg=%.0f pe_chg=%.0f",
            ce_oi_change, pe_oi_change
        )
        return False, "OPPOSING_OPTIONS_FLOW"

    # Mirror check for SHORT signals
    if direction == "SHORT" and pe_oi_change > ce_oi_change * 2:
        logger.debug(
            "[gate6] OPPOSING_OPTIONS_FLOW (short) pe_chg=%.0f ce_chg=%.0f",
            pe_oi_change, ce_oi_change
        )
        return False, "OPPOSING_OPTIONS_FLOW"

    return True, ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def check_macro_gates(
    symbol: str,
    signal_direction: str,
) -> tuple[bool, list[str]]:
    """
    Run all six macro kill switches for *symbol* / *signal_direction*.

    Parameters
    ----------
    symbol           : Underlying NSE symbol, e.g. "RELIANCE"
    signal_direction : "LONG" | "SHORT"

    Returns
    -------
    (passed: bool, failed_gates: list[str])

    *passed* is True only when **all** gates clear.
    *failed_gates* contains labels of every failed gate.

    Special label "WAIT_RETEST" in *failed_gates* means the signal should be
    parked in the retest watchlist rather than discarded outright.
    """
    snap = await _load_snapshot(symbol)
    if not snap:
        logger.warning("[macro_gatekeeper] No snapshot for %s — all gates fail", symbol)
        return False, ["NO_SNAPSHOT"]

    failed: list[str] = []

    # Run gates sequentially; collect all failures (don't short-circuit)
    # so the caller can see the complete failure picture.
    for gate_fn, args in [
        (_gate1_choppiness,          (snap,)),
        (_gate2_vwap,                (snap, signal_direction)),
        (_gate3_nifty_trend,         (signal_direction,)),
        (_gate4_rsi_exhaustion,      (snap, signal_direction)),
        (_gate5_max_extension,       (snap, signal_direction)),
        (_gate6_opposing_options_flow, (symbol, snap, signal_direction)),
    ]:
        passed_gate, label = await gate_fn(*args)  # type: ignore[operator]
        if not passed_gate:
            failed.append(label)

    all_passed = len(failed) == 0

    if not all_passed:
        logger.info(
            "[macro_gatekeeper] %s %s — BLOCKED gates=%s",
            symbol, signal_direction, failed
        )
    else:
        logger.debug("[macro_gatekeeper] %s %s — ALL GATES PASSED", symbol, signal_direction)

    return all_passed, failed
