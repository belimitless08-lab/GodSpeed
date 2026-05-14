"""
strategy_brain/macro_gatekeeper.py
=====================================
Macro Gatekeeper v2 — 4 hard gates.
Replaces the old 6-gate design.

Gate 1: RSI Extremes   — LONG blocked if RSI > 80, SHORT if RSI < 20
                         Bypassed before 10:30 AM. Uses 5m RSI.
Gate 2: EMA9 Extension — LTP > 3% away from EMA9 (both directions)
                         Uses 5m EMA9 updated on 5m close.
Gate 3: ORB Range Hold — Active after 9:30 AM only.
                         Blocks if LTP is inside 9:15–9:30 opening range.
                         Stock still consolidating — no edge to trade.

Public API (backward compatible with brain.py):
    passed, failed_gates = await check_macro_gates(symbol, direction)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.redis_client import get_redis

logger = logging.getLogger(__name__)

_IST = timezone(timedelta(hours=5, minutes=30))

DRCG_LOOKBACK        = 8     # 40 min rolling window
DRCG_MIN_COIL        = 5     # 25 min minimum coiling
DRCG_MAX_WIDTH_PCT   = 2.5   # Will be overridden by ATR check below
DRCG_BREAKOUT_BUFFER = 0.08  # 0.08% buffer against fakeouts


def _sf(v, d: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def _now_ist() -> datetime:
    return datetime.now(_IST)


async def _is_consolidating(redis_client, symbol: str, ltp: float) -> tuple[bool, str]:
    """
    Dynamic Rolling Consolidation Gate.
    Reads last 8 x 5m candles from Redis. Returns (blocked, reason).
    """
    if ltp <= 0 or not symbol:
        return False, "DRCG:ALLOW_BAD_INPUT"
    try:
        raw = await redis_client.lrange(f"candles:5m:{symbol}", -DRCG_LOOKBACK, -1)
    except Exception:
        return False, "DRCG:ALLOW_REDIS_ERROR"

    if not raw or len(raw) < DRCG_LOOKBACK:
        return False, "DRCG:ALLOW_INSUFFICIENT_DATA"

    try:
        candles = [json.loads(c) for c in raw]
        # Candles stored as [ts, open, high, low, close, volume]
        highs = [c[2] for c in candles]
        lows  = [c[3] for c in candles]
    except Exception:
        return False, "DRCG:ALLOW_PARSE_ERROR"

    rolling_high    = max(highs)
    rolling_low     = min(lows)
    range_width_pct = (rolling_high - rolling_low) / rolling_low * 100 if rolling_low > 0 else 0

    # Immediate breakout on latest candle → allow
    curr = candles[-1]
    if curr[2] > rolling_high or curr[3] < rolling_low:
        return False, f"DRCG:ALLOW_BREAKOUT({range_width_pct:.2f}%)"

    # Range too wide relative to stock's ATR → not consolidation → allow
    # Use ATR-relative threshold: if range > 1.5x ATR, it's a wide range
    # This handles high-volatility stocks correctly (CANBK, BANKNIFTY etc.)
    # Note: atr14 not in snapshot here — use range_pct proxy
    # Tight consolidation = range < 1.5 ATR ≈ range_pct < 1.2% for most F&O
    if range_width_pct >= 1.2:
        return False, f"DRCG:ALLOW_WIDE({range_width_pct:.2f}%)"

    # Count consecutive closes inside buffered range
    buf_high = rolling_high * (1 + DRCG_BREAKOUT_BUFFER / 100)
    buf_low  = rolling_low  * (1 - DRCG_BREAKOUT_BUFFER / 100)
    coil = 0
    for c in reversed(candles):
        if buf_low <= c[4] <= buf_high:
            coil += 1
        else:
            break

    if coil < DRCG_MIN_COIL:
        return False, f"DRCG:ALLOW_SHORT_COIL({coil})"

    return True, f"DRCG:BLOCK tight={range_width_pct:.2f}% coil={coil}c [{rolling_low:.2f}–{rolling_high:.2f}]"


async def check_macro_gates(
    symbol: str,
    signal_direction: str,
) -> tuple[bool, list[str]]:
    """
    Run all 3 macro gates for symbol / direction.
    Returns (passed: bool, failed_gates: list[str])
    passed is True only when all gates clear.
    """
    redis = await get_redis()
    snap  = await redis.hgetall(f"snapshot:{symbol}")

    if not snap:
        logger.warning("[gatekeeper] No snapshot for %s", symbol)
        return False, ["NO_SNAPSHOT"]

    def _get(key: str, d: float = 0.0) -> float:
        v = snap.get(key.encode()) or snap.get(key)
        return _sf(v, d)

    ltp      = _get("ltp")
    # Use 5m RSI (seeded and updated on 5m closes) when available.
    # Falls back to base RSI value in snapshot if 5m RSI is not yet populated.
    rsi14    = _get("rsi14_5m") if _get("rsi14_5m") > 0 else _get("rsi14", 50.0)
    ema9     = _get("ema9")
    orb_high = _get("orb_high", 0.0)
    orb_low  = _get("orb_low",  0.0)
    now      = _now_ist()

    if ltp <= 0:
        return False, ["NO_LTP"]

    failed: list[str] = []

    # ── Gate 1: RSI Extremes (bypassed before 10:30 AM) ─────────────────
    gate1_active = not (now.hour == 9 or (now.hour == 10 and now.minute < 30))
    if gate1_active:
        if signal_direction == "LONG"  and rsi14 > 80:
            failed.append("RSI_OVERBOUGHT")
            logger.info("[gate1] %s BLOCKED RSI=%.1f overbought", symbol, rsi14)
        if signal_direction == "SHORT" and rsi14 < 20:
            failed.append("RSI_OVERSOLD")
            logger.info("[gate1] %s BLOCKED RSI=%.1f oversold", symbol, rsi14)

    # ── Gate 2: EMA9 Extension (>3%) ────────────────────────────────────
    if ema9 > 0:
        ext_pct = abs(ltp - ema9) / ema9 * 100
        if ext_pct > 3.0:
            failed.append("EMA9_EXTENDED")
            logger.info("[gate2] %s BLOCKED EMA9 ext=%.2f%%", symbol, ext_pct)


    # ── Gate 3: ORB Range Hold (active 9:30 AM – ~9:55 AM window) ───────
    gate3_active = (now.hour == 9 and now.minute >= 30) or now.hour > 9
    if gate3_active and orb_high > 0 and orb_low > 0:
        orb_range_pct = (orb_high - orb_low) / ltp * 100 if ltp > 0 else 0
        if orb_range_pct >= 0.3 and orb_low < ltp < orb_high:
            failed.append("IN_ORB_RANGE")
            logger.info(
                "[gate3] %s BLOCKED — LTP %.2f inside ORB [%.2f–%.2f] range=%.2f%%",
                symbol, ltp, orb_low, orb_high, orb_range_pct,
            )

    # ── Gate 4: DRCG — Dynamic Rolling Consolidation (all day) ─────────
    _drcg_blocked, _drcg_reason = await _is_consolidating(redis, symbol, ltp)
    if _drcg_blocked:
        failed.append("DYNAMIC_CONSOLIDATION")
        logger.info("[gate4] %s %s", symbol, _drcg_reason)

    passed = len(failed) == 0
    if passed:
        logger.debug("[gatekeeper] %s %s PASSED all gates", symbol, signal_direction)
    else:
        logger.info("[gatekeeper] %s %s BLOCKED %s", symbol, signal_direction, failed)

    return passed, failed
