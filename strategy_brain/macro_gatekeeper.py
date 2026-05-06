"""
strategy_brain/macro_gatekeeper.py
=====================================
Macro Gatekeeper v2 — 3 hard gates only.
Replaces the old 6-gate design.

Gate 1: RSI Extremes   — LONG blocked if RSI > 80, SHORT if RSI < 20
                         Bypassed before 10:30 AM (RSI unreliable on 5m)
Gate 2: EMA9 Extension — LTP > 3% away from EMA9 (both directions)
Gate 3: Minimum ATR    — ATR14 < 0.2% of LTP (dead stock filter)

Public API (backward compatible with brain.py):
    passed, failed_gates = await check_macro_gates(symbol, direction)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.redis_client import get_redis

logger = logging.getLogger(__name__)

_IST = timezone(timedelta(hours=5, minutes=30))


def _sf(v, d: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def _now_ist() -> datetime:
    return datetime.now(_IST)


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

    ltp   = _get("ltp")
    rsi14 = _get("rsi14", 50.0)
    ema9  = _get("ema9")
    atr14 = _get("atr14")
    now   = _now_ist()

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

    # ── Gate 3: Minimum ATR ──────────────────────────────────────────────
    if ltp > 0 and atr14 > 0:
        atr_pct = atr14 / ltp * 100
        if atr_pct < 0.2:
            failed.append("ATR_TOO_LOW")
            logger.info("[gate3] %s BLOCKED ATR=%.3f%%", symbol, atr_pct)

    passed = len(failed) == 0
    if passed:
        logger.debug("[gatekeeper] %s %s PASSED all gates", symbol, signal_direction)
    else:
        logger.info("[gatekeeper] %s %s BLOCKED %s", symbol, signal_direction, failed)

    return passed, failed
