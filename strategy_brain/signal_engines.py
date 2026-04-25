"""
strategy_brain/signal_engines.py
==================================
Signal detection layer — Node 3a.

Detects raw trading signals from pre-calculated snapshot data.
Does NOT score or gate — purely structural pattern recognition.

Each detector returns a signal dict or None.

Snapshot field contract (written by candle builder, all guaranteed):
    vwap              float  — current VWAP
    vwap_slope        float  — % slope over last 5 candles
    rsi14             float  — current RSI-14
    choppiness_class  str    — "TRENDING" | "NEUTRAL" | "CHOPPY"

Usage
-----
    from strategy_brain.signal_engines import scan_all_signals

    signals = await scan_all_signals("RELIANCE")
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.redis_client import get_redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_IST = timezone(timedelta(hours=5, minutes=30))

_OPENING_DRIVE_END_H = 10
_OPENING_DRIVE_END_M = 0   # runs 9:15 – 10:00


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Optional[str], default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _now_ist() -> datetime:
    return datetime.now(_IST)


async def _load_snapshot(symbol: str) -> dict:
    redis = await get_redis()
    raw = await redis.hgetall(f"snapshot:{symbol}")
    return raw or {}


async def _load_candles_5m(symbol: str, n: int = 15) -> list[dict]:
    """
    Load last *n* five-minute candles from Redis list candles:5m:{symbol}.
    Each candle stored as JSON string: {ts, open, high, low, close, volume}.
    """
    redis = await get_redis()
    raw_list = await redis.lrange(f"candles:5m:{symbol}", -n, -1)
    candles = []
    for raw in raw_list:
        try:
            candles.append(json.loads(raw))
        except (json.JSONDecodeError, TypeError):
            continue
    return candles


async def _load_prev_snapshot(symbol: str) -> dict:
    """Load previous-candle snapshot from Redis (stored by candle builder)."""
    redis = await get_redis()
    raw = await redis.get(f"snapshot_prev:{symbol}")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _now_ist_str() -> str:
    return _now_ist().isoformat()


# ---------------------------------------------------------------------------
# Individual signal detectors
# ---------------------------------------------------------------------------

def _detect_opening_drive(snapshot: dict, candles_5m: list[dict]) -> Optional[dict]:
    """
    Opening Drive — fires 9:15–10:00 AM only.
    Requires: close > R1, body ratio > 60%, RVOL > 2x, gap < 3%.
    """
    now = _now_ist()
    after_open  = now.hour == 9 or (now.hour == _OPENING_DRIVE_END_H and now.minute < _OPENING_DRIVE_END_M)
    if not after_open:
        return None

    if not candles_5m:
        return None

    current_5m = candles_5m[-1]
    o = _safe_float(current_5m.get("open"))
    c = _safe_float(current_5m.get("close"))
    h = _safe_float(current_5m.get("high"))
    lo = _safe_float(current_5m.get("low"))

    body        = abs(c - o)
    candle_range = h - lo
    body_ratio  = body / max(candle_range, 0.01)

    pivot_r1   = _safe_float(snapshot.get("r1")) or _safe_float(snapshot.get("pivot_r1"))
    rvol_open  = _safe_float(snapshot.get("rvol_open"),  _safe_float(snapshot.get("rvol")))
    gap_pct    = abs(_safe_float(snapshot.get("gap_pct")))
    atr14      = _safe_float(snapshot.get("atr14"), 1.0)

    above_r1   = c > pivot_r1
    good_body  = body_ratio > 0.6
    volume_ok  = rvol_open > 2.0
    gap_ok     = gap_pct < 3.0

    if above_r1 and good_body and volume_ok and gap_ok:
        entry = pivot_r1
        stop  = pivot_r1 - atr14 * 0.5
        logger.info("[signal] OPENING_DRIVE detected for snapshot body_ratio=%.2f rvol=%.2f",
                    body_ratio, rvol_open)
        return {
            "type":        "OPENING_DRIVE",
            "direction":   "LONG",
            "entry_price": round(entry, 2),
            "stop_loss":   round(stop,  2),
            "detected_at": _now_ist_str(),
        }
    return None


def _detect_orb_breakout(snapshot: dict, candles_5m: list[dict]) -> Optional[dict]:
    """
    ORB Breakout — Opening Range Breakout (set at 9:30 AM).
    Waits ≥ 2 candles after ORB establishment; non-extended breakout.
    """
    if not candles_5m:
        return None

    current_5m = candles_5m[-1]
    c = _safe_float(current_5m.get("close"))

    orb_high         = _safe_float(snapshot.get("orb_high"))
    orb_low          = _safe_float(snapshot.get("orb_low"))
    orb_range_pct    = _safe_float(snapshot.get("orb_range_pct"))
    candles_since_orb = int(_safe_float(snapshot.get("candles_since_orb"), 0))
    rvol             = _safe_float(snapshot.get("rvol"))

    if orb_high == 0:
        return None  # ORB not yet established

    orb_extended     = orb_range_pct > 1.5
    consolidation    = candles_since_orb >= 2
    breakout         = c > orb_high
    not_overextended = c < orb_high * 1.01
    volume_ok        = rvol > 2.0

    if not orb_extended and consolidation and breakout and not_overextended and volume_ok:
        logger.info("[signal] ORB_BREAKOUT detected orb_high=%.2f rvol=%.2f", orb_high, rvol)
        return {
            "type":        "ORB_BREAKOUT",
            "direction":   "LONG",
            "entry_price": round(orb_high, 2),
            "stop_loss":   round(orb_low,  2),
            "detected_at": _now_ist_str(),
        }
    return None


def _detect_hourly_breakout(snapshot: dict, candles_5m: list[dict]) -> Optional[dict]:
    """
    Hourly Breakout — rolling 1-hour (12 x 5m candles) high break.
    Requires RSI 55–68, positive VWAP slope, RVOL > 1.5x.

    Keys consumed (all guaranteed by candle builder):
        rsi14       float  — snapshot["rsi14"]
        vwap        float  — snapshot["vwap"]
        vwap_slope  float  — snapshot["vwap_slope"]
    """
    if not candles_5m:
        return None

    current_5m = candles_5m[-1]
    c = _safe_float(current_5m.get("close"))
    lo = _safe_float(current_5m.get("low"))

    rolling_1h_high      = _safe_float(snapshot.get("rolling_1h_high"))
    prev_rolling_1h_high = _safe_float(snapshot.get("prev_rolling_1h_high"), rolling_1h_high)
    rsi14       = _safe_float(snapshot.get("rsi14"), 50.0)          # guaranteed
    vwap_slope  = _safe_float(snapshot.get("vwap_slope"))            # guaranteed
    rvol        = _safe_float(snapshot.get("rvol"))
    vwap        = _safe_float(snapshot.get("vwap"))                  # guaranteed

    if rolling_1h_high == 0:
        return None

    breakout      = c > rolling_1h_high
    not_lower_high = rolling_1h_high >= prev_rolling_1h_high
    rsi_ok         = 55 <= rsi14 <= 68
    vwap_slope_ok  = vwap_slope > 0
    volume_ok      = rvol > 1.5

    if breakout and not_lower_high and rsi_ok and vwap_slope_ok and volume_ok:
        stop = max(vwap, lo)
        logger.info("[signal] HOURLY_BREAKOUT detected high=%.2f rsi=%.1f rvol=%.2f",
                    rolling_1h_high, rsi14, rvol)
        return {
            "type":        "HOURLY_BREAKOUT",
            "direction":   "LONG",
            "entry_price": round(rolling_1h_high, 2),
            "stop_loss":   round(stop, 2),
            "detected_at": _now_ist_str(),
        }
    return None


def _detect_choppiness_breakout(snapshot: dict, candles_5m: list[dict]) -> Optional[dict]:
    """
    Choppiness Breakout — price escapes a consolidation range.
    8–25 consecutive choppy candles, then price breaks range high.

    Uses snapshot["choppiness_class"] (guaranteed by candle builder) to
    confirm the breakout instead of re-computing from raw chop values.
    """
    if not candles_5m:
        return None

    current_5m = candles_5m[-1]
    c  = _safe_float(current_5m.get("close"))
    lo = _safe_float(current_5m.get("low"))

    choppy_count      = int(_safe_float(snapshot.get("consecutive_choppy_candles"), 0))
    choppy_range_high = _safe_float(snapshot.get("choppy_range_high"))
    rvol              = _safe_float(snapshot.get("rvol"))
    chop14            = _safe_float(snapshot.get("choppiness14"),     50.0)
    chop_avg          = _safe_float(snapshot.get("choppiness_5d_avg"), 50.0)

    if choppy_range_high == 0:
        return None

    enough_compression = 8 <= choppy_count <= 25
    price_breaks_range = c > choppy_range_high
    volume_ok          = rvol > 1.5

    if enough_compression and price_breaks_range and volume_ok:
        # Use choppiness_class from candle builder when available;
        # fall back to inline comparison for backward compatibility.
        chop_class = snapshot.get("choppiness_class", "")
        if chop_class:
            confirmed = chop_class == "TRENDING"
        else:
            confirmed = chop14 < chop_avg

        comp_strength = round(choppy_count / 25, 3)

        sig_type = ("CHOPPINESS_BREAKOUT_CONFIRMED"
                    if confirmed else "CHOPPINESS_BREAKOUT_UNCONFIRMED")

        logger.info("[signal] %s detected choppy_count=%d rvol=%.2f confirmed=%s",
                    sig_type, choppy_count, rvol, confirmed)
        return {
            "type":                sig_type,
            "direction":           "LONG",
            "entry_price":         round(choppy_range_high, 2),
            "stop_loss":           round(lo, 2),
            "compression_strength": comp_strength,
            "detected_at":         _now_ist_str(),
        }
    return None


def _detect_supertrend_flip(snapshot: dict, prev_snapshot: dict) -> Optional[dict]:
    """
    Supertrend Flip — direction change on latest candle close.
    """
    curr_dir = snapshot.get("supertrend_dir", "")
    prev_dir = prev_snapshot.get("supertrend_dir", "")

    if not curr_dir or not prev_dir:
        return None

    flipped = curr_dir != prev_dir
    if flipped:
        direction = "LONG" if curr_dir == "BULL" else "SHORT"
        ltp   = _safe_float(snapshot.get("ltp"))
        band  = _safe_float(snapshot.get("supertrend_band"))

        logger.info("[signal] SUPERTREND_FLIP detected direction=%s ltp=%.2f band=%.2f",
                    direction, ltp, band)
        return {
            "type":        "SUPERTREND_FLIP",
            "direction":   direction,
            "entry_price": round(ltp,  2),
            "stop_loss":   round(band, 2),
            "detected_at": _now_ist_str(),
        }
    return None


def _detect_volume_surge(snapshot: dict) -> Optional[dict]:
    """
    Volume Surge — standalone auxiliary signal (no direction).
    Returned alongside directional signals; feeds bonus in scorer.
    """
    rvol = _safe_float(snapshot.get("live_volume_ratio"),
                       _safe_float(snapshot.get("rvol")))

    if rvol >= 3.0:
        logger.debug("[signal] VOLUME_SURGE_STRONG rvol=%.2f", rvol)
        return {"type": "VOLUME_SURGE_STRONG",  "rvol": round(rvol, 2), "detected_at": _now_ist_str()}
    elif rvol >= 2.0:
        logger.debug("[signal] VOLUME_SURGE_MODERATE rvol=%.2f", rvol)
        return {"type": "VOLUME_SURGE_MODERATE", "rvol": round(rvol, 2), "detected_at": _now_ist_str()}
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def scan_all_signals(symbol: str) -> list[dict]:
    """
    Scan all signal detectors for *symbol* and return detected signals.

    Parameters
    ----------
    symbol : NSE underlying symbol

    Returns
    -------
    list[dict] — each element is a signal dict with keys:
        type, direction (if directional), entry_price, stop_loss, detected_at
        Additional keys vary by signal type.
    """
    snapshot      = await _load_snapshot(symbol)
    prev_snapshot = await _load_prev_snapshot(symbol)
    candles_5m    = await _load_candles_5m(symbol, n=15)

    if not snapshot:
        logger.warning("[signal_engines] No snapshot for %s — skipping scan", symbol)
        return []

    detected: list[dict] = []

    # ── Directional signals ─────────────────────────────────────────────
    for detector, args in [
        (_detect_opening_drive,      (snapshot, candles_5m)),
        (_detect_orb_breakout,       (snapshot, candles_5m)),
        (_detect_hourly_breakout,    (snapshot, candles_5m)),
        (_detect_choppiness_breakout,(snapshot, candles_5m)),
    ]:
        try:
            sig = detector(*args)
            if sig:
                sig["symbol"] = symbol
                detected.append(sig)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[signal_engines] Detector %s failed for %s: %s",
                           detector.__name__, symbol, exc)

    # Supertrend flip needs prev snapshot
    try:
        sig = _detect_supertrend_flip(snapshot, prev_snapshot)
        if sig:
            sig["symbol"] = symbol
            detected.append(sig)
    except Exception as exc:
        logger.warning("[signal_engines] supertrend_flip failed for %s: %s", symbol, exc)

    # ── Volume surge (auxiliary, direction-neutral) ─────────────────────
    try:
        surge = _detect_volume_surge(snapshot)
        if surge:
            surge["symbol"] = symbol
            detected.append(surge)
    except Exception as exc:
        logger.warning("[signal_engines] volume_surge failed for %s: %s", symbol, exc)

    if detected:
        types = [s["type"] for s in detected]
        logger.info("[signal_engines] %s — detected signals: %s", symbol, types)

    return detected
