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
from statistics import mean
from typing import Optional

from core.redis_client import get_redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_IST = timezone(timedelta(hours=5, minutes=30))

_OPENING_DRIVE_END_H = 10
_OPENING_DRIVE_END_M = 0   # runs 9:15 – 10:00

DEBUG_SYMBOL = "NIFTY"  # change as needed
INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Optional[str], default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def normalize_value(v):
    if isinstance(v, bytes):
        v = v.decode()
    if isinstance(v, str):
        return v.strip()
    return v


def normalize_snapshot(snapshot: dict) -> dict:
    return {k: normalize_value(v) for k, v in snapshot.items()}


def is_valid_snapshot(symbol: str, s: dict) -> bool:
    try:
        ltp = float(s.get("ltp", 0) or 0)
        ema9 = float(s.get("ema9", 0) or 0)
        ema200 = float(s.get("ema200", 0) or 0)
    except Exception:
        return False

    if symbol in INDEX_SYMBOLS:
        return ltp > 0 and ema9 > 0 and ema200 > 0

    try:
        vwap = float(s.get("vwap", 0) or 0)
    except Exception:
        vwap = 0

    return ltp > 0 and ema9 > 0 and ema200 > 0 and vwap > 0


def _now_ist() -> datetime:
    return datetime.now(_IST)


async def _load_snapshot(symbol: str) -> dict:
    redis = await get_redis()
    raw = await redis.hgetall(f"snapshot:{symbol}")
    return raw or {}


async def _load_candles(symbol: str, timeframe: str, n: int = 20) -> list:
    """Load last *n* candles for timeframe from Redis list candles:{timeframe}:{symbol}."""
    redis = await get_redis()
    raw_list = await redis.lrange(f"candles:{timeframe}:{symbol}", -n, -1)
    candles = []
    for raw in raw_list:
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue

        if isinstance(parsed, list) and len(parsed) >= 6:
            candles.append({"ts": parsed[0], "open": parsed[1], "high": parsed[2], "low": parsed[3], "close": parsed[4], "volume": parsed[5]})
        elif isinstance(parsed, dict):
            candles.append(parsed)
    return candles


async def _load_candles_5m(symbol: str, n: int = 15) -> list:
    return await _load_candles(symbol, "5m", n)


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
# Calculation helpers (new)
# ---------------------------------------------------------------------------

def _body_health(o: float, h: float, l: float, c: float) -> float:
    candle_range = h - l
    if candle_range < 0.001:
        return 0.0
    return abs(c - o) / candle_range


def _calc_efficiency_ratio(closes: list, period: int = 12) -> float:
    if len(closes) < period + 1:
        return 0.5
    net_move = abs(closes[-1] - closes[-(period + 1)])
    total_move = sum(abs(closes[i] - closes[i - 1]) for i in range(-period, 0))
    if total_move == 0:
        return 0.0
    return round(net_move / total_move, 4)


def _calc_rsi_14(closes: list) -> float:
    if len(closes) < 15:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-14:]) / 14
    avg_loss = sum(losses[-14:]) / 14
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 2)


def _gap_direction(snapshot: dict) -> str:
    gap = _safe_float(snapshot.get("gap_pct"))
    if gap > 0.5:
        return "UP"
    if gap < -0.5:
        return "DOWN"
    return "FLAT"


def _cum_rvol(snapshot: dict) -> float:
    return _safe_float(snapshot.get("cum_rvol"), 0.0)


# ---------------------------------------------------------------------------
# Individual signal detectors (v2)
# ---------------------------------------------------------------------------

async def _detect_opening_drive(
    symbol: str,
    snapshot: dict,
    candles_5m: list,
    vol_5m: dict,
) -> Optional[dict]:
    """
    OPENING_DRIVE v2.3 — 9:15 to 9:55 AM, two tiers.
    PRIMARY trigger: close above PDH (LONG) or below PDL (SHORT).
    R1/S1 cleared = bonus flag, not a hard requirement.
    """
    now = _now_ist()
    if not (now.hour == 9 and 15 <= now.minute <= 55):
        return None

    redis = await get_redis()
    if await redis.exists(f"opening_drive_fired:{symbol}"):
        return None

    if not candles_5m:
        return None

    candle = candles_5m[-1]
    o   = _safe_float(candle.get("open"))
    h_c = _safe_float(candle.get("high"))
    l_c = _safe_float(candle.get("low"))
    c   = _safe_float(candle.get("close"))
    vol = _safe_float(candle.get("volume"))
    if c == 0 or o == 0:
        return None

    is_tier1 = now.minute < 30
    min_body = 0.80 if is_tier1 else 0.72

    bh = _body_health(o, h_c, l_c, c)
    if bh < min_body:
        return None

    gap_pct = _safe_float(snapshot.get("gap_pct"))
    if abs(gap_pct) > 2.2:
        return None

    gap_dir = _gap_direction(snapshot)

    slot_key = f"{now.hour:02d}{(now.minute // 5) * 5:02d}"
    avg_slot_vol = float(vol_5m.get(slot_key, 0) or 0)
    lot_size = int(_safe_float(snapshot.get("lot_size"), 0))
    min_vol_mult = (2.3 if lot_size <= 500 else 2.0) if is_tier1 else 1.9
    slot_rvol = 0.0
    if avg_slot_vol > 0:
        slot_rvol = vol / avg_slot_vol
        if slot_rvol < min_vol_mult:
            return None

    pdh = _safe_float(snapshot.get("prev_high"))
    pdl = _safe_float(snapshot.get("prev_low"))
    r1  = _safe_float(snapshot.get("r1") or snapshot.get("pivot_r1"))
    s1  = _safe_float(snapshot.get("s1") or snapshot.get("pivot_s1"))
    atr14 = _safe_float(snapshot.get("atr14"), 1.0)

    direction = None
    if gap_dir in ("UP", "FLAT") and pdh > 0 and c > pdh:
        direction = "LONG"
    if gap_dir in ("DOWN", "FLAT") and pdl > 0 and c < pdl and direction is None:
        direction = "SHORT"
    if direction is None:
        return None

    # Stricter body/volume for large gap candles
    if gap_pct > 1.6 and direction == "LONG":
        if bh < 0.82 or (avg_slot_vol > 0 and slot_rvol < 2.6):
            return None
    if gap_pct < -1.6 and direction == "SHORT":
        if bh < 0.82 or (avg_slot_vol > 0 and slot_rvol < 2.6):
            return None

    r1_cleared = (r1 > 0 and c > r1) if direction == "LONG" else (s1 > 0 and c < s1)

    await redis.set(f"opening_drive_fired:{symbol}", 1, ex=86400)

    sl    = (pdh - atr14 * 0.5) if direction == "LONG" else (pdl + atr14 * 0.5)
    tier  = "TIER1" if is_tier1 else "TIER2"

    logger.info("[signal] OPENING_DRIVE %s %s bh=%.2f slot_rvol=%.2f r1_cleared=%s",
                tier, direction, bh, slot_rvol, r1_cleared)
    return {
        "type":        "OPENING_DRIVE",
        "direction":   direction,
        "tier":        tier,
        "entry_price": round(pdh if direction == "LONG" else pdl, 2),
        "stop_loss":   round(sl, 2),
        "r1_cleared":  r1_cleared,
        "body_health": round(bh, 3),
        "detected_at": _now_ist_str(),
    }


async def _detect_range_breakout(
    symbol: str,
    snapshot: dict,
    candles_5m: list,
) -> Optional[dict]:
    """
    RANGE_BREAKOUT — two phases:
      Phase 1 ORB:       9:30–11:30 AM  (orb_high / orb_low)
      Phase 2 POSTLUNCH: 1:30–3:00 PM   (half_day_high / half_day_low)
      Dead zone:         11:30–1:30      nothing fires
    """
    now = _now_ist()
    h, m = now.hour, now.minute

    phase = None
    if (h == 9 and m >= 30) or h == 10 or (h == 11 and m < 30):
        phase = "ORB"
    elif (h == 13 and m >= 30) or h == 14 or (h == 15 and m == 0):
        phase = "POSTLUNCH"
    if phase is None:
        return None

    redis = await get_redis()
    if await redis.exists(f"range_breakout_fired:{symbol}"):
        return None

    if not candles_5m:
        return None

    candle = candles_5m[-1]
    o   = _safe_float(candle.get("open"))
    h_c = _safe_float(candle.get("high"))
    l_c = _safe_float(candle.get("low"))
    c   = _safe_float(candle.get("close"))
    if c == 0:
        return None

    bh = _body_health(o, h_c, l_c, c)
    if bh < 0.70:
        return None

    cr = _cum_rvol(snapshot)
    if cr > 0 and cr < 1.3:
        return None

    gap_dir = _gap_direction(snapshot)

    if phase == "ORB":
        range_high = _safe_float(snapshot.get("orb_high"))
        range_low  = _safe_float(snapshot.get("orb_low"))
        if range_high == 0 or range_low == 0:
            return None
        range_pct = (range_high - range_low) / max(range_low, 1) * 100
        if not (0.3 <= range_pct <= 2.5):
            return None
    else:
        range_high = _safe_float(snapshot.get("postlunch_high"))
        range_low  = _safe_float(snapshot.get("postlunch_low"))
        if range_high == 0 or range_low == 0:
            return None

    direction = None
    if c > range_high and c > o and gap_dir != "DOWN":
        direction = "LONG"
    elif c < range_low and c < o and gap_dir != "UP":
        direction = "SHORT"
    if direction is None:
        return None

    await redis.set(f"range_breakout_fired:{symbol}", 1, ex=86400)

    atr14 = _safe_float(snapshot.get("atr14"), 1.0)
    sl    = (range_low - atr14 * 0.3) if direction == "LONG" else (range_high + atr14 * 0.3)

    logger.info("[signal] RANGE_BREAKOUT %s %s range=%.2f–%.2f cum_rvol=%.2f bh=%.2f",
                phase, direction, range_low, range_high, cr, bh)
    return {
        "type":        "RANGE_BREAKOUT",
        "direction":   direction,
        "phase":       phase,
        "entry_price": round(range_high if direction == "LONG" else range_low, 2),
        "stop_loss":   round(sl, 2),
        "body_health": round(bh, 3),
        "cum_rvol":    round(cr, 3),
        "detected_at": _now_ist_str(),
    }


async def _detect_hourly_breakout(
    symbol: str,
    snapshot: dict,
    candles_5m: list,
) -> Optional[dict]:
    """
    HOURLY_BREAKOUT — valid 10:15 AM to 3:00 PM.
    LONG: close > 1h rolling high + RSI-14 on 5m rising over last 30 min.
    SHORT: close < 1h rolling low + RSI-14 falling.
    """
    now = _now_ist()
    h, m = now.hour, now.minute
    after_start = (h == 10 and m >= 15) or (11 <= h <= 14) or (h == 15 and m == 0)
    if not after_start:
        return None

    if not candles_5m or len(candles_5m) < 7:
        return None

    candle = candles_5m[-1]
    o   = _safe_float(candle.get("open"))
    h_c = _safe_float(candle.get("high"))
    l_c = _safe_float(candle.get("low"))
    c   = _safe_float(candle.get("close"))

    bh = _body_health(o, h_c, l_c, c)
    if bh < 0.70:
        return None

    cr = _cum_rvol(snapshot)
    if cr > 0 and cr < 1.3:
        return None

    rolling_1h_high = _safe_float(snapshot.get("rolling_1h_high"))
    rolling_1h_low  = _safe_float(snapshot.get("rolling_1h_low", 0))
    if rolling_1h_high == 0:
        return None

    closes_5m = [_safe_float(x.get("close")) for x in candles_5m if _safe_float(x.get("close")) > 0]
    if len(closes_5m) < 15:
        return None

    rsi_now     = _calc_rsi_14(closes_5m)
    rsi_30m_ago = _calc_rsi_14(closes_5m[:-6]) if len(closes_5m) >= 20 else rsi_now

    direction = None
    if c > rolling_1h_high and rsi_now > rsi_30m_ago:
        direction = "LONG"
    elif rolling_1h_low > 0 and c < rolling_1h_low and rsi_now < rsi_30m_ago:
        direction = "SHORT"
    if direction is None:
        return None

    redis = await get_redis()
    fire_key = f"hourly_breakout_fired:{symbol}:{direction}"
    if await redis.exists(fire_key):
        return None
    await redis.set(fire_key, 1, ex=5400)  # 90 min cooldown per direction

    vwap  = _safe_float(snapshot.get("vwap"))
    atr14 = _safe_float(snapshot.get("atr14"), 1.0)
    sl    = (max(vwap, l_c) - atr14 * 0.2) if direction == "LONG" else (min(vwap, h_c) + atr14 * 0.2)

    logger.info("[signal] HOURLY_BREAKOUT %s high=%.2f rsi_now=%.1f rsi_30m=%.1f cum_rvol=%.2f",
                direction, rolling_1h_high, rsi_now, rsi_30m_ago, cr)
    return {
        "type":        "HOURLY_BREAKOUT",
        "direction":   direction,
        "entry_price": round(rolling_1h_high if direction == "LONG" else rolling_1h_low, 2),
        "stop_loss":   round(sl, 2),
        "body_health": round(bh, 3),
        "cum_rvol":    round(cr, 3),
        "rsi_now":     round(rsi_now, 1),
        "detected_at": _now_ist_str(),
    }


async def _detect_choppiness_breakout(
    symbol: str,
    snapshot: dict,
    candles_5m: list,
) -> Optional[dict]:
    """
    CHOPPINESS_BREAKOUT v2 — Range Contraction + Efficiency Ratio.
    No classic Choppiness Index used.
    Compression: ER < 0.35 + range contracting for 2–10 candles.
    Breakout: ER > 0.45 on breakout candle + body ≥ 70% + cum_rvol ≥ 1.3x.
    """
    if not candles_5m or len(candles_5m) < 6:
        return None

    candle = candles_5m[-1]
    o   = _safe_float(candle.get("open"))
    h_c = _safe_float(candle.get("high"))
    l_c = _safe_float(candle.get("low"))
    c   = _safe_float(candle.get("close"))

    bh = _body_health(o, h_c, l_c, c)
    if bh < 0.70:
        return None

    cr = _cum_rvol(snapshot)
    if cr > 0 and cr < 1.3:
        return None

    closes = [_safe_float(x.get("close")) for x in candles_5m]
    highs  = [_safe_float(x.get("high"))  for x in candles_5m]
    lows   = [_safe_float(x.get("low"))   for x in candles_5m]
    ranges = [h - l for h, l in zip(highs, lows)]

    er_now = _calc_efficiency_ratio(closes, period=12)
    if er_now <= 0.45:
        return None

    prior = candles_5m[:-1]
    if len(prior) < 2:
        return None

    compression_count = 0
    for i in range(len(prior) - 1, -1, -1):
        prior_closes = [_safe_float(x.get("close")) for x in candles_5m[:i + 1]]
        if len(prior_closes) < 5:
            break
        er_prior = _calc_efficiency_ratio(prior_closes, period=min(12, len(prior_closes) - 1))
        if er_prior < 0.35:
            compression_count += 1
        else:
            break

    if not (2 <= compression_count <= 10):
        return None

    comp_start = len(prior) - compression_count
    comp_indices = list(range(comp_start, len(prior)))
    before_indices = list(range(max(0, comp_start - 3), comp_start))

    if not before_indices:
        return None

    avg_comp   = sum(ranges[i] for i in comp_indices)  / len(comp_indices)
    avg_before = sum(ranges[i] for i in before_indices) / len(before_indices)
    if avg_comp >= avg_before * 0.95:
        return None

    comp_high = max(highs[i] for i in comp_indices)
    comp_low  = min(lows[i]  for i in comp_indices)

    direction = None
    if c > comp_high:
        direction = "LONG"
    elif c < comp_low:
        direction = "SHORT"
    if direction is None:
        return None

    redis = await get_redis()
    chop_key = f"choppiness_fired:{symbol}"
    if await redis.exists(chop_key):
        return None
    await redis.set(chop_key, 1, ex=3600)  # 60 min cooldown

    atr14 = _safe_float(snapshot.get("atr14"), 1.0)
    sl    = (comp_low - atr14 * 0.2) if direction == "LONG" else (comp_high + atr14 * 0.2)

    logger.info("[signal] CHOPPINESS_BREAKOUT %s er=%.3f compressed=%d cum_rvol=%.2f",
                direction, er_now, compression_count, cr)
    return {
        "type":             "CHOPPINESS_BREAKOUT",
        "direction":        direction,
        "entry_price":      round(comp_high if direction == "LONG" else comp_low, 2),
        "stop_loss":        round(sl, 2),
        "compression_bars": compression_count,
        "er_breakout":      round(er_now, 3),
        "body_health":      round(bh, 3),
        "cum_rvol":         round(cr, 3),
        "detected_at":      _now_ist_str(),
    }


def _detect_supertrend_flip(
    symbol: str,
    snapshot: dict,
    prev_snapshot: dict,
) -> Optional[dict]:
    """
    SUPERTREND_FLIP — direction change detected on 5m candle close.
    ATR must be meaningful. RSI must not be extreme.
    LTP must be on correct side of supertrend band.
    """
    curr_dir = normalize_value(snapshot.get("supertrend_dir", ""))
    prev_dir = normalize_value(prev_snapshot.get("supertrend_dir", ""))

    if not curr_dir or not prev_dir or curr_dir == prev_dir:
        return None

    direction = "LONG" if curr_dir == "BULL" else "SHORT"

    ltp   = _safe_float(snapshot.get("ltp"))
    band  = _safe_float(snapshot.get("supertrend_band"))
    rsi14 = _safe_float(snapshot.get("rsi14"), 50.0)
    atr14 = _safe_float(snapshot.get("atr14"), 1.0)

    if ltp > 0 and (atr14 / ltp * 100) < 0.3:
        return None
    if direction == "LONG"  and rsi14 > 75:
        return None
    if direction == "SHORT" and rsi14 < 25:
        return None
    if direction == "LONG"  and band > 0 and ltp < band:
        return None
    if direction == "SHORT" and band > 0 and ltp > band:
        return None

    logger.info("[signal] SUPERTREND_FLIP %s ltp=%.2f band=%.2f rsi=%.1f",
                direction, ltp, band, rsi14)
    return {
        "type":        "SUPERTREND_FLIP",
        "direction":   direction,
        "entry_price": round(ltp, 2),
        "stop_loss":   round(band, 2),
        "rsi14":       round(rsi14, 1),
        "detected_at": _now_ist_str(),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def scan_all_signals(symbol: str, snapshot: dict | None = None) -> list[dict]:
    """
    Scan all signal detectors for *symbol* and return detected signals.
    Volume Surge is no longer a signal — it is a dashboard panel only.
    """
    if snapshot is None:
        snapshot = await _load_snapshot(symbol)
    if not snapshot:
        return []

    snapshot = normalize_snapshot(snapshot)

    if not is_valid_snapshot(symbol, snapshot):
        if symbol == DEBUG_SYMBOL:
            logger.info("%s: snapshot failed validation", symbol)
        return []

    prev_snapshot  = normalize_snapshot(await _load_prev_snapshot(symbol))
    candles_5m_raw = await _load_candles_5m(symbol, n=25)
    candles_5m = []
    for c in candles_5m_raw:
        if isinstance(c, list) and len(c) >= 6:
            candles_5m.append({
                "open": c[1], "high": c[2], "low": c[3],
                "close": c[4], "volume": c[5], "ts": c[0],
            })
        elif isinstance(c, dict):
            candles_5m.append(c)

    if not candles_5m:
        return []

    # Fetch vol_profile:5m once for opening drive
    redis = await get_redis()
    vol_5m: dict = {}
    try:
        vp_raw = await redis.get(f"vol_profile:5m:{symbol}")
        if vp_raw:
            vol_5m = json.loads(vp_raw)
    except Exception:
        pass

    detected: list[dict] = []

    # Async detectors
    for coro in [
        _detect_opening_drive(symbol, snapshot, candles_5m, vol_5m),
        _detect_range_breakout(symbol, snapshot, candles_5m),
    ]:
        try:
            sig = await coro
            if sig:
                sig["symbol"] = symbol
                detected.append(sig)
        except Exception as exc:
            logger.warning("[signal_engines] async detector failed for %s: %s", symbol, exc)

    # Mixed detectors
    for coro_or_fn, args, is_async in [
        (_detect_hourly_breakout,    (symbol, snapshot, candles_5m), True),
        (_detect_choppiness_breakout,(symbol, snapshot, candles_5m), True),
        (_detect_supertrend_flip,    (symbol, snapshot, prev_snapshot), False),
    ]:
        try:
            sig = await coro_or_fn(*args) if is_async else coro_or_fn(*args)
            if sig:
                sig["symbol"] = symbol
                detected.append(sig)
        except Exception as exc:
            logger.warning("[signal_engines] %s failed for %s: %s",
                           detector.__name__, symbol, exc)

    if symbol == DEBUG_SYMBOL and detected:
        logger.info("%s: %d signals — %s", symbol, len(detected),
                    [s["type"] for s in detected])

    return detected
