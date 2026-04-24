"""
math_engine/candle_builder.py
==============================
Real-time 1-minute candle builder for Market Pulse Pro v2.

Pipeline
--------
Redis "ticks" pub/sub  →  in-memory accumulator per symbol
  →  on 1m boundary: all indicator math runs inline on the event loop (O(1))
    →  incremental EMA9, EMA16, EMA200, ATR14, RSI14, VWAP, Supertrend, Choppiness-14
    →  append candle to candles:1m:{symbol} (Redis, keep last 500)
    →  update snapshot:{symbol} hash
    →  publish to "candles:1m" channel (for Brain)
  →  higher-TF accumulators (5m / 15m) maintained in memory only
    →  on close: publish to "candles:5m" / "candles:15m" channels

Candle alignment
----------------
  * Aligned to exact clock minutes (9:15, 9:16, …, 15:29)
  * Uses exchange_timestamp from tick — not system clock
  * No candle before 9:15:00 IST; last candle closes at 15:30:00 IST
  * EOD force-close fired at 15:20 IST; feed halts at 15:30 IST

Standalone test
---------------
    python -m math_engine.candle_builder
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from core.redis_client import get_redis
from core.universe_builder import get_symbols

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Market session constants (IST = UTC+5:30)
# ---------------------------------------------------------------------------
_IST = timezone(timedelta(hours=5, minutes=30))

_MARKET_OPEN_H,  _MARKET_OPEN_M  = 9,  15   # 09:15 IST
_EOD_CLOSE_H,    _EOD_CLOSE_M    = 15, 20   # 15:20 IST — force-close
_MARKET_HALT_H,  _MARKET_HALT_M  = 15, 30   # 15:30 IST — stop processing

# Higher-timeframe close minutes
_5M_MINUTES  = {0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}
_15M_MINUTES = {0, 15, 30, 45}
_1HR_MINUTES = {0}  # hourly bar closes at :00

# Redis channel / key names
_CH_TICKS    = "ticks"
_CH_1M       = "candles:1m"
_CH_5M       = "candles:5m"
_CH_15M      = "candles:15m"
_CH_1HR      = "candles:1hr"
_KEY_STATUS  = "candle_builder:status"

# Indicator periods
_EMA16_PERIOD  = 16
_EMA200_PERIOD = 200
_ATR_PERIOD    = 14
_CHOP_PERIOD   = 14
_ST_MULTIPLIER = 3.0

# Health / logging
_HEALTH_LOG_INTERVAL = 60   # seconds

# ---------------------------------------------------------------------------
# Candle accumulator type alias
# ---------------------------------------------------------------------------
# accumulators[symbol] = {
#     "minute": "09:15",   # HH:MM string (IST)
#     "open":   float,
#     "high":   float,
#     "low":    float,
#     "close":  float,
#     "volume": int,
#     "ts":     str,        # ISO-8601 candle open timestamp
# }

# tf_accumulators[symbol][tf] = same structure
# tf = "5m" | "15m"

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
accumulators:    dict[str, dict[str, Any]] = {}
tf_accumulators: dict[str, dict[str, dict[str, Any]]] = {}

# Indicator state per symbol — populated from Redis snapshots at startup
# indicators[symbol] = {
#     "ema9":             float,
#     "ema16":            float,
#     "ema200":           float,
#     "atr14":            float,
#     "supertrend_dir":   int,    # 1 = BULL, -1 = BEAR
#     "supertrend_band":  float,
#     "rsi14":            float,
#     "rsi_avg_gain":     float,  # seeded by math_engine/seeder.py
#     "rsi_avg_loss":     float,  # seeded by math_engine/seeder.py
#     "vwap":             float,
#     "vwap_cum_tp_vol":  float,  # in-memory only (not in snapshot)
#     "vwap_cum_vol":     float,  # in-memory only (not in snapshot)
#     "vwap_history":     list,   # last 5 VWAP values, in-memory only
#     "vwap_slope":       float,
#     "last_close":       float,
#     "last_high":        float,
#     "last_low":         float,
# }
indicators: dict[str, dict[str, Any]] = {}

# Rate counter for health logging
_candles_closed_since_last_log: int = 0

# EOD flag — set to True after force-close fires
_eod_done: bool = False

# ---------------------------------------------------------------------------
# Pure math helpers — all O(1) incremental, safe to call inline on event loop
# ---------------------------------------------------------------------------

def update_ema(current_close: float, prev_ema: float, period: int) -> float:
    """O(1) EMA using standard exponential smoothing formula."""
    if not prev_ema:
        return current_close
    alpha = 2 / (period + 1)
    return (current_close - prev_ema) * alpha + prev_ema


def update_atr(high: float, low: float, prev_close: float,
               prev_atr: float, period: int = 14) -> float:
    """O(1) ATR using Wilder smoothing."""
    tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
    if not prev_atr:
        return tr
    return ((prev_atr * (period - 1)) + tr) / period


def update_rsi(current_close: float, prev_close: float,
               prev_avg_gain: float, prev_avg_loss: float,
               period: int = 14) -> tuple[float, float, float]:
    """
    O(1) RSI14 using Wilder's smoothing.

    NOTE: The seeder (math_engine/seeder.py) must seed initial rsi_avg_gain
    and rsi_avg_loss alongside rsi14 into snapshot:{symbol} so the first
    incremental update here starts from a valid state.

    Returns: (rsi14, avg_gain, avg_loss)
    """
    change = current_close - prev_close
    gain = max(change, 0.0)
    loss = max(-change, 0.0)
    avg_gain = (prev_avg_gain * (period - 1) + gain) / period
    avg_loss = (prev_avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0, avg_gain, avg_loss
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi, avg_gain, avg_loss


def update_vwap(prev_cum_tp_vol: float, prev_cum_vol: float,
                high: float, low: float, close: float,
                volume: float) -> tuple[float, float, float]:
    """
    O(1) incremental VWAP.  Resets to 0 at 09:15 IST each day (caller
    responsibility — pass prev_cum_tp_vol=0, prev_cum_vol=0 on reset).

    Returns: (vwap, cum_tp_vol, cum_vol)
    """
    typical_price = (high + low + close) / 3
    cum_tp_vol = prev_cum_tp_vol + (typical_price * volume)
    cum_vol = prev_cum_vol + volume
    vwap = cum_tp_vol / max(cum_vol, 1)
    return vwap, cum_tp_vol, cum_vol


def _update_supertrend(
    prev_direction: int,
    prev_band: float,
    new_high: float,
    new_low: float,
    new_close: float,
    new_atr: float,
    multiplier: float = _ST_MULTIPLIER,
) -> tuple[int, float]:
    hl2   = (new_high + new_low) / 2.0
    upper = hl2 + multiplier * new_atr
    lower = hl2 - multiplier * new_atr

    if prev_direction == 1:          # was BULL
        band      = max(lower, prev_band)
        direction = -1 if new_close < band else 1
    else:                            # was BEAR
        band      = min(upper, prev_band)
        direction = 1 if new_close > band else -1

    return direction, band


def _calc_choppiness(candles_14: list[list]) -> float:
    """
    Choppiness index over the last 14 candles.
    candles_14: list of [ts, open, high, low, close, volume]

    Pure Python loop — 14-item iteration runs in ~1 µs, safe inline on event loop.
    No NumPy. No threading.
    """
    if len(candles_14) < 2:
        return 50.0

    highs  = [row[2] for row in candles_14]
    lows   = [row[3] for row in candles_14]
    closes = [row[4] for row in candles_14]

    atr_sum = 0.0
    for i in range(1, len(candles_14)):
        tr = max(
            highs[i]  - lows[i],
            abs(highs[i]  - closes[i - 1]),
            abs(lows[i]   - closes[i - 1]),
        )
        atr_sum += tr

    hh = max(highs)
    ll = min(lows)

    if hh == ll or atr_sum == 0:
        return 50.0

    import math
    return 100.0 * math.log10(atr_sum / (hh - ll)) / math.log10(_CHOP_PERIOD)


# ---------------------------------------------------------------------------
# Redis snapshot write + candle store
# ---------------------------------------------------------------------------

async def _flush_candle_to_redis(
    symbol: str,
    candle: dict[str, Any],
    updated_ind: dict[str, Any],
) -> None:
    """Append candle to Redis list, update snapshot hash, publish to candles:1m."""
    redis = await get_redis()
    ts = candle["ts"]
    o, h, l, c, v = candle["open"], candle["high"], candle["low"], candle["close"], candle["volume"]

    now_iso = datetime.now(timezone.utc).isoformat()

    direction  = updated_ind["supertrend_dir"]
    st_label   = "BULL" if direction == 1 else "BEAR"

    candle_arr  = json.dumps([ts, o, h, l, c, v])
    snapshot_key = f"snapshot:{symbol}"
    candle_key   = f"candles:1m:{symbol}"

    async with redis.pipeline(transaction=False) as pipe:
        # 1. Append + trim candle history
        pipe.rpush(candle_key, candle_arr)
        pipe.ltrim(candle_key, -500, -1)

        # 2. Update snapshot
        pipe.hset(snapshot_key, mapping={
            "ema9":            str(updated_ind["ema9"]),
            "ema16":           str(updated_ind["ema16"]),
            "ema200":          str(updated_ind["ema200"]),
            "atr14":           str(updated_ind["atr14"]),
            "choppiness14":    str(updated_ind["choppiness14"]),
            "supertrend_dir":  st_label,
            "supertrend_band": str(updated_ind["supertrend_band"]),
            # RSI14 — rsi_avg_gain / rsi_avg_loss stored for next incremental update
            "rsi14":           str(updated_ind["rsi14"]),
            "rsi_avg_gain":    str(updated_ind["rsi_avg_gain"]),
            "rsi_avg_loss":    str(updated_ind["rsi_avg_loss"]),
            # VWAP — only vwap scalar in snapshot; accumulators live in memory
            "vwap":            str(updated_ind["vwap"]),
            "vwap_slope":      str(updated_ind["vwap_slope"]),
            "last_close":      str(c),
            "last_high":       str(h),
            "last_low":        str(l),
            "last_volume":     str(v),
            "last_candle_ts":  ts,
            "updated_at":      now_iso,
        })

        await pipe.execute()

    # 3. Publish closed candle for Brain
    pub_payload = json.dumps({
        "symbol":          symbol,
        "ts":              ts,
        "open":            o,
        "high":            h,
        "low":             l,
        "close":           c,
        "volume":          v,
        "ema9":            updated_ind["ema9"],
        "ema16":           updated_ind["ema16"],
        "ema200":          updated_ind["ema200"],
        "supertrend_dir":  st_label,
        "choppiness":      updated_ind["choppiness14"],
        "rsi14":           updated_ind["rsi14"],
        "vwap":            updated_ind["vwap"],
        "vwap_slope":      updated_ind["vwap_slope"],
    })
    await redis.publish(_CH_1M, pub_payload)


# ---------------------------------------------------------------------------
# Higher-timeframe candle helpers
# ---------------------------------------------------------------------------

def _tf_minute_closed(minute_str: str, tf: str) -> bool:
    """Return True if the given 'HH:MM' minute boundary closes the TF candle."""
    m = int(minute_str.split(":")[1])
    if tf == "5m":
        return m in _5M_MINUTES
    if tf == "15m":
        return m in _15M_MINUTES
    if tf == "1hr":
        return m in _1HR_MINUTES
    return False


async def _maybe_close_tf_candles(symbol: str, new_minute: str) -> None:
    """
    Called whenever a 1m candle closes (we have the new minute boundary).
    If the new_minute aligns to a 5m/15m/1hr boundary: flush in-memory TF
    candle, append to `candles:{tf}:{symbol}` LIST, and publish to
    `candles:{tf}` pub/sub channel.

    List writes enable chart reads via /api/candles.
    Pub/sub publishes preserve the trigger monitor and brain consumers.
    """
    redis = await get_redis()

    for tf, ch in (("5m", _CH_5M), ("15m", _CH_15M), ("1hr", _CH_1HR)):
        sym_tf = tf_accumulators.get(symbol, {}).get(tf)
        if sym_tf is None:
            continue

        if not _tf_minute_closed(new_minute, tf):
            continue

        # Serialize list entry in same [ts, o, h, l, c, v] shape as 1m list
        candle_arr = json.dumps([
            sym_tf["ts"],
            sym_tf["open"],
            sym_tf["high"],
            sym_tf["low"],
            sym_tf["close"],
            sym_tf["volume"],
        ])
        list_key = f"candles:{tf}:{symbol}"

        # Also build pub/sub payload (unchanged shape so consumers don't break)
        pub_payload = json.dumps({
            "symbol": symbol,
            "tf":     tf,
            "ts":     sym_tf["ts"],
            "open":   sym_tf["open"],
            "high":   sym_tf["high"],
            "low":    sym_tf["low"],
            "close":  sym_tf["close"],
            "volume": sym_tf["volume"],
        })

        async with redis.pipeline(transaction=False) as pipe:
            pipe.rpush(list_key, candle_arr)
            pipe.ltrim(list_key, -500, -1)
            pipe.publish(ch, pub_payload)
            await pipe.execute()

        # Reset accumulator — will re-open on next 1m candle
        tf_accumulators[symbol][tf] = None


def _update_tf_accumulator(symbol: str, candle: dict[str, Any]) -> None:
    """Merge a closed 1m candle into 5m, 15m, and 1hr in-memory accumulators."""
    tf_accumulators.setdefault(symbol, {"5m": None, "15m": None, "1hr": None})

    for tf in ("5m", "15m", "1hr"):
        acc = tf_accumulators[symbol][tf]
        if acc is None:
            # Start fresh TF candle from this 1m candle
            tf_accumulators[symbol][tf] = {
                "ts":     candle["ts"],
                "open":   candle["open"],
                "high":   candle["high"],
                "low":    candle["low"],
                "close":  candle["close"],
                "volume": candle["volume"],
            }
        else:
            acc["high"]   = max(acc["high"],   candle["high"])
            acc["low"]    = min(acc["low"],     candle["low"])
            acc["close"]  = candle["close"]
            acc["volume"] += candle["volume"]


# ---------------------------------------------------------------------------
# Core candle-close handler
# ---------------------------------------------------------------------------

async def _on_candle_close(symbol: str, closed: dict[str, Any], new_minute: str) -> None:
    """
    Triggered when a 1m candle boundary is crossed.

    All indicator math is O(1) — runs inline on the event loop.
    No asyncio.to_thread() — thread-spawn overhead exceeds math cost at O(1).

    1. Fetch last 14 candles from Redis for choppiness
    2. Compute EMA9/16/200, ATR14, Supertrend, Choppiness14, RSI14, VWAP inline
    3. Write results to Redis
    4. Update in-memory indicator state (incl. vwap_history for slope)
    5. Merge into TF accumulators, check TF closes
    """
    global _candles_closed_since_last_log

    try:
        redis = await get_redis()

        # Fetch last 14 closed candles for choppiness window
        raw_candles = await redis.lrange(f"candles:1m:{symbol}", -_CHOP_PERIOD, -1)
        candles_14  = [json.loads(c) for c in raw_candles] if raw_candles else []

        # Current indicator state for this symbol
        ind = indicators.get(symbol, {})

        c = closed["close"]
        h = closed["high"]
        l = closed["low"]

        prev_close = ind.get("last_close", c)
        prev_atr   = ind.get("atr14", 1.0)

        # --- EMA (O(1) incremental) ---
        new_ema9   = update_ema(c, ind.get("ema9",   c), 9)
        new_ema16  = update_ema(c, ind.get("ema16",  c), _EMA16_PERIOD)
        new_ema200 = update_ema(c, ind.get("ema200", c), _EMA200_PERIOD)

        # --- ATR (O(1) Wilder smoothing) ---
        new_atr = update_atr(h, l, prev_close, prev_atr)

        # --- Supertrend ---
        direction, band = _update_supertrend(
            prev_direction = ind.get("supertrend_dir",  1),
            prev_band      = ind.get("supertrend_band", c),
            new_high  = h,
            new_low   = l,
            new_close = c,
            new_atr   = new_atr,
        )

        # --- Choppiness (pure Python 14-item loop, ~1 µs, safe inline) ---
        choppiness = _calc_choppiness(candles_14)

        # --- RSI14 (O(1) Wilder smoothing) ---
        # NOTE: seeder must seed rsi_avg_gain and rsi_avg_loss into snapshot:{symbol}
        rsi14, new_avg_gain, new_avg_loss = update_rsi(
            current_close  = c,
            prev_close     = prev_close,
            prev_avg_gain  = ind.get("rsi_avg_gain", 0.0),
            prev_avg_loss  = ind.get("rsi_avg_loss", 0.0),
        )

        # --- VWAP (O(1) incremental; resets at 09:15 each day) ---
        # Use candle's own minute (derived from exchange timestamp) — not system clock
        is_first_candle = (closed["minute"] == "09:15")
        if is_first_candle:
            # Day reset — start VWAP accumulators from zero
            ind["vwap_cum_tp_vol"] = 0.0
            ind["vwap_cum_vol"]    = 0.0
            ind["vwap_history"]    = []
            prev_cum_tp_vol = 0.0
            prev_cum_vol    = 0.0
        else:
            prev_cum_tp_vol = ind.get("vwap_cum_tp_vol", 0.0)
            prev_cum_vol    = ind.get("vwap_cum_vol",    0.0)

        new_vwap, new_cum_tp_vol, new_cum_vol = update_vwap(
            prev_cum_tp_vol = prev_cum_tp_vol,
            prev_cum_vol    = prev_cum_vol,
            high   = h,
            low    = l,
            close  = c,
            volume = closed["volume"],
        )

        # vwap_history: rolling list of last 5 VWAP values (oldest first)
        vwap_history: list[float] = list(ind.get("vwap_history", []))
        vwap_history.append(new_vwap)
        if len(vwap_history) > 5:
            vwap_history = vwap_history[-5:]

        # vwap_slope: % change over last 5 candles (or 0 if window not full yet)
        if len(vwap_history) == 5 and vwap_history[0] != 0:
            vwap_slope = (vwap_history[-1] - vwap_history[0]) / vwap_history[0] * 100
        else:
            vwap_slope = 0.0

        # Assemble updated indicator dict
        updated_ind: dict[str, Any] = {
            "ema9":            new_ema9,
            "ema16":           new_ema16,
            "ema200":          new_ema200,
            "atr14":           new_atr,
            "choppiness14":    choppiness,
            "supertrend_dir":  direction,
            "supertrend_band": band,
            "rsi14":           rsi14,
            "rsi_avg_gain":    new_avg_gain,
            "rsi_avg_loss":    new_avg_loss,
            "vwap":            new_vwap,
            "vwap_cum_tp_vol": new_cum_tp_vol,   # in-memory only
            "vwap_cum_vol":    new_cum_vol,       # in-memory only
            "vwap_history":    vwap_history,      # in-memory only
            "vwap_slope":      vwap_slope,
        }

        # Persist to Redis (async I/O — back on event loop)
        await _flush_candle_to_redis(symbol, closed, updated_ind)

        # Update in-memory indicator state
        indicators.setdefault(symbol, {}).update(updated_ind)
        indicators[symbol]["last_close"] = c
        indicators[symbol]["last_high"]  = h
        indicators[symbol]["last_low"]   = l

        # Higher-TF aggregation
        _update_tf_accumulator(symbol, closed)
        await _maybe_close_tf_candles(symbol, new_minute)

        _candles_closed_since_last_log += 1

        # -----------------------------------------------------------------------
        # Addition 1 — ORB tracking
        # ORB is formed from the 09:15–09:29 candles and locked at 09:30.
        # -----------------------------------------------------------------------
        state = indicators  # alias for clarity in additions below

        if closed["minute"] == "09:15":
            # First candle — initialise ORB tracking
            state[symbol]["orb_high"] = closed["high"]
            state[symbol]["orb_low"]  = closed["low"]
            state[symbol]["orb_set"]  = False
            state[symbol]["candles_since_orb"] = 0

        elif not state[symbol].get("orb_set", False):
            # Expand ORB until 9:30
            if closed["minute"] <= "09:29":
                state[symbol]["orb_high"] = max(
                    state[symbol].get("orb_high", closed["high"]),
                    closed["high"],
                )
                state[symbol]["orb_low"] = min(
                    state[symbol].get("orb_low", closed["low"]),
                    closed["low"],
                )
            elif closed["minute"] == "09:30":
                # Lock ORB at 9:30
                state[symbol]["orb_set"] = True
                orb_range = state[symbol]["orb_high"] - state[symbol]["orb_low"]
                state[symbol]["orb_range_pct"] = (
                    orb_range / max(state[symbol]["orb_low"], 0.01) * 100
                )

        if state[symbol].get("orb_set", False):
            state[symbol]["candles_since_orb"] = (
                state[symbol].get("candles_since_orb", 0) + 1
            )

        # Write ORB fields to snapshot
        await redis.hset(f"snapshot:{symbol}", mapping={
            "orb_high":         str(state[symbol].get("orb_high", 0)),
            "orb_low":          str(state[symbol].get("orb_low", 0)),
            "orb_range_pct":    str(state[symbol].get("orb_range_pct", 0)),
            "candles_since_orb": str(state[symbol].get("candles_since_orb", 0)),
        })

        # -----------------------------------------------------------------------
        # Addition 2 — Rolling 1H high/low (deque of last 60 x 1m candles)
        # -----------------------------------------------------------------------
        if "rolling_highs" not in state[symbol]:
            state[symbol]["rolling_highs"]       = deque(maxlen=60)
            state[symbol]["rolling_lows"]        = deque(maxlen=60)
            state[symbol]["prev_rolling_1h_high"] = 0.0

        state[symbol]["prev_rolling_1h_high"] = state[symbol].get("rolling_1h_high", 0)
        state[symbol]["rolling_highs"].append(closed["high"])
        state[symbol]["rolling_lows"].append(closed["low"])

        rolling_1h_high = max(state[symbol]["rolling_highs"])
        rolling_1h_low  = min(state[symbol]["rolling_lows"])
        state[symbol]["rolling_1h_high"] = rolling_1h_high
        state[symbol]["rolling_1h_low"]  = rolling_1h_low

        # Write to snapshot
        await redis.hset(f"snapshot:{symbol}", mapping={
            "rolling_1h_high":      str(rolling_1h_high),
            "rolling_1h_low":       str(rolling_1h_low),
            "prev_rolling_1h_high": str(state[symbol]["prev_rolling_1h_high"]),
        })

        # -----------------------------------------------------------------------
        # Addition 3 — Consecutive choppy candles + choppy range high/low
        # choppiness_class derived from the choppiness14 value computed above.
        # -----------------------------------------------------------------------
        if choppiness >= 61.8:
            choppiness_class = "CHOPPY"
        elif choppiness <= 38.2:
            choppiness_class = "TRENDING"
        else:
            choppiness_class = "NEUTRAL"

        if choppiness_class == "CHOPPY":
            state[symbol]["consecutive_choppy_candles"] = (
                state[symbol].get("consecutive_choppy_candles", 0) + 1
            )
            # Track highest high / lowest low during choppy period
            state[symbol]["choppy_range_high"] = max(
                state[symbol].get("choppy_range_high", closed["high"]),
                closed["high"],
            )
            state[symbol]["choppy_range_low"] = min(
                state[symbol].get("choppy_range_low", closed["low"]),
                closed["low"],
            )
        else:
            # Reset when choppiness breaks
            state[symbol]["consecutive_choppy_candles"] = 0
            state[symbol]["choppy_range_high"]          = 0.0
            state[symbol]["choppy_range_low"]           = 0.0

        # Write to snapshot
        await redis.hset(f"snapshot:{symbol}", mapping={
            "consecutive_choppy_candles": str(state[symbol]["consecutive_choppy_candles"]),
            "choppy_range_high":          str(state[symbol].get("choppy_range_high", 0)),
            "choppy_range_low":           str(state[symbol].get("choppy_range_low", 0)),
        })
    except Exception as e:
        logger.exception("[candle_builder] _on_candle_close failed for %s: %s", symbol, e)


# ---------------------------------------------------------------------------
# Tick routing
# ---------------------------------------------------------------------------

def _parse_minute(ts: str) -> Optional[str]:
    """
    Extract 'HH:MM' (IST) from an ISO-8601 timestamp string.
    Returns None if parsing fails.
    """
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            # Assume UTC if no tz info
            dt = dt.replace(tzinfo=timezone.utc)
        dt_ist = dt.astimezone(_IST)
        return dt_ist.strftime("%H:%M")
    except Exception:
        return None


def _within_session(minute_str: str) -> bool:
    h, m = int(minute_str[:2]), int(minute_str[3:])
    after_open = (h, m) >= (_MARKET_OPEN_H, _MARKET_OPEN_M)
    before_halt = (h, m) < (_MARKET_HALT_H, _MARKET_HALT_M)
    return after_open and before_halt


async def _route_tick(symbol: str, ltp: float, volume: int, ts: str) -> None:
    """
    Route a single tick to the in-memory accumulator for `symbol`.
    Closes the current candle and opens a new one when the minute boundary rolls.
    """
    minute = _parse_minute(ts)
    if minute is None:
        logger.debug("[candle_builder] Could not parse ts=%r for %s", ts, symbol)
        return

    if not _within_session(minute):
        return

    if symbol not in indicators:
        # Snapshot not seeded at startup — skip until seeder runs
        logger.debug("[candle_builder] %s has no snapshot — skipping.", symbol)
        return

    acc = accumulators.get(symbol)

    if acc is None:
        # First tick for this symbol — open accumulator
        accumulators[symbol] = {
            "minute": minute,
            "open":   ltp,
            "high":   ltp,
            "low":    ltp,
            "close":  ltp,
            "volume": volume,
            "ts":     ts,
        }
        return

    if minute == acc["minute"]:
        # Same candle — update OHLCV
        acc["high"]   = max(acc["high"],   ltp)
        acc["low"]    = min(acc["low"],     ltp)
        acc["close"]  = ltp
        acc["volume"] += volume
        return

    # New minute — close current candle, open new one
    closed = dict(acc)  # snapshot before reset

    # Open new accumulator immediately so we don't miss this tick
    accumulators[symbol] = {
        "minute": minute,
        "open":   ltp,
        "high":   ltp,
        "low":    ltp,
        "close":  ltp,
        "volume": volume,
        "ts":     ts,
    }

    # Fire candle-close processing (non-blocking from caller's perspective
    # because _on_candle_close itself is async and we await it here;
    # the I/O + thread dispatch is still non-blocking relative to other symbols
    # handled by separate subscription tasks)
    task = asyncio.create_task(_on_candle_close(symbol, closed, minute))
    task.add_done_callback(
        lambda t: logger.error(
            "[candle_builder] candle close task failed for %s: %s",
            symbol, t.exception()
        ) if t.exception() else None
    )


# ---------------------------------------------------------------------------
# Redis pub/sub subscriber
# ---------------------------------------------------------------------------

async def _subscribe_ticks() -> None:
    """
    Subscribe to the Redis 'ticks' channel and route each message to the
    appropriate in-memory accumulator.
    """
    while True:
        try:
            redis  = await get_redis()
            pubsub = redis.pubsub()
            await pubsub.subscribe(_CH_TICKS)
            logger.info("[candle_builder] Subscribed to '%s' channel.", _CH_TICKS)

            health_ts = time.monotonic()

            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                try:
                    data = json.loads(message["data"])
                except (json.JSONDecodeError, TypeError):
                    continue

                try:
                    symbol = data.get("symbol")
                    ltp    = data.get("ltp")
                    volume = data.get("volume", 0)
                    ts     = data.get("ts")

                    if not symbol or ltp is None or not ts:
                        continue

                    await _route_tick(symbol, float(ltp), int(volume), ts)

                    # Health / rate log every 60 s
                    now = time.monotonic()
                    if now - health_ts >= _HEALTH_LOG_INTERVAL:
                        global _candles_closed_since_last_log
                        logger.info(
                            "[candle_builder] Health — candles_closed_last_60s=%d  "
                            "active_accumulators=%d",
                            _candles_closed_since_last_log,
                            len(accumulators),
                        )
                        _candles_closed_since_last_log = 0
                        health_ts = now

                    # Check for EOD trigger
                    if not _eod_done:
                        now_ist = datetime.now(_IST)
                        if (now_ist.hour, now_ist.minute) >= (_EOD_CLOSE_H, _EOD_CLOSE_M):
                            asyncio.create_task(_handle_eod())
                except Exception as e:
                    logger.error("[candle_builder] Message processing error: %s", e)
        except Exception as e:
            logger.warning("[candle_builder] Pub/sub connection dropped, reconnecting in 2s: %s", e)
            await asyncio.sleep(2)


# ---------------------------------------------------------------------------
# Startup: load indicator state from Redis snapshots
# ---------------------------------------------------------------------------

async def _seed_indicators() -> None:
    """
    Load snapshot:{symbol} data from Redis into the in-memory `indicators`
    dict so the first candle close can do incremental updates.

    Canonical runtime format is Redis HASH for snapshot:{symbol}.  Older
    deployments may still have legacy Redis STRING JSON snapshots. To stay
    backward-compatible, startup will auto-convert legacy STRING snapshots
    into HASH format:
      1. Read the string value
      2. Parse JSON
      3. Delete string key
      4. Re-write as flattened HASH (string values)

    If the key is already a HASH, it is used as-is.

    Symbols without a snapshot are logged as warnings and skipped.
    """
    redis   = await get_redis()
    symbols = await get_symbols()
    seeded, missing, converted = 0, 0, 0

    for symbol in symbols:
        key = f"snapshot:{symbol}"

        # Detect the key type — seeder writes a string, cruncher writes a hash.
        try:
            key_type = await redis.type(key)
        except Exception as exc:
            logger.warning("[candle_builder] %s: redis.type failed: %s", symbol, exc)
            missing += 1
            continue

        if isinstance(key_type, bytes):
            key_type = key_type.decode("utf-8", errors="replace")

        raw: dict = {}

        if key_type == "none":
            logger.warning(
                "[candle_builder] No snapshot for %s — skipping until seeder runs.",
                symbol,
            )
            missing += 1
            continue

        elif key_type == "string":
            # Seeder wrote a JSON-encoded string. Parse it and convert the key
            # to a flat hash so cruncher's own HSET calls don't hit WRONGTYPE.
            try:
                raw_str = await redis.get(key)
                if raw_str is None:
                    missing += 1
                    continue
                parsed = json.loads(raw_str)
            except Exception as exc:
                logger.warning(
                    "[candle_builder] %s: JSON parse of seeder snapshot failed: %s",
                    symbol, exc,
                )
                missing += 1
                continue

            # Flatten nested objects (pivots, supertrend, prev_day, etc.) into
            # top-level string fields so the later _f() reads still work.
            flat: dict = {}
            for k, v in parsed.items():
                if isinstance(v, dict):
                    # Nested objects — stringify them as JSON for later consumers
                    # (signal_engines reads these back). Also, flatten commonly
                    # accessed fields to top-level so _f() below can find them.
                    flat[k] = json.dumps(v)
                    if k == "supertrend":
                        flat["supertrend_dir"]  = str(v.get("direction", "BULL"))
                        flat["supertrend_band"] = str(v.get("band", 0.0))
                    elif k == "prev_day":
                        for subk in ("open", "high", "low", "close", "volume"):
                            if subk in v:
                                flat[f"last_{subk}"] = str(v[subk])
                elif v is None:
                    flat[k] = ""
                else:
                    flat[k] = str(v)

            # Delete the string key and re-create as a hash
            try:
                await redis.delete(key)
                if flat:
                    await redis.hset(key, mapping=flat)
                raw = flat
                converted += 1
            except Exception as exc:
                logger.warning(
                    "[candle_builder] %s: hash conversion failed: %s",
                    symbol, exc,
                )
                missing += 1
                continue

        elif key_type == "hash":
            # Already in the right format (from a previous cruncher run).
            raw = await redis.hgetall(key)
            if not raw:
                missing += 1
                continue

        else:
            logger.warning(
                "[candle_builder] %s: unexpected snapshot type %r — skipping.",
                symbol, key_type,
            )
            missing += 1
            continue

        def _f(key: str, default: float = 0.0) -> float:
            try:
                return float(raw.get(key, default))
            except (TypeError, ValueError):
                return default

        indicators[symbol] = {
            "ema9":            _f("ema9"),
            "ema16":           _f("ema16"),
            "ema200":          _f("ema200"),
            "atr14":           _f("atr14", 1.0),
            "choppiness14":    _f("choppiness14", 50.0),
            "supertrend_dir":  1 if raw.get("supertrend_dir", "BULL") == "BULL" else -1,
            "supertrend_band": _f("supertrend_band"),
            # RSI14 — rsi_avg_gain / rsi_avg_loss must be seeded by math_engine/seeder.py
            "rsi14":           _f("rsi14", 50.0),
            "rsi_avg_gain":    _f("rsi_avg_gain", 0.0),
            "rsi_avg_loss":    _f("rsi_avg_loss", 0.0),
            # VWAP — cum accumulators start at 0 (reset daily); vwap_history empty until candles flow
            "vwap":            _f("vwap", 0.0),
            "vwap_cum_tp_vol": 0.0,
            "vwap_cum_vol":    0.0,
            "vwap_history":    [],
            "vwap_slope":      _f("vwap_slope", 0.0),
            "last_close":      _f("last_close"),
            "last_high":       _f("last_high"),
            "last_low":        _f("last_low"),
        }

        # Also initialise TF accumulator slots
        tf_accumulators.setdefault(symbol, {"5m": None, "15m": None, "1hr": None})

        seeded += 1

    logger.info(
        "[candle_builder] Seeded %d symbols from snapshots "
        "(%d missing, %d converted from seeder JSON to hash).",
        seeded, missing, converted,
    )


# ---------------------------------------------------------------------------
# EOD handling
# ---------------------------------------------------------------------------

async def _handle_eod() -> None:
    """
    Force-close all open accumulators at 15:20 IST.
    Publishes final candles and writes EOD status to Redis.
    """
    global _eod_done
    if _eod_done:
        return
    _eod_done = True

    logger.info("[candle_builder] EOD force-close triggered — closing %d open candles.", len(accumulators))

    redis = await get_redis()

    for symbol, acc in list(accumulators.items()):
        try:
            closed = dict(acc)
            asyncio.create_task(_on_candle_close(symbol, closed, "15:20"))
        except Exception as exc:
            logger.warning("[candle_builder] EOD close error for %s: %s", symbol, exc)

    accumulators.clear()

    eod_status = json.dumps({
        "status": "eod",
        "ts":     datetime.now(timezone.utc).isoformat(),
    })
    await redis.set(_KEY_STATUS, eod_status)
    logger.info("[candle_builder] EOD status written to Redis.")

    # Schedule halt at 15:30
    now_ist = datetime.now(_IST)
    halt_ist = now_ist.replace(hour=_MARKET_HALT_H, minute=_MARKET_HALT_M, second=0, microsecond=0)
    delay = max(0.0, (halt_ist - now_ist).total_seconds())
    logger.info("[candle_builder] Tick processing will halt in %.0fs.", delay)
    await asyncio.sleep(delay)

    logger.info("[candle_builder] 15:30 IST — clearing all in-memory accumulators.")
    accumulators.clear()
    tf_accumulators.clear()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def run_candle_builder() -> None:
    """
    Main coroutine — seed state from Redis, then subscribe to ticks forever.
    Designed to run as a long-lived asyncio task alongside the equity feed.
    """
    logger.info("[candle_builder] Starting up …")

    await _seed_indicators()

    redis = await get_redis()
    await redis.set(
        _KEY_STATUS,
        json.dumps({"status": "running", "ts": datetime.now(timezone.utc).isoformat()}),
    )

    logger.info("[candle_builder] Entering tick subscription loop.")
    await _subscribe_ticks()


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )

    from core.config import validate
    validate()

    logger.info("=== candle_builder standalone run ===")
    await run_candle_builder()


if __name__ == "__main__":
    asyncio.run(_main())
