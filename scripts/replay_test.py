#!/usr/bin/env python3
"""
scripts/replay_test.py
======================
Weekend replay testing script.
Simulates live market signals to verify the full pipeline:
    brain (signal scan) → gatekeeper → scorer → WebSocket → frontend card

HOW IT WORKS
------------
1. Writes fake snapshot + candle history to Redis for a test symbol
2. Publishes a trigger to candles:1m  → brain's on_1m_candle fires
                                       → scan_all_signals runs
                                       → signal queued to _PENDING_SCORE_KEY
3. Waits 3s, publishes to candles:5m  → brain's on_5m_candle fires
                                       → score_signal runs
                                       → trade_execution published
                                       → WebSocket → frontend card appears
4. Cleans up ALL keys it created

SETUP (one-time, on Railway brain service env vars)
---------------------------------------------------
    REPLAY_MODE=1
    FAKE_MARKET_TIME=<see batch table below>

REPLAY_BATCH (env var on this replay service)
---------------------------------------------
    A  → OPENING_DRIVE Tier 1   (brain FAKE_MARKET_TIME = 2026-05-08T09:28:00)
    B  → OPENING_DRIVE Tier 2   (brain FAKE_MARKET_TIME = 2026-05-08T09:42:00)
    C  → RANGE_BREAKOUT ORB + HOURLY_BREAKOUT + CHOPPINESS_BREAKOUT + SUPERTREND_FLIP
                                 (brain FAKE_MARKET_TIME = 2026-05-08T10:30:00)

⚠️  AFTER TESTING: Remove REPLAY_MODE and FAKE_MARKET_TIME from brain service immediately.
⚠️  CLEANUP: This script deletes every Redis key it creates before exiting.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import timedelta, timezone

import redis.asyncio as aioredis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("replay_test")

_IST = timezone(timedelta(hours=5, minutes=30))

# ---------------------------------------------------------------------------
# Key tracker — every key written is recorded here for cleanup
# ---------------------------------------------------------------------------
_WRITTEN_KEYS: list[str] = []


# ---------------------------------------------------------------------------
# Redis helpers — all writes go through these so keys are tracked
# ---------------------------------------------------------------------------

async def _hset(r, key: str, mapping: dict):
    await r.hset(key, mapping=mapping)
    if key not in _WRITTEN_KEYS:
        _WRITTEN_KEYS.append(key)


async def _set(r, key: str, value: str):
    await r.set(key, value)
    if key not in _WRITTEN_KEYS:
        _WRITTEN_KEYS.append(key)


async def _rpush_candles(r, key: str, candles: list[dict]):
    """Push candle dicts as JSON strings to a Redis list."""
    values = [json.dumps(c) for c in candles]
    await r.rpush(key, *values)
    if key not in _WRITTEN_KEYS:
        _WRITTEN_KEYS.append(key)


async def _del(r, key: str):
    """Delete a key and stop tracking it."""
    await r.delete(key)
    if key in _WRITTEN_KEYS:
        _WRITTEN_KEYS.remove(key)


async def cleanup(r):
    if not _WRITTEN_KEYS:
        log.info("[cleanup] No keys to remove.")
        return
    log.info("[cleanup] Removing %d keys created during this test run...", len(_WRITTEN_KEYS))
    for key in list(_WRITTEN_KEYS):
        try:
            await r.delete(key)
            log.info("[cleanup] Deleted: %s", key)
        except Exception as exc:
            log.warning("[cleanup] Failed to delete %s: %s", key, exc)
    _WRITTEN_KEYS.clear()
    log.info("[cleanup] Done ✅")


# ---------------------------------------------------------------------------
# Candle / snapshot helpers
# ---------------------------------------------------------------------------

def _candle(open_: float, high: float, low: float, close: float,
             volume: int, ts: str) -> dict:
    """Build a single candle dict."""
    return {
        "open":   round(open_, 4),
        "high":   round(high,  4),
        "low":    round(low,   4),
        "close":  round(close, 4),
        "volume": volume,
        "ts":     ts,
    }


def _trending_candles(start: float = 98.0, step: float = 0.3,
                       count: int = 22) -> list[dict]:
    """
    Candles trending cleanly upward.
    Body health ~0.75 each, ER of the full set ≈ 0.90.
    Used by: OPENING_DRIVE, RANGE_BREAKOUT, HOURLY_BREAKOUT, SUPERTREND_FLIP.

    Produces RSI that is rising over the last 6 candles, satisfying
    HOURLY_BREAKOUT's rsi_now > rsi_30m_ago check (needs 20+ closes,
    rsi_30m_ago = rsi on closes[:-6]).
    """
    candles = []
    prev_c = start
    for i in range(count):
        o = prev_c
        c = round(o + step, 4)
        h = round(c + 0.10, 4)   # tiny upper wick
        l = round(o - 0.10, 4)   # tiny lower wick
        # body_health = (c-o)/(h-l) = step/(step+0.20)
        candles.append(_candle(o, h, l, c, volume=15_000,
                                ts=f"2026-05-08T09:{15+i:02d}:00+05:30"))
        prev_c = c
    return candles


def _compressed_candles(base: float = 100.0) -> list[dict]:
    """
    15 candles designed to trigger CHOPPINESS_BREAKOUT.

    Layout (all ranges and closes are deliberate to satisfy the
    efficiency-ratio and range-contraction checks):

    Candles 0–6  (7 candles) : wide choppy range ≈ 1.0
                                ER of the slice is << 0.35 (goes nowhere)
    Candles 7–13 (7 candles) : tight compressed range ≈ 0.20
                                avg_range < avg_range_before * 0.95 ✓
                                ER of any suffix ending here << 0.35 ✓
    Candle  14   (breakout)  : strong bullish body, close breaks above
                                compression high, ER of full 15-candle
                                window > 0.45 ✓

    Body health of breakout candle:
        open=100.05, high=101.60, low=99.95, close=101.45
        body = 1.40, range = 1.65, health = 0.848 ≥ 0.70 ✓

    comp_high (max high of candles 7-13) = 100.20
    close (101.45) > comp_high (100.20) → direction = LONG ✓
    """
    # Wide choppy candles — oscillate ±0.5 around base
    wide = [
        _candle(100.2, 100.7, 99.7, 99.8,  12_000, "2026-05-08T09:15:00+05:30"),
        _candle(99.8,  100.4, 99.3, 100.3, 13_000, "2026-05-08T09:20:00+05:30"),
        _candle(100.3, 100.8, 99.8, 99.7,  11_000, "2026-05-08T09:25:00+05:30"),
        _candle(99.7,  100.3, 99.2, 100.2, 14_000, "2026-05-08T09:30:00+05:30"),
        _candle(100.2, 100.6, 99.7, 99.9,  12_000, "2026-05-08T09:35:00+05:30"),
        _candle(99.9,  100.5, 99.4, 100.1, 11_000, "2026-05-08T09:40:00+05:30"),
        _candle(100.1, 100.6, 99.6, 99.8,  13_000, "2026-05-08T09:45:00+05:30"),
    ]
    # Tight compressed candles — range ≈ 0.20, oscillate ±0.05 around base
    tight = [
        _candle(99.95, 100.10, 99.90, 100.05, 9_000, "2026-05-08T09:50:00+05:30"),
        _candle(100.05,100.20, 99.95, 99.95,  8_500, "2026-05-08T09:55:00+05:30"),
        _candle(99.95, 100.10, 99.85, 100.00, 8_000, "2026-05-08T10:00:00+05:30"),
        _candle(100.00,100.15, 99.90, 100.05, 8_200, "2026-05-08T10:05:00+05:30"),
        _candle(100.05,100.15, 99.95, 99.98,  7_800, "2026-05-08T10:10:00+05:30"),
        _candle(99.98, 100.10, 99.88, 100.02, 8_100, "2026-05-08T10:15:00+05:30"),
        _candle(100.02,100.18, 99.92, 100.00, 7_900, "2026-05-08T10:20:00+05:30"),
    ]
    # Breakout candle
    # body_health = (101.45-100.05)/(101.60-99.95) = 1.40/1.65 = 0.848 ✓
    # close(101.45) > comp_high(100.20) ✓
    # volume surge for cum_rvol confirmation
    breakout = [
        _candle(100.05, 101.60, 99.95, 101.45, 28_000, "2026-05-08T10:25:00+05:30"),
    ]
    return wide + tight + breakout


def _base_snapshot(symbol: str, ltp: float, prev_close: float,
                    extra: dict | None = None) -> dict:
    """
    Common snapshot fields satisfying all 3 gatekeeper gates and
    producing a high conviction score:

    Gate 1: rsi14=58   → not extreme (< 80 for LONG)
    Gate 2: ema9=101.0 → |ltp-ema9|/ema9 * 100 ≈ 1% → < 3% ✓
    Gate 3: atr14=0.80 → 0.80/102 * 100 = 0.78% → > 0.2% ✓

    Scorer P1: cum_rvol=1.90 → 25 pts
    Scorer P2: ltp=102, prev_close=100 → +2.0% vs NIFTY +0.9% → RS=1.1% → 25 pts
    Scorer P4: ltp > vwap, vwap_slope > 0 → 20 pts
    """
    snap = {
        "ltp":            str(round(ltp, 4)),
        "prev_close":     str(round(prev_close, 4)),
        "rsi14":          "58",
        "ema9":           "101.0",
        "atr14":          "0.80",
        "vwap":           "101.2",
        "vwap_slope":     "0.3",
        "cum_rvol":       "1.90",
        "sector":         "TEST",
        "lot_size":       "100",
        "strike_step":    "5",
        # Fields for signal conditions
        "prev_high":      "101.50",
        "prev_low":       "98.50",
        "gap_pct":        "0.50",       # gap up → LONG only
        "supertrend_dir": "BULL",
        "ema200":         "97.5",
        "supertrend_band":"100.20",
        "orb_high":       "101.80",
        "orb_low":        "99.50",
        "rolling_1h_high":"102.00",
        "rolling_1h_low": "99.00",
        "postlunch_high": "101.50",
        "postlunch_low":  "99.00",
    }
    if extra:
        snap.update(extra)
    return snap


async def _write_nifty(r):
    """NIFTY snapshot needed by scorer Pillar 2 (RS) and Pillar 5 (alignment)."""
    # NIFTY up 0.9% — BULL supertrend → P5 alignment bonus fires for LONG signals
    await _hset(r, "snapshot:NIFTY", {
        "ltp":            "22500",
        "prev_close":     "22300",
        "supertrend_dir": "BULL",
        "ema9":           "22450",
        "rsi14":          "58",
        "atr14":          "120",
        "vwap":           "22480",
        "vwap_slope":     "0.5",
        "cum_rvol":       "1.5",
        "sector":         "INDEX",
    })
    log.info("[setup] NIFTY snapshot written")


async def _publish_trigger(r, symbol: str, ts_1m: str, ts_5m: str):
    """
    Step 1: publish to candles:1m → triggers on_1m_candle → scan_all_signals
    Step 2: wait 3s for brain to detect + queue the signal
    Step 3: publish to candles:5m → triggers on_5m_candle → score + publish
    """
    trigger_1m = json.dumps({"symbol": symbol, "timeframe": "1m", "ts": ts_1m,
                              "open": 0, "high": 0, "low": 0, "close": 0, "volume": 0})
    await r.publish("candles:1m", trigger_1m)
    log.info("[trigger] Published to candles:1m for %s", symbol)

    log.info("[trigger] Waiting 3s for brain to detect signal...")
    await asyncio.sleep(3)

    trigger_5m = json.dumps({"symbol": symbol, "timeframe": "5m", "ts": ts_5m,
                              "open": 0, "high": 0, "low": 0, "close": 0, "volume": 0})
    await r.publish("candles:5m", trigger_5m)
    log.info("[trigger] Published to candles:5m for %s — scorer will run now", symbol)


# ---------------------------------------------------------------------------
# Signal setup functions
# Each returns the symbol it set up.
# ---------------------------------------------------------------------------

async def setup_opening_drive_t1(r) -> str:
    """
    OPENING_DRIVE Tier 1 — LONG
    FAKE_MARKET_TIME: 2026-05-08T09:28:00
    Trigger time: slot 0925 (now.minute=28, (28//5)*5=25)

    Candle (last in list):
        open=102.00, high=103.10, low=101.90, close=103.00
        body_health = (103-102)/(103.1-101.9) = 1.0/1.2 = 0.833 ≥ 0.80 ✓
        close(103.00) > prev_high(101.50) → LONG ✓

    Vol check:
        vol_profile:5m slot "0925" = 10,000 shares
        candle volume = 25,000
        vol_mult = 2.50 ≥ 2.30 (Tier 1 threshold for lot_size≤500) ✓
    """
    sym = "TEST_OD1"

    snap = _base_snapshot(sym, ltp=103.0, prev_close=100.0, extra={
        "prev_high": "101.50",
        "gap_pct":   "0.50",   # gap up → LONG only
    })
    await _hset(r, f"snapshot:{sym}", snap)

    # vol_profile slot "0925" = 10,000 shares
    await _set(r, f"vol_profile:5m:{sym}",
               json.dumps({"0915": 12000, "0920": 11000, "0925": 10000, "0930": 10500}))

    # Candle history (trending) for scorer ER + RSI calculations
    candles = _trending_candles(start=98.0, step=0.25, count=22)

    # Replace last candle with the actual OPENING_DRIVE trigger candle
    # body_health = 1.0/1.2 = 0.833 ✓,  close > prev_high ✓,  volume = 25000
    candles[-1] = _candle(102.00, 103.10, 101.90, 103.00, 25_000,
                           "2026-05-08T09:30:00+05:30")
    await _rpush_candles(r, f"candles:5m:{sym}", candles)

    # Clear cooldown
    await r.delete(f"opening_drive_fired:{sym}")

    log.info("[setup] TEST_OD1 (OPENING_DRIVE T1) ready")
    return sym


async def setup_opening_drive_t2(r) -> str:
    """
    OPENING_DRIVE Tier 2 — LONG
    FAKE_MARKET_TIME: 2026-05-08T09:42:00
    Trigger time: slot 0940 (now.minute=42, (42//5)*5=40)

    Candle:
        open=102.00, high=103.60, low=101.90, close=103.44
        body_health = 1.44/1.70 = 0.847 ≥ 0.72 ✓
        close(103.44) > prev_high(101.50) → LONG ✓

    Vol check:
        vol_profile:5m slot "0940" = 10,000
        candle volume = 20,000
        vol_mult = 2.00 ≥ 1.90 (Tier 2) ✓
    """
    sym = "TEST_OD2"

    snap = _base_snapshot(sym, ltp=103.44, prev_close=100.0, extra={
        "prev_high": "101.50",
        "gap_pct":   "0.50",
    })
    await _hset(r, f"snapshot:{sym}", snap)

    await _set(r, f"vol_profile:5m:{sym}",
               json.dumps({"0930": 11000, "0935": 10500, "0940": 10000, "0945": 10200}))

    candles = _trending_candles(start=98.0, step=0.25, count=22)
    candles[-1] = _candle(102.00, 103.60, 101.90, 103.44, 20_000,
                           "2026-05-08T09:45:00+05:30")
    await _rpush_candles(r, f"candles:5m:{sym}", candles)

    await r.delete(f"opening_drive_fired:{sym}")

    log.info("[setup] TEST_OD2 (OPENING_DRIVE T2) ready")
    return sym


async def setup_range_breakout_orb(r) -> str:
    """
    RANGE_BREAKOUT ORB — LONG
    FAKE_MARKET_TIME: 2026-05-08T10:30:00 (9:30–11:30 window ✓)

    ORB range: orb_high=101.80, orb_low=99.50
    Range = 2.30, range_pct = 2.30/100 = 2.3% → within 0.3%-2.5% ✓

    Candle:
        open=102.00, high=103.80, low=101.90, close=103.50
        body_health = 1.50/1.90 = 0.789 ≥ 0.70 ✓
        close(103.50) > orb_high(101.80) → LONG ✓

    cum_rvol = 1.80 ≥ 1.30 ✓
    gap_pct = 0.50 (gap up → LONG only) ✓
    """
    sym = "TEST_RB_ORB"

    snap = _base_snapshot(sym, ltp=103.50, prev_close=100.0, extra={
        "orb_high": "101.80",
        "orb_low":  "99.50",
        "cum_rvol": "1.80",
        "gap_pct":  "0.50",
    })
    await _hset(r, f"snapshot:{sym}", snap)

    candles = _trending_candles(start=98.0, step=0.25, count=22)
    candles[-1] = _candle(102.00, 103.80, 101.90, 103.50, 20_000,
                           "2026-05-08T10:30:00+05:30")
    await _rpush_candles(r, f"candles:5m:{sym}", candles)

    await r.delete(f"range_breakout_fired:{sym}")

    log.info("[setup] TEST_RB_ORB (RANGE_BREAKOUT ORB) ready")
    return sym


async def setup_hourly_breakout(r) -> str:
    """
    HOURLY_BREAKOUT — LONG
    FAKE_MARKET_TIME: 2026-05-08T10:30:00 (after 10:15 ✓)

    Requires len(closes_5m) >= 20 for rsi_30m_ago comparison.
    rsi_now > rsi_30m_ago: trending closes guarantee this.
    _trending_candles(count=22) gives 22 candles → 22 closes ✓

    rolling_1h_high = 102.00 (from snapshot)

    Candle:
        open=102.10, high=103.20, low=102.00, close=103.00
        body_health = 0.90/1.20 = 0.750 ≥ 0.70 ✓
        close(103.00) > rolling_1h_high(102.00) → LONG ✓

    cum_rvol = 1.80 ≥ 1.30 ✓
    """
    sym = "TEST_HB"

    snap = _base_snapshot(sym, ltp=103.00, prev_close=100.0, extra={
        "rolling_1h_high": "102.00",
        "rolling_1h_low":  "99.00",
        "cum_rvol":        "1.80",
    })
    await _hset(r, f"snapshot:{sym}", snap)

    # 22 trending candles ensures:
    # - closes_5m has 22 elements ≥ 20 needed for rsi_30m_ago
    # - RSI of closes[:-6] < RSI of closes (trending up → RSI rises) ✓
    candles = _trending_candles(start=96.0, step=0.30, count=22)
    candles[-1] = _candle(102.10, 103.20, 102.00, 103.00, 18_000,
                           "2026-05-08T10:30:00+05:30")
    await _rpush_candles(r, f"candles:5m:{sym}", candles)

    await r.delete(f"hourly_breakout_fired:{sym}:LONG")

    log.info("[setup] TEST_HB (HOURLY_BREAKOUT) ready")
    return sym


async def setup_choppiness_breakout(r) -> str:
    """
    CHOPPINESS_BREAKOUT — LONG
    FAKE_MARKET_TIME: 2026-05-08T10:30:00

    See _compressed_candles() docstring for exact verification of:
    - compression_count (7 tight candles → ER < 0.35 each)
    - range contraction (avg tight < avg wide * 0.95)
    - breakout candle body_health = 0.848 ≥ 0.70
    - close(101.45) > comp_high(100.20)
    - er_now (full 15-candle window) > 0.45

    cum_rvol = 1.80 ≥ 1.30 ✓
    """
    sym = "TEST_CB"

    snap = _base_snapshot(sym, ltp=101.45, prev_close=100.0, extra={
        "cum_rvol": "1.80",
    })
    await _hset(r, f"snapshot:{sym}", snap)

    candles = _compressed_candles(base=100.0)
    await _rpush_candles(r, f"candles:5m:{sym}", candles)

    await r.delete(f"choppiness_fired:{sym}")

    log.info("[setup] TEST_CB (CHOPPINESS_BREAKOUT) ready")
    return sym


async def setup_supertrend_flip(r) -> str:
    """
    SUPERTREND_FLIP — LONG (BEAR → BULL flip)
    FAKE_MARKET_TIME: 2026-05-08T10:30:00

    snapshot:       supertrend_dir = BULL (current, just flipped)
    snapshot_prev:  supertrend_dir = BEAR (yesterday = old direction)
    → flip detected: BEAR→BULL → LONG signal ✓

    ltp(103.00) > supertrend_band(100.20) (ltp on correct side) ✓
    rsi14(62) < 75 ✓
    atr14(0.85) / ltp(103) * 100 = 0.825% > 0.3% ✓
    cooldown key cleared ✓
    """
    sym = "TEST_ST"

    snap = _base_snapshot(sym, ltp=103.00, prev_close=100.0, extra={
        "supertrend_dir":  "BULL",
        "supertrend_band": "100.20",
        "rsi14":           "62",
        "atr14":           "0.85",
    })
    await _hset(r, f"snapshot:{sym}", snap)

    # snapshot_prev must be a JSON STRING (GET key, not HGET)
    await _set(r, f"snapshot_prev:{sym}",
               json.dumps({"supertrend_dir": "BEAR", "supertrend_band": "102.80"}))

    candles = _trending_candles(start=98.0, step=0.25, count=22)
    candles[-1] = _candle(102.00, 103.50, 101.80, 103.00, 20_000,
                           "2026-05-08T10:30:00+05:30")
    await _rpush_candles(r, f"candles:5m:{sym}", candles)

    # Clear both direction cooldowns
    await r.delete(f"st_flip_fired:{sym}:BULL")
    await r.delete(f"st_flip_fired:{sym}:BEAR")

    log.info("[setup] TEST_ST (SUPERTREND_FLIP BEAR→BULL) ready")
    return sym


# ---------------------------------------------------------------------------
# Batch definitions
# ---------------------------------------------------------------------------

BATCH_CONFIG = {
    "A": {
        "required_fake_time": "2026-05-08T09:28:00",
        "signals": [
            ("OPENING_DRIVE Tier 1", setup_opening_drive_t1,
             "2026-05-08T09:29:00+05:30", "2026-05-08T09:30:00+05:30"),
        ],
    },
    "B": {
        "required_fake_time": "2026-05-08T09:42:00",
        "signals": [
            ("OPENING_DRIVE Tier 2", setup_opening_drive_t2,
             "2026-05-08T09:44:00+05:30", "2026-05-08T09:45:00+05:30"),
        ],
    },
    "C": {
        "required_fake_time": "2026-05-08T10:30:00",
        "signals": [
            ("RANGE_BREAKOUT ORB",   setup_range_breakout_orb,
             "2026-05-08T10:29:00+05:30", "2026-05-08T10:30:00+05:30"),
            ("HOURLY_BREAKOUT",      setup_hourly_breakout,
             "2026-05-08T10:29:00+05:30", "2026-05-08T10:30:00+05:30"),
            ("CHOPPINESS_BREAKOUT",  setup_choppiness_breakout,
             "2026-05-08T10:29:00+05:30", "2026-05-08T10:30:00+05:30"),
            ("SUPERTREND_FLIP",      setup_supertrend_flip,
             "2026-05-08T10:29:00+05:30", "2026-05-08T10:30:00+05:30"),
        ],
    },
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    batch = os.environ.get("REPLAY_BATCH", "").upper().strip()
    if batch not in BATCH_CONFIG:
        log.error("=" * 60)
        log.error("REPLAY_BATCH env var not set or invalid.")
        log.error("")
        log.error("Set REPLAY_BATCH on this Railway service to one of:")
        log.error("")
        log.error("  A  → OPENING_DRIVE Tier 1")
        log.error("       Set brain FAKE_MARKET_TIME = 2026-05-08T09:28:00")
        log.error("")
        log.error("  B  → OPENING_DRIVE Tier 2")
        log.error("       Set brain FAKE_MARKET_TIME = 2026-05-08T09:42:00")
        log.error("")
        log.error("  C  → RANGE_BREAKOUT ORB + HOURLY + CHOPPINESS + SUPERTREND_FLIP")
        log.error("       Set brain FAKE_MARKET_TIME = 2026-05-08T10:30:00")
        log.error("=" * 60)
        sys.exit(1)

    cfg = BATCH_CONFIG[batch]

    log.info("=" * 60)
    log.info("GodSpeed Replay Test — Batch %s", batch)
    log.info("Required FAKE_MARKET_TIME on brain service: %s", cfg["required_fake_time"])
    log.info("Signals to test: %s", [s[0] for s in cfg["signals"]])
    log.info("=" * 60)
    log.info("")
    log.info("⚠️  Confirm REPLAY_MODE=1 is set on the brain service before continuing.")
    log.info("⚠️  Confirm FAKE_MARKET_TIME=%s is set on the brain service.", cfg["required_fake_time"])
    log.info("")

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")

    r = await aioredis.from_url(
        redis_url,
        encoding="utf-8",
        decode_responses=True,
        max_connections=5,
    )

    try:
        await r.ping()
        log.info("[redis] Connected ✅")
        log.info("[replay] Waiting 45s for brain to subscribe to channels...")
        await asyncio.sleep(45)
    except Exception as exc:
        log.error("[redis] Cannot connect: %s", exc)
        sys.exit(1)

    try:
        # Write NIFTY snapshot first — scorer Pillar 2 and 5 need it
        await _write_nifty(r)
        await asyncio.sleep(0.3)

        for signal_name, setup_fn, ts_1m, ts_5m in cfg["signals"]:
            log.info("-" * 50)
            log.info("[test] Setting up: %s", signal_name)

            sym = await setup_fn(r)
            await asyncio.sleep(0.5)   # let Redis writes settle

            await _publish_trigger(r, sym, ts_1m, ts_5m)

            log.info("[test] ✅ %s published", signal_name)
            log.info("[test]    Symbol on dashboard: %s", sym)
            log.info("[test]    Expected badge: ✅ EXEC or 👁 WATCH")
            log.info("[test]    Check brain logs for: [brain] ★ EXECUTION PUBLISHED")

            # Gap between signals so brain doesn't process them simultaneously
            await asyncio.sleep(8)

        log.info("=" * 60)
        log.info("[test] All signals published for Batch %s.", batch)
        log.info("[test] Check the dashboard for signal cards now.")
        log.info("[test] Waiting 20s before cleanup to let scoring finish...")
        await asyncio.sleep(20)

    finally:
        await cleanup(r)
        await r.aclose()
        log.info("")
        log.info("=" * 60)
        log.info("⚠️  IMPORTANT: Remove these from the brain Railway service now:")
        log.info("    REPLAY_MODE")
        log.info("    FAKE_MARKET_TIME")
        log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
