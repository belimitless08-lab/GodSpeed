"""
strategy_brain/conviction_scorer.py
=====================================
ICI Conviction Scorer v2.
Replaces fetch_scoring_inputs + compute_ici_score with single score_signal().

Max pillar score : 100
Max bonus        : 10
Max total        : 110

Thresholds: EXECUTE >= 62 | WATCHLIST >= 48 | IGNORE < 48
"""
from __future__ import annotations
import json
import logging
from typing import Optional
from core.redis_client import get_redis

logger = logging.getLogger(__name__)

EXECUTE_THRESHOLD   = 62
WATCHLIST_THRESHOLD = 48

SIGNAL_BONUS = {
    "OPENING_DRIVE_TIER1":        10,
    "OPENING_DRIVE_TIER2":         5,
    "CHOPPINESS_BREAKOUT":         8,
    "RANGE_BREAKOUT_ORB":          5,
    "RANGE_BREAKOUT_POSTLUNCH":    5,
    "HOURLY_BREAKOUT":             4,
    "SUPERTREND_FLIP":             5,
}


def _sf(v, d: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def _decode(v) -> str:
    if isinstance(v, bytes):
        return v.decode()
    return str(v or "")


def _calc_er(closes: list, period: int = 12) -> float:
    if len(closes) < period + 1:
        return 0.4
    net  = abs(closes[-1] - closes[-(period + 1)])
    tot  = sum(abs(closes[i] - closes[i - 1]) for i in range(-period, 0))
    return round(net / tot, 4) if tot > 0 else 0.0


def _grade(score: float) -> str:
    if score >= EXECUTE_THRESHOLD:   return "EXECUTE"
    if score >= WATCHLIST_THRESHOLD: return "WATCHLIST"
    return "IGNORE"


async def score_signal(signal: dict, snapshot: dict) -> dict:
    """
    Score a detected signal. Enriches signal dict with:
        ici_score, ici_grade, pillar_breakdown,
        ltp, change_pct, sector, atr14, rsi14
    """
    redis     = await get_redis()
    symbol    = signal.get("symbol", "")
    direction = signal.get("direction", "LONG")
    sig_type  = signal.get("type", "")

    # --- Always enrich with display fields ---
    ltp        = _sf(snapshot.get("ltp"))
    prev_close = _sf(snapshot.get("prev_close"))
    change_pct = round((ltp - prev_close) / max(prev_close, 1) * 100, 2) if prev_close > 0 else 0.0
    signal["ltp"]        = round(ltp, 2)
    signal["change_pct"] = change_pct
    signal["sector"]     = _decode(snapshot.get("sector", "UNKNOWN"))
    signal["atr14"]      = round(_sf(snapshot.get("atr14")), 2)
    signal["rsi14"]      = round(_sf(snapshot.get("rsi14"), 50.0), 1)

    # ── Pillar 1: Cumulative RVOL base + boost (25 pts max) ─────────────
    cr  = _sf(snapshot.get("cum_rvol"))
    va  = _sf(snapshot.get("vol_accel"))
    con = _sf(snapshot.get("consec_rvol"))
    if   cr >= 1.8: base = 25
    elif cr >= 1.5: base = 18
    elif cr >= 1.3: base = 10
    else:            base = 0
    # Boost: vol_accel >= 2.0 adds 2pts, consec_rvol >= 3 adds 1pt
    boost = (2 if va >= 2.0 else (1 if va >= 1.5 else 0)) + (1 if con >= 3 else 0)
    p1 = min(25, base + boost)

    # ── Pillar 2: Relative Strength vs NIFTY (25 pts) ───────────────────
    nifty = {}
    try:
        nifty = await redis.hgetall("snapshot:NIFTY")
        n_ltp  = _sf(nifty.get(b"ltp")        or nifty.get("ltp"))
        n_prev = _sf(nifty.get(b"prev_close")  or nifty.get("prev_close"))
        n_chg  = (n_ltp - n_prev) / max(n_prev, 1) * 100 if n_prev > 0 else 0.0
    except Exception:
        n_chg = 0.0

    rs = (change_pct - n_chg) if direction == "LONG" else (n_chg - change_pct)
    if   rs > 1.0: p2 = 25
    elif rs > 0.5: p2 = 18
    elif rs > 0.0: p2 = 10
    else:           p2 = 0

    # ── Pillar 3: Options OI Flow (20 pts) ──────────────────────────────
    p3 = 12  # neutral default
    try:
        strike_step = _sf(snapshot.get("strike_step"), 50.0)
        atm         = round(ltp / strike_step) * strike_step
        opt_type    = "CE" if direction == "LONG" else "PE"
        tick_raw    = await redis.hgetall(f"options:tick:{symbol}:{int(atm)}{opt_type}")
        prev_raw    = await redis.get(f"options:prev:{symbol}")

        if tick_raw and prev_raw:
            prev      = json.loads(prev_raw)
            curr_oi   = _sf(tick_raw.get(b"oi")  or tick_raw.get("oi"))
            prev_oi   = _sf(prev.get("oi"))
            curr_ltp  = _sf(tick_raw.get(b"ltp") or tick_raw.get("ltp"))
            prev_ltp  = _sf(prev.get("ltp"))

            if curr_oi > 0 and prev_oi > 0:
                oi_up    = curr_oi   > prev_oi   * 1.02
                price_up = curr_ltp  > prev_ltp  * 1.01
                if   oi_up and price_up:      p3 = 20
                elif not oi_up and price_up:  p3 = 15
                elif oi_up and not price_up:  p3 = 0
                else:                          p3 = 12
    except Exception:
        p3 = 12

    # ── Pillar 4: VWAP Position + Slope (20 pts) ────────────────────────
    vwap  = _sf(snapshot.get("vwap"))
    slope = _sf(snapshot.get("vwap_slope"))
    if direction == "LONG":
        if   ltp > vwap and slope > 0: p4 = 20
        elif ltp > vwap:                p4 = 12
        else:                            p4 = 0
    else:
        if   ltp < vwap and slope < 0: p4 = 20
        elif ltp < vwap:                p4 = 12
        else:                            p4 = 0

    # ── Pillar 5: Market Regime (ER) + NIFTY Alignment (10 pts) ─────────
    try:
        raw_5m = await redis.lrange(f"candles:5m:{symbol}", -14, -1)
        closes = []
        for r in raw_5m:
            try:
                c = json.loads(r)
                closes.append(float(c[4] if isinstance(c, list) else c.get("close", 0)))
            except Exception:
                pass
        er_val = _calc_er(closes, period=12) if len(closes) >= 13 else 0.4
    except Exception:
        er_val = 0.4

    if sig_type == "CHOPPINESS_BREAKOUT":
        p5_regime = 7 if er_val < 0.25 else (5 if er_val < 0.50 else 0)
    else:
        p5_regime = 7 if er_val > 0.50 else (5 if er_val > 0.25 else 0)

    # Supertrend alignment removed — direction unreliable until 5m 
    # supertrend seeding is verified. P5 is regime score only.
    p5 = min(p5_regime, 10)

    # ── Signal bonus ─────────────────────────────────────────────────────
    bkey = sig_type
    if sig_type == "OPENING_DRIVE":
        bkey = f"OPENING_DRIVE_{signal.get('tier', 'TIER2')}"
    elif sig_type == "RANGE_BREAKOUT":
        bkey = f"RANGE_BREAKOUT_{signal.get('phase', 'ORB')}"
    bonus = SIGNAL_BONUS.get(bkey, 3)
    # Multi-day volume build confirmation (+3 bonus, cap total at 13)
    vol_trend = _decode(snapshot.get("vol_trend_3d", "FLAT"))
    if vol_trend == "RISING":
        bonus = min(bonus + 3, 13)

    total = p1 + p2 + p3 + p4 + p5 + bonus
    grade = _grade(total)

    signal["ici_score"] = total
    signal["ici_grade"] = grade
    signal["pillar_breakdown"] = {
        "rvol": p1, "rs_nifty": p2, "options": p3,
        "vwap": p4, "regime": p5, "bonus": bonus,
    }

    logger.info(
        "[scorer] %s %s %s score=%d grade=%s "
        "p1=%d(cr=%.2f va=%.1f con=%.0f) p2=%d p3=%d p4=%d p5=%d "
        "bonus=%d(trend=%s) er=%.3f",
        symbol, sig_type, direction, total, grade,
        p1, cr, va, con, p2, p3, p4, p5,
        bonus, vol_trend, er_val,
    )
    return signal


# ---------------------------------------------------------------------------
# Backward-compatibility shims (brain.py currently calls these)
# Remove after brain.py is updated.
# ---------------------------------------------------------------------------

async def fetch_scoring_inputs(symbol, signal_direction, vix=None, market_time=None):
    """Shim — returns snapshot dict for compute_ici_score compatibility."""
    from core.redis_client import get_redis as _gr
    r = await _gr()
    snap = await r.hgetall(f"snapshot:{symbol}")
    return {"snapshot": snap or {}, "symbol": symbol,
            "direction": signal_direction}


def compute_ici_score(score_data, symbol, direction, sig_type,
                      active_types=None, vix=None, market_time=None):
    """Shim — returns minimal score_result dict."""
    return {"score": 0, "grade": "IGNORE", "action": "IGNORE"}
