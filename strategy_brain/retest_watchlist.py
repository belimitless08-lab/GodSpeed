"""
strategy_brain/retest_watchlist.py
=====================================
In-memory retest watchlist — manages signals parked in WAIT_RETEST state.

When Gate 5 (Max Extension) flags a signal as "WAIT_RETEST", the signal
is not dropped — instead it's stored here with an anchor level and expiry.
On every 1m candle close, `check_retest_triggers()` is called to see if
price has pulled back to the anchor level.  If so, the signal fires.

State
-----
Pure Python dict — never written to Redis.  If process restarts, the
watchlist resets (acceptable: these are very short-lived positions).

Thread safety
-------------
All reads/writes happen on the single asyncio event loop thread.
No locks needed.

Usage
-----
    from strategy_brain.retest_watchlist import (
        add_to_retest,
        check_retest_triggers,
        get_watchlist_snapshot,
    )
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_IST = timezone(timedelta(hours=5, minutes=30))

_RETEST_EXPIRY_MINUTES   = 30   # 6 x 5m candles
_RETEST_TOUCH_THRESHOLD  = 0.2  # within 0.2% of anchor = "touched"

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
# retest_watchlist[symbol] = {
#     "signal":        dict,      # original signal payload
#     "retest_level":  float,     # EMA9 or VWAP anchor at signal time
#     "expires_at":    datetime,
#     "direction":     str,       # "LONG" | "SHORT"
#     "added_at":      str,       # ISO timestamp for diagnostics
# }
retest_watchlist: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def add_to_retest(symbol: str, signal: dict, snapshot: dict) -> None:
    """
    Park *signal* in the retest watchlist.

    The anchor level is the nearer of EMA9 and VWAP at signal time —
    whichever the price is more likely to pull back to first.

    If *symbol* is already being watched, the existing entry is replaced
    (the newer signal supersedes).

    Parameters
    ----------
    symbol   : NSE underlying symbol
    signal   : Original signal dict (from signal_engines)
    snapshot : Current snapshot dict (hgetall result, values are strings)
    """
    def _f(key: str, default: float = 0.0) -> float:
        try:
            return float(snapshot.get(key, default))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return default

    ltp  = _f("ltp")
    ema9 = _f("ema9",  ltp)
    vwap = _f("vwap",  ltp)

    # Anchor = the level closest to current price (pullback target)
    retest_level = min(ema9, vwap, key=lambda v: abs(v - ltp))
    if retest_level == 0:
        retest_level = ema9 if ema9 > 0 else vwap

    now        = datetime.now(_IST)
    expires_at = now + timedelta(minutes=_RETEST_EXPIRY_MINUTES)

    retest_watchlist[symbol] = {
        "signal":       signal,
        "retest_level": retest_level,
        "expires_at":   expires_at,
        "direction":    signal.get("direction", "LONG"),
        "added_at":     now.isoformat(),
    }

    logger.info(
        "[retest_watchlist] Added %s — level=%.2f expires=%s direction=%s",
        symbol, retest_level, expires_at.strftime("%H:%M"), signal.get("direction"),
    )


async def check_retest_triggers(current_snapshots: dict[str, dict]) -> list[dict]:
    """
    Evaluate every watched symbol against current prices.

    Called on every 1m candle close.  Removes expired and triggered entries.

    Parameters
    ----------
    current_snapshots : {symbol: snapshot_dict} for all active symbols.
                        Each snapshot_dict is the raw hgetall result.

    Returns
    -------
    list[dict] — signals that just touched their retest level.
    Each item is the original signal dict with an extra key:
        "entry_type": "RETEST"
    """
    triggered: list[dict]   = []
    to_remove:  list[str]   = []
    now = datetime.now(_IST)

    for symbol, watch in retest_watchlist.items():
        # ── Expiry check ─────────────────────────────────────────────
        if now > watch["expires_at"]:
            logger.info("[retest_watchlist] %s expired — removing", symbol)
            to_remove.append(symbol)
            continue

        # ── Price check ──────────────────────────────────────────────
        snap = current_snapshots.get(symbol)
        if not snap:
            continue

        try:
            ltp = float(snap.get("ltp", 0) or 0)
        except (TypeError, ValueError):
            continue

        if ltp == 0:
            continue

        level   = watch["retest_level"]
        dist_pct = abs(ltp - level) / level * 100 if level else 100.0

        if dist_pct < _RETEST_TOUCH_THRESHOLD:
            logger.info(
                "[retest_watchlist] %s TRIGGERED — ltp=%.2f level=%.2f dist=%.3f%%",
                symbol, ltp, level, dist_pct,
            )
            triggered_signal = {
                **watch["signal"],
                "entry_type":   "RETEST",
                "entry_price":  level,   # actual retest level becomes entry
                "triggered_at": now.isoformat(),
            }
            triggered.append(triggered_signal)
            to_remove.append(symbol)

    # ── Cleanup ──────────────────────────────────────────────────────
    for symbol in to_remove:
        retest_watchlist.pop(symbol, None)

    return triggered


def remove_from_retest(symbol: str) -> None:
    """Manually remove a symbol from the watchlist (e.g. on position open)."""
    removed = retest_watchlist.pop(symbol, None)
    if removed:
        logger.info("[retest_watchlist] Manually removed %s", symbol)


def get_watchlist_snapshot() -> list[dict]:
    """
    Return a JSON-safe snapshot of the current watchlist for diagnostics /
    API exposure.

    Returns
    -------
    list[dict] — one entry per watched symbol:
        {symbol, direction, retest_level, expires_at (ISO str), added_at}
    """
    now = datetime.now(_IST)
    result = []
    for symbol, watch in retest_watchlist.items():
        remaining = (watch["expires_at"] - now).total_seconds()
        result.append({
            "symbol":        symbol,
            "direction":     watch["direction"],
            "retest_level":  watch["retest_level"],
            "expires_at":    watch["expires_at"].isoformat(),
            "added_at":      watch["added_at"],
            "remaining_sec": max(0, int(remaining)),
        })
    return result
