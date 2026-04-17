"""
execution/order_manager.py
===========================
Paper trade lifecycle manager for Market Pulse Pro v2.

Responsibilities
----------------
* Listens to Redis ``trade_execution`` pub/sub channel published by Brain.
* Executes paper orders using live LTP from Redis tick hashes.
* Monitors open trades for stop-loss hits every 5 seconds.
* Updates unrealised PnL every 10 seconds.
* Force-closes all open trades at EOD (called by api_server.py scheduler).
* Exposes a clean async API consumed by api_server.py endpoints.

Paper account state — Redis hash ``paper:account``
---------------------------------------------------
{
    "starting_balance": 1_000_000,
    "available_margin": float,
    "used_margin":      float,
    "realised_pnl":     float,
    "unrealised_pnl":   float,
    "total_pnl":        float,   # realised + unrealised
    "trade_count":      int,
    "win_count":        int,
    "loss_count":       int,
    "updated_at":       ISO-8601
}

All Redis I/O goes through core.redis_client.get_redis() — never import
redis directly.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from uuid import uuid4

from core.redis_client import get_redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_IST = timezone(timedelta(hours=5, minutes=30))

_STARTING_BALANCE: float = 1_000_000.0   # ₹10 lakh

# Paper intraday margin rate (20% = 5x leverage)
_INTRADAY_MARGIN_RATE: float = 0.20

# Redis key / channel names
_ACCT_KEY         = "paper:account"
_OPEN_TRADES_KEY  = "paper:open_trades"
_CLOSED_TRADES_KEY= "paper:closed_trades"
_TRADE_KEY_PREFIX = "paper:trade:"
_CH_EXECUTION     = "trade_execution"
_CH_OPTS_UNSUB    = "options:unsubscribe"
_MKT_STATUS_KEY   = "market:status"

# Tick staleness threshold (seconds)
_TICK_MAX_AGE_S: int = 60

# SL monitor / unrealised-PnL intervals
_SL_MONITOR_INTERVAL_S:  int = 5
_UNRL_UPDATE_INTERVAL_S: int = 10


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_ist() -> datetime:
    return datetime.now(_IST)


def _iso_now() -> str:
    return _now_ist().isoformat()


def _parse_ts(ts_str: Optional[str]) -> datetime:
    """
    Parse an ISO-8601 timestamp string (with or without timezone info) and
    return a timezone-aware datetime in IST.  Falls back to epoch on failure.
    """
    if not ts_str:
        return datetime(1970, 1, 1, tzinfo=_IST)
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_IST)
        return dt.astimezone(_IST)
    except (ValueError, TypeError):
        return datetime(1970, 1, 1, tzinfo=_IST)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalise_expiry(expiry_str: str) -> str:
    """Normalise any expiry format to YYYY-MM-DD (what universe_builder stores)."""
    if not expiry_str:
        return ""
    s = expiry_str.strip().upper()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    from datetime import datetime as _dt
    for fmt in ("%d%b%Y", "%d-%b-%Y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return _dt.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    logger.warning("[order_manager] Could not normalise expiry %r", expiry_str)
    return expiry_str


async def _get_execution_ltp(symbol: str, instrument: str, atm_strike=None) -> float:
    """
    Correct LTP per instrument:
      EQ  -> tick:{symbol}.ltp
      CE  -> options:tick:{symbol}:{atm_strike}CE
      PE  -> options:tick:{symbol}:{atm_strike}PE
    Falls back to spot tick if options feed has no data.
    """
    redis = await get_redis()
    if instrument in ("CE", "PE") and atm_strike:
        suffix = "CE" if instrument == "CE" else "PE"
        opts = await redis.hgetall(f"options:tick:{symbol}:{atm_strike}{suffix}")
        ltp = _safe_float(opts.get("ltp"))
        if ltp > 0:
            return ltp
        logger.warning(
            "[order_manager] options:tick:%s:%s%s has no LTP — falling back to spot",
            symbol, atm_strike, suffix,
        )
    tick = await redis.hgetall(f"tick:{symbol}")
    return _safe_float(tick.get("ltp"))


# ---------------------------------------------------------------------------
# Paper account management
# ---------------------------------------------------------------------------

_DEFAULT_ACCOUNT: dict = {
    "starting_balance": _STARTING_BALANCE,
    "available_margin": _STARTING_BALANCE,
    "used_margin":      0.0,
    "realised_pnl":     0.0,
    "unrealised_pnl":   0.0,
    "total_pnl":        0.0,
    "trade_count":      0,
    "win_count":        0,
    "loss_count":       0,
    "updated_at":       "",
}


async def init_paper_account() -> None:
    """
    Load account from Redis on startup.  Initialises with starting_balance
    = ₹10L if the key does not exist yet.
    """
    redis = await get_redis()
    raw = await redis.hgetall(_ACCT_KEY)

    if not raw:
        logger.info("[order_manager] No existing paper account — initialising with ₹%.0f.", _STARTING_BALANCE)
        initial = dict(_DEFAULT_ACCOUNT)
        initial["updated_at"] = _iso_now()
        await redis.hset(_ACCT_KEY, mapping={k: str(v) for k, v in initial.items()})
    else:
        logger.info("[order_manager] Paper account loaded from Redis — balance=%.2f.",
                    _safe_float(raw.get("available_margin")))


async def get_paper_account() -> dict:
    """Return the current paper account state as a typed dict."""
    redis = await get_redis()
    raw = await redis.hgetall(_ACCT_KEY)
    if not raw:
        return dict(_DEFAULT_ACCOUNT)
    return {
        "starting_balance": _safe_float(raw.get("starting_balance"), _STARTING_BALANCE),
        "available_margin": _safe_float(raw.get("available_margin"), _STARTING_BALANCE),
        "used_margin":      _safe_float(raw.get("used_margin")),
        "realised_pnl":     _safe_float(raw.get("realised_pnl")),
        "unrealised_pnl":   _safe_float(raw.get("unrealised_pnl")),
        "total_pnl":        _safe_float(raw.get("total_pnl")),
        "trade_count":      int(_safe_float(raw.get("trade_count"))),
        "win_count":        int(_safe_float(raw.get("win_count"))),
        "loss_count":       int(_safe_float(raw.get("loss_count"))),
        "updated_at":       raw.get("updated_at", ""),
    }


async def _update_paper_account(
    margin_used:        float = 0.0,   # positive = consume margin (open)
    margin_released:    float = 0.0,   # positive = release margin (close)
    realised_pnl_delta: float = 0.0,
    trade_opened:       bool  = False,
    win:                Optional[bool] = None,
) -> None:
    """
    Atomically update the paper account hash in Redis.

    Parameters are additive deltas applied to the current values.
    """
    redis  = await get_redis()
    raw    = await redis.hgetall(_ACCT_KEY)

    avail  = _safe_float(raw.get("available_margin"), _STARTING_BALANCE)
    used   = _safe_float(raw.get("used_margin"))
    rpnl   = _safe_float(raw.get("realised_pnl"))
    upnl   = _safe_float(raw.get("unrealised_pnl"))
    tcount = int(_safe_float(raw.get("trade_count")))
    wcount = int(_safe_float(raw.get("win_count")))
    lcount = int(_safe_float(raw.get("loss_count")))

    avail -= margin_used
    avail += margin_released
    used  += margin_used
    used  -= margin_released
    rpnl  += realised_pnl_delta
    # available margin also changes by realised PnL (profit/loss affects balance)
    avail += realised_pnl_delta

    if trade_opened:
        tcount += 1
    if win is True:
        wcount += 1
    elif win is False:
        lcount += 1

    total_pnl = rpnl + upnl

    mapping = {
        "available_margin": str(round(avail, 4)),
        "used_margin":      str(round(max(used, 0.0), 4)),
        "realised_pnl":     str(round(rpnl, 4)),
        "total_pnl":        str(round(total_pnl, 4)),
        "trade_count":      str(tcount),
        "win_count":        str(wcount),
        "loss_count":       str(lcount),
        "updated_at":       _iso_now(),
    }
    await redis.hset(_ACCT_KEY, mapping=mapping)


# ---------------------------------------------------------------------------
# Market status helpers
# ---------------------------------------------------------------------------

async def check_market_open() -> bool:
    """
    Returns True if the market is open.

    Reads the ``market:status`` key written by the feed layer.  Falls back
    to a time-based check (9:15–15:30 IST on weekdays) if the key is absent.
    """
    redis = await get_redis()
    status_raw = await redis.get(_MKT_STATUS_KEY)

    if status_raw:
        try:
            status = json.loads(status_raw)
            return str(status.get("open", "false")).lower() == "true"
        except (json.JSONDecodeError, TypeError):
            pass

    # Fallback: time-based check
    now = _now_ist()
    if now.weekday() >= 5:   # Saturday=5, Sunday=6
        return False
    market_open  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close


# ---------------------------------------------------------------------------
# Trade accessors
# ---------------------------------------------------------------------------

async def get_open_trades() -> list[dict]:
    """Return all currently open paper trades, ordered newest→oldest."""
    redis = await get_redis()
    open_ids = await redis.smembers("paper:trades:open")
    trades = []
    for trade_id in open_ids:
        raw = await redis.get(f"paper:trade:{trade_id}")
        if raw:
            try:
                trades.append(json.loads(raw))
            except json.JSONDecodeError:
                logger.warning("[order_manager] Corrupt trade JSON for id=%s", trade_id)
    return sorted(trades, key=lambda t: t["entry_ts"], reverse=True)


async def get_closed_trades(limit: int = 50) -> list[dict]:
    """Return the most-recently closed paper trades (up to ``limit``)."""
    redis = await get_redis()
    trade_ids = await redis.lrange(_CLOSED_TRADES_KEY, 0, limit - 1)
    trades = []
    for tid in trade_ids:
        raw = await redis.get(f"{_TRADE_KEY_PREFIX}{tid}")
        if raw:
            try:
                trades.append(json.loads(raw))
            except json.JSONDecodeError:
                logger.warning("[order_manager] Corrupt trade JSON for id=%s", tid)
    return trades


async def has_other_open_trades(symbol: str) -> bool:
    """Return True if any other open trade exists for ``symbol``."""
    trades = await get_open_trades()
    return any(t["symbol"] == symbol for t in trades)


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

async def place_paper_order(execution_payload: dict) -> dict:
    """
    Simulate order execution using live LTP from Redis.

    Parameters
    ----------
    execution_payload : dict
        Published by Brain on the ``trade_execution`` channel.
        Schema documented in the module docstring.

    Returns
    -------
    dict
        {"status": "FILLED", "trade": <trade dict>}   — on success
        {"status": "REJECTED", "reason": <str>}         — on failure
    """
    symbol  = execution_payload.get("symbol", "")
    signal  = execution_payload.get("signal", {})
    score   = execution_payload.get("score", {})
    options = execution_payload.get("options", {})
    badge   = execution_payload.get("badge", {})

    if not symbol:
        logger.error("[order_manager] Rejected — missing symbol in execution_payload.")
        return {"status": "REJECTED", "reason": "MISSING_SYMBOL"}

    redis = await get_redis()

    # ── Market hours guard ───────────────────────────────────────────────────
    market_open = await check_market_open()
    if not market_open:
        logger.warning("[order_manager] Rejected %s — market closed.", symbol)
        return {"status": "REJECTED", "reason": "MARKET_CLOSED"}

    # ── LTP guard ────────────────────────────────────────────────────────────
    tick = await redis.hgetall(f"tick:{symbol}")
    ltp  = _safe_float(tick.get("ltp"))

    if ltp <= 0:
        logger.error("[order_manager] Rejected %s — no LTP available.", symbol)
        return {"status": "REJECTED", "reason": "NO_LTP"}

    tick_age = (_now_ist() - _parse_ts(tick.get("ts"))).total_seconds()
    if tick_age > _TICK_MAX_AGE_S:
        logger.error(
            "[order_manager] Rejected %s — stale tick (%.0fs old, max=%ds).",
            symbol, tick_age, _TICK_MAX_AGE_S,
        )
        return {"status": "REJECTED", "reason": "STALE_TICK"}

    # ── Lot size from snapshot ───────────────────────────────────────────────
    snapshot = await redis.hgetall(f"snapshot:{symbol}")
    lot_size = int(_safe_float(snapshot.get("lot_size"), 1))
    if lot_size < 1:
        lot_size = 1

    # ── Margin check ─────────────────────────────────────────────────────────
    margin_required = round(ltp * lot_size * _INTRADAY_MARGIN_RATE, 4)
    account = await get_paper_account()

    if account["available_margin"] < margin_required:
        logger.warning(
            "[order_manager] Rejected %s — insufficient margin (need ₹%.2f, have ₹%.2f).",
            symbol, margin_required, account["available_margin"],
        )
        return {"status": "REJECTED", "reason": "INSUFFICIENT_MARGIN"}

    # ── Build trade record ───────────────────────────────────────────────────
    trade_id = str(uuid4())
    direction = signal.get("direction", "LONG")

    trade: dict = {
        "id":            trade_id,
        "symbol":        symbol,
        "direction":     direction,
        "signal_type":   signal.get("type", "UNKNOWN"),
        "entry_price":   ltp,                           # actual LTP at execution
        "signal_entry":  signal.get("entry_price", ltp),  # what signal suggested
        "stop_loss":     signal.get("stop_loss", 0.0),
        "lot_size":      lot_size,
        "quantity":      lot_size,
        "margin_used":   margin_required,
        "ici_score":     _safe_float(score.get("score")),
        "ici_grade":     score.get("grade", ""),
        "action_type":   score.get("action", "EXECUTE_MARKET"),
        "status":        "OPEN",
        "entry_ts":      _iso_now(),
        "exit_price":    None,
        "exit_ts":       None,
        "pnl_abs":       None,
        "pnl_pct":       None,
        "exit_reason":   None,
        # Preserve options context for UI display
        "options_context": {
            "atm_strike":   options.get("atm_strike"),
            "ce_ltp":       options.get("ce_ltp"),
            "pe_ltp":       options.get("pe_ltp"),
            "primary_side": badge.get("primary_side"),
            "ce_badge":     badge.get("ce", {}),
            "pe_badge":     badge.get("pe", {}),
        },
    }

    # ── Persist trade + update account atomically ────────────────────────────
    async with redis.pipeline(transaction=True) as pipe:
        pipe.set(f"{_TRADE_KEY_PREFIX}{trade_id}", json.dumps(trade))
        pipe.lpush(_OPEN_TRADES_KEY, trade_id)
        await pipe.execute()

    await redis.sadd("paper:trades:open", trade["id"])

    await _update_paper_account(
        margin_used=margin_required,
        trade_opened=True,
    )

    logger.info(
        "[order_manager] FILLED %s %s | ltp=%.2f lot=%d margin=₹%.2f ici=%.1f/%s trade_id=%s",
        symbol, direction, ltp, lot_size, margin_required,
        trade["ici_score"], trade["ici_grade"], trade_id,
    )

    return {"status": "FILLED", "trade": trade}


# ---------------------------------------------------------------------------
# Trade close
# ---------------------------------------------------------------------------

async def close_trade(trade_id: str, exit_price: float, reason: str) -> dict:
    """
    Close an open paper trade at ``exit_price`` with ``reason``.

    Returns
    -------
    dict
        {"status": "CLOSED", "trade": <updated trade>}
        {"status": "ERROR",  "reason": <str>}
    """
    redis = await get_redis()
    raw   = await redis.get(f"{_TRADE_KEY_PREFIX}{trade_id}")

    if not raw:
        logger.error("[order_manager] close_trade — trade_id=%s not found.", trade_id)
        return {"status": "ERROR", "reason": "TRADE_NOT_FOUND"}

    try:
        trade = json.loads(raw)
    except json.JSONDecodeError:
        return {"status": "ERROR", "reason": "CORRUPT_TRADE_DATA"}

    if trade.get("status") != "OPEN":
        return {"status": "ERROR", "reason": "TRADE_ALREADY_CLOSED"}

    # ── PnL calculation ──────────────────────────────────────────────────────
    entry    = trade["entry_price"]
    quantity = trade["quantity"]

    if trade["direction"] == "LONG":
        pnl_abs = (exit_price - entry) * quantity
    else:  # SHORT
        pnl_abs = (entry - exit_price) * quantity

    pnl_pct = pnl_abs / (entry * quantity) * 100 if entry > 0 else 0.0

    trade.update({
        "status":      "CLOSED",
        "exit_price":  exit_price,
        "exit_ts":     _iso_now(),
        "pnl_abs":     round(pnl_abs, 2),
        "pnl_pct":     round(pnl_pct, 2),
        "exit_reason": reason,
    })

    # ── Persist updated trade + update lists ─────────────────────────────────
    async with redis.pipeline(transaction=True) as pipe:
        pipe.set(f"{_TRADE_KEY_PREFIX}{trade_id}", json.dumps(trade))
        pipe.lrem(_OPEN_TRADES_KEY, 0, trade_id)
        pipe.lpush(_CLOSED_TRADES_KEY, trade_id)
        await pipe.execute()

    await redis.srem("paper:trades:open", trade_id)

    # ── Update account ───────────────────────────────────────────────────────
    await _update_paper_account(
        margin_released=trade["margin_used"],
        realised_pnl_delta=pnl_abs,
        win=(pnl_abs > 0),
    )

    logger.info(
        "[order_manager] CLOSED %s %s | exit=%.2f pnl=₹%.2f (%.2f%%) reason=%s trade_id=%s",
        trade["symbol"], trade["direction"], exit_price,
        pnl_abs, pnl_pct, reason, trade_id,
    )

    # ── Unsubscribe options if no remaining open trades for this symbol ───────
    other_open = await has_other_open_trades(trade["symbol"])
    if not other_open:
        try:
            await redis.publish(
                _CH_OPTS_UNSUB,
                json.dumps({"symbol": trade["symbol"]}),
            )
        except Exception as exc:
            logger.warning(
                "[order_manager] Failed to publish options:unsubscribe for %s: %s",
                trade["symbol"], exc,
            )

    return {"status": "CLOSED", "trade": trade}


# ---------------------------------------------------------------------------
# EOD forced close
# ---------------------------------------------------------------------------

async def get_best_exit_price(symbol: str, entry_price: float, instrument: str = "EQ", atm_strike=None) -> float:
    # 1. Live price — instrument-aware
    ltp = await _get_execution_ltp(symbol, instrument, atm_strike)
    if ltp > 0:
        return ltp

    # 2. Last known candle close from snapshot
    snap = await redis.hgetall(f"snapshot:{symbol}")
    last_close = float(snap.get("last_close", 0))
    if last_close > 0:
        logger.warning(f"EOD {symbol}: using snapshot last_close {last_close}")
        return last_close

    # 3. Entry price — last resort, always log
    logger.error(f"EOD {symbol}: no price found, falling back to entry {entry_price}")
    return entry_price


async def eod_close_all() -> None:
    """
    Force-close every open trade at their current LTP.
    Called by api_server.py scheduler at 15:20 IST.
    Also writes an EOD snapshot of the account to Redis.
    """
    redis = await get_redis()
    open_ids = await redis.smembers("paper:trades:open")

    logger.info("[order_manager] EOD close starting — %d open trades.", len(open_ids))

    for trade_id in open_ids:
        raw = await redis.get(f"paper:trade:{trade_id}")
        if not raw:
            continue
        try:
            trade = json.loads(raw)
        except json.JSONDecodeError:
            continue

        ltp = await get_best_exit_price(trade["symbol"], trade["entry_price"], trade.get("instrument", "EQ"), trade.get("atm_strike"))

        await close_trade(trade_id, ltp, reason="EOD_CLOSE")

    # ── Save EOD account snapshot ────────────────────────────────────────────
    account = await get_paper_account()
    eod_key = f"paper:eod:{date.today().isoformat()}"
    await redis.set(eod_key, json.dumps(account))

    logger.info(
        "[order_manager] EOD complete — realised_pnl=₹%.2f total_pnl=₹%.2f",
        account["realised_pnl"], account["total_pnl"],
    )


# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------

async def monitor_stop_losses() -> None:
    """
    Background task — checks every 5 seconds whether any open trade has hit
    its stop loss at the current LTP and closes it automatically.
    """
    logger.info("[order_manager] Stop-loss monitor started.")
    redis = await get_redis()

    while True:
        try:
            open_ids = await redis.smembers("paper:trades:open")

            for trade_id in open_ids:
                raw = await redis.get(f"paper:trade:{trade_id}")
                if not raw:
                    continue
                try:
                    trade = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                if trade.get("status") != "OPEN":
                    continue

                ltp = await _get_execution_ltp(
                    trade["symbol"], trade.get("instrument", "EQ"), trade.get("atm_strike")
                )
                if ltp <= 0:
                    continue

                sl = _safe_float(trade.get("stop_loss"))
                direction = trade.get("direction", "LONG")

                sl_hit = (
                    (direction == "LONG"  and ltp <= sl) or
                    (direction == "SHORT" and ltp >= sl)
                )

                tp = _safe_float(trade.get("take_profit"))
                tp_hit = tp > 0 and (
                    (direction == "LONG"  and ltp >= tp) or
                    (direction == "SHORT" and ltp <= tp)
                )

                if sl_hit:
                    logger.info(
                        "[order_manager] SL hit — %s %s ltp=%.2f sl=%.2f trade_id=%s",
                        trade["symbol"], direction, ltp, sl, trade_id,
                    )
                    await close_trade(trade_id, ltp, reason="STOP_LOSS")
                elif tp_hit:
                    logger.info(
                        "[order_manager] TP hit — %s %s ltp=%.2f tp=%.2f trade_id=%s",
                        trade["symbol"], direction, ltp, tp, trade_id,
                    )
                    await close_trade(trade_id, ltp, reason="TAKE_PROFIT")

        except Exception as exc:
            logger.error("[order_manager] monitor_stop_losses error: %s", exc, exc_info=True)

        await asyncio.sleep(_SL_MONITOR_INTERVAL_S)


async def update_unrealised_pnl() -> None:
    """
    Background task — recalculates total unrealised PnL every 10 seconds
    and writes it to the ``paper:account`` hash.
    """
    logger.info("[order_manager] Unrealised PnL updater started.")
    redis = await get_redis()

    while True:
        try:
            open_ids = await redis.smembers("paper:trades:open")
            total_unrealised = 0.0

            for trade_id in open_ids:
                raw = await redis.get(f"paper:trade:{trade_id}")
                if not raw:
                    continue
                try:
                    trade = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                ltp = await _get_execution_ltp(
                    trade["symbol"], trade.get("instrument", "EQ"), trade.get("atm_strike")
                )
                if ltp <= 0:
                    ltp = _safe_float(trade.get("entry_price"))

                entry    = _safe_float(trade.get("entry_price"))
                quantity = int(_safe_float(trade.get("quantity"), 1))

                if trade.get("direction") == "LONG":
                    unrealised = (ltp - entry) * quantity
                else:
                    unrealised = (entry - ltp) * quantity

                total_unrealised += unrealised

            # Update only unrealised_pnl + total_pnl in the account hash
            acct = await get_paper_account()
            total_pnl = acct["realised_pnl"] + total_unrealised
            await redis.hset(
                _ACCT_KEY,
                mapping={
                    "unrealised_pnl": str(round(total_unrealised, 4)),
                    "total_pnl":      str(round(total_pnl, 4)),
                    "updated_at":     _iso_now(),
                },
            )

        except Exception as exc:
            logger.error("[order_manager] update_unrealised_pnl error: %s", exc, exc_info=True)

        await asyncio.sleep(_UNRL_UPDATE_INTERVAL_S)


# ---------------------------------------------------------------------------
# Index option helpers
# ---------------------------------------------------------------------------

_INDEX_SYMBOLS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"}

_INDEX_STRIKE_INTERVALS: dict[str, int] = {
    "NIFTY":      50,
    "BANKNIFTY":  100,
    "FINNIFTY":   50,
    "MIDCPNIFTY": 25,
    "SENSEX":     100,
}


async def get_index_option_token(
    index: str,
    strike: int,
    option_type: str,
    expiry_date: str,
) -> str | None:
    """
    Finds NFO option token for index options.

    Parameters
    ----------
    index       : NIFTY / BANKNIFTY / FINNIFTY / MIDCPNIFTY / SENSEX
    strike      : strike price as int
    option_type : CE / PE
    expiry_date : YYYY-MM-DD

    Reads from Redis key: universe:index_options:{index}
    Returns token string or None if not found.
    """
    redis = await get_redis()
    raw = await redis.get(f"universe:index_options:{index}")
    if not raw:
        return None
    options = json.loads(raw)
    strike_int = int(strike) if strike else 0
    expiry_norm = _normalise_expiry(expiry_date)

    logger.info(
        "[order_manager] Token lookup %s %s strike=%d expiry=%s (%d contracts)",
        index, option_type, strike_int, expiry_norm, len(options),
    )

    for contract in options:
        if (
            int(contract["strike"]) == strike_int
            and contract["option_type"] == option_type
            and contract["expiry"] == expiry_norm
        ):
            return contract["token"]

    expiries = sorted({c["expiry"] for c in options})
    strikes_this_expiry = sorted({
        int(c["strike"]) for c in options
        if c["expiry"] == expiry_norm and c["option_type"] == option_type
    })
    logger.warning(
        "[order_manager] No token: %s %s strike=%d expiry=%s. "
        "Expiries in Redis: %s. %s strikes for this expiry: %s",
        index, option_type, strike_int, expiry_norm,
        expiries[:5], option_type, strikes_this_expiry[:10],
    )
    return None


async def get_index_atm_strike(index: str) -> int:
    """
    Gets current ATM strike for an index from live tick.
    Rounds to the nearest standard strike interval.

    NIFTY: nearest 50   | BANKNIFTY: nearest 100
    FINNIFTY: nearest 50 | MIDCPNIFTY: nearest 25 | SENSEX: nearest 100
    """
    redis = await get_redis()
    tick = await redis.hgetall(f"tick:{index}")
    ltp = _safe_float(tick.get("ltp"))
    if ltp <= 0:
        logger.error("[order_manager] tick:%s has no LTP — index feed not subscribed. Pass atm_strike manually.", index)
    interval = _INDEX_STRIKE_INTERVALS.get(index, 50)
    atm = round(ltp / interval) * interval
    if ltp > 0:
        logger.info("[order_manager] ATM %s: ltp=%.2f -> strike=%d", index, ltp, atm)
    return atm


# ---------------------------------------------------------------------------
# Trigger-order system
# ---------------------------------------------------------------------------

async def place_trigger_order(payload: dict) -> dict:
    """
    Stores a pending trigger order in Redis.
    Fires when a 5m candle closes above/below trigger_price.

    payload keys
    ------------
    symbol, instrument   : str  — EQ / CE / PE
    direction            : str  — LONG / SHORT
    trigger_price        : float or None  (None → market order, execute immediately)
    lots                 : int
    sl_pct, tg_pct       : float
    atm_strike           : int | None  (required for CE/PE; auto-derived for indices)
    expiry_date          : str  (YYYY-MM-DD, required for index CE/PE orders)
    """
    redis = await get_redis()

    # ── Index option: resolve ATM strike and option token ────────────────────
    if payload["instrument"] in ("CE", "PE") and payload["symbol"] in _INDEX_SYMBOLS:
        if not payload.get("atm_strike"):
            atm = await get_index_atm_strike(payload["symbol"])
            if atm > 0:
                payload["atm_strike"] = atm
            else:
                # Index feed offline — no ATM available.
                # We still allow the paper order to proceed without a token.
                # The user should enter the strike manually for accurate pricing.
                logger.warning(
                    "[order_manager] %s index feed offline — no ATM strike. "
                    "Paper order will proceed without option token. "
                    "Enter strike manually for accurate CE/PE pricing.",
                    payload["symbol"],
                )

        # Token lookup — best-effort. Paper trades don't need it.
        token = None
        if payload.get("atm_strike") and payload.get("expiry_date"):
            token = await get_index_option_token(
                payload["symbol"],
                payload["atm_strike"],
                payload["instrument"],
                payload.get("expiry_date", ""),
            )
        payload["option_token"] = token  # None is fine for paper trading

    order_id = str(uuid4())
    order = {
        "id":            order_id,
        "symbol":        payload["symbol"],
        "instrument":    payload["instrument"],
        "direction":     payload["direction"],
        "trigger_price": payload.get("trigger_price"),
        "lots":          payload["lots"],
        "sl_pct":        payload["sl_pct"],
        "tg_pct":        payload["tg_pct"],
        "atm_strike":    payload.get("atm_strike"),
        "option_token":  payload.get("option_token"),
        "expiry_date":   payload.get("expiry_date"),
        "status":        "PENDING",
        "created_at":    _now_ist().isoformat(),
    }

    if payload.get("trigger_price") is None:
        # Market order — execute immediately
        return await place_paper_order_from_trigger(order)

    # Store as pending trigger order
    await redis.set(f"pending:order:{order_id}", json.dumps(order))
    await redis.sadd("pending:orders", order_id)
    logger.info(
        "[order_manager] Trigger order PENDING — %s %s %s @ %.2f order_id=%s",
        order["symbol"], order["instrument"], order["direction"],
        order["trigger_price"], order_id,
    )
    return {"status": "PENDING", "order_id": order_id, "order": order}


async def place_paper_order_from_trigger(order: dict) -> dict:
    """
    Called when a trigger fires or a market order is placed.
    Reads current LTP, calculates SL/TG prices, and creates a trade record.
    """
    redis  = await get_redis()
    symbol = order["symbol"]

    instrument = order.get("instrument", "EQ")
    atm_strike_val = order.get("atm_strike")
    ltp = await _get_execution_ltp(symbol, instrument, atm_strike_val)
    if ltp <= 0:
        logger.error("[order_manager] Trigger fill rejected — no LTP for %s %s", symbol, instrument)
        return {"status": "REJECTED", "reason": "NO_LTP"}

    snap     = await redis.hgetall(f"snapshot:{symbol}")
    lot_size = int(_safe_float(snap.get("lot_size"), 1))
    if lot_size < 1:
        lot_size = 1
    if instrument == "EQ":
        quantity = order["lots"]
    else:
        quantity = order["lots"] * lot_size

    if order["direction"] == "LONG":
        sl_price = ltp * (1 - order["sl_pct"] / 100)
        tg_price = ltp * (1 + order["tg_pct"] / 100)
    else:
        sl_price = ltp * (1 + order["sl_pct"] / 100)
        tg_price = ltp * (1 - order["tg_pct"] / 100)

    margin  = ltp * quantity * _INTRADAY_MARGIN_RATE
    account = await get_paper_account()
    if account["available_margin"] < margin:
        logger.warning(
            "[order_manager] Trigger fill rejected — insufficient margin "
            "(need ₹%.2f, have ₹%.2f) symbol=%s",
            margin, account["available_margin"], symbol,
        )
        return {"status": "REJECTED", "reason": "INSUFFICIENT_MARGIN"}

    trade = {
        "id":            str(uuid4()),
        "symbol":        symbol,
        "instrument":    order["instrument"],
        "direction":     order["direction"],
        "entry_price":   ltp,
        "stop_loss":     round(sl_price, 2),
        "take_profit":   round(tg_price, 2),
        "sl_pct":        order["sl_pct"],
        "tg_pct":        order["tg_pct"],
        "lots":          order["lots"],
        "quantity":      quantity,
        "lot_size":      lot_size,
        "atm_strike":    order.get("atm_strike"),
        "option_token":  order.get("option_token"),
        "expiry_date":   order.get("expiry_date"),
        "margin_used":   round(margin, 2),
        "status":        "OPEN",
        "entry_ts":      _now_ist().isoformat(),
        "trigger_price": order.get("trigger_price"),
        "exit_price":    None,
        "exit_ts":       None,
        "pnl_abs":       None,
        "pnl_pct":       None,
        "exit_reason":   None,
        # Keep legacy fields expected by _parse_trade in api_server
        "signal_type":   "TRIGGER",
        "ici_score":     0.0,
        "ici_grade":     "",
    }

    await redis.set(f"paper:trade:{trade['id']}", json.dumps(trade))
    await redis.sadd("paper:trades:open", trade["id"])
    await _update_paper_account(margin_used=margin, trade_opened=True)

    logger.info(
        "[order_manager] Trigger FILLED — %s %s %s @ %.2f lots=%d trade_id=%s",
        symbol, trade["instrument"], trade["direction"], ltp,
        order["lots"], trade["id"],
    )
    return {"status": "FILLED", "trade": trade}


async def monitor_trigger_orders() -> None:
    """
    Immortal background loop — subscribes to ``candles:5m`` pub/sub channel.
    Checks all pending trigger orders on each 5m candle close.
    """
    while True:
        try:
            redis  = await get_redis()
            pubsub = redis.pubsub()
            await pubsub.subscribe("candles:5m")
            logger.info("[order_manager] Trigger monitor subscribed to candles:5m.")
            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                try:
                    candle = json.loads(message["data"])
                    await _check_pending_orders(candle)
                except Exception as exc:
                    logger.error("[order_manager] Trigger monitor candle error: %s", exc)
        except Exception as exc:
            logger.warning("[order_manager] Trigger monitor reconnecting: %s", exc)
            await asyncio.sleep(2)


async def _check_pending_orders(candle: dict) -> None:
    """Check whether any pending trigger order fires on this 5m candle close."""
    redis      = await get_redis()
    pending_ids = await redis.smembers("pending:orders")

    for order_id in pending_ids:
        raw = await redis.get(f"pending:order:{order_id}")
        if not raw:
            continue
        order = json.loads(raw)
        if order["symbol"] != candle.get("symbol"):
            continue

        close_price = candle.get("close", 0)
        instrument = order.get("instrument", "EQ")

        if instrument == "PE":
            # PE buy = bearish bet = fires when price DROPS below trigger
            # PE sell = fires when price RISES above trigger
            triggered = (
                order["direction"] == "LONG"
                and close_price <= order["trigger_price"]
            ) or (
                order["direction"] == "SHORT"
                and close_price >= order["trigger_price"]
            )
        else:
            # EQ and CE — normal direction
            triggered = (
                order["direction"] == "LONG"
                and close_price >= order["trigger_price"]
            ) or (
                order["direction"] == "SHORT"
                and close_price <= order["trigger_price"]
            )

        if not triggered:
            continue

        result = await place_paper_order_from_trigger(order)
        await redis.delete(f"pending:order:{order_id}")
        await redis.srem("pending:orders", order_id)
        await redis.publish(
            "order:filled",
            json.dumps({"order_id": order_id, "result": result}),
        )
        logger.info(
            "[order_manager] Trigger fired — %s %s close=%.2f trigger=%.2f",
            order["symbol"], order["direction"], close_price, order["trigger_price"],
        )


async def cancel_pending_order(order_id: str) -> dict:
    """Cancel a pending trigger order by ID."""
    redis = await get_redis()
    raw   = await redis.get(f"pending:order:{order_id}")
    if not raw:
        return {"status": "ERROR", "reason": "NOT_FOUND"}
    await redis.delete(f"pending:order:{order_id}")
    await redis.srem("pending:orders", order_id)
    logger.info("[order_manager] Pending order CANCELLED — order_id=%s", order_id)
    return {"status": "CANCELLED", "order_id": order_id}


async def update_trade_levels(
    trade_id: str,
    stop_loss: float | None = None,
    take_profit: float | None = None,
) -> dict:
    """Edit stop-loss and/or take-profit on an open trade."""
    redis = await get_redis()
    raw   = await redis.get(f"paper:trade:{trade_id}")
    if not raw:
        return {"status": "ERROR", "reason": "NOT_FOUND"}
    trade = json.loads(raw)
    if trade["status"] != "OPEN":
        return {"status": "ERROR", "reason": "TRADE_NOT_OPEN"}
    if stop_loss is not None:
        trade["stop_loss"] = stop_loss
    if take_profit is not None:
        trade["take_profit"] = take_profit
    await redis.set(f"paper:trade:{trade_id}", json.dumps(trade))
    logger.info(
        "[order_manager] Trade levels updated — sl=%s tp=%s trade_id=%s",
        trade.get("stop_loss"), trade.get("take_profit"), trade_id,
    )
    return {"status": "UPDATED", "trade": trade}


async def get_pending_orders() -> list[dict]:
    """Return all pending trigger orders, newest first."""
    redis      = await get_redis()
    pending_ids = await redis.smembers("pending:orders")
    orders = []
    for order_id in pending_ids:
        raw = await redis.get(f"pending:order:{order_id}")
        if raw:
            orders.append(json.loads(raw))
    return sorted(orders, key=lambda o: o["created_at"], reverse=True)


# ---------------------------------------------------------------------------
# Trade execution listener (Brain → order_manager)
# ---------------------------------------------------------------------------

async def run_execution_listener() -> None:
    """
    Subscribe to the ``trade_execution`` Redis pub/sub channel published by
    the Brain node.  Route each payload to ``place_paper_order``.

    Designed to run as a long-lived asyncio task.
    """
    while True:
        try:
            redis  = await get_redis()
            pubsub = redis.pubsub()
            await pubsub.subscribe(_CH_EXECUTION)
            logger.info("[order_manager] Subscribed to '%s' channel.", _CH_EXECUTION)

            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue

                try:
                    payload = json.loads(message["data"])
                except (json.JSONDecodeError, TypeError):
                    logger.warning("[order_manager] Could not parse execution payload: %s", message.get("data"))
                    continue

                try:
                    result = await place_paper_order(payload)
                    logger.debug("[order_manager] Execution result: %s", result)
                except Exception as exc:
                    logger.error("[order_manager] Unhandled error in place_paper_order: %s", exc, exc_info=True)
        except Exception as e:
            logger.warning("[order_manager] Pub/sub connection dropped, reconnecting in 2s: %s", e)
            await asyncio.sleep(2)
