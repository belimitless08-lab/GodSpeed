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
_TRADE_HISTORY_KEY = "trades:history"
_TRADE_KEY_PREFIX = "paper:trade:"
_CH_EXECUTION     = "trade_execution"
_CH_OPTS_UNSUB    = "options:unsubscribe"
_MKT_STATUS_KEY   = "market:status"

# Tick staleness threshold (seconds)
_TICK_MAX_AGE_S: int = 60

# SL monitor / unrealised-PnL intervals
_SL_MONITOR_INTERVAL_S:  int = 1
_UNRL_UPDATE_INTERVAL_S: int = 10

_sl_dwell_counts: dict = {}
_tp_dwell_counts: dict = {}


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


async def _get_execution_ltp(
    symbol: str,
    instrument: str,
    atm_strike=None,
    expiry_date: str | None = None,
) -> tuple[float, str]:
    """
    4-tier pricing fallback for order execution.

    Returns
    -------
    (ltp, price_source) tuple.

    price_source values:
        "LIVE_WS"      — Redis tick fresh (<10s old, market open)
        "LAST_CLOSE"   — Redis tick stale (>10s) or market closed but key exists
        "REST"         — Fetched via AngelOne REST fallback
        "FAILED"       — No price available from any source; caller must reject
    """
    redis = await get_redis()

    # ── Tier 1 & 2: Redis ticks ───────────────────────────────────────
    if instrument in ("CE", "PE") and atm_strike:
        suffix = instrument  # "CE" or "PE"
        tick_key = f"options:tick:{symbol}:{atm_strike}{suffix}"
    else:
        tick_key = f"tick:{symbol}"

    tick = await redis.hgetall(tick_key)
    ltp = _safe_float(tick.get("ltp"))
    ts_raw = tick.get("ts", "")

    if ltp > 0:
        age_sec = (
            (_now_ist() - _parse_ts(ts_raw)).total_seconds()
            if ts_raw else 999999
        )

        # Tier 1: Live fresh tick (age < 10s)
        if age_sec < 10:
            return ltp, "LIVE_WS"

        # Tier 2: Stale but recent (10s–120s) — usable
        if age_sec < 120:
            return ltp, "LAST_CLOSE"

        # Tier 2.5: Too old (>120s) — reject, fall through to REST
        # Prevents 42-hour-old Redis keys from being used as execution price.
        logger.warning(
            "[order_manager] Stale tick rejected — key=%s age=%.0fs "
            "ltp=%.2f falling through to REST",
            tick_key,
            age_sec,
            ltp,
        )

    # ── Tier 1.5: Short bounded wait for fresh WS tick on CE/PE ─────────
    # TEMPORARY: this bounded fallback keeps execution latency predictable.
    # Long-term this should become fully event-driven (await tick arrival).
    if instrument in ("CE", "PE") and atm_strike:
        if True:  # market open already verified by caller
            for _ in range(3):  # max ~150ms wait
                await asyncio.sleep(0.05)
                tick = await redis.hgetall(tick_key)
                if tick:
                    try:
                        import time as _time
                        _ts = float(tick.get("updated_at_ts") or 0)
                        if _ts == 0:
                            _ts_str = tick.get("ts", "")
                            _ts = datetime.fromisoformat(
                                _ts_str.replace("Z", "+00:00")
                            ).timestamp() if _ts_str else 0
                        _age = _time.time() - _ts if _ts > 0 else 9999
                        if _age > 120:
                            tick = None  # stale → fall through to REST
                    except Exception:
                        tick = None  # unparseable → treat as stale
                if tick:
                    ltp = float(tick.get("ltp", 0))
                    if ltp <= 0:
                        tick = None  # zero LTP = unusable → fall through to REST
                if tick and ltp > 0:
                    ts_raw = tick.get("ts", "")
                    age_sec = (_now_ist() - _parse_ts(ts_raw)).total_seconds() if ts_raw else 999999
                    if age_sec < 10:
                        logger.info(
                            "[order_manager] Tier 1.5 hit — fresh WS tick for "
                            "%s %s%d ltp=%.2f",
                            symbol, instrument, atm_strike, ltp,
                        )
                        return ltp, "LIVE_WS"

    # ── Tier 3: REST fallback for options only ────────────────────────
    # For equity (EQ), no REST fallback — we require live tick data.
    if instrument in ("CE", "PE") and atm_strike:
        token, tradingsymbol, exchange = await _lookup_option_contract_meta(
            symbol, atm_strike, instrument, expiry_date,
        )
        if token and tradingsymbol and exchange:
            try:
                from execution.options_rest import fetch_option_ltp
                rest_ltp = await fetch_option_ltp(exchange, tradingsymbol, token)
                if rest_ltp and rest_ltp > 0:
                    logger.info(
                        "[order_manager] REST fallback filled LTP for %s %s%d: ₹%.2f",
                        symbol, instrument, atm_strike, rest_ltp,
                    )
                    return rest_ltp, "REST"
            except Exception as exc:
                logger.warning("[order_manager] REST fallback error: %s", exc)

    # ── Tier 4: All tiers failed ──────────────────────────────────────
    logger.error(
        "[order_manager] No LTP available — symbol=%s instrument=%s strike=%s expiry=%s",
        symbol, instrument, atm_strike, expiry_date,
    )
    return 0.0, "FAILED"


async def _lookup_option_contract_meta(
    symbol: str,
    strike: int,
    option_type: str,
    expiry_date: str | None,
) -> tuple[str | None, str | None, str | None]:
    """
    Look up option contract metadata from the unified universe.

    Returns (token, tradingsymbol, exchange) tuple or (None, None, None)
    if the contract isn't in the universe.

    Reads NEW key: universe:options:{symbol}
    Field format:  "{strike}{CE|PE}:{YYYY-MM-DD}"
    """
    if not expiry_date:
        return None, None, None

    expiry_norm = _normalise_expiry(expiry_date)
    contract_key = f"{int(strike)}{option_type}:{expiry_norm}"

    redis = await get_redis()
    raw = await redis.hget(f"universe:options:{symbol}", contract_key)
    if not raw:
        return None, None, None

    try:
        data = json.loads(raw)
        return (
            data.get("token"),
            data.get("tradingsymbol"),
            data.get("exchange"),
        )
    except (json.JSONDecodeError, KeyError):
        return None, None, None


async def _resolve_nearest_stock_strike(
    symbol: str,
    ref_price: float,
    expiry_date: str | None,
) -> int | None:
    """
    Find the nearest available stock option strike for (symbol, expiry).

    If expiry_date is None, uses the nearest upcoming expiry.
    Returns None if no strikes found (caller should reject order).
    """
    redis = await get_redis()

    # Resolve expiry
    if expiry_date:
        expiry_norm = _normalise_expiry(expiry_date)
    else:
        # Use nearest expiry
        expiries = await redis.zrange(f"universe:options:{symbol}:expiries", 0, 0)
        if not expiries:
            logger.warning("[order_manager] No expiries found for %s", symbol)
            return None
        expiry_norm = expiries[0] if isinstance(expiries[0], str) else expiries[0].decode()

    # Use ZRANGEBYSCORE to find nearest strikes efficiently
    # Search a ±10% band around ref_price; if empty, widen to ±20%
    strike_key = f"universe:options:{symbol}:strikes:{expiry_norm}"

    lo = ref_price * 0.9
    hi = ref_price * 1.1
    candidates = await redis.zrangebyscore(strike_key, lo, hi)

    if not candidates:
        # Widen search
        candidates = await redis.zrangebyscore(strike_key, ref_price * 0.8, ref_price * 1.2)

    if not candidates:
        # Full fallback: get all strikes for this expiry
        candidates = await redis.zrange(strike_key, 0, -1)

    if not candidates:
        logger.warning(
            "[order_manager] No strikes found for %s expiry=%s",
            symbol, expiry_norm,
        )
        return None

    # Decode and find nearest
    strikes = [int(s if isinstance(s, str) else s.decode()) for s in candidates]
    nearest = min(strikes, key=lambda s: abs(s - ref_price))

    logger.info(
        "[order_manager] Nearest strike for %s near ₹%.2f (expiry=%s): %d",
        symbol, ref_price, expiry_norm, nearest,
    )
    return nearest


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
    raw_trades = await redis.lrange(_TRADE_HISTORY_KEY, 0, max(limit - 1, 0))
    trades: list[dict] = []
    for raw in raw_trades:
        try:
            trade = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("[order_manager] Corrupt trade JSON in %s", _TRADE_HISTORY_KEY)
            continue
        if trade.get("status") == "CLOSED":
            trades.append(trade)
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
        # Top-level options fields for LTP tracking in positions drawer
        "instrument":    payload.get("instrument", "EQ"),
        "atm_strike":    options.get("atm_strike"),
        "strike":        options.get("atm_strike"),
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
        pipe.lpush(_TRADE_HISTORY_KEY, json.dumps(trade))
        pipe.ltrim(_TRADE_HISTORY_KEY, 0, 200)
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
        pipe.lpush(_TRADE_HISTORY_KEY, json.dumps(trade))
        pipe.ltrim(_TRADE_HISTORY_KEY, 0, 200)
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

async def get_best_exit_price(
    symbol: str,
    entry_price: float,
    instrument: str = "EQ",
    atm_strike=None,
    expiry_date: str | None = None,
) -> tuple[float, str]:
    """
    3-tier fallback for EOD close pricing.

    Returns (exit_price, price_source) — caller uses price_source to log
    how the final exit price was derived.
    """
    # Tier 1-3: use main LTP function (WS + stale + REST)
    ltp, source = await _get_execution_ltp(symbol, instrument, atm_strike, expiry_date)
    if ltp > 0:
        return ltp, source

    # Tier 4: snapshot last_close (FIX: get redis connection)
    redis = await get_redis()
    snap = await redis.hgetall(f"snapshot:{symbol}")
    last_close = _safe_float(snap.get("last_close"))
    if last_close > 0:
        logger.warning(
            "[order_manager] EOD %s: using snapshot last_close ₹%.2f",
            symbol, last_close,
        )
        return last_close, "SNAPSHOT_CLOSE"

    # Tier 5: entry price fallback
    logger.error(
        "[order_manager] EOD %s: no price source, falling back to entry ₹%.2f",
        symbol, entry_price,
    )
    return entry_price, "ENTRY_FALLBACK"


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

        ltp, exit_source = await get_best_exit_price(
            trade["symbol"],
            trade["entry_price"],
            trade.get("instrument", "EQ"),
            trade.get("atm_strike"),
            trade.get("expiry_date"),
        )
        # Mark the trade with exit price source for audit
        trade["exit_price_source"] = exit_source
        await redis.set(f"paper:trade:{trade['id']}", json.dumps(trade))

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
    Legacy polling SL/TP monitor retained for backward compatibility only.
    Primary authority is monitor_stop_losses_event_driven().
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

                ltp, _ = await _get_execution_ltp(
                    trade["symbol"],
                    trade.get("instrument", "EQ"),
                    trade.get("atm_strike"),
                    trade.get("expiry_date"),
                )
                if ltp <= 0:
                    if trade.get("instrument") in ("CE", "PE") and trade.get("atm_strike") and trade.get("option_token"):
                        try:
                            key = f"opt_resub:{trade.get('id')}"
                            if not await redis.get(key):
                                await redis.setex(key, 5, 1)
                                await redis.publish("options:subscribe", json.dumps({
                                    "symbol": trade["symbol"],
                                    "contracts": [{
                                        "token": str(trade["option_token"]),
                                        "strike": int(trade["atm_strike"]),
                                        "type": trade["instrument"],
                                    }]
                                }))
                        except Exception as exc:
                            logger.warning("[order_manager] resubscribe failed: %s", exc)
                    continue

                sl = _safe_float(trade.get("stop_loss"))
                direction = trade.get("direction", "LONG")

                tp = _safe_float(trade.get("take_profit"))
                _id = trade.get("trade_id", trade.get("id", ""))

                # Dwell filter — avoid single-tick wick exits.
                if direction == "LONG":
                    if ltp <= sl:
                        _sl_dwell_counts[_id] = (
                            _sl_dwell_counts.get(_id, 0) + 1
                        )
                    else:
                        _sl_dwell_counts[_id] = 0

                    if tp > 0 and ltp >= tp:
                        _tp_dwell_counts[_id] = (
                            _tp_dwell_counts.get(_id, 0) + 1
                        )
                    else:
                        _tp_dwell_counts[_id] = 0

                else:
                    if ltp >= sl:
                        _sl_dwell_counts[_id] = (
                            _sl_dwell_counts.get(_id, 0) + 1
                        )
                    else:
                        _sl_dwell_counts[_id] = 0

                    if tp > 0 and ltp <= tp:
                        _tp_dwell_counts[_id] = (
                            _tp_dwell_counts.get(_id, 0) + 1
                        )
                    else:
                        _tp_dwell_counts[_id] = 0

                sl_hit = _sl_dwell_counts.get(_id, 0) >= 3
                tp_hit = tp > 0 and _tp_dwell_counts.get(_id, 0) >= 2

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


async def monitor_stop_losses_event_driven() -> None:
    """
    Event-driven complement to monitor_stop_losses poller.
    Listens to tick pub/sub and checks SL/TG on each tick for open positions.
    Gives sub-second SL/TG firing vs the 1s poll interval.
    Runs in parallel with monitor_stop_losses; close_trade is idempotent so
    double-fire is safe (second call sees status != "OPEN" and skips).
    """
    logger.info("[order_manager] Event-driven SL monitor started.")
    while True:
        try:
            redis = await get_redis()
            pubsub = redis.pubsub()
            await pubsub.subscribe("ticks", "options:ticks")
            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                try:
                    raw = message["data"]
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    tick = json.loads(raw)
                    tick_symbol = tick.get("symbol")
                    tick_ltp = _safe_float(tick.get("ltp"))
                    if not tick_symbol or tick_ltp <= 0:
                        continue

                    tick_strike = tick.get("strike")
                    tick_type = tick.get("type")   # "CE" / "PE" if option

                    open_ids = await redis.smembers("paper:trades:open")
                    for trade_id in open_ids:
                        trade_raw = await redis.get(f"paper:trade:{trade_id}")
                        if not trade_raw:
                            continue
                        try:
                            trade = json.loads(trade_raw)
                        except json.JSONDecodeError:
                            continue
                        if trade.get("status") != "OPEN":
                            continue
                        if trade.get("symbol") != tick_symbol:
                            continue

                        # Option tick must match strike AND type
                        trade_inst = trade.get("instrument", "EQ")
                        if trade_inst in ("CE", "PE"):
                            if tick_type != trade_inst:
                                continue
                            if int(tick_strike or 0) != int(trade.get("atm_strike") or 0):
                                continue
                        else:
                            # EQ trade — ignore option ticks
                            if tick_type in ("CE", "PE"):
                                continue

                        sl = _safe_float(trade.get("stop_loss"))
                        tp = _safe_float(trade.get("take_profit"))
                        direction = trade.get("direction", "LONG")

                        _id = trade.get("trade_id", trade.get("id", ""))

                        # Dwell filter — avoid single-tick wick exits.
                        if direction == "LONG":
                            if tick_ltp <= sl:
                                _sl_dwell_counts[_id] = (
                                    _sl_dwell_counts.get(_id, 0) + 1
                                )
                            else:
                                _sl_dwell_counts[_id] = 0

                            if tp > 0 and tick_ltp >= tp:
                                _tp_dwell_counts[_id] = (
                                    _tp_dwell_counts.get(_id, 0) + 1
                                )
                            else:
                                _tp_dwell_counts[_id] = 0

                        else:
                            if tick_ltp >= sl:
                                _sl_dwell_counts[_id] = (
                                    _sl_dwell_counts.get(_id, 0) + 1
                                )
                            else:
                                _sl_dwell_counts[_id] = 0

                            if tp > 0 and tick_ltp <= tp:
                                _tp_dwell_counts[_id] = (
                                    _tp_dwell_counts.get(_id, 0) + 1
                                )
                            else:
                                _tp_dwell_counts[_id] = 0

                        sl_hit = _sl_dwell_counts.get(_id, 0) >= 3
                        tp_hit = tp > 0 and _tp_dwell_counts.get(_id, 0) >= 2

                        if sl_hit:
                            logger.info(
                                "[order_manager] SL hit (event) — %s %s ltp=%.2f sl=%.2f trade_id=%s",
                                trade["symbol"], direction, tick_ltp, sl, trade_id,
                            )
                            await close_trade(trade_id, tick_ltp, reason="STOP_LOSS")
                        elif tp_hit:
                            logger.info(
                                "[order_manager] TP hit (event) — %s %s ltp=%.2f tp=%.2f trade_id=%s",
                                trade["symbol"], direction, tick_ltp, tp, trade_id,
                            )
                            await close_trade(trade_id, tick_ltp, reason="TAKE_PROFIT")
                except Exception as e:
                    logger.error("[order_manager] Event monitor processing error: %s", e)
        except Exception as e:
            logger.warning("[order_manager] Event monitor reconnecting: %s", e)
            await asyncio.sleep(2)


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

                ltp, _ = await _get_execution_ltp(
                    trade["symbol"],
                    trade.get("instrument", "EQ"),
                    trade.get("atm_strike"),
                    trade.get("expiry_date"),
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


async def get_option_token(
    symbol: str,
    strike: int,
    option_type: str,
    expiry_date: str,
) -> str | None:
    """
    Look up option token from the unified options universe.

    Works for both indices (NIFTY, BANKNIFTY, ...) and stocks (RELIANCE, TCS, ...).
    Reads from Redis hash: universe:options:{symbol}
    Field format: "{strike}{CE|PE}:{YYYY-MM-DD}"

    Returns token string or None if contract is not in the universe.
    """
    if not expiry_date or not strike or option_type not in ("CE", "PE"):
        return None

    expiry_norm = _normalise_expiry(expiry_date)
    contract_key = f"{int(strike)}{option_type}:{expiry_norm}"

    redis = await get_redis()
    raw = await redis.hget(f"universe:options:{symbol}", contract_key)

    if not raw:
        # Debug logging: show what IS available to help diagnose misses
        total = await redis.hlen(f"universe:options:{symbol}")
        expiries = await redis.zrange(f"universe:options:{symbol}:expiries", 0, -1)
        logger.warning(
            "[order_manager] Token not found: %s %s strike=%d expiry=%s. "
            "Universe has %d contracts, %d expiries: %s",
            symbol, option_type, int(strike), expiry_norm,
            total, len(expiries), expiries[:5],
        )
        return None

    try:
        data = json.loads(raw)
        return data.get("token")
    except (json.JSONDecodeError, KeyError):
        return None


# Keep old name as alias for backward compat during transition — remove in Session 4
get_index_option_token = get_option_token


async def get_option_lot_size(
    symbol: str,
    strike: int,
    option_type: str,
    expiry_date: str,
) -> int:
    """
    Look up lot_size from the unified options universe for a specific contract.

    Authoritative source — lot_size is stored per-contract by the universe
    builder, read directly from AngelOne's instrument master. Don't use
    snapshot.lot_size for options because:
      - Indices have no snapshot at all
      - Stock snapshots have equity lot_size (1), not option lot_size

    Returns 1 if contract not found (defensive default).
    """
    if not expiry_date or not strike or option_type not in ("CE", "PE"):
        return 1

    expiry_norm = _normalise_expiry(expiry_date)
    contract_key = f"{int(strike)}{option_type}:{expiry_norm}"

    redis = await get_redis()
    raw = await redis.hget(f"universe:options:{symbol}", contract_key)
    if not raw:
        return 1

    try:
        data = json.loads(raw)
        return int(data.get("lot_size", 1))
    except (json.JSONDecodeError, TypeError, ValueError):
        return 1


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

    # ── Options: resolve strike from trigger_price (not current LTP) ────────
    if payload["instrument"] in ("CE", "PE"):
        if not payload.get("atm_strike"):
            # Strike is based on trigger price — the level the user targets.
            # Using trigger_price (not current LTP) means:
            #   trigger=1200, close=1220 → buy 1200 CE (the breakout level)
            #   NOT the 1220 CE (which is ATM at execution, wrong for breakout trades)
            ref_price = payload.get("trigger_price") or 0

            if ref_price <= 0:
                # Market order — derive from underlying price via fallback chain:
                #   1. Live spot tick     (market hours)
                #   2. Snapshot prev_day  (stocks only, seeded at 8:30 AM)
                #   3. AngelOne REST      (indices or totally-fresh startup)
                redis_tmp = await get_redis()

                # 1. Live tick
                spot = await redis_tmp.hgetall(f"tick:{payload['symbol']}")
                ref_price = _safe_float(spot.get("ltp"))

                # 2. Snapshot fallback (hash-first, legacy string fallback)
                if ref_price <= 0:
                    # Current canonical runtime format: HASH
                    snap_hash = await redis_tmp.hgetall(f"snapshot:{payload['symbol']}")
                    if snap_hash:
                        ref_price = _safe_float(
                            snap_hash.get("prev_close")
                            or snap_hash.get("last_close")
                            or snap_hash.get("ltp")
                        )
                        if ref_price > 0:
                            logger.info(
                                "[order_manager] Using snapshot-hash ref ₹%.2f for %s",
                                ref_price, payload['symbol'],
                            )

                    # Legacy format: JSON string
                    if ref_price <= 0:
                        snap_raw = await redis_tmp.get(f"snapshot:{payload['symbol']}")
                        if snap_raw:
                            try:
                                snap_str = snap_raw if isinstance(snap_raw, str) else snap_raw.decode()
                                snap = json.loads(snap_str)
                                ref_price = _safe_float(
                                    snap.get("prev_close")
                                    or snap.get("last_close")
                                    or snap.get("ltp")
                                    or snap.get("prev_day", {}).get("close")
                                )
                                if ref_price > 0:
                                    logger.info(
                                        "[order_manager] Using snapshot-string ref ₹%.2f for %s",
                                        ref_price, payload['symbol'],
                                    )
                            except (json.JSONDecodeError, AttributeError, TypeError):
                                pass

                # 3. REST fallback (indices especially)
                if ref_price <= 0:
                    try:
                        from execution.options_rest import fetch_underlying_ltp
                        rest_ltp = await fetch_underlying_ltp(payload['symbol'])
                        if rest_ltp and rest_ltp > 0:
                            ref_price = rest_ltp
                            logger.info(
                                "[order_manager] Using REST underlying LTP ₹%.2f for %s",
                                ref_price, payload['symbol'],
                            )
                    except Exception as exc:
                        logger.warning(
                            "[order_manager] Underlying REST fetch failed for %s: %s",
                            payload['symbol'], exc,
                        )

            if ref_price > 0:
                if payload["symbol"] in _INDEX_SYMBOLS:
                    # Index: round to fixed interval
                    interval = _INDEX_STRIKE_INTERVALS.get(payload["symbol"], 50)
                    payload["atm_strike"] = round(ref_price / interval) * interval
                else:
                    # Equity: find nearest available strike in the unified universe
                    # for the specified expiry (or nearest expiry if none given).
                    payload["atm_strike"] = await _resolve_nearest_stock_strike(
                        payload["symbol"],
                        ref_price,
                        payload.get("expiry_date"),
                    )

            logger.info(
                "[order_manager] Strike resolved from trigger_price=%.2f -> atm_strike=%s for %s %s",
                ref_price, payload.get("atm_strike"), payload["symbol"], payload["instrument"],
            )

        # Auto-pick nearest expiry if user didn't specify one.
        # UI may or may not send expiry; backend guarantees one gets chosen.
        if not payload.get("expiry_date"):
            expiries_zset = await redis.zrange(
                f"universe:options:{payload['symbol']}:expiries", 0, 0
            )
            if expiries_zset:
                first = expiries_zset[0]
                payload["expiry_date"] = first if isinstance(first, str) else first.decode()
                logger.info(
                    "[order_manager] Auto-picked nearest expiry %s for %s market order",
                    payload["expiry_date"], payload["symbol"],
                )

        # Token lookup — best-effort. Paper trades don't require it.
        token = None
        if payload.get("atm_strike") and payload.get("expiry_date"):
            token = await get_option_token(
                payload["symbol"],
                payload["atm_strike"],
                payload["instrument"],
                payload["expiry_date"],
            )
        payload["option_token"] = token

        # Force-subscribe options WS to this strike so SL/TG monitoring uses
        # LIVE_WS ticks instead of REST fallback. Fire-and-forget — REST fallback
        # still works if the feed is down. Payload shape must match what
        # angel_ws_options._command_listener expects (JSON with symbol + contracts).
        if token and payload.get("atm_strike"):
            try:
                subscribe_payload = json.dumps({
                    "symbol": payload["symbol"],
                    "contracts": [{
                        "token": str(token),
                        "strike": int(payload["atm_strike"]),
                        "type": payload["instrument"],   # "CE" or "PE"
                    }],
                })
                await redis.publish("options:subscribe", subscribe_payload)
                logger.info(
                    "[order_manager] Force-subscribed options WS for %s %s%d token=%s",
                    payload["symbol"], payload["instrument"],
                    payload["atm_strike"], token,
                )
            except Exception as exc:
                logger.warning(
                    "[order_manager] Could not publish options:subscribe for %s: %s",
                    payload["symbol"], exc,
                )

    order_id = str(uuid4())

    # Resolve lot_size at order-placement time so the pending order is
    # self-contained for UI display and future fill computation.
    # Options: read from universe; Equity: always 1.
    order_lot_size = 1
    if payload["instrument"] in ("CE", "PE"):
        order_lot_size = await get_option_lot_size(
            payload["symbol"],
            payload.get("atm_strike"),
            payload["instrument"],
            payload.get("expiry_date"),
        )

    order = {
        "id":            order_id,
        "symbol":        payload["symbol"],
        "instrument":    payload["instrument"],
        "direction":     payload["direction"],
        "trigger_price": payload.get("trigger_price"),
        "lots":          payload["lots"],
        "lot_size":      order_lot_size,
        "quantity":      payload["lots"] * order_lot_size,
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

    # Store as pending trigger order — TTL of 1 trading day so stale orders
    # never survive a restart or an overnight Redis persistence flush.
    await redis.set(f"pending:order:{order_id}", json.dumps(order), ex=86400)
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
    if not await check_market_open():
        logger.warning(
            "[order_manager] Trigger fill rejected — market closed. symbol=%s order_id=%s",
            order.get("symbol"), order.get("id"),
        )
        return {"status": "REJECTED", "reason": "MARKET_CLOSED"}

    redis  = await get_redis()
    symbol = order["symbol"]

    instrument = order.get("instrument", "EQ")

    # Strike was already resolved at order placement time from trigger_price.
    # Use it as-is — do not recalculate from close price at execution time.
    # This preserves the user's intended breakout level (e.g. 1200 CE, not 1220 CE).
    atm_strike_val = order.get("atm_strike")

    ltp, price_source = await _get_execution_ltp(
        symbol, instrument, atm_strike_val, order.get("expiry_date"),
    )
    if ltp <= 0:
        logger.error(
            "[order_manager] Trigger fill rejected — no LTP for %s %s strike=%s expiry=%s",
            symbol, instrument, atm_strike_val, order.get("expiry_date"),
        )
        return {"status": "REJECTED", "reason": "NO_LTP"}

    # Lot size source differs by instrument type:
    #   EQ — use 1 (quantity = number of shares user typed directly)
    #   CE/PE — read from the option contract in the universe (authoritative)
    if instrument == "EQ":
        lot_size = 1
        quantity = order["lots"]
        account  = await get_paper_account()
    else:
        # Parallel read — saves one sequential Redis round trip
        lot_size_result, account = await asyncio.gather(
            get_option_lot_size(
                symbol,
                order.get("atm_strike"),
                instrument,
                order.get("expiry_date"),
            ),
            get_paper_account(),
        )
        lot_size = lot_size_result if lot_size_result >= 1 else 1
        quantity = order["lots"] * lot_size

    if order["direction"] == "LONG":
        sl_price = ltp * (1 - order["sl_pct"] / 100)
        tg_price = ltp * (1 + order["tg_pct"] / 100)
    else:
        sl_price = ltp * (1 + order["sl_pct"] / 100)
        tg_price = ltp * (1 - order["tg_pct"] / 100)

    margin = ltp * quantity * _INTRADAY_MARGIN_RATE
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
        "atm_strike":    atm_strike_val,
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
        "price_source":      price_source,          # NEW: LIVE_WS | LAST_CLOSE | REST
        "price_age_seconds": None,                  # NEW: computed below for LAST_CLOSE
        "underlying_at_fill": None,                 # NEW: spot price when option filled
        "broker":            "PAPER",               # NEW: placeholder for Groww swap
    }

    # Populate underlying_at_fill for CE/PE (useful for backtesting P&L realism)
    if instrument in ("CE", "PE"):
        spot_tick = await redis.hgetall(f"tick:{symbol}")
        trade["underlying_at_fill"] = _safe_float(spot_tick.get("ltp")) or None

    # Compute price age if it was a stale tick
    if price_source in ("LAST_CLOSE", "REST"):
        tick_key = (
            f"options:tick:{symbol}:{atm_strike_val}{instrument}"
            if instrument in ("CE", "PE") else f"tick:{symbol}"
        )
        ts_raw = (await redis.hgetall(tick_key)).get("ts", "")
        if ts_raw:
            age = (_now_ist() - _parse_ts(ts_raw)).total_seconds()
            trade["price_age_seconds"] = round(age, 1)

    async with redis.pipeline(transaction=True) as pipe:
        pipe.set(f"paper:trade:{trade['id']}", json.dumps(trade))
        pipe.sadd("paper:trades:open", trade["id"])
        await pipe.execute()
    # History written on CLOSE only — not on open
    # This prevents duplicate records in trades:history
    if instrument in ("CE", "PE") and trade.get("option_token") and trade.get("atm_strike"):
        try:
            await redis.publish("options:subscribe", json.dumps({
                "symbol": symbol,
                "contracts": [{
                    "token": str(trade["option_token"]),
                    "strike": int(trade["atm_strike"]),
                    "type": instrument,
                }]
            }))
        except Exception as exc:
            logger.warning("[order_manager] Re-subscribe at fill failed: %s", exc)
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
    redis       = await get_redis()
    pending_ids = await redis.smembers("pending:orders")
    today_str   = date.today().isoformat()   # "YYYY-MM-DD"

    if not pending_ids:
        return

    # Batch-read all pending orders in one pipeline — eliminates
    # N sequential Redis round trips (one per pending order)
    async with redis.pipeline(transaction=False) as pipe:
        for order_id in pending_ids:
            pipe.get(f"pending:order:{order_id}")
        raw_results = await pipe.execute()

    for order_id, raw in zip(pending_ids, raw_results):
        if not raw:
            await redis.srem("pending:orders", order_id)
            continue
        order = json.loads(raw)

        # Expire orders from previous trading days — they must never fire
        # across session boundaries.  created_at is "YYYY-MM-DDTHH:MM:SS..."
        order_date = (order.get("created_at") or "")[:10]
        if order_date and order_date != today_str:
            logger.warning(
                "[order_manager] Expiring stale pending order from %s — order_id=%s symbol=%s",
                order_date, order_id, order.get("symbol"),
            )
            await redis.delete(f"pending:order:{order_id}")
            await redis.srem("pending:orders", order_id)
            continue

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
        logger.info(
            "[order_manager] Trigger latency — %s filled in %.0fms after candle close",
            order["symbol"],
            (datetime.now(timezone(timedelta(hours=5, minutes=30))) -
             _parse_ts(candle.get("ts", ""))).total_seconds() * 1000
            if candle.get("ts") else -1,
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
                    # AUTO-EXECUTION DISABLED — signals display on dashboard only
                    # Manual order required via + ORDER button
                    logger.info(
                        "[order_manager] Signal received (auto-execution DISABLED)"
                        " — %s %s score=%s grade=%s",
                        payload.get("symbol", ""),
                        payload.get("direction", ""),
                        payload.get("ici_score", ""),
                        payload.get("ici_grade", ""),
                    )
                except Exception as exc:
                    logger.error("[order_manager] Unhandled error in place_paper_order: %s", exc, exc_info=True)
        except Exception as e:
            logger.warning("[order_manager] Pub/sub connection dropped, reconnecting in 2s: %s", e)
            await asyncio.sleep(2)
