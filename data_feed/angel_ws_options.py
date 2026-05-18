"""
data_feed/angel_ws_options.py
=============================
Live NSE options tick feed via AngelOne SmartAPI WebSocket.

Unlike the equity feed (static subscription at startup), this feed is fully
dynamic — it subscribes and unsubscribes option contracts on-demand based on
commands the Brain node publishes to Redis pub/sub channels.

Architecture
------------
* Separate AngelOne WebSocket connection with its own 500-token budget.
* Two concurrent asyncio tasks per connection:
    Task 1 — tick_receiver : read binary frames → parse → write Redis
    Task 2 — command_listener : subscribe to Redis pub/sub → handle
              options:subscribe / options:unsubscribe commands
* On any disconnect / error: log reason, backoff, get_fresh_session(),
  reconnect, immediately resubscribe all previously active tokens.

Redis pub/sub channels
----------------------
  options:subscribe   → {"symbol": "RELIANCE", "contracts": [
                            {"token": "12345", "strike": 2500, "type": "CE"},
                            ...]}
  options:unsubscribe → {"symbol": "RELIANCE"}

Standalone test
---------------
    python -m data_feed.angel_ws_options
"""

from __future__ import annotations

import asyncio
import json
import logging
import struct
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pyotp
import httpx
import websockets
from SmartApi import SmartConnect
from websockets.exceptions import ConnectionClosed, WebSocketException

from core.config import cfg
from core.redis_client import get_redis
from execution.options_rest import publish_angel_jwt

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SMARTAPI_BASE     = "https://apiconnect.angelbroking.com"
MARKET_STATUS_URL = f"{SMARTAPI_BASE}/rest/secure/angelbroking/order/v1/getMarketData"
WS_URL            = "wss://smartapisocket.angelone.in/smart-stream"

TOKEN_BUDGET      = 500         # AngelOne hard limit per WebSocket connection
HEALTH_INTERVAL   = 60          # seconds between health writes
MARKET_POLL_SLEEP = 300         # 5 minutes when market is closed
BACKOFF_INITIAL   = 15          # seconds — respect AngelOne 1 req/sec rate limit
BACKOFF_MAX       = 120         # seconds

EXCHANGE_NFO      = 2           # NFO exchange type for options
ACTION_SUBSCRIBE  = 1
ACTION_UNSUBSCRIBE = 0
MODE_FULL         = 3           # Full mode: Greeks + OI + bid-ask + LTP

CORRELATION_ID    = "options_feed"

REDIS_SUBSCRIBE_CHANNEL   = "options:subscribe"
REDIS_UNSUBSCRIBE_CHANNEL = "options:unsubscribe"
REDIS_TICKS_CHANNEL       = "options:ticks"
REDIS_HEALTH_KEY          = "options_feed:health"

# ---------------------------------------------------------------------------
# ATM Turnover tracking — static subscription for all F&O universe symbols
# Runs on same WS connection alongside brain dynamic subscriptions.
# Token limit = 1000/connection. Brain uses ~200, ATM uses ~418 = 618 total.
# ---------------------------------------------------------------------------
# token_str → (symbol, opt_type, lot_size, atm_strike)
_ATM_REGISTRY: dict[str, tuple[str, str, int, int]] = {}
# "SYMBOL:CE" / "SYMBOL:PE" → last cumulative volume seen
_ATM_LAST_VOL: dict[str, int] = {}
# symbols awaiting first-tick baseline (prevents double-count on reconnect)
_ATM_PRIMING: set[str] = set()

# ---------------------------------------------------------------------------
# Contract registry
# ---------------------------------------------------------------------------

@dataclass
class Contract:
    """Metadata for a single options contract."""
    token:  str
    symbol: str
    strike: int
    type:   str   # "CE" or "PE"


# ---------------------------------------------------------------------------
# Binary tick parser — AngelOne SNAP_QUOTE mode (mode=3)
# ---------------------------------------------------------------------------
# SNAP_QUOTE packet layout (little-endian) — per official SmartWebSocketV2:
#   Bytes 0:1    : subscription_mode   (uint8)
#   Bytes 1:2    : exchange_type       (uint8)
#   Bytes 2:27   : token string        (25 bytes, null-padded)
#   Bytes 27:35  : sequence_number     (int64)
#   Bytes 35:43  : exchange_timestamp  (int64, ms since epoch)
#   Bytes 43:51  : last_traded_price   (int64, price × 100)
#   Bytes 51:59  : last_traded_quantity(int64)
#   Bytes 59:67  : avg_traded_price    (int64, price × 100)
#   Bytes 67:75  : volume_traded_today (int64)
#   Bytes 75:83  : total_buy_quantity  (float64)
#   Bytes 83:91  : total_sell_quantity (float64)
#   Bytes 91:99  : open_price          (int64, price × 100)
#   Bytes 99:107 : high_price          (int64, price × 100)
#   Bytes 107:115: low_price           (int64, price × 100)
#   Bytes 115:123: close_price         (int64, price × 100)
#   Bytes 123:131: last_traded_time    (int64, ms)
#   Bytes 131:139: open_interest       (int64)
#   Bytes 139:147: open_interest_chg   (float64)
#   Bytes 147:347: best-5 buy/sell     (200 bytes, skipped here)
#   Bytes 347:355: upper_circuit       (int64, price × 100)
#   Bytes 355:363: lower_circuit       (int64, price × 100)
#   Bytes 363:371: 52w_high            (int64, price × 100)
#   Bytes 371:379: 52w_low             (int64, price × 100)
#
# We only need the fields up to OI (byte 139). After that is the best-5 depth
# block which we skip. Use a smaller struct that reads only what we need.
# Format: BB + 25s + 6q + 2d + 4q + q + q + d = 147 bytes.

_FULL_STRUCT = struct.Struct("<BB25sqqqqqqddqqqqqqd")
_FULL_SIZE   = _FULL_STRUCT.size  # 147 bytes — parse up to oi_chg
_MIN_PACKET  = 147                # packet must be at least this big


def _parse_tick_full(raw: bytes) -> Optional[dict]:
    """
    Parse a binary AngelOne FULL-mode tick (mode=3).
    Returns None if the packet is too short or malformed.
    """
    if len(raw) < _FULL_SIZE:
        return None

    try:
        (
            sub_mode,
            exch_type,
            token_bytes,
            seq_no,
            exch_ts_ms,
            ltp_raw,
            ltq,
            atp_raw,
            volume,
            total_buy_qty,
            total_sell_qty,
            open_raw,
            high_raw,
            low_raw,
            close_raw,
            last_traded_time,
            open_interest,
            oi_chg,
        ) = _FULL_STRUCT.unpack(raw[:_FULL_SIZE])
    except struct.error as exc:
        logger.debug("[options_ws] Tick parse error: %s", exc)
        return None

    token = token_bytes.rstrip(b"\x00").decode("ascii", errors="replace").strip()

    try:
        ts = datetime.fromtimestamp(exch_ts_ms / 1000.0, tz=timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError):
        ts = datetime.now(timezone.utc).isoformat()

    return {
        "token":  token,
        "ltp":    round(ltp_raw / 100.0, 2),
        "volume": volume,
        "oi":     open_interest,
        "ts":     ts,
    }


# ---------------------------------------------------------------------------
# Session management (same pattern as equity feed)
# ---------------------------------------------------------------------------

async def get_fresh_session() -> dict:
    """
    Authenticate using SmartAPI Python library.
    generateSession accepts MPIN for AngelOne accounts.
    Runs in thread to avoid blocking the asyncio event loop.
    """
    def _login():
        totp = pyotp.TOTP(cfg.ANGELONE_TOTP_SECRET).now()
        obj = SmartConnect(api_key=cfg.ANGELONE_API_KEY)
        data = obj.generateSession(
            cfg.ANGELONE_CLIENT_ID,
            cfg.ANGELONE_PASSWORD,  # MPIN goes here
            totp
        )
        if not data or not data.get('status'):
            raise RuntimeError(
                f"[options_ws] Login rejected: {data.get('message', data)}"
            )
        token_data = data.get('data', {})
        feed_token = obj.getfeedToken()
        jwt = token_data.get('jwtToken', '')

        if not jwt or not feed_token:
            raise RuntimeError(
                f"[options_ws] Missing jwtToken or feedToken: {token_data}"
            )
        return {
            'jwt':         jwt,
            'feed_token':  feed_token,
            'api_key':     cfg.ANGELONE_API_KEY,
            'client_code': cfg.ANGELONE_CLIENT_ID,
        }

    session = await asyncio.to_thread(_login)

    # Publish JWT to Redis for consumers (e.g. options_rest REST fallback).
    # Done AFTER _login() returns so we're back in async context.
    await publish_angel_jwt(session['jwt'])

    logger.info("[options_ws] Session obtained for client %s", cfg.ANGELONE_CLIENT_ID)
    return session


# ---------------------------------------------------------------------------
# Market status check
# ---------------------------------------------------------------------------

async def _is_nse_open(jwt: str) -> bool:
    """
    Check if NSE is currently in its trading session using IST wall-clock.

    AngelOne's getMarketData endpoint returns empty bodies intermittently
    and cannot be trusted. We use Mon-Fri, 09:15-15:30 IST instead. The
    `jwt` parameter is kept for signature compatibility with callers but
    is not used. Holidays are handled by the reconnect loop — the
    WebSocket will reject subscriptions on closed days and back off.
    """
    from datetime import timedelta
    ist = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist)

    # Monday = 0, Sunday = 6
    if now_ist.weekday() >= 5:
        logger.info("[options_ws] NSE closed — weekend (%s).", now_ist.strftime("%A"))
        return False

    open_t  = now_ist.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_t = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)

    is_open = open_t <= now_ist <= close_t
    logger.info(
        "[options_ws] NSE clock check → now=%s is_open=%s",
        now_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
        is_open,
    )
    return is_open


async def _wait_for_market_open() -> None:
    """Block until NSE is OPEN, polling every MARKET_POLL_SLEEP seconds."""
    while True:
        try:
            session = await get_fresh_session()
        except RuntimeError as exc:
            logger.warning(
                "[options_ws] Could not get session for market check: %s — "
                "sleeping %ds.", exc, MARKET_POLL_SLEEP
            )
            await asyncio.sleep(MARKET_POLL_SLEEP)
            continue

        if await _is_nse_open(session["jwt"]):
            return

        logger.info(
            "[options_ws] NSE is CLOSED — sleeping %d minutes before recheck.",
            MARKET_POLL_SLEEP // 60,
        )
        await asyncio.sleep(MARKET_POLL_SLEEP)


# ---------------------------------------------------------------------------
# WebSocket message builders
# ---------------------------------------------------------------------------

def _build_subscribe_msg(tokens: list[str]) -> str:
    return json.dumps({
        "correlationID": CORRELATION_ID,
        "action": ACTION_SUBSCRIBE,
        "params": {
            "mode": MODE_FULL,
            "tokenList": [{"exchangeType": EXCHANGE_NFO, "tokens": tokens}],
        },
    })


def _build_unsubscribe_msg(tokens: list[str]) -> str:
    return json.dumps({
        "correlationID": CORRELATION_ID,
        "action": ACTION_UNSUBSCRIBE,
        "params": {
            "mode": MODE_FULL,
            "tokenList": [{"exchangeType": EXCHANGE_NFO, "tokens": tokens}],
        },
    })


# ---------------------------------------------------------------------------
# Health writer
# ---------------------------------------------------------------------------

async def _write_health(
    redis,
    *,
    connected: bool,
    active_tokens: int = 0,
) -> None:
    mapping = {
        "connected":       "true" if connected else "false",
        "active_tokens":   str(active_tokens),
        "budget_remaining": str(TOKEN_BUDGET - active_tokens),
        "updated_at":      datetime.now(timezone.utc).isoformat(),
    }
    try:
        await redis.hset(REDIS_HEALTH_KEY, mapping=mapping)
    except Exception as exc:
        logger.warning("[options_ws] Could not write health: %s", exc)


async def _load_atm_registry(redis) -> int:
    """
    Load ATM CE/PE tokens for all F&O symbols from options:prev:{symbol}.
    Token path: options:prev.strikes.atm.ce_token / pe_token
    Written by morning_seeder Phase B — no instrument master parsing needed.
    Returns number of symbols registered.
    """
    from core.universe_builder import get_symbols
    symbols    = await get_symbols()
    registered = 0
    skipped    = 0

    for sym in symbols:
        try:
            prev_raw = await redis.get(f"options:prev:{sym}")
            if not prev_raw:
                skipped += 1
                continue
            prev       = json.loads(prev_raw)
            strikes    = prev.get("strikes", {})
            atm_info   = strikes.get("atm", {})
            ce_token   = str(atm_info.get("ce_token", "")).strip()
            pe_token   = str(atm_info.get("pe_token", "")).strip()
            _raw_strike = atm_info.get("strike") or prev.get("atm_strike") or 0
            _fs = float(_raw_strike)
            atm_strike = int(_fs) if _fs == int(_fs) else _fs
            if not ce_token or not pe_token or atm_strike <= 0:
                skipped += 1
                continue
            lot_raw  = await redis.hget(f"snapshot:{sym}", "lot_size")
            lot_size = int(float(lot_raw or 1))
            _ATM_REGISTRY[ce_token] = (sym, "CE", lot_size, atm_strike)
            _ATM_REGISTRY[pe_token] = (sym, "PE", lot_size, atm_strike)
            _ATM_PRIMING.add(sym)
            registered += 1
        except Exception as exc:
            logger.debug("[options_ws] ATM registry skip %s: %s", sym, exc)
            skipped += 1

    logger.info(
        "[options_ws] ATM registry loaded — symbols=%d tokens=%d skipped=%d",
        registered, registered * 2, skipped,
    )
    return registered


async def _accumulate_atm(redis, token: str, ltp: float, volume: int) -> None:
    """
    Accumulate ATM option turnover for Options Volume Leaders.
    Turnover formula: delta_volume × ltp (₹ turnover).
    volume_traded_today is in underlying SHARES (units), not contracts.
    No lot_size multiplication — that would overcount by 250×.
    volume_traded_today in AngelOne packets is cumulative session volume.
    Priming mechanism prevents double-count on reconnect.
    """
    entry = _ATM_REGISTRY.get(token)
    if not entry:
        return
    sym, opt_type, lot_size, _ = entry
    cache_key = f"{sym}:{opt_type}"

    # First tick after startup/reconnect — set baseline without accumulating
    if sym in _ATM_PRIMING:
        _ATM_LAST_VOL[cache_key] = volume
        other = "PE" if opt_type == "CE" else "CE"
        # Find other type's token to check if it's also primed
        other_primed = any(
            cache_key.replace(f":{opt_type}", f":{other}") in _ATM_LAST_VOL
            for _ in [1]
        )
        if other_primed:
            _ATM_PRIMING.discard(sym)
        return

    last      = _ATM_LAST_VOL.get(cache_key, 0)
    delta_vol = volume - last
    if delta_vol <= 0:
        _ATM_LAST_VOL[cache_key] = volume
        return

    try:
        await redis.incrbyfloat(
            f"options:atm_turnover_today:{sym}",
            delta_vol * ltp,
        )

        if opt_type == "CE":
            await redis.incrbyfloat(
                f"options:atm_ce_turnover_today:{sym}",
                delta_vol * ltp,
            )
        elif opt_type == "PE":
            await redis.incrbyfloat(
                f"options:atm_pe_turnover_today:{sym}",
                delta_vol * ltp,
            )
        # else: unknown opt_type — skip to avoid corrupting CE/PE keys
        _ATM_LAST_VOL[cache_key] = volume
    except Exception as exc:
        logger.debug("[options_ws] ATM incrbyfloat error %s: %s", sym, exc)


# ---------------------------------------------------------------------------
# In-session state — shared between the two concurrent tasks
# ---------------------------------------------------------------------------

class _FeedState:
    """
    Mutable state shared between tick_receiver and command_listener tasks
    within a single WebSocket session.

    Uses an asyncio.Lock to protect concurrent modifications from the two
    tasks.  The websocket handle is set after connection and used by
    command_listener to send subscribe/unsubscribe messages.
    """

    def __init__(self) -> None:
        self.lock: asyncio.Lock = asyncio.Lock()

        # token → Contract metadata
        self.active_contracts: dict[str, Contract] = {}

        # symbol → set of tokens (for fast unsubscribe-by-symbol)
        self.symbol_tokens: dict[str, set[str]] = {}

        # WebSocket handle — set by the session runner before tasks start
        self.ws: Optional[websockets.WebSocketClientProtocol] = None

        # Asyncio Event — set when the WS session ends, unblocks command_listener
        self.done: asyncio.Event = asyncio.Event()

    # ------------------------------------------------------------------ #
    # Convenience properties                                               #
    # ------------------------------------------------------------------ #

    @property
    def active_token_count(self) -> int:
        return len(self.active_contracts)

    @property
    def budget_remaining(self) -> int:
        return TOKEN_BUDGET - self.active_token_count

    # ------------------------------------------------------------------ #
    # Subscribe                                                            #
    # ------------------------------------------------------------------ #

    async def subscribe(self, symbol: str, contracts: list[dict]) -> list[str]:
        """
        Add contracts for *symbol* to the active set.

        Returns the list of token strings actually added (may be fewer than
        requested if budget would overflow — we subscribe as many as fit and
        log which were dropped).
        """
        async with self.lock:
            new_tokens: list[str] = []
            dropped: list[str] = []

            for c in contracts:
                token  = str(c["token"])
                strike = int(c["strike"])
                ctype  = str(c["type"]).upper()

                if token in self.active_contracts:
                    continue   # already subscribed

                if self.active_token_count + len(new_tokens) >= TOKEN_BUDGET:
                    dropped.append(token)
                    continue

                new_tokens.append(token)
                self.active_contracts[token] = Contract(
                    token=token, symbol=symbol, strike=strike, type=ctype
                )
                self.symbol_tokens.setdefault(symbol, set()).add(token)

            if dropped:
                logger.warning(
                    "[options_ws] Budget overflow — %d token(s) dropped for %s: %s",
                    len(dropped), symbol, dropped,
                )

            if new_tokens:
                logger.info(
                    "[options_ws] SUBSCRIBE %s: +%d tokens | active=%d | budget_remaining=%d",
                    symbol, len(new_tokens),
                    self.active_token_count,
                    self.budget_remaining,
                )

            return new_tokens

    # ------------------------------------------------------------------ #
    # Unsubscribe                                                          #
    # ------------------------------------------------------------------ #

    async def unsubscribe(self, symbol: str) -> list[str]:
        """
        Remove all contracts for *symbol* from the active set.
        Returns the list of token strings removed.
        """
        async with self.lock:
            tokens_to_remove = list(self.symbol_tokens.pop(symbol, set()))
            for t in tokens_to_remove:
                self.active_contracts.pop(t, None)

            if tokens_to_remove:
                logger.info(
                    "[options_ws] UNSUBSCRIBE %s: -%d tokens | active=%d | budget_remaining=%d",
                    symbol, len(tokens_to_remove),
                    self.active_token_count,
                    self.budget_remaining,
                )
            return tokens_to_remove

    # ------------------------------------------------------------------ #
    # Snapshot for reconnect restore                                       #
    # ------------------------------------------------------------------ #

    async def snapshot_contracts(self) -> dict[str, list[dict]]:
        """
        Return a dict {symbol: [contract_dict, ...]} for all active contracts.
        Used to restore subscriptions after a reconnect.
        """
        async with self.lock:
            result: dict[str, list[dict]] = {}
            for token, c in self.active_contracts.items():
                result.setdefault(c.symbol, []).append({
                    "token":  c.token,
                    "strike": c.strike,
                    "type":   c.type,
                })
            return result


# ---------------------------------------------------------------------------
# Tick writer
# ---------------------------------------------------------------------------

async def _write_options_tick(redis, contract: Contract, tick: dict) -> None:
    """Write an options tick to Redis and publish to options:ticks channel."""
    ltp    = tick["ltp"]
    oi     = tick["oi"]
    volume = tick["volume"]
    ts     = tick["ts"]

    redis_key = f"options:tick:{contract.symbol}:{contract.strike}{contract.type}"
    await redis.hset(redis_key, mapping={
        "ltp":    str(ltp),
        "oi":     str(oi),
        "volume": str(volume),
        "ts":     ts,
    })

    pub_payload = json.dumps({
        "_source": "options",
        "symbol":  contract.symbol,
        "strike":  contract.strike,
        "type":    contract.type,
        "ltp":     ltp,
        "oi":      oi,
        "volume":  volume,
        "ts":      ts,
    })
    await redis.publish(REDIS_TICKS_CHANNEL, pub_payload)


# ---------------------------------------------------------------------------
# Task 1 — Tick receiver
# ---------------------------------------------------------------------------

async def _tick_receiver(ws, state: _FeedState, redis) -> None:
    """
    Receive binary tick frames from AngelOne WebSocket, parse and write
    to Redis.  Runs until the WebSocket closes or raises.
    """
    tick_count     = 0
    health_window  = 0
    last_health_ts = time.monotonic()

    async for message in ws:
        if isinstance(message, str):
            logger.debug("[options_ws] Text frame: %s", message[:120])
            continue

        tick = _parse_tick_full(message)
        if tick is None:
            continue

        token = tick["token"]

        async with state.lock:
            contract = state.active_contracts.get(token)

        is_atm = token in _ATM_REGISTRY

        # Skip completely unknown tokens (not brain-subscribed AND not ATM)
        if contract is None and not is_atm:
            logger.debug("[options_ws] Unknown token %r — skipping.", token)
            continue

        # Write tick for brain-subscribed contracts (unchanged path)
        if contract is not None:
            try:
                await _write_options_tick(redis, contract, tick)
            except Exception as exc:
                logger.warning("[options_ws] Redis write error for %s: %s", token, exc)

        # ATM turnover accumulation — independent of brain subscriptions
        if is_atm:
            try:
                await _accumulate_atm(redis, token, tick["ltp"], tick["volume"])
            except Exception as exc:
                logger.debug("[options_ws] ATM accum error %s: %s", token, exc)

            # Write tick hash for ATM tokens so options leaders can read
            # live LTP. Dynamic brain subscriptions already wrote above.
            if contract is None:
                try:
                    sym, opt_type, lot_size, atm_strike = _ATM_REGISTRY[token]
                    atm_contract = Contract(
                        token=token,
                        symbol=sym,
                        strike=atm_strike,
                        type=opt_type,
                    )
                    await _write_options_tick(redis, atm_contract, tick)
                except Exception as exc:
                    logger.debug(
                        "[options_ws] ATM tick write error %s: %s", token, exc
                    )

        tick_count    += 1
        health_window += 1

        now = time.monotonic()
        if now - last_health_ts >= HEALTH_INTERVAL:
            logger.info(
                "[options_ws] Health — ticks_last_60s=%d  total=%d  active_tokens=%d",
                health_window, tick_count, state.active_token_count,
            )
            await _write_health(redis, connected=True, active_tokens=state.active_token_count)
            health_window  = 0
            last_health_ts = now


# ---------------------------------------------------------------------------
# Task 2 — Redis command listener
# ---------------------------------------------------------------------------

async def _command_listener(state: _FeedState) -> None:
    """
    Subscribe to Redis pub/sub channels and handle subscribe/unsubscribe
    commands from the Brain node.

    Runs concurrently with _tick_receiver.  Uses a *separate* Redis
    connection (pub/sub blocks the connection) obtained via get_redis()
    which returns the shared pool — redis.asyncio supports concurrent
    pub/sub and command connections transparently.
    """
    while True:
        try:
            redis  = await get_redis()
            pubsub = redis.pubsub()
            await pubsub.subscribe(REDIS_SUBSCRIBE_CHANNEL, REDIS_UNSUBSCRIBE_CHANNEL)
            logger.info(
                "[options_ws] Listening on Redis channels: %s, %s",
                REDIS_SUBSCRIBE_CHANNEL, REDIS_UNSUBSCRIBE_CHANNEL,
            )

            try:
                async for raw_msg in pubsub.listen():
                    if state.done.is_set():
                        return

                    if raw_msg["type"] != "message":
                        continue

                    channel = raw_msg["channel"]
                    try:
                        payload = json.loads(raw_msg["data"])
                    except json.JSONDecodeError as exc:
                        logger.warning("[options_ws] Bad JSON on %s: %s", channel, exc)
                        continue

                    try:
                        # ----------------------------------------------------------
                        # Handle SUBSCRIBE command
                        # ----------------------------------------------------------
                        if channel == REDIS_SUBSCRIBE_CHANNEL:
                            symbol    = payload.get("symbol", "")
                            contracts = payload.get("contracts", [])

                            if not symbol or not contracts:
                                logger.warning("[options_ws] Malformed subscribe payload: %s", payload)
                                continue

                            new_tokens = await state.subscribe(symbol, contracts)
                            # Register new ATM tokens for turnover tracking
                            for c in contracts:
                                _tok = str(c.get("token", ""))
                                _ctype = str(c.get("type", "")).upper()
                                if _tok and _ctype in ("CE", "PE"):
                                    try:
                                        _lot_raw = await redis.hget(
                                            f"snapshot:{symbol}", "lot_size"
                                        )
                                        _lot_size = int(float(_lot_raw or 1))
                                        _strike = int(c.get("strike", 0))
                                        _ATM_REGISTRY[_tok] = (
                                            symbol, _ctype, _lot_size, _strike
                                        )
                                    except Exception:
                                        pass
                            # Re-prime so first tick of new ATM is absorbed
                            _ATM_PRIMING.add(symbol)

                            if new_tokens and state.ws is not None:
                                try:
                                    await state.ws.send(_build_subscribe_msg(new_tokens))
                                except Exception as exc:
                                    logger.warning(
                                        "[options_ws] Failed to send subscribe to WS: %s", exc
                                    )

                            # Update Redis: options:active:{symbol}
                            try:
                                active_data = [
                                    {"token": c["token"], "strike": c["strike"], "type": c["type"]}
                                    for c in contracts
                                    if str(c["token"]) in {t for t in new_tokens}
                                ]
                                if active_data:
                                    await redis.set(
                                        f"options:active:{symbol}",
                                        json.dumps(active_data),
                                    )
                            except Exception as exc:
                                logger.warning("[options_ws] Could not update options:active:%s: %s", symbol, exc)

                        # ----------------------------------------------------------
                        # Handle UNSUBSCRIBE command
                        # ----------------------------------------------------------
                        elif channel == REDIS_UNSUBSCRIBE_CHANNEL:
                            symbol = payload.get("symbol", "")
                            if not symbol:
                                logger.warning("[options_ws] Malformed unsubscribe payload: %s", payload)
                                continue

                            removed_tokens = await state.unsubscribe(symbol)

                            if removed_tokens and state.ws is not None:
                                try:
                                    await state.ws.send(_build_unsubscribe_msg(removed_tokens))
                                except Exception as exc:
                                    logger.warning(
                                        "[options_ws] Failed to send unsubscribe to WS: %s", exc
                                    )

                            # Remove Redis active key
                            try:
                                await redis.delete(f"options:active:{symbol}")
                            except Exception as exc:
                                logger.warning(
                                    "[options_ws] Could not delete options:active:%s: %s", symbol, exc
                                )
                    except Exception as e:
                        logger.error("[options_ws] Message processing error: %s", e)

            except asyncio.CancelledError:
                return
            finally:
                try:
                    await pubsub.unsubscribe()
                    await pubsub.aclose()
                except Exception:
                    pass

        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning("[options_ws] Pub/sub connection dropped, reconnecting in 2s: %s", e)
            await asyncio.sleep(2)


# ---------------------------------------------------------------------------
# Core WebSocket session
# ---------------------------------------------------------------------------

async def _run_ws_session(session: dict, state: _FeedState) -> None:
    """
    Run a single WebSocket connection session.

    1. Connect with fresh auth headers.
    2. Restore all previously active subscriptions (from state).
    3. Spawn tick_receiver and command_listener concurrently.
    4. Exit (raising) when either task finishes or the WS closes.
    """
    jwt         = session["jwt"]
    feed_token  = session["feed_token"]
    api_key     = session["api_key"]
    client_code = session["client_code"]

    redis = await get_redis()

    ws_headers = {
        "Authorization": f"Bearer {jwt}",
        "x-api-key":     api_key,
        "x-client-code": client_code,
        "x-feed-token":  feed_token,
    }

    logger.info(
        "[options_ws] Connecting to %s (client=%s) …", WS_URL, client_code
    )

    async with websockets.connect(
        WS_URL,
        additional_headers=ws_headers,
        ping_interval=None,     # Disable library ping; AngelOne wants literal "ping" text
        ping_timeout=None,
        close_timeout=10,
        max_size=2**20,
    ) as ws:
        state.ws   = ws
        state.done.clear()

        logger.info("[options_ws] WebSocket connected.")

        # ── Restore subscriptions after reconnect ──────────────────────
        snapshot = await state.snapshot_contracts()
        if snapshot:
            all_tokens: list[str] = []
            for sym_contracts in snapshot.values():
                all_tokens.extend(c["token"] for c in sym_contracts)

            if all_tokens:
                logger.info(
                    "[options_ws] Restoring %d active tokens after reconnect.",
                    len(all_tokens),
                )
                try:
                    await ws.send(_build_subscribe_msg(all_tokens))
                except Exception as exc:
                    logger.warning(
                        "[options_ws] Failed to send restore subscription: %s", exc
                    )
        else:
            logger.info("[options_ws] No prior subscriptions to restore.")

        # Re-prime on every reconnect (prevents double-count)
        _ATM_LAST_VOL.clear()
        for sym in list(_ATM_PRIMING):
            pass  # already in priming set from initial load
        # Re-add all ATM symbols to priming
        for tok, entry in _ATM_REGISTRY.items():
            _ATM_PRIMING.add(entry[0])

        # Subscribe all static ATM tokens (separate from brain subscriptions)
        if _ATM_REGISTRY:
            atm_tokens = list(_ATM_REGISTRY.keys())
            for i in range(0, len(atm_tokens), 200):
                batch = atm_tokens[i:i + 200]
                try:
                    await ws.send(_build_subscribe_msg(batch))
                    await asyncio.sleep(0.1)
                except Exception as exc:
                    logger.warning("[options_ws] ATM subscribe batch failed: %s", exc)
            logger.info(
                "[options_ws] Static ATM subscription sent — %d tokens",
                len(atm_tokens),
            )

        await _write_health(redis, connected=True, active_tokens=state.active_token_count)

        # AngelOne SmartStream requires an application-level "ping" every
        # ~30 seconds or the server closes the connection.
        async def _heartbeat():
            try:
                while True:
                    await asyncio.sleep(10)
                    await ws.send("ping")
                    logger.info("[options_ws] Heartbeat ping sent.")
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("[options_ws] Heartbeat failed: %s", exc)

        # ── Spawn concurrent tasks ─────────────────────────────────────
        tick_task      = asyncio.create_task(_tick_receiver(ws, state, redis), name="tick_receiver")
        command_task   = asyncio.create_task(_command_listener(state), name="command_listener")
        heartbeat_task = asyncio.create_task(_heartbeat(), name="heartbeat")

        # Wait for whichever task finishes first (WS disconnect or command error)
        done_tasks, pending_tasks = await asyncio.wait(
            {tick_task, command_task, heartbeat_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Signal the command listener that the session is ending
        state.done.set()
        state.ws = None

        # Cancel remaining task
        for task in pending_tasks:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        # Re-raise any exception from the completed task so the reconnect
        # loop can classify the disconnect reason.
        for task in done_tasks:
            if task.exception() is not None:
                raise task.exception()   # type: ignore[misc]


# ---------------------------------------------------------------------------
# Main reconnect loop
# ---------------------------------------------------------------------------

async def run_options_feed() -> None:
    """
    Permanent async loop — never returns under normal circumstances.

    Implements:
      * Market-closed wait (5-min poll)
      * Fresh JWT on every reconnect
      * State-preserving reconnect (active subscriptions survive)
      * Exponential backoff (2 → 4 → 8 → … → 60 s)
    """
    state   = _FeedState()
    backoff = BACKOFF_INITIAL
    attempt = 0

    # Stagger startup so equity feed gets its login slot first.
    # AngelOne rate-limits at ~1 login/sec per client code; with 5 services
    # racing to login simultaneously we'd otherwise hammer the endpoint.
    logger.info("[options_ws] Startup stagger — waiting 20s to let equity login first.")
    await asyncio.sleep(20)

    # Load ATM registry once at startup — reads options:prev:{symbol} from Redis
    # Requires morning_seeder Phase B to have run first (8:30 AM)
    try:
        _startup_redis = await get_redis()
        _atm_count = await _load_atm_registry(_startup_redis)
        if _atm_count == 0:
            logger.warning(
                "[options_ws] ATM registry empty — seeder may not have run yet. "
                "ATM turnover tracking disabled for this session."
            )
    except Exception as _atm_exc:
        logger.warning("[options_ws] ATM registry load failed: %s", _atm_exc)

    while True:
        attempt += 1
        logger.info("[options_ws] === Reconnect attempt #%d ===", attempt)

        # ── 1. Wait for market open ──────────────────────────────────────
        await _wait_for_market_open()

        # ── 2. Fresh session ─────────────────────────────────────────────
        try:
            session = await get_fresh_session()
        except Exception as exc:
            # Catches RuntimeError, DataException (rate-limit), network errors, etc.
            err_str = str(exc)
            if "access rate" in err_str.lower() or "rate" in err_str.lower():
                logger.warning("[options_ws] Rate-limited — extended backoff 60s.")
                await asyncio.sleep(60)
                continue
            logger.error("[options_ws] Session creation failed: %s", exc)
            _log_backoff(backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
            continue

        # ── 3. Run session ───────────────────────────────────────────────
        disconnect_reason = "unknown"
        try:
            await _run_ws_session(session, state)
            disconnect_reason = "clean close"
        except ConnectionClosed as exc:
            disconnect_reason = (
                f"ConnectionClosed code={exc.rcvd.code if exc.rcvd else '?'} "
                f"reason={exc.rcvd.reason if exc.rcvd else ''}"
            )
        except WebSocketException as exc:
            disconnect_reason = f"WebSocketException: {exc}"
        except asyncio.TimeoutError:
            disconnect_reason = "asyncio.TimeoutError"
        except Exception as exc:  # noqa: BLE001
            disconnect_reason = f"{type(exc).__name__}: {exc}"

        logger.warning("[options_ws] Session ended — reason: %s", disconnect_reason)

        # Mark disconnected in Redis (best-effort)
        try:
            redis = await get_redis()
            await _write_health(redis, connected=False, active_tokens=state.active_token_count)
        except Exception:
            pass

        # ── 4. Exponential backoff ────────────────────────────────────────
        _log_backoff(backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, BACKOFF_MAX)


def _log_backoff(backoff: float) -> None:
    logger.info("[options_ws] Backing off for %.0fs before next reconnect attempt.", backoff)


# ---------------------------------------------------------------------------
# Public helper — Options explosion snapshot (callable by Brain)
# ---------------------------------------------------------------------------

async def get_options_snapshot(symbol: str) -> dict:
    """
    Returns current live state for all active contracts of *symbol*.

    Compares live volume/OI against the options:prev:{symbol} baseline
    written by the seeder, and returns explosion ratios.

    If baseline data is missing or any live tick is missing, the
    corresponding ratio fields are None rather than raising.

    Parameters
    ----------
    symbol : str
        Underlying symbol, e.g. "RELIANCE".

    Returns
    -------
    {
        "symbol":          str,
        "ce_ltp":          float | None,
        "pe_ltp":          float | None,
        "ce_volume":       int | None,
        "pe_volume":       int | None,
        "ce_oi":           int | None,
        "pe_oi":           int | None,
        "ce_volume_ratio": float | None,
        "pe_volume_ratio": float | None,
        "ce_oi_ratio":     float | None,
        "pe_oi_ratio":     float | None,
        "atm_strike":      int | None,
    }
    """
    redis = await get_redis()

    # Load baseline (written by seeder)
    prev_raw = await redis.get(f"options:prev:{symbol}")
    prev: dict = {}
    if prev_raw:
        try:
            prev = json.loads(prev_raw)
        except json.JSONDecodeError:
            prev = {}

    # Determine ATM strike: prefer baseline, else scan active contracts for
    # the strike with most subscriptions (proxy for ATM prominence).
    atm_strike: Optional[int] = prev.get("atm_strike") or prev.get("atm")

    if atm_strike is None:
        # Fallback: find the most common strike among active CE+PE pairs
        active_raw = await redis.get(f"options:active:{symbol}")
        if active_raw:
            try:
                active_list = json.loads(active_raw)
                strike_counts: dict[int, int] = {}
                for c in active_list:
                    s = int(c.get("strike", 0))
                    if s:
                        strike_counts[s] = strike_counts.get(s, 0) + 1
                if strike_counts:
                    atm_strike = max(strike_counts, key=lambda k: strike_counts[k])
            except (json.JSONDecodeError, ValueError, TypeError):
                pass

    def _safe_float(mapping: dict, key: str) -> Optional[float]:
        try:
            return float(mapping[key])
        except (KeyError, TypeError, ValueError):
            return None

    def _safe_int(mapping: dict, key: str) -> Optional[int]:
        try:
            return int(mapping[key])
        except (KeyError, TypeError, ValueError):
            return None

    def _ratio(live: Optional[float], baseline: Optional[float]) -> Optional[float]:
        if live is None or baseline is None or baseline == 0:
            return None
        return round(live / baseline, 4)

    # Fetch live CE + PE ticks
    ce_key = f"options:tick:{symbol}:{atm_strike}CE" if atm_strike else None
    pe_key = f"options:tick:{symbol}:{atm_strike}PE" if atm_strike else None

    live_ce: dict = await redis.hgetall(ce_key) if ce_key else {}
    live_pe: dict = await redis.hgetall(pe_key) if pe_key else {}

    ce_ltp    = _safe_float(live_ce, "ltp")
    pe_ltp    = _safe_float(live_pe, "ltp")
    ce_volume = _safe_int(live_ce, "volume")
    pe_volume = _safe_int(live_pe, "volume")
    ce_oi     = _safe_int(live_ce, "oi")
    pe_oi     = _safe_int(live_pe, "oi")

    # Baseline averages from seeder
    ce_avg_vol = _safe_float(prev, "ce_avg_volume_5d")
    pe_avg_vol = _safe_float(prev, "pe_avg_volume_5d")
    ce_avg_oi  = _safe_float(prev, "ce_avg_oi_5d")
    pe_avg_oi  = _safe_float(prev, "pe_avg_oi_5d")

    return {
        "symbol":          symbol,
        "atm_strike":      atm_strike,
        "ce_ltp":          ce_ltp,
        "pe_ltp":          pe_ltp,
        "ce_volume":       ce_volume,
        "pe_volume":       pe_volume,
        "ce_oi":           ce_oi,
        "pe_oi":           pe_oi,
        "ce_volume_ratio": _ratio(ce_volume, ce_avg_vol),
        "pe_volume_ratio": _ratio(pe_volume, pe_avg_vol),
        "ce_oi_ratio":     _ratio(ce_oi, ce_avg_oi),
        "pe_oi_ratio":     _ratio(pe_oi, pe_avg_oi),
    }


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )
    logger.info("=== angel_ws_options standalone run ===")
    logger.info(
        "Config — client: %s  api_key: %s***",
        cfg.ANGELONE_CLIENT_ID,
        cfg.ANGELONE_API_KEY[:4],
    )

    from core.config import validate
    validate()

    await run_options_feed()


if __name__ == "__main__":
    asyncio.run(_main())
