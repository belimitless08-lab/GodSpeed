"""
data_feed/angel_ws_equities.py
==============================
Live NSE equity tick feed via AngelOne SmartAPI WebSocket.

Lifecycle
---------
1.  Poll AngelOne market-status API — if NSE is CLOSED, sleep 5 min and retry.
2.  Call get_fresh_session() for a brand-new JWT (never reuse across reconnects).
3.  Open WebSocket with fresh auth headers.
4.  Load universe:token_map from Redis; subscribe up to TOKEN_BUDGET tokens.
5.  Receive ticks → write tick:{symbol} hash + publish to "ticks" channel.
6.  Health heartbeat every 60 s → feed:health hash in Redis.
7.  On ANY error / disconnect: log reason, exponential backoff, goto 1.

Standalone test
---------------
    python -m data_feed.angel_ws_equities
"""

from __future__ import annotations

import asyncio
import json
import logging
import struct
import time
from datetime import datetime, timezone
from typing import Optional

import pyotp
import httpx
import websockets
from SmartApi import SmartConnect
from websockets.exceptions import (
    ConnectionClosed,
    WebSocketException,
)

from core.config import cfg
from core.instrument_registry import get_index_ws_tokens
from core.redis_client import get_redis
from core.universe_builder import get_token_map
from execution.options_rest import publish_angel_jwt

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SMARTAPI_BASE      = "https://apiconnect.angelbroking.com"
MARKET_STATUS_URL  = f"{SMARTAPI_BASE}/rest/secure/angelbroking/order/v1/getMarketData"
WS_URL             = "wss://smartapisocket.angelone.in/smart-stream"

TOKEN_BUDGET       = 500        # AngelOne hard limit
HEALTH_INTERVAL    = 60         # seconds between health log/write
MARKET_POLL_SLEEP  = 300        # 5 minutes when market is closed
BACKOFF_INITIAL    = 15         # seconds — respect AngelOne 1 req/sec rate limit
BACKOFF_MAX        = 120        # seconds

# AngelOne exchange codes
EXCHANGE_NSE = 1
EXCHANGE_BSE = 3

# Subscribe action code
ACTION_SUBSCRIBE = 1

# Mode 2 = Quote (LTP + volume + basic market depth)
MODE_LTP   = 1
MODE_QUOTE = 2

# ---------------------------------------------------------------------------
# Session management
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
                f"[angel_ws] Login rejected: {data.get('message', data)}"
            )
        token_data = data.get('data', {})
        feed_token = obj.getfeedToken()
        jwt = token_data.get('jwtToken', '')
        if not jwt or not feed_token:
            raise RuntimeError(
                f"[angel_ws] Missing jwtToken or feedToken: {token_data}"
            )
        return {
            'jwt':         jwt,
            'feed_token':  feed_token,
            'api_key':     cfg.ANGELONE_API_KEY,
            'client_code': cfg.ANGELONE_CLIENT_ID,
        }

    session = await asyncio.to_thread(_login)
    # Publish JWT to Redis for consumers (e.g. options_rest REST fallback)
    await publish_angel_jwt(session['jwt'])
    logger.info("[angel_ws] Session obtained for client %s", cfg.ANGELONE_CLIENT_ID)
    return session


# ---------------------------------------------------------------------------
# Market status check
# ---------------------------------------------------------------------------

async def _is_nse_open(jwt: str) -> bool:
    """
    Check if NSE is currently in its cash-equity trading session.

    Uses IST wall-clock (Mon-Fri, 09:15-15:30) instead of AngelOne's
    getMarketData endpoint, which returns empty bodies intermittently and
    cannot be trusted. The `jwt` parameter is kept for signature
    compatibility with callers but is not used.

    Note: This does not account for NSE holidays. On a holiday the feed
    will try to connect to the WebSocket and AngelOne will reject the
    subscription; the reconnect loop then backs off automatically.
    """
    from datetime import timezone, timedelta
    ist = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist)

    # Monday = 0, Sunday = 6
    if now_ist.weekday() >= 5:
        logger.info("[angel_ws] NSE closed — weekend (%s).", now_ist.strftime("%A"))
        return False

    open_t  = now_ist.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_t = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)

    is_open = open_t <= now_ist <= close_t
    logger.info(
        "[angel_ws] NSE clock check → now=%s is_open=%s",
        now_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
        is_open,
    )
    return is_open


async def _wait_for_market_open() -> str:
    """
    Block until NSE is OPEN.  Obtains a fresh session purely for the status
    check, then loops with MARKET_POLL_SLEEP between checks.

    Returns the JWT from the session that confirmed market OPEN — the caller
    should still call get_fresh_session() for the actual WebSocket connection
    to keep the pattern consistent.
    """
    while True:
        try:
            session = await get_fresh_session()
            jwt = session["jwt"]
        except RuntimeError as exc:
            logger.warning(
                "[angel_ws] Could not get session for market check: %s — "
                "sleeping %ds.", exc, MARKET_POLL_SLEEP
            )
            await asyncio.sleep(MARKET_POLL_SLEEP)
            continue

        if await _is_nse_open(jwt):
            return jwt

        logger.info(
            "[angel_ws] NSE is CLOSED — sleeping %d minutes before recheck.",
            MARKET_POLL_SLEEP // 60,
        )
        await asyncio.sleep(MARKET_POLL_SLEEP)


# ---------------------------------------------------------------------------
# Token list helpers
# ---------------------------------------------------------------------------

async def _load_token_list() -> tuple[list[str], dict[str, str]]:
    """
    Load universe:token_map from Redis.

    Returns
    -------
    nse_tokens   : NSE equity token strings to subscribe
    reversed_map : token → symbol (for tick processing)
    """
    token_map: dict[str, str] = await get_token_map()   # symbol → token
    reversed_map: dict[str, str] = {v: k for k, v in token_map.items()}

    tokens = sorted(token_map.values())   # F&O stock tokens

    if len(tokens) > TOKEN_BUDGET:
        logger.warning(
            "[angel_ws] Universe has %d tokens — exceeds budget of %d. "
            "Trimming to first %d (alphabetical). Some symbols will not be "
            "tracked this session.",
            len(tokens), TOKEN_BUDGET, TOKEN_BUDGET,
        )
        tokens = tokens[:TOKEN_BUDGET]
        reversed_map = {t: reversed_map[t] for t in tokens if t in reversed_map}

    return list(tokens), reversed_map


def _build_subscribe_message(nse_tokens: list[str], bse_tokens: list[str] = None) -> str:
    """Build QUOTE-mode subscription for equity stocks."""
    token_list = []
    if nse_tokens:
        token_list.append({"exchangeType": EXCHANGE_NSE, "tokens": nse_tokens})
    if bse_tokens:
        token_list.append({"exchangeType": EXCHANGE_BSE, "tokens": bse_tokens})
    return json.dumps({
        "correlationID": "feed",
        "action": ACTION_SUBSCRIBE,
        "params": {
            "mode": MODE_QUOTE,
            "tokenList": token_list,
        },
    })


def _build_index_subscribe_message(nse_index_tokens: list[str], bse_index_tokens: list[str] = None) -> str:
    """Build LTP-mode subscription for index tokens (no volume/OI data available)."""
    token_list = []
    if nse_index_tokens:
        token_list.append({"exchangeType": EXCHANGE_NSE, "tokens": nse_index_tokens})
    if bse_index_tokens:
        token_list.append({"exchangeType": EXCHANGE_BSE, "tokens": bse_index_tokens})
    return json.dumps({
        "correlationID": "index_feed",
        "action": ACTION_SUBSCRIBE,
        "params": {
            "mode": MODE_LTP,
            "tokenList": token_list,
        },
    })


# ---------------------------------------------------------------------------
# Binary tick parser (AngelOne SmartStream QUOTE mode)
# ---------------------------------------------------------------------------
# AngelOne QUOTE mode (mode=2) binary packet layout (little-endian):
#   Bytes 0:1    : subscription_mode (uint8)
#   Bytes 1:2    : exchange_type (uint8)
#   Bytes 2:27   : token string (25 bytes, null-terminated)
#   Bytes 27:35  : sequence_number (int64)
#   Bytes 35:43  : exchange_timestamp (int64, ms since epoch)
#   Bytes 43:51  : last_traded_price (int64, price × 100)
#   Bytes 51:59  : last_traded_quantity (int64)
#   Bytes 59:67  : avg_traded_price (int64, price × 100)
#   Bytes 67:75  : volume_traded_today (int64)
#   Bytes 75:83  : total_buy_quantity (float64)
#   Bytes 83:91  : total_sell_quantity (float64)
#   Bytes 91:99  : open_price (int64, price × 100)
#   Bytes 99:107 : high_price (int64, price × 100)
#   Bytes 107:115: low_price (int64, price × 100)
#   Bytes 115:123: close_price (int64, price × 100)
# Source: AngelOne SmartWebSocketV2 official Python SDK.
# Format: BB + 25s + 6q + 2d + 4q = 123 bytes total.

_QUOTE_STRUCT = struct.Struct("<BB25sqqqqqqddqqqq")
_QUOTE_SIZE   = _QUOTE_STRUCT.size   # 123 bytes

# LTP mode (mode=1) packet — used for indices which have no volume/OI data
# Layout: sub_mode(1) + exch_type(1) + token(25) + seq_no(8) + exch_ts_ms(8) + ltp(8) = 51 bytes
_LTP_STRUCT = struct.Struct("<BB25sqqq")
_LTP_SIZE   = _LTP_STRUCT.size  # 51 bytes


def _parse_tick(raw: bytes) -> Optional[dict]:
    """
    Parse a binary AngelOne QUOTE-mode tick.
    Returns None if the packet is too short or malformed.
    """
    if len(raw) < _QUOTE_SIZE:
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
        ) = _QUOTE_STRUCT.unpack(raw[:_QUOTE_SIZE])
    except struct.error as exc:
        logger.debug("[angel_ws] Tick parse error: %s", exc)
        return None

    token = token_bytes.rstrip(b"\x00").decode("ascii", errors="replace").strip()

    # Convert exchange timestamp (ms) to ISO string
    try:
        ts = datetime.fromtimestamp(exch_ts_ms / 1000.0, tz=timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError):
        ts = datetime.now(timezone.utc).isoformat()

    return {
        "token":  token,
        "ltp":    round(ltp_raw / 100.0, 2),
        "volume": volume,
        "ltq":    ltq,
        "atp":    round(atp_raw / 100.0, 2),
        "open":   round(open_raw / 100.0, 2),
        "high":   round(high_raw / 100.0, 2),
        "low":    round(low_raw / 100.0, 2),
        "close":  round(close_raw / 100.0, 2),
        "ts":     ts,
    }


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

def _parse_ltp_tick(raw: bytes) -> Optional[dict]:
    """
    Parse an AngelOne LTP-mode (mode=1) binary tick.
    Used for index tokens which stream in LTP mode, not QUOTE mode.
    Returns None if packet is too short or malformed.
    """
    if len(raw) < _LTP_SIZE:
        return None
    try:
        sub_mode, exch_type, token_bytes, seq_no, exch_ts_ms, ltp_raw = (
            _LTP_STRUCT.unpack(raw[:_LTP_SIZE])
        )
    except struct.error:
        return None

    token = token_bytes.rstrip(b"\x00").decode("ascii", errors="replace").strip()
    try:
        ts = datetime.fromtimestamp(exch_ts_ms / 1000.0, tz=timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError):
        ts = datetime.now(timezone.utc).isoformat()

    return {
        "token":  token,
        "ltp":    round(ltp_raw / 100.0, 2),
        "volume": 0,
        "ltq":    0,
        "atp":    round(ltp_raw / 100.0, 2),
        "open":   0.0,
        "high":   0.0,
        "low":    0.0,
        "close":  0.0,
        "ts":     ts,
    }


async def _write_tick(redis, symbol: str, tick: dict) -> None:
    """Write tick data to Redis hash and publish to ticks channel."""
    mapping = {
        "ltp":    str(tick["ltp"]),
        "volume": str(tick["volume"]),
        "ltq":    str(tick["ltq"]),
        "atp":    str(tick["atp"]),
        "open":   str(tick["open"]),
        "high":   str(tick["high"]),
        "low":    str(tick["low"]),
        "close":  str(tick["close"]),
        "ts":     tick["ts"],
    }
    await redis.hset(f"tick:{symbol}", mapping=mapping)

    pub_payload = json.dumps({
        "symbol": symbol,
        "ltp":    tick["ltp"],
        "volume": tick["volume"],
        "ts":     tick["ts"],
    })
    await redis.publish("ticks", pub_payload)


async def _write_health(
    redis,
    *,
    connected: bool,
    ticks_last_60s: int = 0,
    last_tick_ts: str = "",
    symbol_count: int = 0,
) -> None:
    """Write feed health status to Redis."""
    mapping = {
        "connected":      "true" if connected else "false",
        "ticks_last_60s": str(ticks_last_60s),
        "last_tick_ts":   last_tick_ts,
        "symbol_count":   str(symbol_count),
        "updated_at":     datetime.now(timezone.utc).isoformat(),
    }
    try:
        await redis.hset("feed:health", mapping=mapping)
    except Exception as exc:
        logger.warning("[angel_ws] Could not write feed:health: %s", exc)


# ---------------------------------------------------------------------------
# Core WebSocket session
# ---------------------------------------------------------------------------

async def _run_ws_session(session: dict) -> None:
    """
    Run a single WebSocket connection session until it disconnects or errors.
    Raises on exit so the outer reconnect loop can handle backoff.
    """
    jwt        = session["jwt"]
    feed_token = session["feed_token"]
    api_key    = session["api_key"]
    client_code = session["client_code"]

    # Load token universe fresh on every reconnect (universe may have refreshed)
    nse_equity_tokens, reversed_map = await _load_token_list()
    redis = await get_redis()

    index_ws_tokens = []
    for attempt in range(10):
        index_ws_tokens = await get_index_ws_tokens()
        if index_ws_tokens:
            logger.info(
                f"[ws_equities] Resolved {len(index_ws_tokens)} "
                f"index WS tokens"
            )
            break
        logger.warning(
            f"[ws_equities] Waiting for index tokens in Redis "
            f"(attempt {attempt+1}/10)..."
        )
        await asyncio.sleep(3)

    if not index_ws_tokens:
        logger.error(
            "[ws_equities] Index tokens not found after 10 attempts "
            "— subscribing equity tokens only. "
            "Macro gating will default to BULL."
        )
    equity_ws_tokens = [f"nse_cm|{token}" for token in nse_equity_tokens]
    subscription_ws_tokens = equity_ws_tokens + index_ws_tokens

    nse_index_tokens: list[str] = []
    bse_index_tokens: list[str] = []
    sensex_token: str | None = None
    for ws_token in index_ws_tokens:
        if ws_token.startswith("nse_cm|"):
            token = ws_token.split("|", 1)[1]
            nse_index_tokens.append(token)
            symbol = await redis.get(f"index:token_to_symbol:{token}")
            if symbol:
                reversed_map[token] = symbol
        elif ws_token.startswith("bse_cm|"):
            token = ws_token.split("|", 1)[1]
            bse_index_tokens.append(token)
            symbol = await redis.get(f"index:token_to_symbol:{token}")
            if symbol:
                reversed_map[token] = symbol
                if str(symbol).upper() == "SENSEX":
                    sensex_token = token

    logger.info(
        "[angel_ws] Subscribing %d tokens total (%d equities + %d index tokens).",
        len(subscription_ws_tokens),
        len(nse_equity_tokens),
        len(index_ws_tokens),
    )

    ws_headers = {
        "Authorization": f"Bearer {jwt}",
        "x-api-key":     api_key,
        "x-client-code": client_code,
        "x-feed-token":  feed_token,
    }

    # Equity stocks: QUOTE mode (mode=2) — has volume, OI, OHLC
    subscribe_msg       = _build_subscribe_message(nse_equity_tokens)
    # Indices: LTP mode (mode=1) — no volume/OI, just price
    index_subscribe_msg = _build_index_subscribe_message(nse_index_tokens)
    sensex_subscribe_msg = (
        _build_index_subscribe_message([], [sensex_token])
        if sensex_token
        else None
    )
    other_bse_index_subscribe_msg = (
        _build_index_subscribe_message([], [t for t in bse_index_tokens if t != sensex_token])
        if any(t != sensex_token for t in bse_index_tokens)
        else None
    )
    symbol_count = len(nse_equity_tokens) + len(nse_index_tokens) + len(bse_index_tokens)

    # Health tracking
    tick_count     = 0
    health_window  = 0          # ticks in current 60-s window
    last_health_ts = time.monotonic()
    last_tick_iso  = ""

    logger.info(
        "[angel_ws] Connecting to %s (client=%s, tokens=%d) …",
        WS_URL, client_code, symbol_count,
    )

    async with websockets.connect(
        WS_URL,
        additional_headers=ws_headers,
        ping_interval=None,     # Disable library ping; AngelOne wants literal "ping" text
        ping_timeout=None,
        close_timeout=10,
        max_size=2**20,         # 1 MiB max message
    ) as ws:
        logger.info("[angel_ws] WebSocket connected. Subscribing …")

        await ws.send(subscribe_msg)
        logger.info("[angel_ws] QUOTE subscription sent — %d equity tokens.", len(nse_equity_tokens))
        if nse_index_tokens:
            await ws.send(index_subscribe_msg)
        if other_bse_index_subscribe_msg:
            await ws.send(other_bse_index_subscribe_msg)
        if sensex_subscribe_msg:
            try:
                await ws.send(sensex_subscribe_msg)
            except Exception:
                logger.warning(
                    "[ws_equities] SENSEX bse_cm token rejected — "
                    "BSE may not be supported on this connection. Skipping."
                )
                bse_index_tokens = [t for t in bse_index_tokens if t != sensex_token]
                symbol_count = len(nse_equity_tokens) + len(nse_index_tokens) + len(bse_index_tokens)
                if sensex_token:
                    reversed_map.pop(sensex_token, None)
        logger.info("[angel_ws] LTP subscription sent — %d index tokens: NSE=%s BSE=%s",
                    len(nse_index_tokens) + len(bse_index_tokens),
                    nse_index_tokens, bse_index_tokens)
        logger.info("[angel_ws] Subscribe payload preview: %s", subscribe_msg[:300])

        # Send immediate ping to verify heartbeat path works, then every 25s.
        try:
            await ws.send("ping")
            logger.info("[angel_ws] Initial ping sent.")
        except Exception as exc:
            logger.warning("[angel_ws] Initial ping failed: %s", exc)

        await _write_health(
            redis,
            connected=True,
            symbol_count=symbol_count,
        )

        # AngelOne SmartStream requires an application-level "ping" every
        # ~30 seconds or the server closes the connection. The websockets
        # library's ping_interval handles WS-protocol pings which AngelOne
        # ignores — we need to send the literal string "ping" as a text frame.
        async def _heartbeat():
            try:
                while True:
                    await asyncio.sleep(10)
                    await ws.send("ping")
                    logger.info("[angel_ws] Heartbeat ping sent.")
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("[angel_ws] Heartbeat failed: %s", exc)

        heartbeat_task = asyncio.create_task(_heartbeat())

        msg_count   = 0
        bin_count   = 0
        first_logged = False

        try:
            async for message in ws:
                msg_count += 1

                # Log the first 5 messages verbosely so we can diagnose
                if msg_count <= 5:
                    mtype = "TEXT" if isinstance(message, str) else "BIN"
                    mlen  = len(message)
                    preview = message[:80] if isinstance(message, str) else message[:20].hex()
                    logger.info(
                        "[angel_ws] MSG #%d type=%s len=%d preview=%r",
                        msg_count, mtype, mlen, preview,
                    )

                # AngelOne sends binary ticks; text frames are control/ack messages
                if isinstance(message, str):
                    if (
                        sensex_token
                        and ("nack" in message.lower() or "error" in message.lower())
                        and (sensex_token in message or "bse_cm" in message.lower())
                    ):
                        logger.warning(
                            "[ws_equities] SENSEX bse_cm token rejected — "
                            "BSE may not be supported on this connection. Skipping."
                        )
                        bse_index_tokens = [t for t in bse_index_tokens if t != sensex_token]
                        symbol_count = len(nse_equity_tokens) + len(nse_index_tokens) + len(bse_index_tokens)
                        reversed_map.pop(sensex_token, None)
                        sensex_token = None
                    if not first_logged:
                        logger.info("[angel_ws] Text frame: %s", message[:200])
                        first_logged = True
                    continue

                bin_count += 1
                tick = _parse_tick(message)
                if tick is None:
                    # Try LTP mode parser (used for index ticks)
                    tick = _parse_ltp_tick(message)
                if tick is None:
                    if bin_count <= 3:
                        logger.warning(
                            "[angel_ws] Binary packet failed parse (len=%d, first 20 bytes=%s)",
                            len(message), message[:20].hex(),
                        )
                    continue

                token  = tick["token"]
                symbol = reversed_map.get(token)
                if symbol is None:
                    if bin_count <= 3:
                        logger.warning("[angel_ws] Unknown token %r — skipping.", token)
                    continue

                await _write_tick(redis, symbol, tick)

                tick_count    += 1
                health_window += 1
                last_tick_iso  = tick["ts"]

                # Health heartbeat
                now = time.monotonic()
                if now - last_health_ts >= HEALTH_INTERVAL:
                    rate = health_window
                    logger.info(
                        "[angel_ws] Health — ticks_last_60s=%d  total=%d  "
                        "symbols=%d  last_tick=%s",
                        rate, tick_count, symbol_count, last_tick_iso,
                    )
                    await _write_health(
                        redis,
                        connected=True,
                        ticks_last_60s=rate,
                        last_tick_ts=last_tick_iso,
                        symbol_count=symbol_count,
                    )
                    health_window  = 0
                    last_health_ts = now
        finally:
            heartbeat_task.cancel()


# ---------------------------------------------------------------------------
# Main reconnect loop
# ---------------------------------------------------------------------------

async def run_equity_feed() -> None:
    """
    Permanent async loop — never returns under normal circumstances.

    Implements:
      * Market-closed wait (5-min poll)
      * Fresh JWT on every reconnect
      * Exponential backoff (2 → 4 → 8 → … → 60 s)
    """
    backoff = BACKOFF_INITIAL
    attempt = 0

    while True:
        attempt += 1
        logger.info("[angel_ws] === Reconnect attempt #%d ===", attempt)

        # ── 1. Wait for market open ──────────────────────────────────────
        logger.info("[angel_ws] Checking NSE market status …")
        await _wait_for_market_open()

        # ── 2. Fresh session — never reuse previous JWT ──────────────────
        try:
            session = await get_fresh_session()
        except Exception as exc:
            # Catches RuntimeError, DataException (rate-limit), network errors, etc.
            err_str = str(exc)
            if "access rate" in err_str.lower() or "rate" in err_str.lower():
                logger.warning("[angel_ws] Rate-limited — extended backoff 60s.")
                await asyncio.sleep(60)
                continue
            logger.error("[angel_ws] Session creation failed: %s", exc)
            _log_backoff(backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
            continue

        # ── 3. Run WebSocket session ─────────────────────────────────────
        disconnect_reason = "unknown"
        try:
            await _run_ws_session(session)
            disconnect_reason = "clean close"
        except ConnectionClosed as exc:
            disconnect_reason = f"ConnectionClosed code={exc.rcvd.code if exc.rcvd else '?'} reason={exc.rcvd.reason if exc.rcvd else ''}"
        except WebSocketException as exc:
            disconnect_reason = f"WebSocketException: {exc}"
        except asyncio.TimeoutError:
            disconnect_reason = "asyncio.TimeoutError"
        except Exception as exc:  # noqa: BLE001
            disconnect_reason = f"{type(exc).__name__}: {exc}"

        logger.warning(
            "[angel_ws] Session ended — reason: %s", disconnect_reason
        )

        # Mark feed as disconnected in Redis (best-effort)
        try:
            redis = await get_redis()
            await _write_health(redis, connected=False)
        except Exception:
            pass

        # ── 4. Exponential backoff before next attempt ───────────────────
        _log_backoff(backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, BACKOFF_MAX)

        # Reset backoff after a sustained session (> 5 minutes connected)
        # so transient blips don't accumulate penalty forever.
        # (Session duration tracking omitted for simplicity; reset on long
        #  runs is handled implicitly since backoff caps at BACKOFF_MAX.)


def _log_backoff(backoff: float) -> None:
    logger.info(
        "[angel_ws] Backing off for %.0fs before next reconnect attempt.", backoff
    )


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )
    logger.info("=== angel_ws_equities standalone run ===")
    logger.info(
        "Config — client: %s  api_key: %s***",
        cfg.ANGELONE_CLIENT_ID,
        cfg.ANGELONE_API_KEY[:4],
    )

    from core.config import validate
    validate()

    await run_equity_feed()


if __name__ == "__main__":
    asyncio.run(_main())
