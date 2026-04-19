"""
execution/options_rest.py
=========================
REST fallback client for option LTP when WebSocket data is missing.

Used by order_manager._get_execution_ltp Tier 3 — only invoked when Redis
tick hashes are empty (market closed, strike not yet subscribed, feed
outage).  Never used during normal live trading — WebSocket is always the
hot path.

JWT handling
------------
JWT is read from Redis key `angel:session:jwt` on every REST call.  This
ensures reconnects (which generate fresh JWTs) don't leave this module
holding stale tokens.  Any service that generates a JWT publishes to that
key; last-writer-wins (all JWTs are equivalent as long as they're fresh).

Rate-limit strategy
-------------------
AngelOne's getLTP endpoint documents 10 req/sec but real-world reliability
is closer to 4 req/sec.  Semaphore cap of 2 concurrent requests.

Results cached for 60 seconds under `options:rest_cache:{tradingsymbol}`.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

import httpx

from core.redis_client import get_redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_ANGEL_BASE_URL = "https://apiconnect.angelone.in"
_LTP_ENDPOINT   = "/rest/secure/angelbroking/order/v1/getLtpData"

# Concurrency cap — 2 in-flight calls protects against rate limits.
_SEMAPHORE = asyncio.Semaphore(2)

# Cache TTL for REST responses
_CACHE_TTL_SECONDS = 60

# Redis key where the active JWT lives
_REDIS_JWT_KEY = "angel:session:jwt"

# JWT TTL safeguard — if no service refreshes for 24h, treat as expired
_JWT_TTL_SECONDS = 24 * 3600

# Hardcoded index token + tradingsymbol mapping.
# AngelOne's REST LTP endpoint requires specific tradingsymbol formats for
# indices that differ from the common ticker symbol. Confirmed from SmartAPI
# forum admin posts. The old tokens (26000/26009) are deprecated.
_INDEX_TOKENS: dict[str, dict] = {
    "NIFTY":      {"token": "99926000", "exchange": "NSE", "tradingsymbol": "Nifty 50"},
    "BANKNIFTY":  {"token": "99926009", "exchange": "NSE", "tradingsymbol": "Nifty Bank"},
    "FINNIFTY":   {"token": "99926037", "exchange": "NSE", "tradingsymbol": "Nifty Fin Service"},
    "MIDCPNIFTY": {"token": "99926074", "exchange": "NSE", "tradingsymbol": "NIFTY MID SELECT"},
    "SENSEX":     {"token": "99919000", "exchange": "BSE", "tradingsymbol": "SENSEX"},
    "BANKEX":     {"token": "99919005", "exchange": "BSE", "tradingsymbol": "BANKEX"},
}


# ---------------------------------------------------------------------------
# Session management — Redis-backed, multi-writer, last-writer-wins
# ---------------------------------------------------------------------------

async def publish_angel_jwt(jwt_token: str) -> None:
    """
    Publish a freshly-generated AngelOne JWT to Redis for consumers.

    Strips any leading 'Bearer ' prefix — newer versions of smartapi-python
    return the JWT already prefixed, and downstream consumers add their own
    'Bearer ' in the Authorization header, causing double-prefix 401 errors.

    Called by any service that performs AngelOne login — equity feed,
    options feed, morning seeder, etc.  All valid JWTs for the same
    account are equivalent, so last-writer-wins is safe.

    Silently swallows Redis errors: failing to publish shouldn't break
    the caller's login flow.
    """
    if not jwt_token:
        return

    # Defensive: strip 'Bearer ' prefix if already present.
    # smartapi-python sometimes returns 'Bearer eyJ...' while the raw
    # AngelOne response contains just 'eyJ...'. Normalize at write time
    # so readers always get a clean token.
    cleaned = jwt_token.strip()
    if cleaned.lower().startswith("bearer "):
        cleaned = cleaned[7:].strip()

    if not cleaned:
        logger.warning("[options_rest] JWT is empty after cleaning; not publishing")
        return

    try:
        redis = await get_redis()
        await redis.set(_REDIS_JWT_KEY, cleaned, ex=_JWT_TTL_SECONDS)
        logger.info("[options_rest] JWT published to Redis (%d chars)", len(cleaned))
    except Exception as exc:
        logger.warning("[options_rest] Failed to publish JWT: %s", exc)


async def _get_current_jwt() -> Optional[str]:
    """Fetch the currently active JWT from Redis. Returns None if missing."""
    try:
        redis = await get_redis()
        raw = await redis.get(_REDIS_JWT_KEY)
        if not raw:
            return None
        return raw if isinstance(raw, str) else raw.decode()
    except Exception as exc:
        logger.warning("[options_rest] Could not read JWT from Redis: %s", exc)
        return None


def _get_api_key() -> Optional[str]:
    """API key from env — static per deployment."""
    return os.environ.get("ANGELONE_API_KEY") or os.environ.get("ANGEL_API_KEY")


# Backward-compat shim — keeps old call sites working.  Now publishes to
# Redis instead of module globals.  The api_key arg is ignored (env-only).
async def set_angel_session(jwt_token: str, api_key: str | None = None) -> None:
    """Backward-compat alias for publish_angel_jwt()."""
    await publish_angel_jwt(jwt_token)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fetch_option_ltp(
    exchange: str,
    tradingsymbol: str,
    token: str,
) -> Optional[float]:
    """
    Fetch last-traded-price for a single option contract via AngelOne REST.

    Returns float LTP on success, None on any failure.  Never raises.
    """
    if not tradingsymbol or not token:
        return None

    # Read JWT fresh from Redis on every call — handles reconnect scenarios
    jwt = await _get_current_jwt()
    api_key = _get_api_key()

    if not jwt:
        logger.warning(
            "[options_rest] No JWT in Redis — cannot fetch LTP for %s. "
            "Verify at least one login site publishes to %s",
            tradingsymbol, _REDIS_JWT_KEY,
        )
        return None
    if not api_key:
        logger.warning("[options_rest] ANGELONE_API_KEY env var not set")
        return None

    # Check cache first (60s TTL)
    redis = await get_redis()
    cache_key = f"options:rest_cache:{tradingsymbol}"
    try:
        cached = await redis.get(cache_key)
        if cached:
            cached_str = cached if isinstance(cached, str) else cached.decode()
            cached_ltp = float(cached_str)
            if cached_ltp > 0:
                logger.debug("[options_rest] Cache hit for %s: ₹%.2f",
                             tradingsymbol, cached_ltp)
                return cached_ltp
    except (TypeError, ValueError):
        pass  # cache value malformed — treat as miss
    except Exception as exc:
        logger.debug("[options_rest] Cache read failed (non-fatal): %s", exc)

    # Throttled REST call
    async with _SEMAPHORE:
        ltp = await _do_rest_call(exchange, tradingsymbol, token, jwt, api_key)

    # Cache successful result
    if ltp is not None and ltp > 0:
        try:
            await redis.set(cache_key, str(ltp), ex=_CACHE_TTL_SECONDS)
        except Exception as exc:
            logger.debug("[options_rest] Cache write failed (non-fatal): %s", exc)

    return ltp


async def fetch_underlying_ltp(symbol: str) -> Optional[float]:
    """
    Fetch last-traded-price of the underlying (index or stock) via REST.

    Used when:
      - Market is closed and tick:{symbol} is stale/empty
      - Seeded snapshot doesn't exist (e.g. indices not in morning_seeder)
      - We need a reference price for strike resolution on a market order

    Index tokens + tradingsymbols are hardcoded (NIFTY, BANKNIFTY, etc.)
    because AngelOne requires specific formats that differ from common
    ticker symbols. Stock tokens read from universe:token_map (populated
    by universe_builder).

    Returns float LTP or None on failure.  Never raises.
    """
    if not symbol:
        return None

    redis = await get_redis()

    # Cache hit first — 30s TTL
    cache_key = f"underlying:rest_cache:{symbol}"
    try:
        cached = await redis.get(cache_key)
        if cached:
            val = float(cached if isinstance(cached, str) else cached.decode())
            if val > 0:
                return val
    except (TypeError, ValueError):
        pass
    except Exception as exc:
        logger.debug("[options_rest] underlying cache read failed: %s", exc)

    # Resolve token + exchange + tradingsymbol
    if symbol in _INDEX_TOKENS:
        meta = _INDEX_TOKENS[symbol]
        token = meta["token"]
        exchange = meta["exchange"]
        tradingsymbol = meta["tradingsymbol"]
    else:
        # Stock: look up NSE equity token from universe:token_map
        try:
            token_map_raw = await redis.get("universe:token_map")
            if not token_map_raw:
                logger.warning("[options_rest] universe:token_map empty, cannot resolve %s", symbol)
                return None
            import json as _json
            token_map = _json.loads(
                token_map_raw if isinstance(token_map_raw, str) else token_map_raw.decode()
            )
            token = token_map.get(symbol)
            if not token:
                logger.warning("[options_rest] No token for stock %s in universe:token_map", symbol)
                return None
            exchange = "NSE"
            tradingsymbol = f"{symbol}-EQ"
        except Exception as exc:
            logger.warning("[options_rest] Token resolution failed for %s: %s", symbol, exc)
            return None

    # Auth
    jwt = await _get_current_jwt()
    api_key = _get_api_key()
    if not jwt or not api_key:
        logger.warning("[options_rest] No JWT/API key — cannot fetch underlying %s", symbol)
        return None

    # Throttled REST call — reuse the options semaphore + _do_rest_call helper
    async with _SEMAPHORE:
        ltp = await _do_rest_call(exchange, tradingsymbol, token, jwt, api_key)

    # Cache for 30 seconds
    if ltp is not None and ltp > 0:
        try:
            await redis.set(cache_key, str(ltp), ex=30)
        except Exception:
            pass
        logger.info("[options_rest] Underlying LTP for %s: ₹%.2f", symbol, ltp)

    return ltp


# ---------------------------------------------------------------------------
# Internal HTTP call — tight exception handling
# ---------------------------------------------------------------------------

async def _do_rest_call(
    exchange: str,
    tradingsymbol: str,
    token: str,
    jwt: str,
    api_key: str,
) -> Optional[float]:
    """Single HTTP POST to AngelOne's getLtpData. Catches all errors, returns None."""
    url = f"{_ANGEL_BASE_URL}{_LTP_ENDPOINT}"
    headers = {
        "Authorization":    f"Bearer {jwt}",
        "Content-Type":     "application/json",
        "Accept":           "application/json",
        "X-UserType":       "USER",
        "X-SourceID":       "WEB",
        "X-ClientLocalIP":  "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress":     "AA:AA:AA:AA:AA:AA",
        "X-PrivateKey":     api_key,
    }
    body = {
        "exchange":      exchange,
        "tradingsymbol": tradingsymbol,
        "symboltoken":   str(token),
    }

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(url, json=body, headers=headers)
    except httpx.TimeoutException as exc:
        logger.warning("[options_rest] Timeout for %s: %s", tradingsymbol, exc)
        return None
    except httpx.RequestError as exc:
        logger.warning("[options_rest] Network error for %s: %s", tradingsymbol, exc)
        return None
    except Exception as exc:
        logger.error("[options_rest] Unexpected error for %s: %s",
                     tradingsymbol, exc, exc_info=True)
        return None

    # Status handling
    if resp.status_code == 401:
        logger.warning(
            "[options_rest] 401 Unauthorized for %s — JWT expired or invalid. "
            "Waiting for next feed reconnect to refresh.",
            tradingsymbol,
        )
        return None
    if resp.status_code == 429:
        logger.warning("[options_rest] 429 Rate limited for %s", tradingsymbol)
        return None
    if resp.status_code != 200:
        logger.warning(
            "[options_rest] HTTP %d for %s: %s",
            resp.status_code, tradingsymbol, resp.text[:200],
        )
        return None

    # Parse response
    try:
        data = resp.json()
    except Exception:
        logger.warning("[options_rest] Non-JSON response for %s", tradingsymbol)
        return None

    if not data.get("status") or not data.get("data"):
        err = data.get("message", "unknown")
        logger.warning("[options_rest] API error for %s: %s", tradingsymbol, err)
        return None

    try:
        ltp = float(data["data"]["ltp"])
        return ltp if ltp > 0 else None
    except (KeyError, TypeError, ValueError):
        return None
