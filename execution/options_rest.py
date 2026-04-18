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


# ---------------------------------------------------------------------------
# Session management — Redis-backed, multi-writer, last-writer-wins
# ---------------------------------------------------------------------------

async def publish_angel_jwt(jwt_token: str) -> None:
    """
    Publish a freshly-generated AngelOne JWT to Redis for consumers.

    Called by any service that performs AngelOne login — equity feed,
    options feed, morning seeder, etc.  All valid JWTs for the same
    account are equivalent, so last-writer-wins is safe.

    Silently swallows Redis errors: failing to publish shouldn't break
    the caller's login flow.
    """
    if not jwt_token:
        return
    try:
        redis = await get_redis()
        await redis.set(_REDIS_JWT_KEY, jwt_token, ex=_JWT_TTL_SECONDS)
        logger.info("[options_rest] JWT published to Redis")
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
