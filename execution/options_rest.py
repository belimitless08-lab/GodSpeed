"""
execution/options_rest.py
=========================
REST fallback client for option LTP when WebSocket data is missing.

Used by order_manager._get_execution_ltp Tier 3 — only invoked when Redis
tick hashes are empty (market closed, strike not yet subscribed, feed
outage).  Never used during normal live trading — WebSocket is always the
hot path.

Rate-limit strategy
-------------------
AngelOne's getLTP endpoint documents 10 req/sec but real-world reliability
is closer to 4 req/sec.  We throttle conservatively with a semaphore cap
of 2 concurrent requests, which on p99 latency of ~300ms gives ~6 req/sec
effective — still below the practical ceiling.

Results are cached for 60 seconds under `options:rest_cache:{tradingsymbol}`
to prevent duplicate REST calls when many orders arrive in the same minute.
"""

from __future__ import annotations

import asyncio
import json
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

# Session reuse — share JWT across order_manager calls
# In session 3 we move this to a shared Redis-backed session helper.
_SHARED_JWT:  Optional[str] = None
_SHARED_API_KEY: Optional[str] = None


# ---------------------------------------------------------------------------
# Session management (minimal — Session 3 will replace with shared Redis JWT)
# ---------------------------------------------------------------------------

def set_angel_session(jwt_token: str, api_key: str) -> None:
    """
    Called by the main app on startup after AngelOne login.
    Stores the JWT in module-level state for REST fetches.
    """
    global _SHARED_JWT, _SHARED_API_KEY
    _SHARED_JWT = jwt_token
    _SHARED_API_KEY = api_key
    logger.info("[options_rest] AngelOne session registered")


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

    Parameters
    ----------
    exchange       : "NFO" | "BFO"
    tradingsymbol  : e.g. "NIFTY24APR26CE" — the full AngelOne symbol
    token          : numeric token string from universe:options:{sym} hash

    Returns
    -------
    Float LTP on success, None if:
        - No JWT registered (session not initialised yet)
        - Rate limit hit (Angel returned 429 or throttle error)
        - Any network or response error
        - Response shape unexpected
    """
    if not _SHARED_JWT or not _SHARED_API_KEY:
        logger.warning("[options_rest] No AngelOne session — cannot fetch LTP for %s", tradingsymbol)
        return None

    if not tradingsymbol or not token:
        return None

    # Check cache first
    redis = await get_redis()
    cache_key = f"options:rest_cache:{tradingsymbol}"
    cached = await redis.get(cache_key)
    if cached:
        try:
            cached_ltp = float(cached)
            if cached_ltp > 0:
                logger.debug("[options_rest] Cache hit for %s: ₹%.2f", tradingsymbol, cached_ltp)
                return cached_ltp
        except (TypeError, ValueError):
            pass

    # Throttled REST call
    async with _SEMAPHORE:
        try:
            ltp = await _do_rest_call(exchange, tradingsymbol, token)
        except Exception as exc:
            logger.warning("[options_rest] REST call failed for %s: %s", tradingsymbol, exc)
            return None

    # Cache successful result
    if ltp is not None and ltp > 0:
        await redis.set(cache_key, str(ltp), ex=_CACHE_TTL_SECONDS)

    return ltp


# ---------------------------------------------------------------------------
# Internal HTTP call
# ---------------------------------------------------------------------------

async def _do_rest_call(
    exchange: str,
    tradingsymbol: str,
    token: str,
) -> Optional[float]:
    url = f"{_ANGEL_BASE_URL}{_LTP_ENDPOINT}"
    headers = {
        "Authorization": f"Bearer {_SHARED_JWT}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "X-UserType":    "USER",
        "X-SourceID":    "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress":  "AA:AA:AA:AA:AA:AA",
        "X-PrivateKey":  _SHARED_API_KEY,
    }
    body = {
        "exchange":      exchange,
        "tradingsymbol": tradingsymbol,
        "symboltoken":   str(token),
    }

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(url, json=body, headers=headers)

    if resp.status_code != 200:
        logger.warning(
            "[options_rest] HTTP %d for %s: %s",
            resp.status_code, tradingsymbol, resp.text[:200],
        )
        return None

    try:
        data = resp.json()
    except Exception:
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
