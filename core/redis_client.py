"""
core/redis_client.py
====================
Async Redis connection pool for Market Pulse Pro v2.

Rules
-----
* This is the ONLY place in the codebase that creates a Redis connection.
  Every other module must import ``get_redis()`` from here — never instantiate
  ``redis.asyncio.Redis`` directly.
* The pool is lazily initialised on the first call to ``get_redis()`` and
  reused for the lifetime of the process.
* AOF persistence is requested via the ``CONFIG SET`` command immediately
  after the pool is created (Railway-managed Redis may or may not honour it
  depending on ACLs, but we always attempt it).
* ``ping()`` is exposed as a lightweight liveness check for /api/health.

Usage
-----
    from core.redis_client import get_redis, ping, close_redis

    redis = await get_redis()
    await redis.set("foo", "bar", ex=60)
    value = await redis.get("foo")

    healthy = await ping()         # True / False

    # On shutdown:
    await close_redis()
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import redis.asyncio as aioredis
from redis.asyncio.connection import ConnectionPool
from redis.exceptions import RedisError

from core.config import cfg

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state — never access directly outside this module
# ---------------------------------------------------------------------------

_pool: Optional[ConnectionPool] = None
_client: Optional[aioredis.Redis] = None
_init_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Pool configuration
# ---------------------------------------------------------------------------

_POOL_KWARGS: dict = {
    # Maximum simultaneous connections.
    # Tune upward if you add more background workers.
    "max_connections": 20,
    # Return connections to the pool rather than closing them.
    "health_check_interval": 30,
    # Decode bytes → str automatically so callers don't have to.
    "decode_responses": True,
    # Connection timeout (seconds)
    "socket_connect_timeout": 5,
    # Send/receive timeout (seconds)
    "socket_timeout": 5,
    # Keep-alive so Railway's internal LB doesn't silently drop idle sockets
    "socket_keepalive": True,
}

# Redis CONFIG SET pairs we attempt immediately after pool creation.
# AOF persistence: every write is fsync'd to disk.
_REDIS_SERVER_CONFIG: dict[str, str] = {
    "appendonly": "yes",
    "appendfsync": "everysec",   # safe balance between durability and throughput
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def get_redis() -> aioredis.Redis:
    """
    Return the shared async Redis client backed by a connection pool.

    Thread-safe (uses asyncio.Lock).  Safe to call from any coroutine;
    the pool is created on the first call and reused thereafter.

    Raises
    ------
    redis.exceptions.RedisError
        If the initial connection to Redis fails.
    """
    global _pool, _client

    if _client is not None:
        return _client

    async with _init_lock:
        # Double-checked locking — another coroutine may have initialised
        # the pool while we were waiting for the lock.
        if _client is not None:
            return _client

        logger.info("[redis] Creating connection pool → %s", _redacted_url())

        _pool = ConnectionPool.from_url(
            cfg.REDIS_URL,
            **_POOL_KWARGS,
        )
        _client = aioredis.Redis(connection_pool=_pool)

        # Verify connectivity immediately so startup fails fast on bad creds.
        try:
            await _client.ping()
            logger.info("[redis] ✓ Connection pool ready.")
        except RedisError as exc:
            _pool = None
            _client = None
            raise RedisError(
                f"[redis] Could not connect to Redis at {_redacted_url()}: {exc}"
            ) from exc

        # Request AOF persistence — best-effort; log but don't abort if the
        # Redis user lacks CONFIG SET privileges.
        await _apply_server_config(_client)

    return _client


async def ping() -> bool:
    """
    Lightweight liveness check.  Returns True if Redis responds to PING,
    False on any error.  Never raises.

    Suitable for /api/health endpoints.
    """
    try:
        client = await get_redis()
        result = await client.ping()
        return result is True
    except Exception as exc:  # noqa: BLE001
        logger.warning("[redis] ping() failed: %s", exc)
        return False


async def close_redis() -> None:
    """
    Gracefully drain and close the connection pool.
    Call from your application shutdown handler (e.g. FastAPI ``on_event("shutdown")``).
    """
    global _pool, _client

    if _client is not None:
        try:
            await _client.aclose()
            logger.info("[redis] Connection pool closed.")
        except Exception as exc:  # noqa: BLE001
            logger.warning("[redis] Error while closing pool: %s", exc)
        finally:
            _pool = None
            _client = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _apply_server_config(client: aioredis.Redis) -> None:
    """
    Attempt to apply server-side persistence settings.

    We do this via CONFIG SET.  If the Redis server is managed (e.g. Railway
    Redis without CONFIG SET ACLs) this call will raise an exception which we
    catch and log as a warning — the pool remains usable.
    """
    try:
        for key, value in _REDIS_SERVER_CONFIG.items():
            await client.config_set(key, value)
        logger.info(
            "[redis] ✓ AOF persistence enabled (appendonly=yes, appendfsync=everysec)."
        )
    except RedisError as exc:
        logger.warning(
            "[redis] Could not apply CONFIG SET (AOF persistence). "
            "If using a managed Redis, enable AOF from the provider dashboard instead. "
            "Error: %s",
            exc,
        )


def _redacted_url() -> str:
    """Return REDIS_URL with the password masked for safe logging."""
    url = cfg.REDIS_URL
    try:
        # redis://:PASSWORD@host:port/db  →  redis://:***@host:port/db
        if "@" in url:
            scheme_creds, rest = url.rsplit("@", 1)
            if ":" in scheme_creds.split("//", 1)[-1]:
                scheme, creds = scheme_creds.split("//", 1)
                user_part = creds.split(":")[0]
                return f"{scheme}//{user_part}:***@{rest}"
        return url[:14] + "***"
    except Exception:  # noqa: BLE001
        return "redis://***"
