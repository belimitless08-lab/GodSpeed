"""
core/redis_client.py
Single shared Redis connection pool for all modules.
Never creates more than one pool per process.
"""
import redis.asyncio as aioredis
import os
import logging

log = logging.getLogger(__name__)

# Single global pool — created once per process
_pool: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    """
    Returns the shared Redis client.
    Creates it once on first call, reuses forever after.
    Thread-safe for asyncio.
    """
    global _pool
    if _pool is None:
        url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        max_conns = int(os.environ.get("REDIS_MAX_CONNECTIONS", "20"))
        _pool = aioredis.from_url(
            url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=max_conns,
            socket_keepalive=True,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
        log.info(f"[redis] Connection pool created (max_connections={max_conns})")
    return _pool


async def ping() -> bool:
    """Health check — returns True if Redis is reachable."""
    try:
        r = await get_redis()
        await r.ping()
        return True
    except Exception as e:
        log.error(f"[redis] Ping failed: {e}")
        return False


async def close():
    """Close the connection pool on shutdown."""
    global _pool
    if _pool:
        await _pool.aclose()
        _pool = None
        log.info("[redis] Connection pool closed")
