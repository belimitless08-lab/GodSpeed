"""
core/redis_client.py
Single shared Redis connection pool for all modules.
Never creates more than one pool per process.
"""
import redis.asyncio as redis
import os
import logging

log = logging.getLogger(__name__)

# Single global pool — created once per process
redis_client: redis.Redis | None = None


async def get_redis() -> redis.Redis:
    """
    Returns the shared Redis client.
    Creates it once on first call, reuses forever after.
    Thread-safe for asyncio.
    """
    global redis_client
    if redis_client is None:
        url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        max_conns = int(os.environ.get("REDIS_MAX_CONNECTIONS", "5"))
        redis_client = redis.from_url(
            url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=max_conns,
            socket_keepalive=True,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
        log.info(f"[redis] Connection pool created (max_connections={max_conns})")
    return redis_client


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
    global redis_client
    if redis_client:
        await redis_client.aclose()
        redis_client = None
        log.info("[redis] Connection pool closed")
