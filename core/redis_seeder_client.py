"""
core/redis_seeder_client.py
============================
Dedicated Redis client for the seeder worker.
Uses max_connections=2 — seeder is sequential, never needs more.
Completely isolated from the shared API server pool.
"""
import redis.asyncio as aioredis
import os
import logging

log = logging.getLogger(__name__)

_seeder_pool: aioredis.Redis | None = None


async def get_seeder_redis() -> aioredis.Redis:
    global _seeder_pool
    if _seeder_pool is None:
        url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        _seeder_pool = aioredis.from_url(
            url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=2,
            socket_keepalive=True,
            socket_connect_timeout=10,
            retry_on_timeout=True,
        )
        log.info("[redis_seeder] Pool created (max_connections=2)")
    return _seeder_pool


async def close_seeder_redis():
    global _seeder_pool
    if _seeder_pool:
        await _seeder_pool.aclose()
        _seeder_pool = None
        log.info("[redis_seeder] Pool closed")
