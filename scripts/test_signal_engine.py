#!/usr/bin/env python3
from core.redis_client import get_redis
from strategy_brain.professional_signal_engine import get_professional_engine
import asyncio
import json

async def main():
    redis = await get_redis()
    engine = await get_professional_engine(redis)
    report = await engine.run_backtest()
    await redis.close()

if __name__ == "__main__":
    asyncio.run(main())
