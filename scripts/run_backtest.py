#!/usr/bin/env python3
"""
ONE-TIME BACKTEST for Professional Signal Engine
Run this manually from seeder service
"""

import asyncio
import json
from core.redis_client import get_redis
from strategy_brain.professional_signal_engine import get_professional_engine

async def main():
    print("🚀 Starting Professional Signal Engine Backtest...")
    redis_client = await get_redis()
    engine = await get_professional_engine(redis_client)
    
    report = await engine.run_backtest()
    
    print("\n✅ BACKTEST REPORT:")
    print(json.dumps(report, indent=2))
    print("\n🎉 Backtest finished!")
    
    await redis_client.close()

if __name__ == "__main__":
    asyncio.run(main())
