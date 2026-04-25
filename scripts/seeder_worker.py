"""
scripts/seeder_worker.py
========================
Standalone entrypoint for the morning seeder worker.
Deployed as a separate Railway service with its own Redis pool.
Runs once at 08:30 IST, seeds all 213 symbols, then exits.

Railway start command: python -m scripts.seeder_worker
"""
import asyncio
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


async def main():
    force = os.environ.get("SEEDER_FORCE", "0") == "1"
    logger.info("[seeder_worker] Starting — force=%s", force)

    try:
        from scripts.morning_seeder import run_seeder
        await run_seeder(force=force)
        logger.info("[seeder_worker] Completed successfully.")
        sys.exit(0)
    except Exception as exc:
        logger.error("[seeder_worker] FATAL: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
