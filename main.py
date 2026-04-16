"""
main.py — Local development only.
Runs all nodes in a single asyncio process.
Railway uses Procfile instead.

Usage: python main.py
"""
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)

log = logging.getLogger('main')


async def main():
    log.info('Starting Market Pulse Pro v2 — local mode')

    # Validate all env vars present before starting
    from core.config import validate
    validate()
    log.info('Config validated')

    # Build universe first — everything depends on this
    from core.universe_builder import build_universe
    await build_universe()
    log.info('Universe built')

    # Import all node entry points
    from data_feed.angel_ws_equities import run_equity_feed
    from data_feed.angel_ws_options import run_options_feed
    from math_engine.candle_builder import run_candle_builder
    from strategy_brain.brain import run_brain

    # Import FastAPI app for uvicorn
    from execution.api_server import app
    import uvicorn

    uvicorn_config = uvicorn.Config(
        app,
        host='0.0.0.0',
        port=8000,
        log_level='info'
    )
    server = uvicorn.Server(uvicorn_config)

    log.info('Starting all nodes...')

    # Run all nodes concurrently
    results = await asyncio.gather(
        run_equity_feed(),
        run_options_feed(),
        run_candle_builder(),
        run_brain(),
        server.serve(),
        return_exceptions=True
    )

    # Log any exceptions
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            log.error(f'Node {i} crashed: {result}')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info('Shutting down')
