web: uvicorn execution.api_server:app --host 0.0.0.0 --port $PORT
feed: python -m data_feed.angel_ws_equities
options: python -m data_feed.angel_ws_options
cruncher: python -m math_engine.candle_builder
brain: python -m strategy_brain.brain
