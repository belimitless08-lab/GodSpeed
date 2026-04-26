"""Dynamic index instrument registry backed by AngelOne instrument master and Redis."""

import logging

import pandas as pd

from core.redis_client import get_redis

logger = logging.getLogger(__name__)

INDEX_SYMBOLS = [
    {"symbol": "NIFTY", "exch_seg": "NSE", "search_name": "NIFTY 50"},
    {"symbol": "BANKNIFTY", "exch_seg": "NSE", "search_name": "NIFTY BANK"},
    {"symbol": "FINNIFTY", "exch_seg": "NSE", "search_name": "NIFTY FIN SERVICE"},
    {"symbol": "MIDCPNIFTY", "exch_seg": "NSE", "search_name": "NIFTY MID SELECT"},
    {"symbol": "SENSEX", "exch_seg": "BSE", "search_name": "SENSEX"},
]

PREFERRED_TOKENS = {
    "NIFTY": "99926000",   # NIFTY 50 — not 99926004 (NIFTY 500)
}


async def resolve_index_tokens(master_df: pd.DataFrame) -> dict:
    """Resolve index tokens from AngelOne master dataframe without hardcoded fallbacks."""
    resolved: dict[str, dict[str, str]] = {}

    if master_df is None or master_df.empty:
        logger.warning("[registry] Empty master_df received while resolving index tokens")
        return resolved

    try:
        for entry in INDEX_SYMBOLS:
            symbol = entry["symbol"]
            exch_seg = entry["exch_seg"]
            search_name = entry["search_name"]

            exch_filtered = master_df[master_df["exch_seg"] == exch_seg]
            matches = exch_filtered[
                exch_filtered["name"].astype(str).str.contains(search_name, case=False, na=False)
            ]

            if matches.empty:
                logger.warning(
                    f"[registry] Could not resolve token for {symbol} — check search_name against master CSV"
                )
                continue

            row = matches.iloc[0]
            token = str(row["token"])
            resolved[symbol] = {"token": token, "exch_seg": exch_seg}
    except Exception as e:
        logger.error(f"[registry] Failed resolving index tokens: {e}")

    for symbol, forced_token in PREFERRED_TOKENS.items():
        if symbol in resolved:
            resolved[symbol]["token"] = forced_token

    return resolved


async def store_index_tokens(tokens: dict) -> None:
    """Persist resolved index tokens and reverse lookups in Redis."""
    if not tokens:
        return

    try:
        r = await get_redis()
        for symbol, data in tokens.items():
            token = str(data.get("token", ""))
            exch_seg = str(data.get("exch_seg", ""))
            if not token or not exch_seg:
                continue

            meta_key = f"index:meta:{symbol}"
            rev_key = f"index:token_to_symbol:{token}"

            await r.hset(meta_key, mapping={"token": token, "exch_seg": exch_seg})
            await r.expire(meta_key, 86400)

            await r.hset("index:tokens", symbol, token)

            await r.set(rev_key, symbol)
            await r.expire(rev_key, 86400)
    except Exception as e:
        logger.error(f"[registry] Failed storing index tokens: {e}")


async def load_index_tokens() -> dict:
    """Load symbol->token mapping from Redis."""
    try:
        r = await get_redis()
        data = await r.hgetall("index:tokens")
        return data or {}
    except Exception as e:
        logger.error(f"[registry] Failed loading index tokens: {e}")
        return {}


async def load_index_symbols() -> set:
    """Load resolved index symbols from Redis."""
    try:
        r = await get_redis()
        data = await r.hgetall("index:tokens")
        return set(data.keys()) if data else set()
    except Exception as e:
        logger.error(f"[registry] Failed loading index symbols: {e}")
        return set()


async def get_index_ws_tokens() -> list[str]:
    """Build AngelOne WS subscription tokens with exchange prefixes from Redis metadata."""
    ws_tokens: list[str] = []

    try:
        r = await get_redis()
        symbol_to_token = await r.hgetall("index:tokens")
        if not symbol_to_token:
            return ws_tokens

        for symbol in symbol_to_token.keys():
            meta = await r.hgetall(f"index:meta:{symbol}")
            token = str(meta.get("token", ""))
            exch_seg = str(meta.get("exch_seg", "")).upper()

            if not token or exch_seg not in {"NSE", "BSE"}:
                continue

            prefix = "nse_cm" if exch_seg == "NSE" else "bse_cm"
            ws_tokens.append(f"{prefix}|{token}")
    except Exception as e:
        logger.error(f"[registry] Failed building WS tokens: {e}")
        return []

    return ws_tokens


async def is_index_token(token: str) -> bool:
    """Check whether token is a registered index token."""
    try:
        r = await get_redis()
        return bool(await r.exists(f"index:token_to_symbol:{token}"))
    except Exception as e:
        logger.error(f"[registry] Failed checking index token {token}: {e}")
        return False


async def get_symbol_for_token(token: str) -> str | None:
    """Get symbol by reverse token lookup."""
    try:
        r = await get_redis()
        value = await r.get(f"index:token_to_symbol:{token}")
        return value if value else None
    except Exception as e:
        logger.error(f"[registry] Failed reverse lookup for token {token}: {e}")
        return None
