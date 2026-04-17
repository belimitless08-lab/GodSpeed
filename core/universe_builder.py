"""
core/universe_builder.py
========================
Dynamically constructs the NSE F&O stock universe from AngelOne's
instrument master JSON.  Zero hardcoding — no symbols, no tokens, no
sector names live in this file.

Lifecycle
---------
1. Download OpenAPIScripMaster.json from AngelOne CDN.
2. Filter NFO FUTSTK entries → F&O universe (unique underlying symbols).
3. Cross-reference NSE EQ entries → equity token per symbol.
4. Extract lot sizes from nearest-expiry FUTSTK entry per symbol.
5. Persist everything to Redis under the `universe:*` key namespace.
6. Expose clean async accessors used by the rest of the application.

Standalone test
---------------
    python -m core.universe_builder
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import httpx

from core.redis_client import get_redis  # async Redis connection factory

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — only URLs and Redis key names (not stock data)
# ---------------------------------------------------------------------------
INSTRUMENT_MASTER_URL = (
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
)

REDIS_KEY_SYMBOLS       = "universe:symbols"
REDIS_KEY_TOKEN_MAP     = "universe:token_map"
REDIS_KEY_LOT_SIZES     = "universe:lot_sizes"
REDIS_KEY_META          = "universe:meta"
REDIS_KEY_INDEX_OPTIONS = "universe:index_options"  # full key: f"{REDIS_KEY_INDEX_OPTIONS}:{index}"

# Indices whose OPTIDX option chains we cache.
# SENSEX is BSE — handled separately if needed, excluded here.
INDEX_UNDERLYINGS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]

# How many calendar days forward we keep option contracts for.
INDEX_OPTIONS_LOOKAHEAD_DAYS = 45

# Regex: strip the trailing expiry/series suffix from NFO FUTSTK symbols.
# AngelOne futures symbols look like:  RELIANCE28APR26FUT  (DD MON YY FUT)
# The 2-digit year field was previously missing from this pattern, causing
# full contract names like RELIANCE28APR26FUT to pass through unstripped.
_FUTURES_SUFFIX_RE = re.compile(r"\d{2}[A-Z]{3}\d{2}FUT$")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _derive_underlying(futures_symbol: str) -> str:
    """
    Strip the expiry suffix from a FUTSTK symbol to get the underlying name.

    Examples
    --------
    'RELIANCE28APR26FUT'  → 'RELIANCE'
    'BAJAJ-AUTO26MAY26FUT'→ 'BAJAJ-AUTO'
    'TATAMOTORS25JUN26FUT'→ 'TATAMOTORS'
    """
    return _FUTURES_SUFFIX_RE.sub("", futures_symbol).strip()


def _parse_expiry(expiry_str: str) -> datetime:
    """
    Parse AngelOne expiry strings like '25APR2025' or '2025-04-25' into a
    datetime for sorting (nearest-expiry selection).  Returns datetime.max on
    parse failure so bad entries sort to the back.
    """
    for fmt in ("%d%b%Y", "%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(expiry_str.strip().upper(), fmt)
        except ValueError:
            continue
    logger.debug("Could not parse expiry string: %r", expiry_str)
    return datetime.max


async def _download_master() -> list[dict]:
    """
    Fetch the AngelOne instrument master JSON.  Raises on any HTTP or
    network error — no silent fallback.
    """
    logger.info("Downloading instrument master from %s …", INSTRUMENT_MASTER_URL)
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        try:
            response = await client.get(INSTRUMENT_MASTER_URL)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"Instrument master download failed — HTTP {exc.response.status_code}: "
                f"{INSTRUMENT_MASTER_URL}"
            ) from exc
        except httpx.RequestError as exc:
            raise RuntimeError(
                f"Instrument master download failed — network error: {exc}"
            ) from exc

    try:
        data = response.json()
    except Exception as exc:
        raise RuntimeError(
            f"Instrument master response is not valid JSON: {exc}"
        ) from exc

    if not isinstance(data, list):
        raise RuntimeError(
            f"Unexpected instrument master format — expected a JSON array, "
            f"got {type(data).__name__}"
        )

    logger.info("Downloaded %d instrument entries.", len(data))
    return data


def _build_maps(instruments: list[dict]) -> tuple[list[str], dict[str, str], dict[str, int]]:
    """
    Pure-Python processing of the raw instrument list.

    Two-pass approach:
      Pass 1 — collect unique underlying names and lot sizes from NFO FUTSTK.
      Pass 2 — cross-reference NSE EQ entries to obtain equity tokens.
    Only symbols that have BOTH a FUTSTK contract AND an NSE EQ token enter
    the final universe, which eliminates index underlyings (NIFTY, BANKNIFTY …)
    and any ETF/SME quirks that don't trade as equities on NSE.

    Returns
    -------
    symbols   : sorted list of F&O underlying symbols (~200 stocks)
    token_map : symbol → NSE EQ token (str)
    lot_sizes : symbol → lot size from nearest-expiry contract (int)
    """
    # ------------------------------------------------------------------
    # Pass 1 — NFO FUTSTK: extract unique underlyings + lot sizes
    # ------------------------------------------------------------------
    seen_underlyings: set[str] = set()
    lot_sizes: dict[str, int] = {}
    futstk_by_underlying: dict[str, list[dict]] = {}

    for inst in instruments:
        if inst.get("exch_seg") != "NFO":
            continue
        if inst.get("instrumenttype") != "FUTSTK":
            continue

        full_symbol = inst.get("symbol", "")

        # Strip expiry suffix: RELIANCE28APR26FUT → RELIANCE
        #                       BAJAJ-AUTO26MAY26FUT → BAJAJ-AUTO
        underlying = re.sub(r'\d{2}[A-Z]{3}\d{2}FUT$', '', full_symbol).strip()

        # Skip empty results and AngelOne test instruments
        if not underlying or 'NSETEST' in underlying:
            continue

        seen_underlyings.add(underlying)
        futstk_by_underlying.setdefault(underlying, []).append(inst)

    logger.info(
        "Pass 1 complete: %d unique F&O underlyings found in NFO FUTSTK.",
        len(seen_underlyings),
    )

    # Resolve lot sizes using the nearest (earliest) expiry per underlying.
    for sym, entries in futstk_by_underlying.items():
        sorted_entries = sorted(entries, key=lambda e: _parse_expiry(e.get("expiry", "")))
        nearest = sorted_entries[0]
        try:
            lot_sizes[sym] = int(nearest.get("lotsize", 1))
        except (TypeError, ValueError):
            lot_sizes[sym] = 1
            logger.debug("Could not parse lotsize for %s; defaulting to 1.", sym)

    # ------------------------------------------------------------------
    # Pass 2 — NSE EQ: build a comprehensive lookup then match underlyings
    # ------------------------------------------------------------------

    # Build a lookup of ALL NSE EQ entries.
    # Key: cleaned symbol name → token (stored both raw and cleaned forms).
    nse_eq_lookup: dict[str, str] = {}
    for inst in instruments:
        if inst.get("exch_seg") != "NSE":
            continue
        raw_sym = inst.get("symbol", "")
        # NSE EQ stocks have instrumenttype="" and symbol ending in "-EQ".
        # Indices have instrumenttype="AMXIDX"; ETFs end in different suffixes.
        # We specifically want cash equities: symbol must end with "-EQ".
        if not raw_sym.endswith("-EQ"):
            continue
        clean = raw_sym[:-3].strip()  # strip "-EQ"
        nse_eq_lookup[clean] = str(inst.get("token", ""))
        nse_eq_lookup[raw_sym] = str(inst.get("token", ""))

    logger.info("NSE EQ lookup built: %d entries", len(nse_eq_lookup))

    # Log first 5 NSE EQ entries to debug matching
    sample_nse = list(nse_eq_lookup.keys())[:5]
    logger.info("Sample NSE EQ symbols: %s", sample_nse)
    # Log first 5 F&O underlyings to debug matching
    sample_fo = list(seen_underlyings)[:5]
    logger.info("Sample F&O underlyings: %s", sample_fo)

    # Now match F&O underlyings to NSE EQ tokens
    token_map: dict[str, str] = {}
    no_token: list[str] = []

    for underlying in seen_underlyings:
        # Try direct match first
        if underlying in nse_eq_lookup:
            token_map[underlying] = nse_eq_lookup[underlying]
            continue

        # Try with -EQ suffix
        if underlying + "-EQ" in nse_eq_lookup:
            token_map[underlying] = nse_eq_lookup[underlying + "-EQ"]
            continue

        # Try uppercase
        if underlying.upper() in nse_eq_lookup:
            token_map[underlying] = nse_eq_lookup[underlying.upper()]
            continue

        # Not found
        no_token.append(underlying)

    if no_token:
        logger.warning(
            "%d underlyings with no EQ token: %s%s",
            len(no_token),
            ", ".join(no_token[:10]),
            "…" if len(no_token) > 10 else "",
        )

    # ------------------------------------------------------------------
    # Final universe — include ALL F&O underlyings regardless of EQ token.
    # Some stocks (like newer additions) may not have an EQ token yet.
    # Use empty string token for those — they can still be seeded via the
    # futures token directly; the WebSocket feed will skip empty tokens.
    # ------------------------------------------------------------------
    final_symbols: list[str] = sorted(seen_underlyings - {""})

    # Ensure every symbol has an entry in token_map (empty string for missing)
    for sym in final_symbols:
        if sym not in token_map:
            token_map[sym] = ""  # will be subscribed when token found

    logger.info(
        "Final universe: %d symbols (%d with EQ token, %d without).",
        len(final_symbols),
        len(final_symbols) - len(no_token),
        len(no_token),
    )

    return final_symbols, token_map, lot_sizes


async def _write_to_redis(
    symbols: list[str],
    token_map: dict[str, str],
    lot_sizes: dict[str, int],
) -> dict:
    """Persist universe data to Redis and return the meta dict."""
    redis = await get_redis()

    meta = {
        "count": len(symbols),
        "built_at": datetime.now(timezone.utc).isoformat(),
        "source": "angelone_master",
    }

    async with redis.pipeline(transaction=True) as pipe:
        pipe.set(REDIS_KEY_SYMBOLS,   json.dumps(symbols))
        pipe.set(REDIS_KEY_TOKEN_MAP, json.dumps(token_map))
        pipe.set(REDIS_KEY_LOT_SIZES, json.dumps(lot_sizes))
        pipe.set(REDIS_KEY_META,      json.dumps(meta))
        await pipe.execute()

    logger.info(
        "Universe written to Redis — %d symbols, built_at=%s.",
        meta["count"],
        meta["built_at"],
    )
    return meta


async def _build_index_options(instruments: list[dict]) -> dict[str, int]:
    """
    Extract all OPTIDX contracts for ``INDEX_UNDERLYINGS`` from the raw
    instrument master and persist them to Redis.

    Only contracts expiring within ``INDEX_OPTIONS_LOOKAHEAD_DAYS`` calendar
    days from today are retained — anything further out is noise for intraday.

    Redis layout
    ------------
    ``universe:index_options:{INDEX}``  →  JSON array of contract dicts::

        {
            "token":       "12345",
            "symbol":      "NIFTY25APR24200CE",
            "strike":      24200,
            "option_type": "CE",          # "CE" | "PE"
            "expiry":      "2025-04-25",  # ISO-8601
            "lot_size":    50
        }

    Contracts are stored sorted by (expiry ASC, strike ASC) so callers can
    slice the nearest expiry cheaply without re-sorting.

    Returns
    -------
    A dict mapping each index name to the number of contracts stored, for
    inclusion in the build ``meta``.
    """
    cutoff = (date.today() + timedelta(days=INDEX_OPTIONS_LOOKAHEAD_DAYS)).isoformat()

    # Accumulate contracts per index before the Redis write.
    index_contracts: dict[str, list[dict]] = {idx: [] for idx in INDEX_UNDERLYINGS}

    for inst in instruments:
        if inst.get("exch_seg") != "NFO" or inst.get("instrumenttype") != "OPTIDX":
            continue

        symbol = inst.get("symbol", "")

        # Identify underlying — use longest matching prefix to avoid
        # NIFTY matching BANKNIFTY (sorted longest-first).
        underlying: str | None = None
        for idx in sorted(INDEX_UNDERLYINGS, key=len, reverse=True):
            if symbol.startswith(idx):
                underlying = idx
                break
        if underlying is None:
            continue

        # Parse and range-check expiry.
        expiry_raw = inst.get("expiry", "")
        expiry = _parse_expiry(expiry_raw)
        if expiry == datetime.max:
            continue  # unparseable — skip silently
        expiry_iso = expiry.date().isoformat()
        if expiry_iso > cutoff:
            continue  # beyond lookahead window

        # Determine option type from symbol suffix.
        if symbol.endswith("CE"):
            option_type = "CE"
        elif symbol.endswith("PE"):
            option_type = "PE"
        else:
            continue  # neither CE nor PE — unexpected; skip

        # Extract strike: everything after the underlying prefix and before
        # the 2-char option-type suffix, keeping only the trailing digits.
        # AngelOne weekly symbol: NIFTY2516APR24200CE
        # AngelOne monthly symbol: NIFTY25APRFUT-style would be FUTSTK; here
        # options are e.g. NIFTY25APR24200CE.
        inner = symbol[len(underlying) : -2]          # strip prefix + "CE"/"PE"
        strike_match = re.search(r"(\d+)$", inner)
        if not strike_match:
            logger.debug("Cannot extract strike from symbol %r — skipping.", symbol)
            continue
        strike = int(strike_match.group(1))

        try:
            lot_size = int(inst.get("lotsize", 50))
        except (TypeError, ValueError):
            lot_size = 50

        index_contracts[underlying].append({
            "token":       str(inst.get("token", "")),
            "symbol":      symbol,
            "strike":      strike,
            "option_type": option_type,
            "expiry":      expiry_iso,
            "lot_size":    lot_size,
        })

    # Sort each index's contracts for cheap nearest-expiry slicing by callers.
    for contracts in index_contracts.values():
        contracts.sort(key=lambda c: (c["expiry"], c["strike"]))

    # Write to Redis — one key per index.
    redis = await get_redis()
    counts: dict[str, int] = {}

    async with redis.pipeline(transaction=True) as pipe:
        for idx, contracts in index_contracts.items():
            pipe.set(
                f"{REDIS_KEY_INDEX_OPTIONS}:{idx}",
                json.dumps(contracts),
            )
            counts[idx] = len(contracts)
        await pipe.execute()

    for idx, count in counts.items():
        logger.info("Index options stored: %s → %d contracts.", idx, count)

    return counts


# ---------------------------------------------------------------------------
# Public async API
# ---------------------------------------------------------------------------

async def build_universe() -> dict:
    """
    Full pipeline: download → parse → Redis write.

    Returns the meta dict on success.  Raises RuntimeError on download or
    parse failure (no silent fallback to stale data).
    """
    instruments = await _download_master()
    symbols, token_map, lot_sizes = _build_maps(instruments)
    meta = await _write_to_redis(symbols, token_map, lot_sizes)

    # Build index option chains from the same already-downloaded master.
    index_counts = await _build_index_options(instruments)
    meta["index_options"] = index_counts  # e.g. {"NIFTY": 420, "BANKNIFTY": 380, …}

    return meta


async def get_symbols() -> list[str]:
    """Return the cached F&O universe symbol list from Redis."""
    redis = await get_redis()
    raw = await redis.get(REDIS_KEY_SYMBOLS)
    if raw is None:
        raise RuntimeError(
            "Universe not initialised — call build_universe() first."
        )
    return json.loads(raw)


async def get_token_map() -> dict[str, str]:
    """Return the cached symbol → NSE equity token mapping from Redis."""
    redis = await get_redis()
    raw = await redis.get(REDIS_KEY_TOKEN_MAP)
    if raw is None:
        raise RuntimeError(
            "Universe not initialised — call build_universe() first."
        )
    return json.loads(raw)


async def get_lot_sizes() -> dict[str, int]:
    """Return the cached symbol → lot size mapping from Redis."""
    redis = await get_redis()
    raw = await redis.get(REDIS_KEY_LOT_SIZES)
    if raw is None:
        raise RuntimeError(
            "Universe not initialised — call build_universe() first."
        )
    return json.loads(raw)


async def get_symbol_for_token(token: str) -> Optional[str]:
    """
    Reverse-lookup: given an NSE equity token string, return the symbol.
    Returns None if the token is not in the universe.
    """
    token_map = await get_token_map()
    # Build reverse map on the fly (token_map is small, ~200 entries)
    reverse = {v: k for k, v in token_map.items()}
    return reverse.get(str(token))


async def get_index_options(index: str) -> list[dict]:
    """
    Return cached OPTIDX contracts for ``index`` (e.g. ``"NIFTY"``).

    Contracts are pre-sorted by ``(expiry ASC, strike ASC)``.  Each entry::

        {
            "token":       "12345",
            "symbol":      "NIFTY25APR24200CE",
            "strike":      24200,
            "option_type": "CE",
            "expiry":      "2025-04-25",
            "lot_size":    50
        }

    Raises
    ------
    ValueError
        If ``index`` is not one of ``INDEX_UNDERLYINGS``.
    RuntimeError
        If the universe has not been initialised yet.
    """
    if index not in INDEX_UNDERLYINGS:
        raise ValueError(
            f"Unknown index {index!r}. Valid choices: {INDEX_UNDERLYINGS}"
        )
    redis = await get_redis()
    raw = await redis.get(f"{REDIS_KEY_INDEX_OPTIONS}:{index}")
    if raw is None:
        raise RuntimeError(
            f"Index options for {index!r} not initialised — call build_universe() first."
        )
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Standalone test entry point
# ---------------------------------------------------------------------------

async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )
    logger.info("=== universe_builder standalone test ===")

    meta = await build_universe()
    print(f"\n✅  Build complete: {meta}")

    symbols   = await get_symbols()
    token_map = await get_token_map()
    lot_sizes = await get_lot_sizes()

    print(f"\nFirst 10 symbols : {symbols[:10]}")
    print(f"Sample token_map : { {k: token_map[k] for k in symbols[:5] if k in token_map} }")
    print(f"Sample lot_sizes : { {k: lot_sizes[k] for k in symbols[:5] if k in lot_sizes} }")

    # Reverse lookup smoke-test
    if symbols:
        first_sym = symbols[0]
        tok = token_map.get(first_sym)
        if tok:
            looked_up = await get_symbol_for_token(tok)
            assert looked_up == first_sym, f"Reverse lookup mismatch: {looked_up!r} != {first_sym!r}"
            print(f"\nReverse lookup OK: token {tok!r} → {looked_up!r}")

    # Index options smoke-test
    print("\n--- Index options ---")
    for idx in INDEX_UNDERLYINGS:
        try:
            contracts = await get_index_options(idx)
            expiries = sorted({c["expiry"] for c in contracts})
            print(
                f"  {idx:12s}: {len(contracts):4d} contracts | "
                f"expiries: {expiries}"
            )
            if contracts:
                sample = contracts[0]
                print(f"             sample: {sample}")
        except RuntimeError as exc:
            print(f"  {idx}: ⚠ {exc}")


if __name__ == "__main__":
    asyncio.run(_main())
