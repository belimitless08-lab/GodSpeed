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

# AMXIDX spot-index lookup hints.
# Match by exact ``name`` only to avoid false positives.
INDEX_SPOT_SEARCH = [
    {"symbol": "NIFTY",      "exch_seg": "NSE", "name": "NIFTY"},
    {"symbol": "BANKNIFTY",  "exch_seg": "NSE", "name": "BANKNIFTY"},
    {"symbol": "FINNIFTY",   "exch_seg": "NSE", "name": "FINNIFTY"},
    {"symbol": "MIDCPNIFTY", "exch_seg": "NSE", "name": "MIDCPNIFTY"},
    {"symbol": "SENSEX",     "exch_seg": "BSE", "name": "SENSEX"},
]
KNOWN_WRONG_TOKENS = {"99926004"}  # NIFTY 500, not NIFTY 50

# Regex: strip the trailing expiry/series suffix from NFO FUTSTK symbols.
# AngelOne futures symbols look like:  RELIANCE28APR26FUT  (DD MON YY FUT)
# The 2-digit year field was previously missing from this pattern, causing
# full contract names like RELIANCE28APR26FUT to pass through unstripped.
_FUTURES_SUFFIX_RE = re.compile(r"\d{2}[A-Z]{3}\d{2}FUT$")

# ---------------------------------------------------------------------------
# NEW — Unified Options Universe constants
# ---------------------------------------------------------------------------


# Indices with options (superset of legacy INDEX_UNDERLYINGS — includes BSE)
INDEX_OPTION_SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"]

# How far ahead to include expiries.
# Indices: 45 days (weeklies + monthlies).
# Stocks:  90 days (monthly-only — need deeper window to guarantee ≥2 expiries).
LOOKAHEAD_INDEX_DAYS = 45
LOOKAHEAD_STOCK_DAYS = 90

# Exchange ↔ index symbol mapping. Defensive guard against malformed instrument
# master entries that would otherwise silently pollute the wrong universe hash.
# NSE indices trade on NFO; BSE indices trade on BFO.
_EXPECTED_INDEX_EXCHANGE: dict[str, str] = {
    "NIFTY":       "NFO",
    "BANKNIFTY":   "NFO",
    "FINNIFTY":    "NFO",
    "MIDCPNIFTY":  "NFO",
    "SENSEX":      "BFO",
    "BANKEX":      "BFO",
}

# All F&O stock options trade on NFO. This is enforced defensively below.
_EXPECTED_STOCK_EXCHANGE = "NFO"

# IST offset for expiry score calculation (UTC+5:30)
_IST_OFFSET = timezone(timedelta(hours=5, minutes=30))

# Pipeline batch size — stay well under Redis's command-count soft limits
_PIPELINE_BATCH = 1000


# ---------------------------------------------------------------------------
# Internal helpers (original)
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


def _extract_index_spot_tokens(instruments: list[dict]) -> dict[str, dict[str, str]]:
    """
    Extract spot-index AMXIDX tokens for configured symbols.

    Matching rules:
      * instrumenttype must be "AMXIDX"
      * exch_seg must match the configured exch_seg
      * exact ``name`` match against configured ``name``
      * first match wins per symbol
    """
    amxidx_entries = [
        inst for inst in instruments
        if str(inst.get("instrumenttype", "")).upper() == "AMXIDX"
    ]

    for inst in amxidx_entries:
        logger.info(
            "[universe] AMXIDX entries found: %s | %s | %s",
            str(inst.get("token", "")),
            str(inst.get("name", "")),
            str(inst.get("exch_seg", "")),
        )

    out: dict[str, dict[str, str]] = {}
    for cfg in INDEX_SPOT_SEARCH:
        symbol = str(cfg.get("symbol", "")).strip().upper()
        exch_seg = str(cfg.get("exch_seg", "")).strip().upper()
        exact_name = str(cfg.get("name", "")).strip()
        if not symbol or not exch_seg or not exact_name:
            continue

        for inst in amxidx_entries:
            if str(inst.get("exch_seg", "")).strip().upper() != exch_seg:
                continue
            name = str(inst.get("name", "")).strip()
            if name == exact_name:
                if str(inst.get("token", "")) in KNOWN_WRONG_TOKENS:
                    continue
                out[symbol] = {
                    "token": str(inst.get("token", "")),
                    "exch_seg": str(inst.get("exch_seg", "")).strip().upper(),
                }
                logger.info(
                    "[universe] Index spot token resolved: %s -> %s (name=%s)",
                    symbol,
                    out[symbol]["token"],
                    name,
                )
                break

        if symbol not in out:
            logger.warning(
                "[universe] Index spot token not found for %s (exch_seg=%s, name=%s)",
                symbol,
                exch_seg,
                cfg.get("name", ""),
            )

    return out


async def store_index_spot_tokens(tokens: dict) -> None:
    redis = await get_redis()

    # Step 1: delete ALL stale index keys unconditionally
    await redis.delete("index:tokens")

    stale_keys = []
    async for key in redis.scan_iter(match="index:meta:*"):
        stale_keys.append(key)
    async for key in redis.scan_iter(match="index:token_to_symbol:*"):
        stale_keys.append(key)
    if stale_keys:
        await redis.delete(*stale_keys)

    # Step 2: write fresh data
    for symbol, meta in tokens.items():
        token = meta["token"]
        exch = meta["exch_seg"]
        await redis.hset("index:tokens", symbol, token)
        await redis.hset(
            f"index:meta:{symbol}",
            mapping={"token": token, "exch_seg": exch}
        )
        await redis.set(
            f"index:token_to_symbol:{token}", symbol, ex=86400
        )
        logger.info(
            "[universe] Stored index spot token: %s -> %s (%s)",
            symbol, token, exch
        )

    logger.info(
        "[universe] store_index_spot_tokens complete: %d tokens written: %s",
        len(tokens), list(tokens.keys())
    )


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
        # Indices have instrumenttype="AMXIDX"; skip them.
        if not raw_sym.endswith("-EQ"):
            continue
        clean = raw_sym[:-3].strip()  # strip "-EQ" suffix
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

    # Add spot-index AMXIDX tokens so indices can be resolved by token users
    # of universe:token_map (e.g. NIFTY/BANKNIFTY/SENSEX).
    token_map.update(
        {symbol: meta["token"] for symbol, meta in _extract_index_spot_tokens(instruments).items()}
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


# ===========================================================================
# NEW — Unified Options Universe
# ===========================================================================

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _parse_angel_expiry(raw: str) -> date | None:
    """
    Normalise an AngelOne instrument-master expiry string to a ``datetime.date``.

    Known formats seen in the wild::

        "24APR2026"    ← most common for NFO options
        "24-APR-2026"
        "2026-04-24"
        "24/04/2026"

    Returns ``None`` on any unparseable input (caller should log and skip).
    Consolidates the same logic previously duplicated in order_manager.
    """
    if not raw:
        return None
    cleaned = raw.strip().upper()
    for fmt in ("%d%b%Y", "%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    logger.debug("_parse_angel_expiry: could not parse %r", raw)
    return None


def _derive_instrument_class(instrument_type: str) -> str | None:
    """
    Map AngelOne ``instrumenttype`` field to our internal classification.

    Returns
    -------
    "INDEX"  for OPTIDX
    "STOCK"  for OPTSTK
    None     for anything else (caller skips that contract)
    """
    mapping = {"OPTIDX": "INDEX", "OPTSTK": "STOCK"}
    return mapping.get(instrument_type)


def _match_symbol_to_name(name_field: str, candidates: list[str]) -> str | None:
    """
    Match the ``name`` field from the instrument master to one of our symbol
    candidates.

    Strategy
    --------
    * **Exact match first** — AngelOne's master stores the clean base name
      (e.g. ``"name": "BANKNIFTY"``), so exact equality works for both
      indices and stocks.
    * **Longest-prefix fallback** — only reached when the name field is NOT
      an exact member of candidates (shouldn't happen for well-maintained
      data, but is a safe guard against edge cases).  Sorted longest-first
      so that "BANKNIFTY" is matched before "NIFTY".

    Returns matched symbol string, or ``None`` if no candidate matches.
    """
    # Exact match (O(1) with set lookup)
    candidate_set = set(candidates)
    if name_field in candidate_set:
        return name_field

    # Longest-prefix fallback
    for candidate in sorted(candidates, key=len, reverse=True):
        if name_field.startswith(candidate):
            return candidate

    return None


def _contract_key(strike: int, option_type: str, expiry_iso: str) -> str:
    """
    Return the deterministic hash-field key used in ``universe:options:{sym}``.

    Format: ``"{strike}{CE|PE}:{expiry_YYYY-MM-DD}"``

    Example::

        _contract_key(24500, "CE", "2026-04-24") → "24500CE:2026-04-24"

    Args
    ----
    strike      : int — NEVER float (enforce conversion upstream)
    option_type : "CE" or "PE"
    expiry_iso  : YYYY-MM-DD string (already normalised)
    """
    return f"{int(strike)}{option_type}:{expiry_iso}"


def _expiry_score(expiry_date: date) -> float:
    """
    Convert an expiry date to a unix timestamp at 15:30 IST.

    Using 15:30 IST (market close) means:
    * Same-day expiries score correctly relative to real-time ``now``.
    * ``ZRANGEBYSCORE ... {now_ts} +inf`` filters past expiries accurately
      throughout the trading day.
    """
    dt_ist = datetime(
        expiry_date.year, expiry_date.month, expiry_date.day,
        15, 30, 0, tzinfo=_IST_OFFSET
    )
    return dt_ist.timestamp()


async def _write_symbol_to_redis(
    redis,
    symbol: str,
    contracts: list[dict],
) -> int:
    """
    Write all three key types for one symbol atomically via pipeline.

    Deletes existing keys BEFORE writing to prevent stale contracts
    accumulating as weekly expiries roll off.

    Pipeline sequence
    -----------------
    1. DEL  universe:options:{sym}
    2. DEL  universe:options:{sym}:expiries
    3. DEL  universe:options:{sym}:strikes:{expiry}  (one per unique expiry)
    4. HSET universe:options:{sym}           (bulk)
    5. ZADD universe:options:{sym}:expiries  (bulk)
    6. ZADD universe:options:{sym}:strikes:{expiry}  (bulk, one per expiry)
    7. SADD universe:options:symbols {sym}

    Batches pipeline flushes every ``_PIPELINE_BATCH`` commands to avoid
    memory spikes with large option chains (~32 k commands for some symbols).

    Parameters
    ----------
    redis     : async Redis client
    symbol    : e.g. "NIFTY" or "RELIANCE"
    contracts : list of pre-parsed, pre-filtered contract dicts containing
                keys: token, lot_size, exchange, instrument_class,
                      tradingsymbol, strike (int), option_type, expiry_iso

    Returns
    -------
    Number of contracts written to the hash.
    """
    if not contracts:
        return 0

    hash_key     = f"universe:options:{symbol}"
    expiries_key = f"universe:options:{symbol}:expiries"

    # Collect unique expiries from the incoming batch.
    unique_expiries: set[str] = {c["expiry_iso"] for c in contracts}

    # ------------------------------------------------------------------
    # Phase 1 — DELETE stale keys
    #
    # We must delete ALL existing strike keys for this symbol, not just
    # those appearing in the incoming batch. Weekly expiries roll off over
    # time, and if we only DEL incoming expiries, rolled-off weeks' strike
    # keys accumulate in Redis forever as zombie data.
    #
    # SCAN is used instead of KEYS to avoid blocking Redis on a large
    # keyspace. Match pattern is scoped tightly to this symbol.
    # ------------------------------------------------------------------
    existing_strike_keys: list[str] = []
    scan_pattern = f"universe:options:{symbol}:strikes:*"
    async for key in redis.scan_iter(match=scan_pattern, count=100):
        # redis-py may return bytes or str depending on decode_responses setting.
        existing_strike_keys.append(
            key.decode() if isinstance(key, bytes) else key
        )

    # Union of existing keys (for cleanup) and incoming keys (in case new
    # expiries are being written that weren't there before — SCAN wouldn't
    # find those, but DEL on a nonexistent key is a no-op, so this is safe).
    incoming_strike_keys = [
        f"universe:options:{symbol}:strikes:{exp}" for exp in unique_expiries
    ]
    all_strike_keys_to_delete = set(existing_strike_keys) | set(incoming_strike_keys)

    async with redis.pipeline(transaction=False) as pipe:
        pipe.delete(hash_key)
        pipe.delete(expiries_key)
        for sk in all_strike_keys_to_delete:
            pipe.delete(sk)
        await pipe.execute()

    if existing_strike_keys:
        rolled_off = set(existing_strike_keys) - set(incoming_strike_keys)
        if rolled_off:
            logger.debug(
                "[universe] %s: cleaning up %d rolled-off expiry keys: %s",
                symbol, len(rolled_off), sorted(rolled_off)
            )

    # ------------------------------------------------------------------
    # Phase 2 — WRITE new data in batches
    # ------------------------------------------------------------------
    # Prepare bulk structures
    hash_fields: dict[str, str] = {}
    expiry_scores: dict[str, float] = {}
    # strikes_per_expiry: expiry_iso → {strike_str: score}
    strikes_per_expiry: dict[str, dict[str, float]] = {}

    for c in contracts:
        exp = c["expiry_iso"]
        strike = int(c["strike"])
        opt_type = c["option_type"]

        field = _contract_key(strike, opt_type, exp)
        value = json.dumps({
            "token":            c["token"],
            "lot_size":         c["lot_size"],
            "exchange":         c["exchange"],
            "instrument_class": c["instrument_class"],
            "tradingsymbol":    c["tradingsymbol"],
        })
        hash_fields[field] = value

        expiry_date = date.fromisoformat(exp)
        expiry_scores[exp] = _expiry_score(expiry_date)

        if exp not in strikes_per_expiry:
            strikes_per_expiry[exp] = {}
        strikes_per_expiry[exp][str(strike)] = float(strike)

    # Write hash fields in batches
    hash_items = list(hash_fields.items())
    for batch_start in range(0, len(hash_items), _PIPELINE_BATCH):
        batch = hash_items[batch_start : batch_start + _PIPELINE_BATCH]
        async with redis.pipeline(transaction=False) as pipe:
            pipe.hset(hash_key, mapping=dict(batch))
            await pipe.execute()

    # Write expiries sorted set
    async with redis.pipeline(transaction=False) as pipe:
        # zadd mapping: {member: score}
        pipe.zadd(expiries_key, {exp: score for exp, score in expiry_scores.items()})
        await pipe.execute()

    # Write per-expiry strike sorted sets in batches
    for exp, strike_map in strikes_per_expiry.items():
        sk = f"universe:options:{symbol}:strikes:{exp}"
        items = list(strike_map.items())
        for batch_start in range(0, len(items), _PIPELINE_BATCH):
            batch = items[batch_start : batch_start + _PIPELINE_BATCH]
            async with redis.pipeline(transaction=False) as pipe:
                pipe.zadd(sk, dict(batch))
                await pipe.execute()

    # Register this symbol in the master set
    async with redis.pipeline(transaction=False) as pipe:
        pipe.sadd("universe:options:symbols", symbol)
        await pipe.execute()

    return len(contracts)


# ---------------------------------------------------------------------------
# Main new function
# ---------------------------------------------------------------------------

async def build_options_universe(
    instrument_master: list[dict],
    fno_stocks: list[str],
    *,
    include_indices: bool = True,
    include_stocks: bool = True,
    index_lookahead_days: int = LOOKAHEAD_INDEX_DAYS,
    stock_lookahead_days: int = LOOKAHEAD_STOCK_DAYS,
) -> dict:
    """
    Populate unified options universe keys for all configured symbols.

    Reads
    -----
    * ``instrument_master``     — the OpenAPIScripMaster.json loaded list
    * ``options_config.FNO_STOCKS``  — list of stock symbols with F&O
    * ``INDEX_OPTION_SYMBOLS``  — list of index symbols with F&O

    Writes (atomically per-symbol via pipeline)
    -------------------------------------------
    * ``universe:options:{sym}``                → HASH of contracts
    * ``universe:options:{sym}:expiries``       → ZSET of expiry dates
    * ``universe:options:{sym}:strikes:{exp}``  → ZSET of strikes per expiry
    * ``universe:options:symbols``              → SET of all symbols populated

    Matching logic
    --------------
    * Indices (instrumenttype == "OPTIDX"): match on ``name`` field using
      _match_symbol_to_name() which does exact-match-first then
      longest-prefix. Processing order within INDEX_OPTION_SYMBOLS is
      longest-name-first (MIDCPNIFTY → BANKNIFTY → FINNIFTY → NIFTY …)
      as a belt-and-suspenders guard.
    * Stocks  (instrumenttype == "OPTSTK"): exact match on ``name`` field
      against FNO_STOCKS list.

    Strike normalisation
    --------------------
    AngelOne stores strikes as floats in paise (e.g. 2450000.0 = ₹24500).
    Convert: ``int(float(raw_strike) / 100)``.

    Returns
    -------
    dict with keys:
      * ``indices_written``        : int — count of index symbols populated
      * ``stocks_written``         : int — count of stock symbols populated
      * ``total_contracts``        : int — total contracts across all symbols
      * ``skipped_expired``        : int — contracts past today
      * ``skipped_out_of_window``  : int — contracts beyond lookahead
      * ``errors``                 : list[str] — non-fatal issues logged
    """
    redis = await get_redis()
    today = date.today()

    index_cutoff = today + timedelta(days=index_lookahead_days)
    stock_cutoff = today + timedelta(days=stock_lookahead_days)

    # Determine which symbol sets are active this run
    active_indices: list[str] = list(INDEX_OPTION_SYMBOLS) if include_indices else []
    active_stocks:  list[str] = list(fno_stocks)           if include_stocks  else []

    # Sort indices longest-name-first as a prefix-match guard
    active_indices_sorted = sorted(active_indices, key=len, reverse=True)

    # Build per-symbol contract buckets
    # key: symbol string → list of pre-parsed contract dicts
    symbol_contracts: dict[str, list[dict]] = {}
    for sym in active_indices + active_stocks:
        symbol_contracts[sym] = []

    # Counters
    skipped_expired        = 0
    skipped_out_of_window  = 0
    errors: list[str]      = []

    # ------------------------------------------------------------------
    # Single pass through instrument master
    # ------------------------------------------------------------------
    for inst in instrument_master:
        inst_type = inst.get("instrumenttype", "")
        instrument_class = _derive_instrument_class(inst_type)
        if instrument_class is None:
            continue  # not an options contract

        is_index = instrument_class == "INDEX"
        is_stock = instrument_class == "STOCK"

        if is_index and not include_indices:
            continue
        if is_stock and not include_stocks:
            continue

        # Exchange filter
        exch = inst.get("exch_seg", "")
        if exch not in ("NFO", "BFO"):
            continue

        # Resolve symbol via name field (most reliable for both indices and stocks)
        name_field = inst.get("name", "").strip().upper()
        if not name_field:
            continue

        if is_index:
            matched_sym = _match_symbol_to_name(name_field, active_indices_sorted)
        else:
            # Stocks: exact match only
            matched_sym = name_field if name_field in set(active_stocks) else None

        if matched_sym is None:
            continue

        # --- Exchange sanity guard ---
        # Prevents silent pollution of the wrong symbol's universe hash if
        # AngelOne's instrument master contains a mislabeled entry.
        if is_index:
            expected = _EXPECTED_INDEX_EXCHANGE.get(matched_sym)
            if expected is None or exch != expected:
                # Log once per unexpected combination to surface data-quality issues
                # without spamming (e.g. an OPTIDX with name=NIFTY on BFO).
                errors.append(
                    f"Exchange mismatch for {matched_sym}: expected {expected}, "
                    f"got {exch} (token {inst.get('token')}, symbol {inst.get('symbol')!r})"
                )
                continue
        else:  # is_stock
            if exch != _EXPECTED_STOCK_EXCHANGE:
                errors.append(
                    f"Exchange mismatch for stock {matched_sym}: expected "
                    f"{_EXPECTED_STOCK_EXCHANGE}, got {exch} "
                    f"(token {inst.get('token')}, symbol {inst.get('symbol')!r})"
                )
                continue

        # --- Expiry ---
        expiry_raw = inst.get("expiry", "")
        expiry_date = _parse_angel_expiry(expiry_raw)
        if expiry_date is None:
            errors.append(f"Unparseable expiry {expiry_raw!r} for token {inst.get('token')}")
            continue

        if expiry_date < today:
            skipped_expired += 1
            continue

        cutoff = index_cutoff if is_index else stock_cutoff
        if expiry_date > cutoff:
            skipped_out_of_window += 1
            continue

        expiry_iso = expiry_date.isoformat()

        # --- Option type ---
        tradingsymbol = inst.get("symbol", "")
        if tradingsymbol.endswith("CE"):
            option_type = "CE"
        elif tradingsymbol.endswith("PE"):
            option_type = "PE"
        else:
            continue

        # --- Strike (paise → rupees, int) ---
        raw_strike = inst.get("strike", 0)
        try:
            strike = int(float(raw_strike) / 100)
        except (TypeError, ValueError):
            errors.append(
                f"Unparseable strike {raw_strike!r} for {tradingsymbol!r} — skipping"
            )
            continue
        if strike <= 0:
            continue

        # --- Lot size (from master — authoritative) ---
        try:
            lot_size = int(inst.get("lotsize", 1))
        except (TypeError, ValueError):
            lot_size = 1

        symbol_contracts[matched_sym].append({
            "token":            str(inst.get("token", "")),
            "lot_size":         lot_size,
            "exchange":         exch,
            "instrument_class": instrument_class,
            "tradingsymbol":    tradingsymbol,
            "strike":           strike,
            "option_type":      option_type,
            "expiry_iso":       expiry_iso,
        })

    # ------------------------------------------------------------------
    # Write to Redis — one symbol at a time
    # ------------------------------------------------------------------
    indices_written = 0
    stocks_written  = 0
    total_contracts = 0

    for sym in active_indices:
        contracts = symbol_contracts.get(sym, [])
        count = await _write_symbol_to_redis(redis, sym, contracts)
        total_contracts += count
        if count > 0:
            indices_written += 1
        logger.info(
            "[universe] Index %s: %d contracts written (%d expiries)",
            sym, count,
            len({c["expiry_iso"] for c in contracts}),
        )

    for sym in active_stocks:
        contracts = symbol_contracts.get(sym, [])
        count = await _write_symbol_to_redis(redis, sym, contracts)
        total_contracts += count
        if count > 0:
            stocks_written += 1
        logger.debug(
            "[universe] Stock %s: %d contracts written (%d expiries)",
            sym, count,
            len({c["expiry_iso"] for c in contracts}),
        )

    stats = {
        "indices_written":       indices_written,
        "stocks_written":        stocks_written,
        "total_contracts":       total_contracts,
        "skipped_expired":       skipped_expired,
        "skipped_out_of_window": skipped_out_of_window,
        "errors":                errors,
    }

    if errors:
        logger.warning("[universe] Non-fatal errors during build: %d", len(errors))
        for err in errors[:10]:  # cap log noise
            logger.warning("[universe]   %s", err)

    return stats


# ---------------------------------------------------------------------------
# Public async API (original)
# ---------------------------------------------------------------------------

async def build_universe() -> dict:
    """
    Full pipeline: download → parse → Redis write.

    Returns the meta dict on success.  Raises RuntimeError on download or
    parse failure (no silent fallback to stale data).
    """
    instruments = await _download_master()
    index_spot = _extract_index_spot_tokens(instruments)
    await store_index_spot_tokens(index_spot)
    symbols, token_map, lot_sizes = _build_maps(instruments)
    meta = await _write_to_redis(symbols, token_map, lot_sizes)

    # OLD (back-compat): write universe:index_options:{idx} for legacy readers.
    # Will be removed in Session 3 once all readers migrate to new keys.
    index_counts = await _build_index_options(instruments)
    meta["index_options"] = index_counts  # e.g. {"NIFTY": 420, "BANKNIFTY": 380, …}

    # NEW: unified options universe (indices + stocks).
    # Pass the F&O stock list built in _build_maps() so we don't need
    # to hardcode or import it — the list already lives in Redis under
    # universe:symbols after _build_maps() runs.
    options_stats = await build_options_universe(instruments, fno_stocks=symbols)
    meta["options_universe"] = options_stats
    logger.info("[universe] Options universe built: %s", options_stats)

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


async def load_index_symbols() -> set:
    """
    Returns set of index symbol strings from Redis index:tokens hash.
    e.g. {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"}
    Returns empty set if key missing, never raises.
    """
    try:
        redis = await get_redis()
        tokens = await redis.hgetall("index:tokens")
        return set(tokens.keys()) if tokens else set()
    except Exception as e:
        logger.warning(f"[universe] load_index_symbols failed: {e}")
        return set()


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

    # Index options smoke-test (legacy keys)
    print("\n--- Index options (legacy keys) ---")
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

    # New unified universe smoke-test
    print("\n--- Unified options universe (new keys) ---")
    redis = await get_redis()
    all_syms = await redis.smembers("universe:options:symbols")
    print(f"  Total symbols in universe:options:symbols: {len(all_syms)}")

    for sym in ["NIFTY", "BANKNIFTY", "RELIANCE", "TCS"]:
        h_count  = await redis.hlen(f"universe:options:{sym}")
        expiries = await redis.zrange(f"universe:options:{sym}:expiries", 0, -1)
        print(f"  {sym:12s}: {h_count:5d} contracts | expiries: {expiries}")
        if expiries:
            nearest_exp = expiries[0]
            strikes = await redis.zrange(
                f"universe:options:{sym}:strikes:{nearest_exp}", 0, 4, withscores=True
            )
            print(f"               first 5 strikes for {nearest_exp}: {strikes}")


if __name__ == "__main__":
    asyncio.run(_main())
