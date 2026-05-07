"""
scripts/morning_seeder.py
=========================
Market Pulse Pro v2 — Morning Seeder
Runs every morning at 08:30 IST (before market open at 09:15).

Two phases:
  Phase A — Equity snapshots  (1m candles → indicators → Redis snapshot:{symbol})
  Phase B — Options baseline   (daily OI/volume → Redis options:prev:{symbol})

Run:
    python -m scripts.morning_seeder
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone

import httpx
import numpy as np
import pandas as pd
import pyotp

from core.config import cfg, validate
from core.instrument_registry import resolve_index_tokens, store_index_tokens

import os as _os
if _os.environ.get("SEEDER_STANDALONE"):
    from core.redis_seeder_client import get_seeder_redis as get_redis
else:
    from core.redis_client import get_redis
from core.universe_builder import (
    build_universe,
    get_lot_sizes,
    load_index_symbols,
    get_symbols,
    get_token_map,
)
from execution.options_rest import publish_angel_jwt
from strategy_brain.ai_pipeline.global_indices_scraper import scrape_and_store as _scrape_global_indices, REDIS_TTL_SEED as _GLOBAL_TTL
from strategy_brain.ai_pipeline.ai_config import get_sector as _get_sector

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("morning_seeder")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ANGELONE_CANDLE_URL = (
    "https://apiconnect.angelbroking.com/rest/secure/angelbroking/historical/v1/getCandleData"
)
ANGELONE_LOGIN_URL = "https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword"

LOG_EVERY = 20

# Market-closed probe — NIFTY BEES NSE EQ token (stable large-cap liquid ETF).
# Used as a single candle fetch to confirm AngelOne has data for yesterday.
# If this returns zero candles the market was closed and we abort without
# touching any Redis key.
PROBE_TOKEN  = "1594"   # NIFTYBEES NSE EQ token in AngelOne instrument master
PROBE_SYMBOL = "NIFTYBEES"


def _snapshot_to_hash_mapping(snapshot: dict) -> dict[str, str]:
    """
    Flatten seeder snapshot payload into Redis HASH-compatible fields.

    NOTE:
    - Keeps nested blocks as JSON strings for debug/backward compatibility.
    - Also emits key top-level fields consumed by API/brain/order_manager.
    """
    prev_day = snapshot.get("prev_day", {}) if isinstance(snapshot.get("prev_day"), dict) else {}
    supertrend = snapshot.get("supertrend", {}) if isinstance(snapshot.get("supertrend"), dict) else {}

    mapping: dict[str, str] = {
        "ema9": str(snapshot.get("ema9", 0.0)),
        "ema16": str(snapshot.get("ema16", 0.0)),
        "ema200": str(snapshot.get("ema200", 0.0)),
        "atr14": str(snapshot.get("atr14", 0.0)),
        "avg_volume_5d": str(snapshot.get("avg_volume_5d", 0.0)),
        "rsi14": str(snapshot.get("rsi14", 50.0)),
        "rsi_avg_gain": str(snapshot.get("rsi_avg_gain", 0.0)),
        "rsi_avg_loss": str(snapshot.get("rsi_avg_loss", 0.0)),
        "choppiness14": "" if snapshot.get("choppiness14") is None else str(snapshot.get("choppiness14")),
        "choppiness_class": str(snapshot.get("choppiness_class", "NEUTRAL")),
        "lot_size": str(snapshot.get("lot_size", 1)),
        "token": str(snapshot.get("token", "")),
        "sector": str(snapshot.get("sector", "OTHER")),
        "seeded_at": str(snapshot.get("seeded_at", "")),
        # Commonly consumed by breadth / risk / price fallbacks
        "prev_open": str(prev_day.get("open", 0.0)),
        "prev_high": str(prev_day.get("high", 0.0)),
        "prev_low": str(prev_day.get("low", 0.0)),
        "prev_close": str(snapshot.get("prev_close", prev_day.get("close", 0.0))),
        "prev_volume": str(prev_day.get("volume", 0.0)),
        "last_close": str(prev_day.get("close", 0.0)),
        "ltp": str(snapshot.get("ltp", prev_day.get("close", 0.0))),
        # Supertrend fields consumed by signal engines
        "supertrend_dir": str(supertrend.get("direction", "BULL")),
        "supertrend_band": str(supertrend.get("band", 0.0)),
        # Keep nested payloads for diagnostics/backward compatibility
        "prev_day": json.dumps(prev_day),
        "supertrend": json.dumps(supertrend),
    }

    # Flatten pivot levels as top-level fields — consumed by signal_engines,
    # api_server /api/pivots endpoint, and frontend chart
    prev_day_raw = snapshot.get("prev_day", {})
    if isinstance(prev_day_raw, dict):
        classic   = prev_day_raw.get("classic", {})
        camarilla = prev_day_raw.get("camarilla", {})
    else:
        classic   = {}
        camarilla = {}

    if classic:
        mapping["pp"] = str(classic.get("pp", 0.0))
        mapping["r1"] = str(classic.get("r1", 0.0))
        mapping["r2"] = str(classic.get("r2", 0.0))
        mapping["r3"] = str(classic.get("r3", 0.0))
        mapping["s1"] = str(classic.get("s1", 0.0))
        mapping["s2"] = str(classic.get("s2", 0.0))
        mapping["s3"] = str(classic.get("s3", 0.0))

    if camarilla:
        mapping["cam_r1"] = str(camarilla.get("r1", 0.0))
        mapping["cam_r2"] = str(camarilla.get("r2", 0.0))
        mapping["cam_r3"] = str(camarilla.get("r3", 0.0))
        mapping["cam_r4"] = str(camarilla.get("r4", 0.0))
        mapping["cam_s1"] = str(camarilla.get("s1", 0.0))
        mapping["cam_s2"] = str(camarilla.get("s2", 0.0))
        mapping["cam_s3"] = str(camarilla.get("s3", 0.0))
        mapping["cam_s4"] = str(camarilla.get("s4", 0.0))

    return mapping


# ---------------------------------------------------------------------------
# AngelOne session
# ---------------------------------------------------------------------------

async def get_angel_session() -> dict:
    """
    Login to AngelOne SmartAPI using credentials from config.
    Returns dict with jwt, feed_token, api_key, client_code.
    """
    totp = pyotp.TOTP(cfg.ANGELONE_TOTP_SECRET).now()

    payload = {
        "clientcode": cfg.ANGELONE_CLIENT_ID,
        "password": cfg.ANGELONE_PASSWORD,
        "totp": totp,
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "00:00:00:00:00:00",
        "X-PrivateKey": cfg.ANGELONE_API_KEY,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(ANGELONE_LOGIN_URL, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()

    if data.get("status") is not True:
        raise RuntimeError(f"AngelOne login failed: {data.get('message')}")

    d = data["data"]
    await publish_angel_jwt(d["jwtToken"])
    return {
        "jwt": d["jwtToken"],
        "feed_token": d["feedToken"],
        "api_key": cfg.ANGELONE_API_KEY,
        "client_code": cfg.ANGELONE_CLIENT_ID,
    }


# ---------------------------------------------------------------------------
# Historical candle fetch
# ---------------------------------------------------------------------------

async def fetch_candles(
    session: dict,
    exchange: str,
    token: str,
    interval: str,
    from_dt: datetime,
    to_dt: datetime,
    http_client: httpx.AsyncClient,
) -> list[list]:
    """
    Fetch candles from AngelOne historical API.
    Returns list of [ts, o, h, l, c, v] (equity) or [ts, o, h, l, c, v, oi] (options daily).
    """
    headers = {
        "Authorization": f"Bearer {session['jwt']}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "00:00:00:00:00:00",
        "X-PrivateKey": session["api_key"],
    }

    body = {
        "exchange": exchange,
        "symboltoken": token,
        "interval": interval,
        "fromdate": from_dt.strftime("%Y-%m-%d %H:%M"),
        "todate": to_dt.strftime("%Y-%m-%d %H:%M"),
    }

    resp = await http_client.post(ANGELONE_CANDLE_URL, json=body, headers=headers)
    resp.raise_for_status()
    result = resp.json()

    if result.get("status") is not True:
        raise RuntimeError(f"Candle fetch failed for token {token}: {result.get('message')}")

    return result.get("data") or []


# ---------------------------------------------------------------------------
# Market-closed probe
# ---------------------------------------------------------------------------

async def probe_market_open(session: dict, from_dt: datetime, to_dt: datetime) -> bool:
    """
    Fetch 1m candles for NIFTYBEES (a highly liquid NSE ETF) to confirm
    AngelOne has trading data for yesterday.

    Returns True  → market traded yesterday, proceed with full seeder.
    Returns False → market was closed (holiday/weekend), abort without
                    touching any existing Redis key.

    Any HTTP/API error is treated conservatively as "closed" so we never
    accidentally wipe good Redis data due to a transient auth issue.
    """
    logger.info(
        "Market probe: fetching 1m candles for %s (token %s) …",
        PROBE_SYMBOL, PROBE_TOKEN,
    )
    try:
        async with httpx.AsyncClient(timeout=20.0) as http_client:
            candles = await fetch_candles(
                session, "NSE", PROBE_TOKEN, "ONE_MINUTE", from_dt, to_dt, http_client
            )
        if candles:
            logger.info(
                "Market probe OK — %d candles returned for %s. Proceeding with seeder.",
                len(candles), PROBE_SYMBOL,
            )
            return True
        else:
            logger.warning(
                "Market probe returned 0 candles for %s — market appears closed today.",
                PROBE_SYMBOL,
            )
            return False
    except Exception as exc:
        logger.warning(
            "Market probe failed for %s (%s) — treating as market closed to preserve Redis data.",
            PROBE_SYMBOL, exc,
        )
        return False


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _get_date_range() -> tuple[datetime, datetime]:
    """
    Returns (from_dt, to_dt) as per spec:
      from_dt = today - 7 calendar days at 09:15
      to_dt   = yesterday at 23:59
    AngelOne handles actual trading day filtering.
    """
    now = datetime.now(timezone.utc)
    # Convert to IST (UTC+5:30) for date logic, but pass naive datetimes to API
    ist_offset = timedelta(hours=5, minutes=30)
    ist_now = now + ist_offset

    today_ist = ist_now.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    from_dt = (today_ist - timedelta(days=7)).replace(hour=9, minute=15)
    to_dt   = (today_ist - timedelta(days=1)).replace(hour=23, minute=59)

    return from_dt, to_dt


def _min_index_lookback_from(from_dt: datetime, to_dt: datetime) -> datetime:
    """
    Ensure index 1m lookback covers at least ~2 trading days for EMA200 warmup.
    NSE trades ~375 minutes/day, so 2 days (~750 bars) is sufficient.
    """
    min_from_dt = (to_dt - timedelta(days=2)).replace(hour=9, minute=15)
    return min(from_dt, min_from_dt)


# ---------------------------------------------------------------------------
# Technical indicator calculations (NumPy vectorized)
# ---------------------------------------------------------------------------

def ema_vectorized(closes: np.ndarray, period: int) -> np.ndarray:
    k = 2 / (period + 1)
    ema = np.zeros_like(closes, dtype=float)
    ema[0] = closes[0]
    for i in range(1, len(closes)):
        ema[i] = closes[i] * k + ema[i - 1] * (1 - k)
    return ema


def compute_atr14(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> float:
    tr = np.maximum(
        highs[1:] - lows[1:],
        np.maximum(
            np.abs(highs[1:] - closes[:-1]),
            np.abs(lows[1:] - closes[:-1]),
        ),
    )
    return float(np.mean(tr[-14:])) if len(tr) >= 14 else float(np.mean(tr))


def compute_choppiness(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, window: int = 14
) -> float:
    tr = np.maximum(
        highs[1:] - lows[1:],
        np.maximum(
            np.abs(highs[1:] - closes[:-1]),
            np.abs(lows[1:] - closes[:-1]),
        ),
    )
    if len(tr) < window:
        return float("nan")
    atr_sum = np.sum(tr[-window:])
    highest_high = np.max(highs[-window:])
    lowest_low = np.min(lows[-window:])
    rng = highest_high - lowest_low
    if rng == 0:
        return float("nan")
    return float(100 * np.log10(atr_sum / rng) / np.log10(window))


def compute_rsi14_wilder(closes: np.ndarray, period: int = 14) -> tuple[float, float, float]:
    """
    Compute RSI using Wilder's smoothing method.

    Phase 1 — seed:  avg_gain / avg_loss = simple mean of the first `period`
                     price changes (closes[1] - closes[0] … closes[period] - closes[period-1]).
    Phase 2 — roll:  Wilder's EMA: avg = (prev_avg * (period-1) + current) / period
                     applied to every subsequent bar.

    Returns (rsi, avg_gain, avg_loss) where avg_gain / avg_loss are the
    Wilder-smoothed running averages at the final bar — ready to hand off to
    the candle builder so it can continue incrementally without warmup candles.

    Edge cases:
      • Fewer than period+1 bars   → returns (50.0, 0.0, 0.0)
      • avg_loss == 0 at final bar → rsi = 100.0
    """
    n = len(closes)
    if n < period + 1:
        return 50.0, 0.0, 0.0

    deltas = np.diff(closes)  # length = n - 1

    # --- Phase 1: seed with simple mean of first `period` changes ---
    seed_gains = np.where(deltas[:period] > 0, deltas[:period], 0.0)
    seed_losses = np.where(deltas[:period] < 0, -deltas[:period], 0.0)
    avg_gain = float(np.mean(seed_gains))
    avg_loss = float(np.mean(seed_losses))

    # --- Phase 2: Wilder's EMA over remaining bars ---
    for delta in deltas[period:]:
        gain = delta if delta > 0 else 0.0
        loss = -delta if delta < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period

    if avg_loss == 0.0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))

    return round(rsi, 4), round(avg_gain, 6), round(avg_loss, 6)


def compute_supertrend(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
    period: int = 14, multiplier: float = 3.0
) -> tuple[str, float]:
    n = len(closes)
    atr_st = np.zeros(n)
    for i in range(1, n):
        tr_val = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        atr_st[i] = (atr_st[i - 1] * (period - 1) + tr_val) / period

    hl2 = (highs + lows) / 2
    upper_band = hl2 + multiplier * atr_st
    lower_band = hl2 - multiplier * atr_st

    direction = 1
    band = float(lower_band[0])

    for i in range(1, n):
        if direction == 1:
            band = max(float(lower_band[i]), band)
            if closes[i] < band:
                direction = -1
                band = float(upper_band[i])
        else:
            band = min(float(upper_band[i]), band)
            if closes[i] > band:
                direction = 1
                band = float(lower_band[i])

    return ("BULL" if direction == 1 else "BEAR"), round(band, 4)


def _compute_vol_profiles(
    candles: list[list],
    timestamps: list[str],
    day_map: dict[str, list[int]],
    sorted_days: list[str],
) -> tuple[dict, dict]:
    """
    Compute per-5m-slot and cumulative volume profiles from 5 days of 1m candles.
    
    Returns:
        vol_5m:  {"0915": avg_vol, "0920": avg_vol, ...}  5m slot averages
        vol_cum: {"0915": avg_cum, "0920": avg_cum, ...}  cumulative averages
    """
    # Define all valid 5m slots 9:15 to 15:25
    slot_keys = []
    h, m = 9, 15
    while (h, m) <= (15, 25):
        slot_keys.append(f"{h:02d}{m:02d}")
        m += 5
        if m >= 60:
            m = 0
            h += 1

    # For each trading day compute per-slot volume and cumulative volume
    days_to_use = sorted_days[-5:]  # last 5 trading days
    slot_volumes_by_day: dict[str, list[float]] = {k: [] for k in slot_keys}
    cum_volumes_by_day:  dict[str, list[float]] = {k: [] for k in slot_keys}

    for day in days_to_use:
        indices = day_map.get(day, [])
        if not indices:
            continue

        # Build slot -> volume map for this day from 1m candles
        day_slot_vol: dict[str, float] = {k: 0.0 for k in slot_keys}
        for idx in indices:
            ts = timestamps[idx]
            try:
                dt = datetime.fromisoformat(ts)
                # Round down to nearest 5m slot
                slot_m = (dt.minute // 5) * 5
                slot_key = f"{dt.hour:02d}{slot_m:02d}"
                if slot_key in day_slot_vol:
                    volume_shares = float(candles[idx][5])
                    close_price   = float(candles[idx][4])
                    day_slot_vol[slot_key] += volume_shares * close_price
            except Exception:
                continue

        # Build cumulative for this day
        running_cum = 0.0
        for slot in slot_keys:
            running_cum += day_slot_vol[slot]
            slot_volumes_by_day[slot].append(day_slot_vol[slot])
            cum_volumes_by_day[slot].append(running_cum)

    # Average across days
    vol_5m = {}
    vol_cum = {}
    for slot in slot_keys:
        sv = slot_volumes_by_day[slot]
        cv = cum_volumes_by_day[slot]
        vol_5m[slot]  = round(float(np.mean(sv)) if sv else 0.0, 2)
        vol_cum[slot] = round(float(np.mean(cv)) if cv else 0.0, 2)

    return vol_5m, vol_cum


def _to_5m_candles(candles_1m: list) -> list:
    """Bucket 1m OHLCV into 5m OHLCV candles."""
    result = []
    bucket = []
    for c in candles_1m:
        bucket.append(c)
        if len(bucket) == 5:
            result.append([
                bucket[0][0],                    # ts = open of first 1m
                bucket[0][1],                    # open
                max(x[2] for x in bucket),       # high
                min(x[3] for x in bucket),       # low
                bucket[-1][4],                   # close = last 1m close
                sum(x[5] for x in bucket),       # volume sum
            ])
            bucket = []
    return result


def compute_pivots_classic(prev_high: float, prev_low: float, prev_close: float) -> dict:
    pp = (prev_high + prev_low + prev_close) / 3
    return {
        "pp": round(pp, 2),
        "r1": round(2 * pp - prev_low, 2),
        "r2": round(pp + (prev_high - prev_low), 2),
        "s1": round(2 * pp - prev_high, 2),
        "s2": round(pp - (prev_high - prev_low), 2),
    }


def compute_pivots_camarilla(prev_high: float, prev_low: float, prev_close: float) -> dict:
    rng = prev_high - prev_low
    return {
        "r1": round(prev_close + rng * 1.1 / 12, 2),
        "r2": round(prev_close + rng * 1.1 / 6, 2),
        "r3": round(prev_close + rng * 1.1 / 4, 2),
        "r4": round(prev_close + rng * 1.1 / 2, 2),
        "s1": round(prev_close - rng * 1.1 / 12, 2),
        "s2": round(prev_close - rng * 1.1 / 6, 2),
        "s3": round(prev_close - rng * 1.1 / 4, 2),
        "s4": round(prev_close - rng * 1.1 / 2, 2),
    }


# ---------------------------------------------------------------------------
# Options helpers — instrument master parsing
# ---------------------------------------------------------------------------

async def _get_instrument_master() -> list[dict]:
    """Fetch AngelOne instrument master JSON (cached in module scope)."""
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()


_instrument_master_cache: list[dict] | None = None


async def get_instrument_master() -> list[dict]:
    global _instrument_master_cache
    if _instrument_master_cache is None:
        _instrument_master_cache = await _get_instrument_master()
        logger.info("Instrument master loaded: %d entries", len(_instrument_master_cache))
    return _instrument_master_cache


def _parse_expiry_str(s: str) -> datetime:
    for fmt in ("%d%b%Y", "%Y-%m-%d", "%d-%b-%Y"):
        try:
            return datetime.strptime(s.strip().upper(), fmt)
        except ValueError:
            continue
    return datetime.max


def find_option_strikes(
    instruments: list[dict],
    symbol: str,
    prev_close: float,
) -> dict | None:
    """
    From the instrument master, find ATM, ATM-1, ATM+1 CE and PE tokens
    for the nearest expiry of `symbol` in NFO OPTSTK.

    Derives strike interval dynamically from available strikes.
    Returns None if no options found.
    """
    # Collect all NFO OPTSTK entries for this symbol
    opts = [
        e for e in instruments
        if e.get("exch_seg") == "NFO"
        and e.get("instrumenttype") == "OPTSTK"
        and e.get("name", "").upper() == symbol.upper()
    ]
    if not opts:
        return None

    # Find nearest expiry
    expiries = sorted(set(e.get("expiry", "") for e in opts), key=_parse_expiry_str)
    if not expiries:
        return None
    nearest_expiry_str = expiries[0]
    nearest_expiry_dt = _parse_expiry_str(nearest_expiry_str)

    # Filter to nearest expiry only
    near_opts = [e for e in opts if e.get("expiry", "") == nearest_expiry_str]

    # Collect all unique strikes to derive interval dynamically
    strikes_available = sorted(set(int(float(e.get("strike", 0))) // 100 for e in near_opts))
    # AngelOne stores strikes multiplied by 100 → divide by 100 to get actual strike
    if len(strikes_available) < 2:
        return None

    # Derive strike gap: most common difference between consecutive strikes
    diffs = [strikes_available[i + 1] - strikes_available[i] for i in range(len(strikes_available) - 1)]
    strike_gap = int(sorted(diffs, key=diffs.count, reverse=True)[0])

    # ATM = round prev_close to nearest strike_gap
    atm_strike = int(round(prev_close / strike_gap) * strike_gap)

    # Build CE/PE token maps: strike → token
    ce_map: dict[int, str] = {}
    pe_map: dict[int, str] = {}
    for e in near_opts:
        strike_val = int(float(e.get("strike", 0))) // 100
        opt_type = e.get("symbol", "")[-2:].upper()  # CE or PE from symbol suffix
        # More reliable: use the optiontype field if available
        ot = e.get("optiontype", "").upper() or opt_type
        tok = str(e.get("token", ""))
        if ot == "CE":
            ce_map[strike_val] = tok
        elif ot == "PE":
            pe_map[strike_val] = tok

    atm_m1 = atm_strike - strike_gap
    atm_p1 = atm_strike + strike_gap

    # All three strikes must have both CE and PE tokens
    for s in [atm_m1, atm_strike, atm_p1]:
        if s not in ce_map or s not in pe_map:
            logger.warning("%s: Missing CE/PE token for strike %d (expiry %s)", symbol, s, nearest_expiry_str)

    expiry_formatted = nearest_expiry_dt.strftime("%Y-%m-%d") if nearest_expiry_dt != datetime.max else nearest_expiry_str

    return {
        "atm_strike": atm_strike,
        "expiry": expiry_formatted,
        "strikes": {
            "atm_minus1": {
                "strike": atm_m1,
                "ce_token": ce_map.get(atm_m1, ""),
                "pe_token": pe_map.get(atm_m1, ""),
            },
            "atm": {
                "strike": atm_strike,
                "ce_token": ce_map.get(atm_strike, ""),
                "pe_token": pe_map.get(atm_strike, ""),
            },
            "atm_plus1": {
                "strike": atm_p1,
                "ce_token": ce_map.get(atm_p1, ""),
                "pe_token": pe_map.get(atm_p1, ""),
            },
        },
    }


def _aggregate_candles(symbol: str, one_min_candles: list, window_seconds: int) -> list:
    """
    Bucket 1m candles into windows of `window_seconds`.
    Each bucket: open=first.open, high=max high, low=min low,
                 close=last.close, volume=sum.
    Preserves timestamp format (ISO string or epoch int) from source.

    DIAGNOSTIC: logs sample + parse-failure counts when bucket count is zero.
    """
    if not one_min_candles:
        logger.warning(
            "[aggregate] %s tf_window=%ds: got ZERO input candles",
            symbol, window_seconds,
        )
        return []
    buckets: dict[int, list] = {}
    bucket_order: list[int] = []
    sample_ts = one_min_candles[0][0]
    use_iso = isinstance(sample_ts, str)
    ts_parse_fails = 0
    numeric_parse_fails = 0
    total_processed = 0

    for cdl in one_min_candles:
        total_processed += 1
        ts_raw = cdl[0]
        if isinstance(ts_raw, str):
            try:
                epoch = int(datetime.fromisoformat(ts_raw).timestamp())
            except Exception as _exc:
                ts_parse_fails += 1
                if ts_parse_fails == 1:
                    logger.warning(
                        "[aggregate] %s: fromisoformat FAILED on sample "
                        "ts_raw=%r type=%s err=%s",
                        symbol, ts_raw, type(ts_raw).__name__, _exc,
                    )
                continue
        elif isinstance(ts_raw, (int, float)):
            epoch = int(ts_raw)
        else:
            ts_parse_fails += 1
            continue
        floored = (epoch // window_seconds) * window_seconds
        try:
            o = float(cdl[1]); h = float(cdl[2]); l = float(cdl[3])
            c = float(cdl[4]); v = float(cdl[5])
        except (ValueError, TypeError):
            numeric_parse_fails += 1
            continue
        if floored not in buckets:
            buckets[floored] = [o, h, l, c, v]
            bucket_order.append(floored)
        else:
            b = buckets[floored]
            if h > b[1]: b[1] = h
            if l < b[2]: b[2] = l
            b[3] = c
            b[4] += v

    out = []
    for floored in bucket_order:
        b = buckets[floored]
        ts_out = datetime.fromtimestamp(floored).isoformat() if use_iso else floored
        out.append([ts_out, b[0], b[1], b[2], b[3], b[4]])

    if not out:
        logger.warning(
            "[aggregate] %s tf_window=%ds: ZERO buckets. "
            "processed=%d ts_fails=%d numeric_fails=%d sample_ts=%r sample_type=%s",
            symbol, window_seconds, total_processed, ts_parse_fails,
            numeric_parse_fails, sample_ts, type(sample_ts).__name__,
        )
    elif window_seconds == 300:
        # Log success once per symbol (5m only to avoid log spam)
        logger.info(
            "[aggregate] %s 5m: produced %d buckets from %d 1m candles",
            symbol, len(out), total_processed,
        )
    return out


# ---------------------------------------------------------------------------
# Phase A — Equity snapshot for one symbol
# ---------------------------------------------------------------------------

async def _seed_equity_symbol(
    symbol: str,
    token: str,
    lot_size: int,
    session: dict,
    from_dt: datetime,
    to_dt: datetime,
    http_client: httpx.AsyncClient,
) -> bool:
    try:
        candles = await fetch_candles(
            session, "NSE", token, "ONE_MINUTE", from_dt, to_dt, http_client
        )
        if not candles or len(candles) < 20:
            logger.warning("Phase A: %s — too few candles (%d), skipping.", symbol, len(candles))
            return False

        # Parse arrays
        opens   = np.array([c[1] for c in candles], dtype=float)
        highs   = np.array([c[2] for c in candles], dtype=float)
        lows    = np.array([c[3] for c in candles], dtype=float)
        closes  = np.array([c[4] for c in candles], dtype=float)
        volumes = np.array([c[5] for c in candles], dtype=float)
        timestamps = [c[0] for c in candles]

        # --- Prev day candles ---
        # Group by date prefix (YYYY-MM-DD) to find last completed day
        day_map: dict[str, list[int]] = {}
        for idx, ts in enumerate(timestamps):
            day_key = ts[:10]  # "YYYY-MM-DD"
            day_map.setdefault(day_key, []).append(idx)

        sorted_days = sorted(day_map.keys())

        # Last trading day = Friday (for ltp and pivot calculations)
        last_day_indices = day_map[sorted_days[-1]]
        last_close = float(closes[last_day_indices[-1]])
        last_high  = float(np.max(highs[last_day_indices]))
        last_low   = float(np.min(lows[last_day_indices]))
        last_open  = float(opens[last_day_indices[0]])

        prev_close = last_close

        pd_volumes = volumes[last_day_indices]

        prev_open   = last_open
        prev_high   = last_high
        prev_low    = last_low
        prev_volume = float(np.sum(pd_volumes))

        # --- Indicators ---
        ema9   = ema_vectorized(closes, 9)
        ema16  = float(ema_vectorized(closes, 16)[-1])
        ema200 = float(ema_vectorized(closes, 200)[-1]) if len(closes) >= 200 else float(ema_vectorized(closes, len(closes))[-1])
        atr14  = compute_atr14(highs, lows, closes)

        # avg_volume_5d: mean of per-day totals across all available days
        daily_volumes = [float(np.sum(volumes[day_map[d]])) for d in sorted_days]
        avg_volume_5d = float(np.mean(daily_volumes[-5:])) if daily_volumes else 0.0

        choppiness14    = compute_choppiness(highs, lows, closes)
        if choppiness14 < 38.2:
            choppiness_class = "TRENDING"
        elif choppiness14 > 61.8:
            choppiness_class = "CHOPPY"
        else:
            choppiness_class = "NEUTRAL"
        st_direction, st_band = compute_supertrend(highs, lows, closes)
        rsi14, rsi_avg_gain, rsi_avg_loss = compute_rsi14_wilder(closes)

        # ltp = Friday's close (best known price, overwritten by
        # live WebSocket at 9:15 AM Monday)
        ltp = last_close

        # Pivots computed from Friday's OHLC (correct for Monday trading)
        classic   = compute_pivots_classic(last_high, last_low, last_close)
        camarilla = compute_pivots_camarilla(last_high, last_low, last_close)

        snapshot = {
            "ema9":          float(ema9[-1]),
            "ema16":         round(ema16, 4),
            "ema200":        round(ema200, 4),
            "atr14":         round(atr14, 4),
            "avg_volume_5d": round(avg_volume_5d, 2),
            "rsi14":         rsi14,
            "rsi_avg_gain":  rsi_avg_gain,
            "rsi_avg_loss":  rsi_avg_loss,
            "prev_day": {
                "open":   round(prev_open, 2),
                "high":   round(prev_high, 2),
                "low":    round(prev_low, 2),
                "close":  round(prev_close, 2),
                "volume": round(prev_volume, 2),
                "classic":   classic,
                "camarilla": camarilla,
            },
            "choppiness14": round(choppiness14, 4) if choppiness14 == choppiness14 else None,
            "choppiness_class": choppiness_class,
            "supertrend": {
                "direction": st_direction,
                "band":      round(st_band, 4),
            },
            "lot_size":  lot_size,
            "token":     token,
            "sector":    _get_sector(symbol),
            "seeded_at": datetime.now(timezone.utc).isoformat(),
            "ltp": round(ltp, 2),
            "prev_close": round(prev_close, 2),
        }

        redis = await get_redis()
        # Store raw candles
        raw_candles = [[c[0], c[1], c[2], c[3], c[4], c[5]] for c in candles]
        candle_key_1m = f"candles:1m:{symbol}"
        async with redis.pipeline(transaction=False) as pipe:
            pipe.delete(candle_key_1m)
            for candle in raw_candles:
                pipe.rpush(candle_key_1m, json.dumps(candle))
            pipe.ltrim(candle_key_1m, -2800, -1)
            await pipe.execute()

        for tf_label, tf_seconds in (("5m", 300), ("15m", 900), ("1hr", 3600)):
            tf_candles = _aggregate_candles(symbol, raw_candles, tf_seconds)
            if not tf_candles:
                logger.warning(
                    "[aggregate] %s: SKIPPING write for tf=%s (aggregator returned empty)",
                    symbol, tf_label,
                )
                continue
            tf_key = f"candles:{tf_label}:{symbol}"
            try:
                async with redis.pipeline(transaction=False) as pipe:
                    pipe.delete(tf_key)
                    for candle in tf_candles:
                        pipe.rpush(tf_key, json.dumps(candle))
                    pipe.ltrim(tf_key, -500, -1)
                    await pipe.execute()
                logger.info(
                    "[aggregate] %s: wrote %d candles to %s",
                    symbol, len(tf_candles), tf_key,
                )
            except Exception as _exc:
                logger.error(
                    "[aggregate] %s: write FAILED for %s — %s",
                    symbol, tf_key, _exc,
                )
        # Store snapshot as Redis HASH (canonical runtime format).
        # This avoids STRING/HASH type flips and WRONGTYPE races at startup.
        raw_candles_list = [[c[0], c[1], c[2], c[3], c[4], c[5]] for c in candles]
        snapshot_hash = _snapshot_to_hash_mapping(snapshot)

        # Convert to 5m candles for ATR and EMA9 (signals fire on 5m)
        candles_5m_list = _to_5m_candles(raw_candles_list)
        if len(candles_5m_list) >= 15:
            highs_5m  = np.array([c[2] for c in candles_5m_list])
            lows_5m   = np.array([c[3] for c in candles_5m_list])
            closes_5m = np.array([c[4] for c in candles_5m_list])
            # ATR14 on 5m
            atr_vals = np.zeros(len(closes_5m))
            for i in range(1, len(closes_5m)):
                tr = max(
                    highs_5m[i] - lows_5m[i],
                    abs(highs_5m[i] - closes_5m[i-1]),
                    abs(lows_5m[i] - closes_5m[i-1]),
                )
                atr_vals[i] = (atr_vals[i-1] * 13 + tr) / 14 if atr_vals[i-1] > 0 else tr
            atr14 = round(float(atr_vals[-1]), 6)
            # EMA9 on 5m
            ema9_5m = float(closes_5m[0])
            for price in closes_5m[1:]:
                ema9_5m = (float(price) - ema9_5m) * (2 / 10) + ema9_5m
            ema9_5m = round(ema9_5m, 4)
        else:
            # fallback to existing values if not enough 5m candles
            ema9_5m = ema9  # keep existing seeded ema9
        # Write both to snapshot — ema9_5m for gatekeeper extension check
        # atr14 is now 5m ATR, ema9 in snapshot will be updated to 5m value
        snapshot_hash["atr14"]   = str(atr14)
        snapshot_hash["ema9_5m"] = str(ema9_5m)

        await redis.hset(
            f"snapshot:{symbol}",
            mapping=snapshot_hash,
        )
        # --- NEW: Write snapshot_prev for SUPERTREND_FLIP to work at 9:15 AM ---
        await redis.set(
            f"snapshot_prev:{symbol}",
            json.dumps({
                "supertrend_dir":  st_direction,
                "supertrend_band": round(st_band, 4),
            }),
            ex=86400,
        )

        # --- NEW: Compute and write volume profiles ---
        vol_5m, vol_cum = _compute_vol_profiles(
            candles=raw_candles_list,
            timestamps=timestamps,
            day_map=day_map,
            sorted_days=sorted_days,
        )
        await redis.set(
            f"vol_profile:5m:{symbol}",
            json.dumps(vol_5m),
            ex=86400,
        )
        await redis.set(
            f"vol_profile:cum:{symbol}",
            json.dumps(vol_cum),
            ex=86400,
        )
        logger.debug(
            "Phase A: %s — vol profiles written (%d slots).", symbol, len(vol_5m)
        )

        return True

    except Exception as exc:
        logger.error("Phase A: %s failed — %s", symbol, exc)
        return False


# ---------------------------------------------------------------------------
# Phase B — Options baseline for one symbol
# ---------------------------------------------------------------------------

async def _seed_options_symbol(
    symbol: str,
    prev_close: float,
    instruments: list[dict],
    session: dict,
    from_dt: datetime,
    to_dt: datetime,
    http_client: httpx.AsyncClient,
) -> bool:
    try:
        strike_info = find_option_strikes(instruments, symbol, prev_close)
        if not strike_info:
            logger.warning("Phase B: %s — no option strikes found, skipping.", symbol)
            return False

        atm_info = strike_info["strikes"]["atm"]
        ce_token = atm_info["ce_token"]
        pe_token = atm_info["pe_token"]

        if not ce_token or not pe_token:
            logger.warning("Phase B: %s — missing ATM CE/PE token, skipping.", symbol)
            return False

        # Fetch 5-day daily candles for ATM CE and PE
        ce_candles = await fetch_candles(
            session, "NFO", ce_token, "ONE_DAY", from_dt, to_dt, http_client
        )
        pe_candles = await fetch_candles(
            session, "NFO", pe_token, "ONE_DAY", from_dt, to_dt, http_client
        )

        def _parse_opt_candles(candles: list[list]) -> dict:
            if not candles:
                return {"volumes": [], "ois": [], "prev_close": 0.0, "prev_oi": 0}
            vols = [c[5] for c in candles]
            ois  = [c[6] if len(c) > 6 else 0 for c in candles]
            return {
                "volumes":    vols,
                "ois":        ois,
                "prev_close": float(candles[-1][4]),
                "prev_oi":    int(candles[-1][6]) if len(candles[-1]) > 6 else 0,
            }

        ce = _parse_opt_candles(ce_candles)
        pe = _parse_opt_candles(pe_candles)

        ce_vols = ce["volumes"][-5:] if ce["volumes"] else []
        pe_vols = pe["volumes"][-5:] if pe["volumes"] else []
        ce_ois  = ce["ois"][-5:] if ce["ois"] else []
        pe_ois  = pe["ois"][-5:] if pe["ois"] else []

        result = {
            "atm_strike":      strike_info["atm_strike"],
            "expiry":          strike_info["expiry"],
            "ce_avg_volume_5d": round(float(np.mean(ce_vols)) if ce_vols else 0.0, 2),
            "pe_avg_volume_5d": round(float(np.mean(pe_vols)) if pe_vols else 0.0, 2),
            "ce_avg_oi_5d":    round(float(np.mean(ce_ois)) if ce_ois else 0.0, 2),
            "pe_avg_oi_5d":    round(float(np.mean(pe_ois)) if pe_ois else 0.0, 2),
            "ce_prev_close":   round(ce["prev_close"], 4),
            "pe_prev_close":   round(pe["prev_close"], 4),
            "ce_prev_oi":      ce["prev_oi"],
            "pe_prev_oi":      pe["prev_oi"],
            "strikes":         strike_info["strikes"],
            "seeded_at":       datetime.now(timezone.utc).isoformat(),
        }

        redis = await get_redis()
        await redis.set(f"options:prev:{symbol}", json.dumps(result))
        return True

    except Exception as exc:
        logger.error("Phase B: %s failed — %s", symbol, exc)
        return False


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

async def run_seeder(force: bool = False) -> None:
    t_total_start = time.monotonic()

    validate()
    logger.info("=== Market Pulse Pro v2 — Morning Seeder ===")

    # Step 1: Login
    logger.info("Logging in to AngelOne…")
    session = await get_angel_session()
    logger.info("Login successful. Client: %s", session["client_code"])

    # Step 2: Build universe (always refresh at 8:30 AM)
    logger.info("Building universe from instrument master…")
    await build_universe()
    instruments = await get_instrument_master()
    master_df = pd.DataFrame(instruments)

    resolved = await resolve_index_tokens(master_df)
    await store_index_tokens(resolved)
    logger.info(
        f"[seeder] Resolved {len(resolved)}/5 index tokens: "
        f"{list(resolved.keys())}"
    )
    if len(resolved) < 4:
        logger.error(
            "[seeder] Too few index tokens resolved — "
            "macro gating will be degraded today"
        )

    symbols   = await get_symbols()
    token_map = await get_token_map()
    lot_sizes = await get_lot_sizes()

    logger.info("Universe: %d F&O symbols loaded.", len(symbols))

    # Date range (same for both phases)
    from_dt, to_dt = _get_date_range()
    logger.info("Date range: %s → %s", from_dt.strftime("%Y-%m-%d %H:%M"), to_dt.strftime("%Y-%m-%d %H:%M"))

    # -----------------------------------------------------------------------
    # MARKET-CLOSED GUARD — probe before touching any Redis key
    # -----------------------------------------------------------------------
    if force:
        logger.warning(
            "force=True — skipping market probe, proceeding with last available data."
        )
    else:
        market_open = await probe_market_open(session, from_dt, to_dt)
        if not market_open:
            logger.warning(
                "Market appears closed today — seeder skipping, Redis data preserved. "
                "Use force=True to override."
            )
            return  # exit without writing anything to Redis

    # -----------------------------------------------------------------------
    # PHASE A — Equity Snapshots
    # -----------------------------------------------------------------------
    logger.info("--- PHASE A: Equity Snapshots ---")
    t_a_start = time.monotonic()

    equity_results: list[bool] = []

    async with httpx.AsyncClient(timeout=30.0) as http_client:
        for i, sym in enumerate(symbols):
            tok = token_map.get(sym)
            if not tok:
                logger.warning("Phase A: %s has no NSE EQ token, skipping.", sym)
                equity_results.append(False)
                continue
            lot = lot_sizes.get(sym, 1)
            try:
                result = await _seed_equity_symbol(sym, tok, lot, session, from_dt, to_dt, http_client)
            except Exception as e:
                logger.error(f"Phase A failed for {sym}: {e}")
                result = False
            equity_results.append(result)
            if (i + 1) % LOG_EVERY == 0:
                ok = sum(equity_results)
                logger.info("Phase A progress: %d/%d symbols processed (%d OK)", i + 1, len(symbols), ok)
            await asyncio.sleep(0.35)  # 2.8 req/sec, under AngelOne 3/sec limit

    t_a_end = time.monotonic()
    phase_a_seconds = round(t_a_end - t_a_start, 2)
    equity_ok = sum(equity_results)
    logger.info(
        "Phase A complete: %d/%d symbols seeded in %.1fs.",
        equity_ok, len(symbols), phase_a_seconds,
    )

    # Seed index snapshots
    index_from_dt = _min_index_lookback_from(from_dt, to_dt)
    index_symbols = await load_index_symbols()
    logger.info(f"[seeder] Seeding snapshots for {len(index_symbols)} indices: {index_symbols}")

    redis = await get_redis()
    async with httpx.AsyncClient(timeout=30.0) as http_client:
        for symbol in index_symbols:
            token = await redis.hget("index:tokens", symbol)
            if not token:
                logger.warning(f"[seeder] No token for index {symbol} — skipping")
                continue

            try:
                # Hard override — instrument_registry may write wrong token
                _PREFERRED = {
                    "NIFTY": "99926000", "BANKNIFTY": "99926009",
                    "FINNIFTY": "99926037", "MIDCPNIFTY": "99926074",
                    "SENSEX": "99919000",
                }
                token_str = token.decode() if isinstance(token, (bytes, bytearray)) else str(token)
                if symbol in _PREFERRED:
                    token_str = _PREFERRED[symbol]
                exchange = "BSE" if symbol == "SENSEX" else "NSE"
                candles = await fetch_candles(
                    session, exchange, token_str, "ONE_MINUTE", index_from_dt, to_dt, http_client
                )
                if not candles or len(candles) < 200:
                    logger.warning(f"[seeder] Too few candles for index {symbol} ({len(candles)}), skipping")
                    continue

                opens = np.array([c[1] for c in candles], dtype=float)
                highs = np.array([c[2] for c in candles], dtype=float)
                lows = np.array([c[3] for c in candles], dtype=float)
                closes = np.array([c[4] for c in candles], dtype=float)
                volumes = np.array([c[5] for c in candles], dtype=float)
                timestamps = [c[0] for c in candles]

                day_map: dict[str, list[int]] = {}
                for idx, ts in enumerate(timestamps):
                    day_key = ts[:10]
                    day_map.setdefault(day_key, []).append(idx)

                sorted_days = sorted(day_map.keys())
                if not sorted_days:
                    logger.warning(f"[seeder] No grouped day data for index {symbol}, skipping")
                    continue
                # Last trading day = Friday (for ltp and pivot calculations)
                last_day_indices = day_map[sorted_days[-1]]
                last_close = float(closes[last_day_indices[-1]])
                last_high = float(np.max(highs[last_day_indices]))
                last_low = float(np.min(lows[last_day_indices]))
                last_open = float(opens[last_day_indices[0]])

                # Previous trading day = Thursday (for prev_close field)
                if len(sorted_days) >= 2:
                    prev_day_indices = day_map[sorted_days[-2]]
                    prev_close = float(closes[prev_day_indices[-1]])
                else:
                    prev_close = last_close

                pd_volumes = volumes[last_day_indices]

                prev_open = last_open
                prev_high = last_high
                prev_low = last_low
                prev_volume = float(np.sum(pd_volumes))

                ema9 = float(ema_vectorized(closes, 9)[-1])
                ema16 = float(ema_vectorized(closes, 16)[-1])
                ema200 = float(ema_vectorized(closes, 200)[-1])
                atr14 = compute_atr14(highs, lows, closes)
                choppiness14 = compute_choppiness(highs, lows, closes)
                if choppiness14 < 38.2:
                    choppiness_class = "TRENDING"
                elif choppiness14 > 61.8:
                    choppiness_class = "CHOPPY"
                else:
                    choppiness_class = "NEUTRAL"
                st_direction, st_band = compute_supertrend(highs, lows, closes)
                rsi14, _, _ = compute_rsi14_wilder(closes)

                # ltp = Friday's close (best known price, overwritten by
                # live WebSocket at 9:15 AM Monday)
                ltp = last_close

                # Pivots computed from Friday's OHLC (correct for Monday trading)
                classic = compute_pivots_classic(last_high, last_low, last_close)
                camarilla = compute_pivots_camarilla(last_high, last_low, last_close)

                raw_candles = [[c[0], c[1], c[2], c[3], c[4], 0] for c in candles]
                candle_key_1m = f"candles:1m:{symbol}"
                async with redis.pipeline(transaction=False) as pipe:
                    pipe.delete(candle_key_1m)
                    for candle in raw_candles:
                        pipe.rpush(candle_key_1m, json.dumps(candle))
                    pipe.ltrim(candle_key_1m, -500, -1)
                    await pipe.execute()
                for tf_label, tf_seconds in (("5m", 300), ("15m", 900), ("1hr", 3600)):
                    tf_candles = _aggregate_candles(symbol, raw_candles, tf_seconds)
                    if not tf_candles:
                        logger.warning(
                            "[aggregate] %s: SKIPPING write for tf=%s (aggregator returned empty)",
                            symbol, tf_label,
                        )
                        continue
                    tf_key = f"candles:{tf_label}:{symbol}"
                    try:
                        async with redis.pipeline(transaction=False) as pipe:
                            pipe.delete(tf_key)
                            for candle in tf_candles:
                                pipe.rpush(tf_key, json.dumps(candle))
                            pipe.ltrim(tf_key, -500, -1)
                            await pipe.execute()
                        logger.info(
                            "[aggregate] %s: wrote %d candles to %s",
                            symbol, len(tf_candles), tf_key,
                        )
                    except Exception as _exc:
                        logger.error(
                            "[aggregate] %s: write FAILED for %s — %s",
                            symbol, tf_key, _exc,
                        )

                snapshot = {
                    "ema9": round(ema9, 4),
                    "ema16": round(ema16, 4),
                    "ema200": round(ema200, 4),
                    "atr14": round(atr14, 4),
                    "avg_volume_5d": 0.0,
                    "rsi14": rsi14,
                    "rsi_avg_gain": 0.0,
                    "rsi_avg_loss": 0.0,
                    "prev_day": {
                        "open": round(prev_open, 2),
                        "high": round(prev_high, 2),
                        "low": round(prev_low, 2),
                        "close": round(prev_close, 2),
                        "volume": round(prev_volume, 2),
                        "classic": classic,
                        "camarilla": camarilla,
                    },
                    "choppiness14": round(choppiness14, 4) if choppiness14 == choppiness14 else None,
                    "choppiness_class": choppiness_class,
                    "supertrend": {
                        "direction": st_direction,
                        "band": round(st_band, 4),
                    },
                    "lot_size": 1,
                    "token": token_str,
                    "sector": "INDEX",
                    "seeded_at": datetime.now(timezone.utc).isoformat(),
                    "ltp": round(ltp, 2),
                    "prev_close": round(prev_close, 2),
                }

                mapping = _snapshot_to_hash_mapping(snapshot)
                mapping.update(
                    {
                        "ltp": str(round(ltp, 2)),
                        "prev_close": str(round(prev_close, 2)),
                        "symbol": symbol,
                        "sector": "INDEX",
                    }
                )
                await redis.hset(f"snapshot:{symbol}", mapping=mapping)
                # Seed yesterday's final supertrend direction so SUPERTREND_FLIP
                # logic works correctly from the very first candle at 9:15.
                await redis.set(
                    f"snapshot_prev:{symbol}",
                    json.dumps({"supertrend_dir": st_direction}),
                    ex=86400,
                )
                logger.info(f"[seeder] Index {symbol}: 1m candles + snapshot seeded")
            except Exception as exc:
                logger.error(f"[seeder] Failed to seed snapshot for index {symbol} — {exc}")

    # -----------------------------------------------------------------------
    # PHASE B — Options Baseline
    # -----------------------------------------------------------------------
    logger.info("--- PHASE B: Options Baseline ---")
    t_b_start = time.monotonic()

    # Get prev_close from Phase A results (from Redis)
    redis = await get_redis()

    options_results: list[bool] = []

    async with httpx.AsyncClient(timeout=30.0) as http_client:
        fno_symbols = []
        fno_prev_closes = []
        for sym in symbols:
            snap_hash = await redis.hgetall(f"snapshot:{sym}")
            if not snap_hash:
                logger.warning("Phase B: %s — no equity snapshot, skipping options.", sym)
                options_results.append(False)
                continue
            try:
                prev_close = float(snap_hash.get("prev_close") or 0.0)
            except (TypeError, ValueError):
                prev_close = 0.0
            if not prev_close:
                logger.warning("Phase B: %s — zero prev_close, skipping.", sym)
                options_results.append(False)
                continue
            fno_symbols.append(sym)
            fno_prev_closes.append(prev_close)

        for i, (sym, prev_close) in enumerate(zip(fno_symbols, fno_prev_closes)):
            try:
                result = await _seed_options_symbol(
                    sym, prev_close, instruments, session, from_dt, to_dt, http_client
                )
            except Exception as e:
                logger.error(f"Phase B failed for {sym}: {e}")
                result = False
            options_results.append(result)
            if (i + 1) % LOG_EVERY == 0:
                ok = sum(r for r in options_results if r)
                logger.info("Phase B progress: %d/%d symbols processed (%d OK)", i + 1, len(fno_symbols), ok)
            await asyncio.sleep(0.35)  # Phase B makes 6 calls per symbol
                                       # but each is sequential inside the function
                                       # so outer 0.35s is sufficient

    t_b_end = time.monotonic()
    phase_b_seconds = round(t_b_end - t_b_start, 2)
    options_ok = sum(options_results)
    logger.info(
        "Phase B complete: %d/%d symbols seeded in %.1fs.",
        options_ok, len(symbols), phase_b_seconds,
    )

    # -----------------------------------------------------------------------
    # Global Indices seed (Groww CFD data — macro pre-market context)
    # -----------------------------------------------------------------------
    logger.info("[seeder] seeding global indices from Groww...")
    try:
        # scrape_and_store creates its own sync Redis connection — no client needed
        ok = await asyncio.get_event_loop().run_in_executor(
            None, lambda: _scrape_global_indices(ttl=_GLOBAL_TTL)
        )
        if ok:
            logger.info("[seeder] global indices seeded ✅")
        else:
            logger.warning("[seeder] global indices seed failed — dashboard will show unavailable")
    except Exception as _gi_exc:
        logger.error("[seeder] global indices seed exception: %s", _gi_exc)

    # -----------------------------------------------------------------------
    # Write completion status to Redis
    # -----------------------------------------------------------------------
    total_seconds = round(time.monotonic() - t_total_start, 2)
    status = {
        "status":           "complete",
        "completed_at":     datetime.now(timezone.utc).isoformat(),
        "equity_count":     equity_ok,
        "options_count":    options_ok,
        "phase_a_seconds":  phase_a_seconds,
        "phase_b_seconds":  phase_b_seconds,
    }
    # ── AI Pipeline (scrape + sentiment + decisions) ──────────────────────
    logger.info("[seeder] Starting AI pipeline (scrape + LLM scoring)...")
    try:
        import os, redis as _redis_sync
        from strategy_brain.ai_pipeline.ai_scraper import run_full_scrape
        from strategy_brain.ai_pipeline.ai_engine import run_ai_pipeline

        _r = _redis_sync.from_url(os.environ["REDIS_URL"])
        scrape_stats = run_full_scrape(_r)
        logger.info("[seeder] AI scrape done: %s", scrape_stats)
        await run_ai_pipeline()
        logger.info("[seeder] AI pipeline complete")

        # Enrich trade list with LTP + change_pct from snapshots
        try:
            trade_raw = await redis.get("ai:trade_list")
            if trade_raw:
                trade_list = json.loads(trade_raw)
                for group in ("top_bullish", "top_bearish"):
                    for item in trade_list.get(group, []):
                        sym = item.get("symbol")
                        if not sym:
                            continue
                        snap = await redis.hgetall(f"snapshot:{sym}")
                        if not snap:
                            continue
                        ltp        = float(snap.get("ltp") or 0)
                        prev_close = float(snap.get("prev_close") or 0)
                        change_pct = round((ltp - prev_close) / prev_close * 100, 2) if prev_close else 0.0
                        item["ltp"]        = ltp
                        item["change_pct"] = change_pct
                        item["rsi14"]      = float(snap.get("rsi14") or 0)
                        item["supertrend_dir"]   = snap.get("supertrend_dir", "")
                        item["choppiness_class"] = snap.get("choppiness_class", "NEUTRAL")
                        item["sector"]           = snap.get("sector", "")
                await redis.setex("ai:premarket", 86400, json.dumps(trade_list))
                logger.info("[seeder] AI trade list enriched with snapshot data → ai:premarket")
        except Exception as _enrich_exc:
            logger.warning("[seeder] AI enrichment failed (non-fatal): %s", _enrich_exc)
    except Exception as _ai_exc:
        logger.error("[seeder] AI pipeline failed (non-fatal): %s", _ai_exc)

    await redis.set("seeder:status", json.dumps(status))

    logger.info(
        "=== Morning Seeder DONE === total=%.1fs | equity=%d | options=%d",
        total_seconds, equity_ok, options_ok,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    os.environ["SEEDER_STANDALONE"] = "1"
    _force = os.environ.get("SEEDER_FORCE", "0") == "1"
    asyncio.run(run_seeder(force=_force))
