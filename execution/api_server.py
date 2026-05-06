"""
execution/api_server.py
========================
FastAPI API server for Market Pulse Pro v2.

Serves all dashboard data to the frontend.  Every typed endpoint uses a
Pydantic response model — no raw dict returns.  All reads go to Redis only;
AngelOne SmartAPI is never called here.

WebSocket endpoints:
    /ws/ticks    — streams live LTP updates (subscribes to Redis "ticks" channel)
    /ws/signals  — streams new execution signals (subscribes to "trade_execution")
    /ws/account  — pushes paper account state every 10 seconds

Scheduler tasks (asyncio background loops, no APScheduler):
    08:30 — rebuild instrument universe
    15:20 — EOD close all open trades
    15:25 — save final account snapshot

Run
---
    python -m execution.api_server
    # or
    uvicorn execution.api_server:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone, time as dtime
from typing import Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from core.config import cfg, validate
from core.redis_client import get_redis, close, ping as redis_ping
from core.universe_builder import build_universe, get_symbols, get_lot_sizes
from strategy_brain.ai_pipeline.global_indices_scraper import scrape_and_store as _scrape_global_indices, REDIS_TTL_SEED as _GLOBAL_TTL
from execution.order_manager import (
    get_paper_account,
    get_open_trades,
    get_closed_trades,
    close_trade,
    place_paper_order,
    eod_close_all,
    # trigger-order system
    place_trigger_order,
    get_pending_orders,
    cancel_pending_order,
    update_trade_levels,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, cfg.LOG_LEVEL, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Timezone
# ---------------------------------------------------------------------------
_IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist() -> datetime:
    return datetime.now(_IST)


def _is_market_hours_ist() -> bool:
    now = _now_ist()
    if now.weekday() >= 5:  # Sat/Sun
        return False
    return dtime(9, 15) <= now.time() <= dtime(15, 30)


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# ===========================================================================
# Pydantic response models
# ===========================================================================

class TickData(BaseModel):
    symbol: str
    ltp: float
    volume: int
    ts: str


class SnapshotData(BaseModel):
    symbol: str
    ltp: float
    ema9: float
    ema16: float
    ema200: float
    atr14: float
    rsi14: float
    vwap: float
    vwap_slope: float
    choppiness14: float
    choppiness_class: str        # TRENDING / NEUTRAL / CHOPPY
    supertrend_dir: str          # BULL / BEAR
    supertrend_band: float
    rolling_1h_high: float
    rolling_1h_low: float
    orb_high: float
    orb_low: float
    consecutive_choppy_candles: int
    lot_size: int
    sector: str
    updated_at: str
    # Pivot levels — populated by morning seeder, zero if not yet available
    pp: float = 0.0
    r1: float = 0.0
    r2: float = 0.0
    s1: float = 0.0
    s2: float = 0.0
    cam_r3: float = 0.0
    cam_s3: float = 0.0
    prev_close: float = 0.0


class PivotData(BaseModel):
    pp: float
    r1: float
    r2: float
    s1: float
    s2: float
    cam_r1: float
    cam_r2: float
    cam_r3: float
    cam_r4: float
    cam_s1: float
    cam_s2: float
    cam_s3: float
    cam_s4: float


class OptionsBadge(BaseModel):
    badge: str                   # GREEN / AMBER / RED / ILLIQUID
    score: float
    spread_pct: Optional[float] = None
    abs_slippage: Optional[float] = None


class OptionsData(BaseModel):
    atm_strike: int
    ce_ltp: float
    pe_ltp: float
    ce_volume_ratio: float
    pe_volume_ratio: float
    ce_oi_ratio: float
    pe_oi_ratio: float
    ce_badge: OptionsBadge
    pe_badge: OptionsBadge
    primary_side: str            # CE or PE
    options_explosion: bool


class TradeRecord(BaseModel):
    id: str
    symbol: str
    direction: str
    signal_type: str
    entry_price: float
    stop_loss: float
    take_profit: Optional[float] = None
    lot_size: int
    quantity: int
    lots: Optional[int] = None
    margin_used: float
    ici_score: float
    ici_grade: str
    status: str
    entry_ts: str
    exit_price: Optional[float] = None
    exit_ts: Optional[str] = None
    pnl_abs: Optional[float] = None
    pnl_pct: Optional[float] = None
    exit_reason: Optional[str] = None
    # Options-specific — null for equity trades
    instrument: Optional[str] = None
    atm_strike: Optional[int] = None
    expiry_date: Optional[str] = None
    option_token: Optional[str] = None
    # Pricing instrumentation (Session 2 additions)
    price_source: Optional[str] = None
    underlying_at_fill: Optional[float] = None
    broker: Optional[str] = None


class PaperAccount(BaseModel):
    starting_balance: float
    available_margin: float
    used_margin: float
    realised_pnl: float
    unrealised_pnl: float
    total_pnl: float
    trade_count: int
    win_count: int
    loss_count: int
    updated_at: str


class MarketBreadth(BaseModel):
    advances: int
    declines: int
    unchanged: int
    ad_ratio: float
    above_ema200: int
    above_ema200_pct: float
    sector_performance: dict[str, float]
    computed_at: str


class SignalData(BaseModel):
    symbol: str
    signal_type: str
    direction: str
    ici_score: float
    ici_grade: str
    entry_price: float
    stop_loss: float
    choppiness_class: str
    supertrend_dir: str
    detected_at: str


_INDEX_DISPLAY_NAMES = {
    "NIFTY":      "NIFTY 50",
    "BANKNIFTY":  "BANK NIFTY",
    "FINNIFTY":   "FIN NIFTY",
    "MIDCPNIFTY": "MIDCAP NIFTY",
    "SENSEX":     "SENSEX",
}

class IndexData(BaseModel):
    symbol: str
    name: str = ""
    ltp: float
    change_pct: float
    prev_close: float = 0.0
    pcr: float = 0.0
    pcr_direction: str = "FLAT"  # UP / DOWN / FLAT


class AIAlignment(BaseModel):
    symbol: str
    news_sentiment: str
    technical_alignment: str
    confidence: float
    summary: str


# ===========================================================================
# Global WebSocket client sets (broadcaster pattern)
# ===========================================================================

tick_clients:    set[WebSocket] = set()
account_clients: set[WebSocket] = set()
background_tasks: set[asyncio.Task] = set()


class SignalConnectionManager:
    def __init__(self):
        self.active_connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)

    async def broadcast(self, message: str):
        dead: set[WebSocket] = set()
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                dead.add(connection)

        for connection in dead:
            self.disconnect(connection)


signal_manager = SignalConnectionManager()


async def refresh_world_indices():
    """Refresh global indices from Groww every 5 minutes during market hours."""
    import asyncio
    from strategy_brain.ai_pipeline.global_indices_scraper import scrape_and_store as _scrape, REDIS_TTL
    logger.info("[api_server] World indices refresh task started.")
    while True:
        try:
            ok = await asyncio.to_thread(_scrape, REDIS_TTL)
            if ok:
                logger.info("[api_server] World indices refreshed OK.")
            else:
                logger.warning("[api_server] World indices refresh failed — keeping stale data.")
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.warning("[api_server] World indices refresh error: %s", exc)
        await asyncio.sleep(300)  # every 5 minutes


@app.get("/api/volume-leaders")
async def get_volume_leaders():
    """Top 10 stocks by cumulative RVOL right now. Powers Volume Surge panel."""
    try:
        redis = await get_redis()
        symbols_raw = await redis.smembers("universe:symbols")
        symbols = [s.decode() if isinstance(s, bytes) else s for s in symbols_raw]
        results = []
        for sym in symbols:
            try:
                cum_rvol = await redis.hget(f"snapshot:{sym}", "cum_rvol")
                ltp      = await redis.hget(f"snapshot:{sym}", "ltp")
                vwap     = await redis.hget(f"snapshot:{sym}", "vwap")
                chg      = await redis.hget(f"snapshot:{sym}", "change_pct")
                if not cum_rvol:
                    continue
                cr    = float(cum_rvol)
                ltp_f = float(ltp or 0)
                vwap_f= float(vwap or 0)
                chg_f = float(chg or 0)
                if cr < 0.1 or ltp_f == 0:
                    continue
                results.append({
                    "symbol":    sym,
                    "cum_rvol":  round(cr, 2),
                    "ltp":       round(ltp_f, 2),
                    "change_pct": round(chg_f, 2),
                    "direction": "BULL" if ltp_f >= vwap_f else "BEAR",
                })
            except Exception:
                continue
        results.sort(key=lambda x: x["cum_rvol"], reverse=True)
        return {"status": "ok", "leaders": results[:10], "total_scanned": len(symbols)}
    except Exception as e:
        logger.error("[api] volume-leaders error: %s", e)
        return {"status": "error", "message": str(e), "leaders": []}


# ===========================================================================
# App lifespan — startup / shutdown
# ===========================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────────
    print("🚀 [API] Lifespan started")
    logger.info("[api_server] Starting up …")
    print("REDIS URL:", cfg.REDIS_URL)
    validate()

    # Launch background tasks
    tasks = [
        asyncio.create_task(broadcast_ticks(),         name="broadcast_ticks"),
        asyncio.create_task(broadcast_signals(),       name="broadcast_signals"),
        asyncio.create_task(broadcast_account(),       name="broadcast_account"),
        asyncio.create_task(broadcast_order_fills(),   name="broadcast_order_fills"),
        asyncio.create_task(refresh_world_indices(),   name="refresh_world_indices"),
    ]
    for task in tasks:
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)

    logger.info("[api_server] All background tasks started.")
    yield

    # ── Shutdown ─────────────────────────────────────────────────────────────
    print("🛑 [API] Lifespan shutdown")
    logger.info("[api_server] Shutting down …")
    for t in list(background_tasks):
        t.cancel()
    await close()


# ===========================================================================
# FastAPI app
# ===========================================================================

app = FastAPI(
    title="Market Pulse Pro v2",
    description="NSE intraday F&O trading dashboard — paper trading engine",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cfg.cors_origins_list or ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ===========================================================================
# Internal Redis helpers
# ===========================================================================

async def _snap(symbol: str) -> dict:
    redis = await get_redis()
    return await redis.hgetall(f"snapshot:{symbol}") or {}


async def _require_snap(symbol: str) -> dict:
    data = await _snap(symbol)
    if not data:
        raise HTTPException(404, f"No snapshot data for symbol '{symbol}'")
    return data


def _sf(d: dict, key: str, default: float = 0.0) -> float:
    return _safe_float(d.get(key), default)


# ===========================================================================
# Market data endpoints
# ===========================================================================

@app.get("/api/snapshot/{symbol}", response_model=SnapshotData)
async def get_snapshot(symbol: str):
    """Full indicator snapshot for a symbol."""
    raw = await _require_snap(symbol)
    return SnapshotData(
        symbol=symbol,
        ltp=_sf(raw, "ltp"),
        ema9=_sf(raw, "ema9"),
        ema16=_sf(raw, "ema16"),
        ema200=_sf(raw, "ema200"),
        atr14=_sf(raw, "atr14"),
        rsi14=_sf(raw, "rsi14", 50.0),
        vwap=_sf(raw, "vwap"),
        vwap_slope=_sf(raw, "vwap_slope"),
        choppiness14=_sf(raw, "choppiness14", 50.0),
        choppiness_class=raw.get("choppiness_class", "NEUTRAL"),
        supertrend_dir=raw.get("supertrend_dir", "BULL"),
        supertrend_band=_sf(raw, "supertrend_band"),
        rolling_1h_high=_sf(raw, "rolling_1h_high"),
        rolling_1h_low=_sf(raw, "rolling_1h_low"),
        orb_high=_sf(raw, "orb_high"),
        orb_low=_sf(raw, "orb_low"),
        consecutive_choppy_candles=int(_sf(raw, "consecutive_choppy_candles")),
        lot_size=int(_sf(raw, "lot_size", 1)),
        sector=raw.get("sector", "UNKNOWN"),
        updated_at=raw.get("updated_at", ""),
        pp=_sf(raw, "pp") or _sf(raw, "pivot_pp"),
        r1=_sf(raw, "r1") or _sf(raw, "pivot_r1"),
        r2=_sf(raw, "r2") or _sf(raw, "pivot_r2"),
        s1=_sf(raw, "s1") or _sf(raw, "pivot_s1"),
        s2=_sf(raw, "s2") or _sf(raw, "pivot_s2"),
        cam_r3=_sf(raw, "cam_r3") or _sf(raw, "pivot_cam_r3"),
        cam_s3=_sf(raw, "cam_s3") or _sf(raw, "pivot_cam_s3"),
        prev_close=_sf(raw, "prev_close"),
    )


@app.get("/api/tick/{symbol}", response_model=TickData)
async def get_tick(symbol: str):
    """Live LTP for a symbol."""
    redis = await get_redis()
    raw = await redis.hgetall(f"tick:{symbol}")
    if not raw:
        raise HTTPException(404, f"No tick data for symbol '{symbol}'")
    return TickData(
        symbol=symbol,
        ltp=_sf(raw, "ltp"),
        volume=int(_sf(raw, "volume")),
        ts=raw.get("ts", ""),
    )


async def _fetch_candles_internal(symbol: str, timeframe: str) -> dict:
    """Shared candle-fetch logic — used by both query-param and path-param endpoints."""
    if timeframe not in {"1m", "5m", "15m", "1hr"}:
        raise HTTPException(400, "timeframe must be one of: 1m, 5m, 15m, 1hr")

    redis = await get_redis()
    key   = f"candles:{timeframe}:{symbol}"
    raw   = await redis.lrange(key, 0, -1)
    logger.debug("Candles fetch key=%s symbol=%s timeframe=%s raw_count=%d", key, symbol, timeframe, len(raw))
    if raw:
        logger.debug("Candles first raw entry key=%s: %s", key, raw[0])
    else:
        logger.debug("Candles key=%s has no raw entries", key)

    # Each entry = JSON-serialized [ts, o, h, l, c, v]
    # Frontend (lightweight-charts) wants objects with unix-second `time`.
    candles = []
    for idx, entry in enumerate(raw):
        try:
            arr = json.loads(entry)
            if not isinstance(arr, list) or len(arr) < 6:
                logger.debug(
                    "Candle parse skipped key=%s index=%d reason=unexpected_format entry=%s",
                    key,
                    idx,
                    entry,
                )
                continue
            ts, o, h, l, c, v = arr[0], arr[1], arr[2], arr[3], arr[4], arr[5]
            # Convert ISO timestamp to UNIX seconds
            if isinstance(ts, str):
                try:
                    dt = datetime.fromisoformat(ts)
                    ts_unix = int(dt.timestamp())
                except Exception:
                    logger.debug(
                        "Candle parse skipped key=%s index=%d reason=timestamp_parse_failed ts=%s",
                        key,
                        idx,
                        ts,
                    )
                    continue
            elif isinstance(ts, (int, float)):
                ts_unix = int(ts)
            else:
                logger.debug(
                    "Candle parse skipped key=%s index=%d reason=unsupported_timestamp_type type=%s",
                    key,
                    idx,
                    type(ts).__name__,
                )
                continue
            candles.append({
                "time":   ts_unix,
                "open":   float(o),
                "high":   float(h),
                "low":    float(l),
                "close":  float(c),
                "volume": float(v),
            })
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            logger.exception(
                "Candle parse error key=%s index=%d entry=%s error=%s",
                key,
                idx,
                entry,
                exc,
            )
            continue

    # Embed pivot data from snapshot so frontend gets everything in one call
    snap_raw = await redis.hgetall(f"snapshot:{symbol}")
    pivots = None
    if snap_raw:
        def _pf(k: str, alt: str = "") -> float:
            v = _sf(snap_raw, k)
            if v == 0.0 and alt:
                v = _sf(snap_raw, alt)
            return v

        pp = _pf("pp", "pivot_pp")
        if pp > 0:
            pivots = {
                "pp": pp,
                "r1": _pf("r1", "pivot_r1"),
                "r2": _pf("r2", "pivot_r2"),
                "s1": _pf("s1", "pivot_s1"),
                "s2": _pf("s2", "pivot_s2"),
                "cam_r3": _pf("cam_r3", "pivot_cam_r3"),
                "cam_s3": _pf("cam_s3", "pivot_cam_s3"),
            }

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "candles": candles,
        "pivots": pivots,
    }


@app.get("/api/candles/{symbol}")
async def get_candles_query(symbol: str, tf: str = "1m"):
    """
    OHLCV candle list — query-param form used by frontend.
    Example: /api/candles/RELIANCE?tf=5m
    """
    return await _fetch_candles_internal(symbol, tf)


@app.get("/api/candles/{symbol}/{timeframe}")
async def get_candles_path(symbol: str, timeframe: str):
    """
    OHLCV candle list — path-param form (legacy / alternate).
    Example: /api/candles/RELIANCE/5m
    """
    return await _fetch_candles_internal(symbol, timeframe)


@app.get("/api/debug/candle-sample/{symbol}")
async def debug_candle_sample(symbol: str):
    redis = await get_redis()
    key = f"candles:1m:{symbol}"
    count = await redis.llen(key)
    first = await redis.lindex(key, 0)
    last = await redis.lindex(key, -1)
    return {
        "key": key,
        "count": count,
        "first_raw": first,
        "last_raw": last,
    }


@app.get("/api/pivots/{symbol}", response_model=PivotData)
async def get_pivots(symbol: str):
    """Classic and Camarilla pivot levels."""
    redis = await get_redis()
    raw = await redis.hgetall(f"pivots:{symbol}")
    if not raw:
        raw = await redis.hgetall(f"snapshot:{symbol}")
    if not raw:
        raise HTTPException(404, f"No pivot data for symbol '{symbol}'")

    def pf(k: str, alt: str = "") -> float:
        v = _sf(raw, k)
        if v == 0.0 and alt:
            v = _sf(raw, alt)
        return v

    pp = pf("pp", "pivot_pp")
    if pp == 0.0:
        raise HTTPException(404, f"No pivot data for symbol '{symbol}'")

    return PivotData(
        pp=pp,
        r1=pf("r1", "pivot_r1"),   r2=pf("r2", "pivot_r2"),
        s1=pf("s1", "pivot_s1"),   s2=pf("s2", "pivot_s2"),
        cam_r1=pf("cam_r1", "pivot_cam_r1"),
        cam_r2=pf("cam_r2", "pivot_cam_r2"),
        cam_r3=pf("cam_r3", "pivot_cam_r3"),
        cam_r4=pf("cam_r4", "pivot_cam_r4"),
        cam_s1=pf("cam_s1", "pivot_cam_s1"),
        cam_s2=pf("cam_s2", "pivot_cam_s2"),
        cam_s3=pf("cam_s3", "pivot_cam_s3"),
        cam_s4=pf("cam_s4", "pivot_cam_s4"),
    )


@app.get("/api/options/{symbol}", response_model=OptionsData)
async def get_options(symbol: str):
    """Live options data and tradability badges for a symbol."""
    redis = await get_redis()
    raw = await redis.hgetall(f"options:summary:{symbol}")
    if not raw:
        return OptionsData(
            atm_strike=0,
            ce_ltp=0.0, pe_ltp=0.0,
            ce_volume_ratio=0.0, pe_volume_ratio=0.0,
            ce_oi_ratio=0.0, pe_oi_ratio=0.0,
            ce_badge=OptionsBadge(badge="UNAVAILABLE", score=0.0),
            pe_badge=OptionsBadge(badge="UNAVAILABLE", score=0.0),
            primary_side="CE",
            options_explosion=False,
        )

    def _badge(prefix: str) -> OptionsBadge:
        return OptionsBadge(
            badge=raw.get(f"{prefix}_badge", "ILLIQUID"),
            score=_sf(raw, f"{prefix}_badge_score"),
            spread_pct=_sf(raw, f"{prefix}_spread_pct") or None,
            abs_slippage=_sf(raw, f"{prefix}_abs_slippage") or None,
        )

    return OptionsData(
        atm_strike=int(_sf(raw, "atm_strike")),
        ce_ltp=_sf(raw, "ce_ltp"),
        pe_ltp=_sf(raw, "pe_ltp"),
        ce_volume_ratio=_sf(raw, "ce_volume_ratio"),
        pe_volume_ratio=_sf(raw, "pe_volume_ratio"),
        ce_oi_ratio=_sf(raw, "ce_oi_ratio"),
        pe_oi_ratio=_sf(raw, "pe_oi_ratio"),
        ce_badge=_badge("ce"),
        pe_badge=_badge("pe"),
        primary_side=raw.get("primary_side", "CE"),
        options_explosion=raw.get("options_explosion", "false").lower() == "true",
    )


@app.get("/api/universe")
async def get_universe():
    """List of all F&O universe symbols with lot sizes and tokens."""
    try:
        symbols   = await get_symbols()
        lot_sizes = await get_lot_sizes()
        from core.universe_builder import get_token_map
        token_map = await get_token_map()
    except RuntimeError as exc:
        raise HTTPException(503, str(exc))

    symbols_with_tokens = sum(1 for s in symbols if token_map.get(s))
    return {
        "count":                len(symbols),
        "symbols_with_tokens":  symbols_with_tokens,
        "symbols_no_tokens":    len(symbols) - symbols_with_tokens,
        "symbols": [
            {
                "symbol":   s,
                "lot_size": lot_sizes.get(s, 1),
                "token":    token_map.get(s, ""),
            }
            for s in symbols
        ],
    }


@app.get("/api/debug/instrument-master")
async def debug_instrument_master():
    """
    DEBUG: Download AngelOne instrument master and return raw samples of
    NSE and NFO entries so we can inspect the actual field structure.
    This is how we figure out why the NSE EQ lookup returns 0 entries.
    """
    import httpx
    INSTRUMENT_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(INSTRUMENT_URL)
        resp.raise_for_status()
        data = resp.json()

    samples = {}
    for inst in data:
        sym = str(inst.get("symbol", ""))
        exch = str(inst.get("exch_seg", ""))
        if "RELIANCE" in sym and exch == "NSE" and "reliance_nse" not in samples:
            samples["reliance_nse"] = inst
        if "TCS" in sym and exch == "NSE" and "tcs_nse" not in samples:
            samples["tcs_nse"] = inst
        if "INFY" in sym and exch == "NSE" and "infy_nse" not in samples:
            samples["infy_nse"] = inst
        if len(samples) == 3:
            break

    nse_types: dict[str, int] = {}
    nse_keys_seen: set[str] = set()
    for inst in data:
        if str(inst.get("exch_seg", "")) != "NSE":
            continue
        itype = str(inst.get("instrumenttype", ""))
        nse_types[itype] = nse_types.get(itype, 0) + 1
        nse_keys_seen.update(inst.keys())

    return {
        "total_instruments":  len(data),
        "samples":            samples,
        "nse_instrumenttype_counts": nse_types,
        "nse_entry_keys":     sorted(nse_keys_seen),
    }


@app.get("/api/debug/live-ticks")
async def debug_live_ticks():
    """
    DEBUG: Check if live ticks are reaching Redis AND whether downstream
    snapshots (built by cruncher) are up to date.
    """
    redis = await get_redis()
    symbols_to_check = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"]
    result = {}
    for sym in symbols_to_check:
        tick = await redis.hgetall(f"tick:{sym}")
        snap = await redis.hgetall(f"snapshot:{sym}")
        result[sym] = {
            "tick":     tick if tick else "NO DATA",
            "snapshot": snap if snap else "NO DATA",
        }

    # Index symbols
    index_state = {}
    for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]:
        tick = await redis.hgetall(f"tick:{sym}")
        snap = await redis.hgetall(f"snapshot:{sym}")
        index_state[sym] = {
            "tick":     tick if tick else "NO DATA",
            "snapshot": snap if snap else "NO DATA",
        }

    # Count various key types
    cursor = 0
    tick_count = 0
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match="tick:*", count=500)
        tick_count += len(keys)
        if cursor == 0:
            break

    cursor = 0
    snap_count = 0
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match="snapshot:*", count=500)
        snap_count += len(keys)
        if cursor == 0:
            break

    cursor = 0
    candle_count = 0
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match="candle:*", count=500)
        candle_count += len(keys)
        if cursor == 0:
            break

    return {
        "redis_key_counts": {
            "tick:*":     tick_count,
            "snapshot:*": snap_count,
            "candle:*":   candle_count,
        },
        "stock_samples":    result,
        "index_state":      index_state,
    }


@app.post("/api/debug/clean-corrupted-keys")
async def debug_clean_corrupted_keys():
    """
    DEBUG: Delete stale/corrupted Redis keys that have wrong types
    from previous schema changes. Returns list of keys deleted.
    """
    redis = await get_redis()
    keys_to_clean = [
        "market:breadth",
        "market:world_indices",
        "ai:premarket",
        "ai:premarket:summary",
    ]
    deleted = []
    for key in keys_to_clean:
        exists = await redis.exists(key)
        if exists:
            await redis.delete(key)
            deleted.append(key)
    return {
        "deleted_keys":    deleted,
        "message":         "Corrupted keys cleaned. Dependent services will repopulate.",
    }


@app.get("/api/debug/clean-corrupted-keys")
async def debug_clean_corrupted_keys_get():
    """
    DEBUG (GET, browser-friendly): same as POST version above.
    Deletes stale/corrupted Redis keys that have wrong types.
    """
    redis = await get_redis()
    keys_to_clean = [
        "market:breadth",
        "market:world_indices",
        "ai:premarket",
        "ai:premarket:summary",
    ]
    deleted = []
    for key in keys_to_clean:
        exists = await redis.exists(key)
        if exists:
            await redis.delete(key)
            deleted.append(key)
    return {
        "deleted_keys":    deleted,
        "message":         "Corrupted keys cleaned. Dependent services will repopulate.",
    }


@app.get("/api/debug/inspect-snapshot/{symbol}")
async def debug_inspect_snapshot(symbol: str):
    """
    DEBUG: Inspect what type a snapshot:{symbol} key is, and show its raw value.
    This tells us if seeder wrote a hash, string, JSON, etc.
    """
    redis = await get_redis()
    key = f"snapshot:{symbol}"

    exists = await redis.exists(key)
    if not exists:
        return {"key": key, "exists": False}

    # Get the type Redis reports
    key_type = await redis.type(key)
    # redis-py may return bytes or str depending on decode_responses
    if isinstance(key_type, bytes):
        key_type = key_type.decode("utf-8", errors="replace")

    raw_value = None
    try:
        if key_type == "string":
            raw_value = await redis.get(key)
            if isinstance(raw_value, bytes):
                raw_value = raw_value.decode("utf-8", errors="replace")
        elif key_type == "hash":
            raw_value = await redis.hgetall(key)
        elif key_type == "list":
            raw_value = await redis.lrange(key, 0, 10)
        elif key_type == "set":
            raw_value = list(await redis.smembers(key))[:20]
        elif key_type == "zset":
            raw_value = await redis.zrange(key, 0, 10, withscores=True)
        else:
            raw_value = f"<unsupported type: {key_type}>"
    except Exception as e:
        raw_value = f"<error reading: {e}>"

    return {
        "key":       key,
        "exists":    True,
        "type":      key_type,
        "value":     raw_value,
    }


# ===========================================================================
# Market breadth and indices
# ===========================================================================

@app.get("/api/breadth", response_model=MarketBreadth)
async def get_market_breadth():
    """Market breadth — advances, declines, sector performance."""
    redis = await get_redis()
    raw: dict = {}
    # Preferred format (current producer): JSON string in market:breadth
    raw_json = await redis.get("market:breadth")
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                raw = parsed
        except (json.JSONDecodeError, TypeError):
            raw = {}
    # Backward compatibility: older deployments may have used Redis HASH
    if not raw:
        try:
            raw = await redis.hgetall("market:breadth")
        except Exception:
            raw = {}
    if not raw:
        raise HTTPException(503, "Market breadth data not yet available")

    # Sector performance stored as individual Redis keys: market:breadth:sector:{SECTOR}
    sector_keys = await redis.keys("market:breadth:sector:*")
    sector_performance: dict[str, float] = {}
    for k in sector_keys:
        sector_name = k.split(":")[-1]
        val = await redis.get(k)
        sector_performance[sector_name] = _safe_float(val)

    advances = int(_sf(raw, "advances"))
    declines  = int(_sf(raw, "declines"))
    ad_ratio  = round(advances / max(declines, 1), 3)

    above_ema200 = int(_sf(raw, "above_ema200"))
    universe_size = int(_sf(raw, "total", 1))

    return MarketBreadth(
        advances=advances,
        declines=declines,
        unchanged=int(_sf(raw, "unchanged")),
        ad_ratio=ad_ratio,
        above_ema200=above_ema200,
        above_ema200_pct=round(above_ema200 / max(universe_size, 1) * 100, 2),
        sector_performance=sector_performance,
        computed_at=raw.get("computed_at", ""),
    )


_INDEX_SYMBOLS = ["NIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]


@app.get("/api/indices", response_model=list[IndexData])
async def get_indices():
    """Nifty50, Sensex, BankNifty, MidcpNifty with PCR."""
    from execution.options_rest import fetch_underlying_ltp
    redis = await get_redis()
    result = []

    for sym in _INDEX_SYMBOLS:
        # LTP fallback chain:
        #   1. Live tick (market hours, WebSocket streaming)
        #   2. Snapshot (seeded morning — for stocks only; indices unseeded)
        #   3. REST fallback (off-hours, or if WS feed dropped)
        # fetch_underlying_ltp caches 30s, safe to call frequently.
        tick = await redis.hgetall(f"tick:{sym}")
        snap = await redis.hgetall(f"snapshot:{sym}")

        ltp = _sf(tick, "ltp") or _sf(snap, "ltp")
        if ltp <= 0:
            try:
                rest_ltp = await fetch_underlying_ltp(sym)
                if rest_ltp and rest_ltp > 0:
                    ltp = rest_ltp
            except Exception:
                pass

        if ltp <= 0:
            continue  # Still nothing — omit tile

        prev_close = _sf(snap, "prev_close")
        if prev_close > 0:
            change_pct = ((ltp - prev_close) / prev_close) * 100
        else:
            change_pct = 0.0

        pcr_raw = await redis.get(f"options:pcr:{sym}")
        pcr     = _safe_float(pcr_raw)

        prev_pcr_raw = await redis.get(f"options:pcr_prev:{sym}")
        prev_pcr     = _safe_float(prev_pcr_raw)

        if pcr > prev_pcr + 0.05:
            pcr_dir = "UP"
        elif pcr < prev_pcr - 0.05:
            pcr_dir = "DOWN"
        else:
            pcr_dir = "FLAT"

        result.append(IndexData(
            symbol=sym,
            name=_INDEX_DISPLAY_NAMES.get(sym, sym),
            ltp=ltp,
            change_pct=round(change_pct, 2),
            prev_close=float(prev_close or 0.0),
            pcr=round(pcr, 3),
            pcr_direction=pcr_dir,
        ))

    return result


@app.get("/api/world-indices")
async def get_world_indices():
    """Dow futures, Nasdaq futures, Hang Seng, SGX Nifty, Crude."""
    redis = await get_redis()
    # Scraper writes to global:indices (list[dict] JSON string)
    # Legacy fallback: market:world_indices (hash or JSON)
    for key in ("global:indices", "market:world_indices"):
        raw_json = await redis.get(key)
        if raw_json:
            try:
                parsed = json.loads(raw_json)
                if isinstance(parsed, list):
                    return {"indices": parsed}
                if isinstance(parsed, dict):
                    if isinstance(parsed.get("indices"), list):
                        return {"indices": parsed["indices"]}
                    return {"indices": list(parsed.values())}
            except json.JSONDecodeError:
                pass

    return {"indices": []}


# ===========================================================================
# Signal endpoints
# ===========================================================================

@app.get("/api/signals", response_model=list[SignalData])
async def get_active_signals():
    """All active signals across the universe, sorted by ICI score descending."""
    redis = await get_redis()
    keys  = await redis.keys("signal:active:*")

    signals = []
    for k in keys:
        try:
            parsed = await _read_signal_payload(redis, k)
            if parsed:
                signals.append(_parse_signal(parsed))
        except Exception:
            continue

    signals.sort(key=lambda s: s.ici_score, reverse=True)
    return signals


@app.get("/api/signals/{symbol}", response_model=list[SignalData])
async def get_symbol_signals(symbol: str):
    """Active signals for a specific symbol."""
    redis = await get_redis()
    keys  = await redis.keys(f"signal:active:{symbol}:*")

    signals = []
    for k in keys:
        try:
            parsed = await _read_signal_payload(redis, k)
            if parsed:
                signals.append(_parse_signal(parsed))
        except Exception:
            continue

    return signals


async def _read_signal_payload(redis, key: str) -> dict:
    """
    Read active signal payload from Redis, supporting both:
      1) STRING JSON (current brain writer), and
      2) HASH fields (legacy format).
    """
    raw_json = await redis.get(key)
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass

    raw_hash = await redis.hgetall(key)
    if raw_hash:
        return raw_hash
    return {}


def _parse_signal(raw: dict) -> SignalData:
    return SignalData(
        symbol=raw.get("symbol", ""),
        signal_type=raw.get("signal_type", ""),
        direction=raw.get("direction", ""),
        ici_score=_sf(raw, "ici_score"),
        ici_grade=raw.get("ici_grade", ""),
        entry_price=_sf(raw, "entry_price"),
        stop_loss=_sf(raw, "stop_loss"),
        choppiness_class=raw.get("choppiness_class", "NEUTRAL"),
        supertrend_dir=raw.get("supertrend_dir", "BULL"),
        detected_at=raw.get("detected_at", ""),
    )


# ===========================================================================
# Paper trading endpoints
# ===========================================================================

@app.get("/api/account", response_model=PaperAccount)
async def get_account():
    """Current paper account state."""
    acct = await get_paper_account()
    return PaperAccount(**acct)


@app.get("/api/trades/open", response_model=list[TradeRecord])
async def get_open_trades_endpoint():
    """All open paper trades."""
    trades = await get_open_trades()
    return [_parse_trade(t) for t in trades]


@app.get("/api/trades/closed")
async def get_closed_trades_endpoint(
    limit: int = 50,
    date_from: str = None,
    date_to: str = None,
):
    """Closed paper trades, most recent first. Optionally filter by exit date (YYYY-MM-DD)."""
    redis = await get_redis()
    pipe = redis.pipeline()
    pipe.lrange("trades:history", 0, max(limit - 1, 0))
    result = await pipe.execute()
    raw_trades = result[0] if result else []
    trades = []
    for raw in raw_trades:
        t = json.loads(raw)
        if t.get("status") != "CLOSED":
            continue
        exit_ts = t.get("exit_ts", "")
        if date_from and exit_ts and exit_ts[:10] < date_from:
            continue
        if date_to and exit_ts and exit_ts[:10] > date_to:
            continue
        trades.append(t)
    return [_parse_trade(t) for t in trades]


@app.post("/api/trades/{trade_id}/close")
async def close_trade_endpoint(trade_id: str, exit_price: Optional[float] = None):
    """
    Manually close an open trade at current market price.
    If exit_price is passed explicitly, it is honored.
    Otherwise the 4-tier pricing resolver handles:
      EQ → live tick / LAST_CLOSE / REST fallback on underlying
      CE/PE → live options tick / REST fallback on option premium
    """
    from execution.order_manager import _get_execution_ltp
    redis = await get_redis()

    if not exit_price:
        raw = await redis.get(f"paper:trade:{trade_id}")
        if not raw:
            raise HTTPException(404, f"Trade '{trade_id}' not found")
        try:
            trade_data = json.loads(raw)
        except json.JSONDecodeError:
            raise HTTPException(500, "Corrupt trade data")

        symbol     = trade_data.get("symbol", "")
        instrument = trade_data.get("instrument", "EQ")
        atm_strike = trade_data.get("atm_strike")
        expiry     = trade_data.get("expiry_date")

        ltp, source = await _get_execution_ltp(symbol, instrument, atm_strike, expiry)
        if ltp <= 0:
            raise HTTPException(
                400,
                f"Cannot close — no valid LTP for {symbol} {instrument} "
                f"strike={atm_strike} expiry={expiry}",
            )
        logger.info(
            "[api_server] Manual close %s using %s LTP ₹%.2f (trade_id=%s)",
            symbol, source, ltp, trade_id,
        )
        exit_price = ltp

    result = await close_trade(trade_id, exit_price, reason="MANUAL_CLOSE")

    if result.get("status") == "ERROR":
        raise HTTPException(400, result.get("reason", "Close failed"))

    return result


def _parse_trade(t: dict) -> TradeRecord:
    # atm_strike is stored as int but may come back as float/string from Redis
    atm_val = t.get("atm_strike")
    atm_int = None
    if atm_val is not None and atm_val != "":
        try:
            atm_int = int(float(atm_val))
        except (TypeError, ValueError):
            atm_int = None

    return TradeRecord(
        id=t.get("id", ""),
        symbol=t.get("symbol", ""),
        direction=t.get("direction", ""),
        signal_type=t.get("signal_type", ""),
        entry_price=_safe_float(t.get("entry_price")),
        stop_loss=_safe_float(t.get("stop_loss")),
        take_profit=t.get("take_profit") and _safe_float(t.get("take_profit")),
        lot_size=int(_safe_float(t.get("lot_size", 1))),
        quantity=int(_safe_float(t.get("quantity", 1))),
        lots=int(_safe_float(t.get("lots"))) if t.get("lots") else None,
        margin_used=_safe_float(t.get("margin_used")),
        ici_score=_safe_float(t.get("ici_score")),
        ici_grade=t.get("ici_grade", ""),
        status=t.get("status", ""),
        entry_ts=t.get("entry_ts", ""),
        exit_price=t.get("exit_price"),
        exit_ts=t.get("exit_ts"),
        pnl_abs=t.get("pnl_abs"),
        pnl_pct=t.get("pnl_pct"),
        exit_reason=t.get("exit_reason"),
        # Options metadata
        instrument=t.get("instrument"),
        atm_strike=atm_int,
        expiry_date=t.get("expiry_date"),
        option_token=t.get("option_token"),
        # Pricing instrumentation
        price_source=t.get("price_source"),
        underlying_at_fill=t.get("underlying_at_fill") and _safe_float(t.get("underlying_at_fill")),
        broker=t.get("broker"),
    )


# ===========================================================================
# Trigger-order and trade-edit endpoints
# ===========================================================================

@app.post("/api/orders")
async def place_order(payload: dict):
    """
    Universal order endpoint.
    trigger_price present → pending trigger order stored in Redis.
    trigger_price null / missing → market order executed immediately.
    """
    return await place_trigger_order(payload)


@app.get("/api/orders/pending")
async def get_pending():
    """All pending trigger orders, newest first."""
    return await get_pending_orders()


@app.delete("/api/orders/{order_id}/pending")
async def cancel_order(order_id: str):
    """Cancel a pending trigger order by ID."""
    result = await cancel_pending_order(order_id)
    if result.get("status") == "ERROR":
        raise HTTPException(404, result.get("reason", "Order not found"))
    return result


@app.patch("/api/trades/{trade_id}")
async def edit_trade(trade_id: str, payload: dict):
    """
    Edit stop-loss or take-profit on an open trade.
    payload: {stop_loss: float} and/or {take_profit: float}
    """
    result = await update_trade_levels(
        trade_id,
        stop_loss=payload.get("stop_loss"),
        take_profit=payload.get("take_profit"),
    )
    if result.get("status") == "ERROR":
        raise HTTPException(400, result.get("reason", "Update failed"))
    return result


@app.get("/api/trades/pending", response_model=list)
async def get_pending_trades():
    """Alias for /api/orders/pending — frontend consistency."""
    return await get_pending_orders()


# ===========================================================================
# Expiry endpoint
# ===========================================================================

@app.get("/api/expiries/{underlying}")
async def get_expiries(underlying: str):
    """
    Returns available expiry dates + suggested ATM strike for index/stock options.

    Reads unified universe: universe:options:{underlying}:expiries
    ATM computed via fallback chain:
      1. Live spot tick (market hours)
      2. Snapshot prev_day.close (stocks, seeded at 8:30 AM)
      3. AngelOne REST fetch (indices, or off-hours for stocks)
    """
    import json as _json
    redis = await get_redis()

    # --- Expiries ---
    expiries = await redis.zrange(f"universe:options:{underlying}:expiries", 0, -1)
    expiries = [e if isinstance(e, str) else e.decode() for e in expiries]

    # --- Spot price: 3-tier fallback ---
    spot_ltp = 0.0

    # 1. Live tick
    try:
        tick = await redis.hgetall(f"tick:{underlying}")
        if tick:
            spot_ltp = float(tick.get("ltp") or 0)
    except Exception:
        pass

    # 2. Seeded snapshot (stocks only — indices aren't seeded)
    if spot_ltp <= 0:
        try:
            # Current runtime format is HASH. Seeder may temporarily leave STRING.
            snap_hash = await redis.hgetall(f"snapshot:{underlying}")
            if snap_hash:
                spot_ltp = float(
                    snap_hash.get("prev_close")
                    or snap_hash.get("last_close")
                    or snap_hash.get("ltp")
                    or 0
                )
            if spot_ltp <= 0:
                snap_raw = await redis.get(f"snapshot:{underlying}")
                if snap_raw:
                    snap_str = snap_raw if isinstance(snap_raw, str) else snap_raw.decode()
                    snap = _json.loads(snap_str)
                    spot_ltp = float(
                        snap.get("prev_close")
                        or snap.get("last_close")
                        or snap.get("ltp")
                        or snap.get("prev_day", {}).get("close")
                        or 0
                    )
        except Exception:
            pass

    # 3. AngelOne REST
    if spot_ltp <= 0:
        try:
            from execution.options_rest import fetch_underlying_ltp
            rest_ltp = await fetch_underlying_ltp(underlying)
            if rest_ltp and rest_ltp > 0:
                spot_ltp = rest_ltp
        except Exception:
            pass

    # --- ATM strike computation ---
    _INTERVALS = {
        "NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50,
        "MIDCPNIFTY": 25, "SENSEX": 100, "BANKEX": 100,
    }
    atm = None
    if spot_ltp > 0:
        if underlying in _INTERVALS:
            # Index: round to fixed strike interval
            interval = _INTERVALS[underlying]
            atm = round(spot_ltp / interval) * interval
        elif expiries:
            # Stock: find nearest available strike in universe for nearest expiry
            try:
                strikes_raw = await redis.zrange(
                    f"universe:options:{underlying}:strikes:{expiries[0]}", 0, -1
                )
                if strikes_raw:
                    strikes = [int(s if isinstance(s, str) else s.decode()) for s in strikes_raw]
                    atm = min(strikes, key=lambda s: abs(s - spot_ltp))
            except Exception:
                pass

    # Compute strike_step from actual universe data (median gap between
    # adjacent strikes in the nearest expiry). Robust against irregular
    # gaps in deep OTM strikes. Same logic works for indices and stocks.
    strike_step = None
    if expiries:
        try:
            strike_raw = await redis.zrange(
                f"universe:options:{underlying}:strikes:{expiries[0]}", 0, -1
            )
            strikes = sorted(
                int(s if isinstance(s, str) else s.decode()) for s in strike_raw
            )
            if len(strikes) >= 2:
                gaps = sorted(strikes[i] - strikes[i - 1] for i in range(1, len(strikes)))
                strike_step = gaps[len(gaps) // 2]  # median
        except Exception:
            pass

    return {
        "underlying": underlying,
        "expiries": expiries,
        "atm": atm,
        "spot": spot_ltp if spot_ltp > 0 else None,
        "strike_step": strike_step,
    }


# ===========================================================================
# AI endpoints
# ===========================================================================

@app.get("/api/ai/premarket")
async def get_premarket_sentiment():
    """Pre-market AI sentiment and top 10 positive/negative stocks."""
    redis = await get_redis()

    # Try legacy key first
    raw = await redis.get("ai:premarket")
    if raw:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass

    # Fall back to ai:trade_list + ai:context (current writer keys)
    trade_raw   = await redis.get("ai:trade_list")
    context_raw = await redis.get("ai:context")

    if not trade_raw:
        raise HTTPException(503, "Pre-market AI analysis not yet available")

    try:
        trade_list = json.loads(trade_raw)
        context    = json.loads(context_raw) if context_raw else {}
    except json.JSONDecodeError:
        raise HTTPException(500, "Corrupt pre-market AI data")

    return {
        "context":      context.get("global_macro", ""),
        "market_bias":  context.get("market_bias", "neutral"),
        "themes":       context.get("themes", []),
        "top_positive": trade_list.get("top_bullish", []),
        "top_negative": trade_list.get("top_bearish", []),
        "generated_at": trade_list.get("generated_at", ""),
    }


@app.get("/api/ai/alignment/{symbol}", response_model=AIAlignment)
async def get_alignment(symbol: str):
    """News vs technicals alignment for a symbol."""
    redis = await get_redis()
    raw: dict = {}
    # Preferred format (current writer): JSON string
    raw_json = await redis.get(f"ai:alignment:{symbol}")
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                raw = parsed
        except (json.JSONDecodeError, TypeError):
            raw = {}
    # Backward compatibility: hash format
    if not raw:
        try:
            raw = await redis.hgetall(f"ai:alignment:{symbol}")
        except Exception:
            raw = {}
    if not raw:
        raise HTTPException(404, f"No AI alignment data for symbol '{symbol}'")

    return AIAlignment(
        symbol=symbol,
        news_sentiment=raw.get("news_sentiment", "NEUTRAL"),
        technical_alignment=raw.get("technical_alignment", "NEUTRAL"),
        confidence=_sf(raw, "confidence"),
        summary=raw.get("summary", ""),
    )


# ===========================================================================
# Health endpoint
# ===========================================================================

@app.get("/api/health")
async def get_health():
    """Full system health — structured node statuses from Redis."""
    redis = await get_redis()

    # Read from Redis health keys written by each node
    feed_health = await redis.hgetall("feed:health")
    opts_health = await redis.hgetall("options_feed:health")
    seeder_status = await redis.get("seeder:status")
    seeder = json.loads(seeder_status) if seeder_status else {}
    account = await redis.hgetall("paper:account")
    open_trades = await redis.smembers("paper:trades:open")
    pending = await redis.smembers("pending:orders")
    ai_data = await redis.get("ai:premarket")
    if not ai_data:
        ai_data = await redis.get("ai:trade_list")
    ai = json.loads(ai_data) if ai_data else {}
    universe_meta = await redis.get("universe:meta")
    meta = json.loads(universe_meta) if universe_meta else {}

    ws_ok = feed_health.get("connected") == "true"
    opts_ok = opts_health.get("connected") == "true"
    seeder_ok = seeder.get("status") == "complete"
    overall = "OK" if (ws_ok and seeder_ok) else "DEGRADED"

    return {
        "overall_status": overall,
        "websockets": {
            "equity_feed_connected":  ws_ok,
            "options_feed_connected": opts_ok,
            "equity_ticks_last_60s":  int(feed_health.get("ticks_last_60s", 0)),
            "options_active_tokens":  int(opts_health.get("active_tokens", 0)),
            "last_tick_ts":           feed_health.get("last_tick_ts", "—"),
        },
        "data": {
            "seeder_status":        seeder.get("status", "not_run"),
            "seeder_completed_at":  seeder.get("completed_at", "—"),
            "universe_symbol_count": meta.get("count", 0),
            "snapshot_freshness":   "OK" if seeder_ok else "STALE",
        },
        "ai": {
            "premarket_run":    bool(ai),
            "last_sentiment_at": ai.get("generated_at", "—"),
            "model":            "openrouter/llama-3.3-70b + gpt-4o-mini",
        },
        "orders": {
            "open_trades":          len(open_trades),
            "pending_orders":       len(pending),
            "paper_account_loaded": bool(account),
        },
        "redis": {
            "connected":   True,
            "aof_enabled": True,
        },
        "market": {
            "status":       feed_health.get("market_status", "UNKNOWN"),
            "last_updated": feed_health.get("updated_at", "—"),
        },
    }


# ===========================================================================
# Admin endpoints
# ===========================================================================

@app.post("/api/admin/run-seeder")
async def manual_run_seeder(force: bool = False):
    """Manually trigger the morning seeder — runs as isolated subprocess."""
    if _is_market_hours_ist() and not force:
        raise HTTPException(
            409,
            "Seeder cannot run during market hours (09:15-15:30 IST). "
            "Pass force=true to override.",
        )
    try:
        import subprocess, sys
        proc = subprocess.Popen(
            [sys.executable, "-m", "scripts.seeder_worker"],
            env={
                **__import__("os").environ,
                "SEEDER_FORCE": "1" if force else "0",
                "SEEDER_STANDALONE": "1",
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        logger.info("[seeder] Started as subprocess PID=%d", proc.pid)
        return {"status": "started", "pid": proc.pid, "message": "Seeder worker running as isolated subprocess"}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/debug/run-seeder")
async def debug_run_seeder(force: bool = False):
    """DEBUG: Trigger morning seeder as isolated subprocess."""
    if _is_market_hours_ist() and not force:
        raise HTTPException(
            409,
            "Seeder cannot run during market hours. Use ?force=true.",
        )
    try:
        import subprocess, sys
        proc = subprocess.Popen(
            [sys.executable, "-m", "scripts.seeder_worker"],
            env={
                **__import__("os").environ,
                "SEEDER_FORCE": "1" if force else "0",
                "SEEDER_STANDALONE": "1",
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        logger.info("[seeder] Started as subprocess PID=%d", proc.pid)
        return {"status": "started", "pid": proc.pid, "message": f"Seeder worker running as subprocess PID={proc.pid}"}
    except Exception as e:
        raise HTTPException(500, str(e))


# ===========================================================================
# Broadcaster tasks (one pub/sub listener shared across all clients)
# ===========================================================================

async def broadcast_ticks() -> None:
    while True:
        try:
            redis = await get_redis()
            pubsub = redis.pubsub()
            await pubsub.subscribe("ticks", "options:ticks")
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    channel = message["channel"]
                    if isinstance(channel, bytes):
                        channel = channel.decode()
                    raw_data = message["data"]
                    if isinstance(raw_data, bytes):
                        raw_data = raw_data.decode()
                    payload = json.loads(raw_data)
                    payload["_source"] = "options" if channel == "options:ticks" else "equity"
                    serialized = json.dumps(payload)
                    dead: set[WebSocket] = set()
                    for ws in tick_clients.copy():
                        try:
                            await ws.send_text(serialized)
                        except Exception:
                            dead.add(ws)
                    # Use in-place update (not rebinding) to avoid making
                    # tick_clients a local variable in this function.
                    tick_clients.difference_update(dead)
                except Exception as e:
                    logger.error("[api_server] broadcast_ticks message processing error: %s", e)
        except Exception as e:
            logger.warning("[api_server] broadcast_ticks pub/sub connection dropped, reconnecting in 2s: %s", e)
            await asyncio.sleep(2)


async def broadcast_signals() -> None:
    print("🟢 [API] broadcast_signals started (resilient mode)")

    while True:
        pubsub = None
        try:
            redis = await get_redis()
            pubsub = redis.pubsub()

            print("⏳ [API] Subscribing...")
            await pubsub.subscribe("trade_execution", "signals_aux")
            print("🟢 [API] Subscribed OK")

            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue

                data = msg["data"]

                if isinstance(data, bytes):
                    data = data.decode()

                if not isinstance(data, str):
                    data = json.dumps(data)

                print(f"🔵 [API] MSG: {data[:100]}")

                await signal_manager.broadcast(data)

        except asyncio.CancelledError:
            print("🛑 [API] Shutdown signal received, exiting loop")
            break

        except Exception as e:
            print(f"🔴 [API] ERROR: {e}")
            _tb.print_exc()

            print("⏳ [API] Reconnecting in 2s...")
            await asyncio.sleep(2)

        finally:
            if pubsub:
                try:
                    await pubsub.close()
                except Exception:
                    pass


async def broadcast_account() -> None:
    global account_clients
    while True:
        try:
            account = await get_paper_account()
            data = json.dumps(account)
            dead: set[WebSocket] = set()
            for ws in account_clients.copy():
                try:
                    await ws.send_text(data)
                except Exception:
                    dead.add(ws)
            account_clients -= dead
        except Exception as e:
            logger.error("[api_server] broadcast_account error: %s", e)
        await asyncio.sleep(10)


async def broadcast_order_fills() -> None:
    """
    Subscribes to ``order:filled`` pub/sub channel and fans out to all
    signal WebSocket clients so the frontend gets instant fill notifications.
    """
    while True:
        try:
            redis  = await get_redis()
            pubsub = redis.pubsub()
            await pubsub.subscribe("order:filled")
            logger.info("[api_server] broadcast_order_fills subscribed to order:filled.")
            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                dead: set[WebSocket] = set()
                for ws in list(signal_manager.active_connections):
                    try:
                        await ws.send_text(message["data"])
                    except Exception:
                        dead.add(ws)
                for ws in dead:
                    signal_manager.disconnect(ws)
        except Exception as exc:
            logger.warning("[api_server] broadcast_order_fills dropped, reconnecting: %s", exc)
            await asyncio.sleep(2)


# ===========================================================================
# WebSocket endpoints — client registration only
# ===========================================================================

@app.websocket("/ws/ticks")
async def ws_ticks(websocket: WebSocket):
    """Register client; ticks are pushed by broadcast_ticks()."""
    await websocket.accept()
    tick_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except Exception:
        tick_clients.discard(websocket)


@app.websocket("/ws/signals")
async def websocket_signals(websocket: WebSocket):
    """Register client; signals are pushed by broadcast_signals()."""
    await signal_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        signal_manager.disconnect(websocket)


@app.websocket("/ws/account")
async def ws_account(websocket: WebSocket):
    """Register client; account state is pushed by broadcast_account()."""
    await websocket.accept()
    account_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except Exception:
        account_clients.discard(websocket)


# ===========================================================================
# Global indices refresh — hourly background task + REST endpoint
# ===========================================================================

async def _global_indices_refresh() -> None:
    """
    Refresh Groww global indices into Redis once per hour during market hours.
    Runs as an asyncio task — uses run_in_executor so the blocking
    requests.Session() call doesn't stall the event loop.

    Schedule:
      8:30 AM  → morning_seeder seeds it first (TTL 3600s)
      9:30 AM  → this task takes over, refreshing every 60 min
      4:30 PM  → stops fetching; Redis key expires naturally
    """
    loop = asyncio.get_event_loop()
    logger.info("[global_indices] background refresh task started (1-hr cadence)")

    while True:
        await asyncio.sleep(3600)   # wait 1 hour between refreshes
        now = _now_ist()
        if now.weekday() < 5 and dtime(8, 0) <= now.time() <= dtime(16, 30):
            try:
                ok = await loop.run_in_executor(
                    None, lambda: _scrape_global_indices(ttl=_GLOBAL_TTL)
                )
                if ok:
                    logger.info("[global_indices] hourly refresh OK")
                else:
                    logger.warning("[global_indices] hourly refresh failed (will retry in 1hr)")
            except Exception as exc:
                logger.error("[global_indices] refresh error: %s", exc)
        else:
            logger.debug("[global_indices] outside market window — skipping refresh")


@app.get("/api/global-indices")
async def get_global_indices():
    """
    Cached global index data (Groww CFD prices, refreshed hourly).

    Response:
    {
        "status":     "ok" | "stale" | "unavailable",
        "source":     "groww_cfd",
        "disclaimer": "CFD prices — market maker prices, not direct exchange feeds",
        "fetched_at": 1714285000,
        "indices": [
            {"name": "GIFT Nifty", "symbol": "SGX NIFTY",
             "ltp": 23954.0, "change": 0.0, "pct": 0.0, "trend": "flat"},
            ...
        ]
    }
    status = "stale"       if data older than 90 min
    status = "unavailable" if Redis key missing (first boot / outside hours)
    """
    redis = await get_redis()
    raw = await redis.get("global:indices")
    ts  = await redis.get("global:indices:ts")

    if not raw:
        return {
            "status":     "unavailable",
            "source":     "groww_cfd",
            "disclaimer": "CFD prices — market maker prices, not direct exchange feeds",
            "fetched_at": None,
            "indices":    [],
        }

    fetched_at = int(ts) if ts else None
    age_s      = (time.time() - fetched_at) if fetched_at else None
    status     = "stale" if (age_s and age_s > 5400) else "ok"

    raw_str = raw if isinstance(raw, str) else raw.decode()
    return {
        "status":     status,
        "source":     "groww_cfd",
        "disclaimer": "CFD prices — market maker prices, not direct exchange feeds",
        "fetched_at": fetched_at,
        "indices":    json.loads(raw_str),
    }


# ===========================================================================
# Scheduler — time-based tasks (asyncio loop, no APScheduler)
# ===========================================================================

async def _scheduler() -> None:
    """
    Lightweight scheduler that fires time-based tasks.
    Checks every 30 seconds to avoid double-firing within the same minute.
    """
    logger.info("[scheduler] Started.")
    fired: dict[str, str] = {}   # task_name → "HH:MM" last fired

    while True:
        now      = datetime.now(_IST)
        time_str = now.strftime("%H:%M")

        async def _fire_once(task_name: str, coro_factory) -> None:
            if fired.get(task_name) == time_str:
                return
            fired[task_name] = time_str
            logger.info("[scheduler] Firing task '%s' at %s IST.", task_name, time_str)
            try:
                await coro_factory()
            except Exception as exc:
                logger.error("[scheduler] Task '%s' failed: %s", task_name, exc, exc_info=True)

        if time_str == "08:30":
            await _fire_once("universe_build", build_universe)

        if time_str == "15:20":
            await _fire_once("eod_close_all", eod_close_all)

        if time_str == "15:25":
            async def _save_final_account():
                redis   = await get_redis()
                account = await get_paper_account()
                await redis.set(
                    f"paper:eod:{date.today().isoformat()}",
                    json.dumps(account),
                )
                logger.info("[scheduler] Final EOD account snapshot saved.")

            await _fire_once("eod_account_snapshot", _save_final_account)

        await asyncio.sleep(30)


# ===========================================================================
# Standalone entry point
# ===========================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "execution.api_server:app",
        host="0.0.0.0",
        port=cfg.port_int,
        log_level=cfg.LOG_LEVEL.lower(),
    )


@app.post("/api/admin/build-universe")
async def manual_build_universe():
    """Manually trigger universe rebuild."""
    try:
        asyncio.create_task(build_universe())
        return {"status": "started", "message": "Universe build running in background"}
    except Exception as e:
        raise HTTPException(500, str(e))


# ===========================================================================
# Serve frontend static files — MUST BE LAST
# ===========================================================================
import os
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

frontend_path = os.path.join(os.path.dirname(__file__), '..', 'frontend')

@app.get('/')
async def serve_root():
    index = os.path.join(frontend_path, 'index.html')
    if os.path.exists(index):
        return FileResponse(index)
    return {'status': 'Market Pulse Pro v2 API'}

@app.get('/index.html')
async def serve_index():
    return FileResponse(os.path.join(frontend_path, 'index.html'))

@app.get('/stock.html')
async def serve_stock():
    return FileResponse(os.path.join(frontend_path, 'stock.html'))


@app.get("/signals-review")
async def signals_review_page():
    from fastapi.responses import FileResponse
    return FileResponse("signals_review.html")


# ═══════════════════════════════════════════════════════════════════
# TEMPORARY STEP-2 DIAGNOSTICS — DELETE AFTER SESSION 2 VERIFIED
# ═══════════════════════════════════════════════════════════════════

@app.get("/api/debug/pricing-test/{symbol}/{instrument}")
async def debug_pricing_test(symbol: str, instrument: str, strike: int = 0, expiry: str = ""):
    """TEMPORARY — test the 4-tier pricing for any symbol/strike/expiry."""
    from execution.order_manager import _get_execution_ltp, _lookup_option_contract_meta

    ltp, source = await _get_execution_ltp(symbol, instrument, strike or None, expiry or None)

    meta = None
    if instrument in ("CE", "PE") and strike and expiry:
        token, tsym, exch = await _lookup_option_contract_meta(symbol, strike, instrument, expiry)
        meta = {"token": token, "tradingsymbol": tsym, "exchange": exch}

    return {
        "symbol": symbol,
        "instrument": instrument,
        "strike": strike,
        "expiry": expiry,
        "ltp": ltp,
        "price_source": source,
        "contract_meta": meta,
    }


# ═══════════════════════════════════════════════════════════════════
# TEMPORARY STEP-1 DIAGNOSTICS — DELETE AFTER VERIFICATION COMPLETE
# ═══════════════════════════════════════════════════════════════════
import traceback as _tb
import json as _json


@app.get("/api/debug/force-universe-build")
async def debug_force_universe_build():
    """Manually trigger the full universe build and return detailed status."""
    try:
        from core.universe_builder import build_universe
        meta = await build_universe()
        return {"status": "SUCCESS", "meta": meta}
    except Exception as exc:
        return {
            "status": "FAILED",
            "error": str(exc),
            "error_type": type(exc).__name__,
            "traceback": _tb.format_exc(),
        }


@app.get("/api/debug/universe-check")
async def debug_universe_check():
    """Inspect the new unified options universe keys."""
    from core.redis_client import get_redis
    redis = await get_redis()

    total_symbols = await redis.scard("universe:options:symbols")
    checks = {}

    for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY",
                "SENSEX", "RELIANCE", "TCS", "INFY", "HDFCBANK"]:
        hash_count = await redis.hlen(f"universe:options:{sym}")
        expiries = await redis.zrange(f"universe:options:{sym}:expiries", 0, -1)

        strike_keys_count = 0
        async for _ in redis.scan_iter(
            match=f"universe:options:{sym}:strikes:*", count=100
        ):
            strike_keys_count += 1

        sample_contract = None
        sample_exchange = None
        sample_class = None
        if hash_count > 0:
            try:
                fields = await redis.hrandfield(
                    f"universe:options:{sym}", count=1, withvalues=True
                )
                if fields and len(fields) >= 2:
                    raw = fields[1] if isinstance(fields[1], str) else fields[1].decode()
                    sample_contract = _json.loads(raw)
                    sample_exchange = sample_contract.get("exchange")
                    sample_class = sample_contract.get("instrument_class")
            except Exception as e:
                sample_contract = f"ERROR: {e}"

        checks[sym] = {
            "contract_count": hash_count,
            "expiry_count": len(expiries),
            "expiries": expiries[:5] if expiries else [],
            "strike_keys_count": strike_keys_count,
            "strike_key_match": strike_keys_count == len(expiries),
            "sample_exchange": sample_exchange,
            "sample_instrument_class": sample_class,
        }

    return {
        "total_symbols_in_master_set": total_symbols,
        "checks": checks,
    }


@app.get("/api/debug/inspect-key/{key:path}")
async def debug_inspect_key(key: str):
    """Check a Redis key's type and sample value — helps debug WRONGTYPE errors."""
    from core.redis_client import get_redis
    redis = await get_redis()
    key_type = await redis.type(key)
    # redis-py may return bytes or str
    key_type_str = key_type if isinstance(key_type, str) else key_type.decode()
    exists = await redis.exists(key)

    sample = None
    if exists:
        try:
            if key_type_str == "string":
                v = await redis.get(key)
                sample = (v if isinstance(v, str) else v.decode())[:200] if v else None
            elif key_type_str == "hash":
                sample = await redis.hrandfield(key, count=1, withvalues=True)
            elif key_type_str == "set":
                sample = list(await redis.srandmember(key, 5))
            elif key_type_str == "list":
                sample = await redis.lrange(key, 0, 4)
            elif key_type_str == "zset":
                sample = await redis.zrange(key, 0, 4, withscores=True)
        except Exception as e:
            sample = f"ERROR reading: {e}"

    return {
        "key": key,
        "exists": bool(exists),
        "type": key_type_str,
        "sample": sample,
    }

@app.get("/api/debug/check-fno-stocks")
async def debug_check_fno_stocks():
    """Diagnose why stock options aren't populating."""
    result = {}

    # Try the import
    try:
        from options_config import FNO_STOCKS
        result["import_status"] = "SUCCESS"
        result["fno_stocks_type"] = type(FNO_STOCKS).__name__
        result["fno_stocks_count"] = len(FNO_STOCKS) if hasattr(FNO_STOCKS, "__len__") else "no len"
        result["fno_stocks_first_10"] = list(FNO_STOCKS)[:10] if FNO_STOCKS else []
    except ImportError as e:
        result["import_status"] = "IMPORT_ERROR"
        result["error"] = str(e)
    except Exception as e:
        result["import_status"] = "OTHER_ERROR"
        result["error"] = str(e)
        result["error_type"] = type(e).__name__

    # Also try alternative locations in case FNO_STOCKS lives elsewhere
    alternatives_tried = {}
    for module_path in ["options_config", "config.options_config", "core.options_config",
                        "execution.options_config", "data_feed.options_config"]:
        try:
            mod = __import__(module_path, fromlist=["*"])
            alternatives_tried[module_path] = {
                "importable": True,
                "has_FNO_STOCKS": hasattr(mod, "FNO_STOCKS"),
                "has_FNO_UNIVERSE": hasattr(mod, "FNO_UNIVERSE"),
                "has_STOCK_UNIVERSE": hasattr(mod, "STOCK_UNIVERSE"),
                "all_uppercase_vars": [v for v in dir(mod) if v.isupper() and not v.startswith("_")],
            }
        except Exception as e:
            alternatives_tried[module_path] = {"importable": False, "error": str(e)}

    result["alternative_paths"] = alternatives_tried

    return result


@app.get("/api/debug/fix-breadth-key")
async def debug_fix_breadth_key():
    """TEMPORARY — delete market:breadth if it has wrong Redis type."""
    from core.redis_client import get_redis
    redis = await get_redis()
    t = await redis.type("market:breadth")
    t_str = t if isinstance(t, str) else t.decode()
    
    if t_str == "none":
        return {"status": "OK", "message": "Key does not exist", "was_type": "none"}
    
    if t_str == "hash":
        return {"status": "OK", "message": "Key is already a hash — no fix needed", "was_type": "hash"}
    
    # Wrong type — delete it
    await redis.delete("market:breadth")
    return {"status": "FIXED", "message": "Deleted wrong-type key", "was_type": t_str}


@app.get("/api/debug/find-login-sites")
async def debug_find_login_sites():
    """Locate every place AngelOne authentication happens in the deployed code."""
    import os
    
    matches = []
    keywords = ["generateSession", "jwtToken", "jwt_token", "getfeedToken",
                "SmartConnect", "get_fresh_session"]
    
    for scan_root in ["/app", "."]:
        if not os.path.isdir(scan_root):
            continue
        for root, dirs, files in os.walk(scan_root):
            dirs[:] = [d for d in dirs if d not in (
                ".git", "__pycache__", ".venv", "venv", "node_modules", "site-packages"
            )]
            for fname in files:
                if not fname.endswith(".py"):
                    continue
                path = os.path.join(root, fname)
                try:
                    with open(path, "r", encoding="utf-8", errors="ignore") as f:
                        for lineno, line in enumerate(f, 1):
                            for kw in keywords:
                                if kw in line:
                                    matches.append({
                                        "file": path.replace("/app/", ""),
                                        "line": lineno,
                                        "keyword": kw,
                                        "text": line.strip()[:160],
                                    })
                                    break
                except Exception:
                    pass
        break
    
    # Group by file for readability
    by_file = {}
    for m in matches:
        by_file.setdefault(m["file"], []).append({
            "line": m["line"], "kw": m["keyword"], "text": m["text"]
        })
    
    return {"login_related_sites_by_file": by_file, "total_hits": len(matches)}


@app.get("/api/debug/check-jwt")
async def debug_check_jwt():
    """TEMPORARY — verify JWT publisher is wired correctly."""
    redis = await get_redis()
    raw = await redis.get("angel:session:jwt")
    if not raw:
        return {
            "jwt_in_redis": False,
            "message": "No JWT — login sites not publishing, OR no service has logged in yet.",
        }
    jwt = raw if isinstance(raw, str) else raw.decode()
    ttl = await redis.ttl("angel:session:jwt")
    return {
        "jwt_in_redis": True,
        "jwt_length": len(jwt),
        "jwt_prefix_20": jwt[:20] + "...",
        "ttl_seconds": ttl,
        "ttl_hours": round(ttl / 3600, 1) if ttl > 0 else None,
    }


@app.get("/api/debug/test-underlying-rest/{symbol}")
async def debug_test_underlying_rest(symbol: str):
    """TEMPORARY — test fetch_underlying_ltp directly with verbose output."""
    import traceback
    result = {"symbol": symbol}

    # Try import — will reveal if fetch_underlying_ltp actually exists now
    try:
        from execution.options_rest import fetch_underlying_ltp, _INDEX_TOKENS
        result["import_ok"] = True
        result["is_index"] = symbol in _INDEX_TOKENS
        result["index_meta"] = _INDEX_TOKENS.get(symbol)
    except ImportError as exc:
        result["import_ok"] = False
        result["import_error"] = str(exc)
        return result  # stop here — function doesn't exist
    except Exception as exc:
        result["import_ok"] = False
        result["error"] = str(exc)
        result["error_type"] = type(exc).__name__
        return result

    # Try calling it
    try:
        ltp = await fetch_underlying_ltp(symbol)
        result["ltp"] = ltp
        result["call_status"] = "SUCCESS" if ltp else "RETURNED_NONE"
    except Exception as exc:
        result["call_status"] = "EXCEPTION"
        result["error"] = str(exc)
        result["error_type"] = type(exc).__name__
        result["traceback"] = traceback.format_exc()

    return result



@app.get("/api/debug/trace-expiries/{underlying}")
async def debug_trace_expiries(underlying: str):
    """Trace exactly what /api/expiries does, step by step."""
    import traceback
    import json as _json
    from core.redis_client import get_redis
    redis = await get_redis()
    trace = {"underlying": underlying, "steps": []}
    
    # Step 1: expiries
    try:
        expiries = await redis.zrange(f"universe:options:{underlying}:expiries", 0, -1)
        expiries = [e if isinstance(e, str) else e.decode() for e in expiries]
        trace["steps"].append({"step": "1_expiries", "count": len(expiries), "first": expiries[:3]})
    except Exception as exc:
        trace["steps"].append({"step": "1_expiries", "error": str(exc)})

    # Step 2: tick
    try:
        tick = await redis.hgetall(f"tick:{underlying}")
        spot_tick = float(tick.get("ltp") or 0) if tick else 0.0
        trace["steps"].append({"step": "2_tick", "tick_exists": bool(tick), "spot_tick": spot_tick})
    except Exception as exc:
        trace["steps"].append({"step": "2_tick", "error": str(exc)})

    # Step 3: snapshot
    try:
        snap_raw = await redis.get(f"snapshot:{underlying}")
        if snap_raw:
            snap = _json.loads(snap_raw if isinstance(snap_raw, str) else snap_raw.decode())
            trace["steps"].append({
                "step": "3_snapshot",
                "exists": True,
                "prev_day_close": snap.get("prev_day", {}).get("close"),
            })
        else:
            trace["steps"].append({"step": "3_snapshot", "exists": False})
    except Exception as exc:
        trace["steps"].append({"step": "3_snapshot", "error": str(exc)})

    # Step 4: REST fallback
    try:
        from execution.options_rest import fetch_underlying_ltp
        rest_ltp = await fetch_underlying_ltp(underlying)
        trace["steps"].append({"step": "4_rest", "ltp": rest_ltp})
    except ImportError as exc:
        trace["steps"].append({"step": "4_rest", "import_error": str(exc)})
    except Exception as exc:
        trace["steps"].append({
            "step": "4_rest",
            "exception": str(exc),
            "traceback": traceback.format_exc(),
        })

    return trace


@app.post("/api/debug/wipe-stale-candles")
async def wipe_stale_candles():
    """
    TEMPORARY — wipe stale candles:*:* STRING keys written by morning_seeder
    (JSON-blob format). candle_builder uses LIST operations (lrange/rpush),
    which fail with WRONGTYPE on these STRING keys.

    Safe: only deletes STRING-type keys. Any existing LIST candles are preserved.
    After wipe, candle_builder creates fresh LIST keys on next candle close.
    """
    redis = await get_redis()
    deleted = {"candles_1m": 0, "candles_5m": 0, "candles_15m": 0, "errors": 0}

    patterns = [
        ("candles:1m:*", "candles_1m"),
        ("candles:5m:*", "candles_5m"),
        ("candles:15m:*", "candles_15m"),
    ]

    for pattern, bucket in patterns:
        async for key in redis.scan_iter(match=pattern, count=100):
            try:
                key_str = key if isinstance(key, str) else key.decode()
                key_type = await redis.type(key_str)
                key_type_str = key_type if isinstance(key_type, str) else key_type.decode()
                if key_type_str == "string":
                    await redis.delete(key_str)
                    deleted[bucket] += 1
            except Exception:
                deleted["errors"] += 1

    return deleted


@app.get("/api/debug/index-tokens")
async def debug_index_tokens():
    """Temporary debug endpoint — check if index tokens resolved correctly."""
    try:
        redis = await get_redis()
        tokens = await redis.hgetall("index:tokens")
        meta = {}
        for symbol in tokens:
            m = await redis.hgetall(f"index:meta:{symbol}")
            meta[symbol] = m
        return {
            "index_tokens": tokens,
            "index_meta": meta,
            "count": len(tokens)
        }
    except Exception as e:
        return {"error": str(e), "index_tokens": {}, "count": 0}

@app.get("/api/debug/index-candles")
async def debug_index_candles():
    redis = await get_redis()
    
    key1 = await redis.llen("candles:1m:NIFTY")
    key2 = await redis.llen("candles:NIFTY:1m")
    key3 = await redis.llen("candles:1m:NIFTY50")
    
    return {
        "candles:1m:NIFTY":   key1,
        "candles:NIFTY:1m":   key2,
        "candles:1m:NIFTY50": key3,
    }



@app.get("/api/debug/scan-signals")
async def debug_scan_signals():
    """Fast signal scanner - scans 3 symbols for diagnosis, then full universe."""
    from strategy_brain.signal_engines import scan_all_signals
    import traceback

    redis = await get_redis()

    # Fast diagnostic - just RELIANCE
    test_sym = "RELIANCE"
    snap = await redis.hgetall(f"snapshot:{test_sym}") or {}
    
    diag = {
        "symbol": test_sym,
        "has_snapshot": bool(snap),
        "ltp": snap.get("ltp"),
        "supertrend_dir": snap.get("supertrend_dir"),
        "rsi14": snap.get("rsi14"),
        "orb_high": snap.get("orb_high"),
        "choppiness_class": snap.get("choppiness_class"),
        "candles_1m": await redis.llen(f"candles:1m:{test_sym}"),
    }

    # Scan just 3 symbols first
    test_scan = []
    for sym in [test_sym, "HDFCBANK", "INFY"]:
        try:
            sigs = await scan_all_signals(sym)
            test_scan.append({"symbol": sym, "signals": sigs, "count": len(sigs) if sigs else 0})
        except Exception as e:
            test_scan.append({"symbol": sym, "error": traceback.format_exc()[-500:]})

    return {
        "diagnostic": diag,
        "test_scan_3_symbols": test_scan,
        "note": "Check test_scan_3_symbols for errors. If signals=[] with no error, signal conditions not met (market closed/no ORB data)."
    }


@app.get("/api/debug/scan-signals-full")
async def debug_scan_signals_full():
    """Full universe scan - may be slow."""
    from strategy_brain.signal_engines import scan_all_signals
    from core.universe_builder import get_symbols

    redis = await get_redis()
    results = []
    symbols = await get_symbols()
    last_candle_date = ""

    for symbol in symbols:
        try:
            signals = await scan_all_signals(symbol)
            if signals:
                snap = await redis.hgetall(f"snapshot:{symbol}")
                updated_at = snap.get("updated_at", "")
                if updated_at and (not last_candle_date or updated_at > last_candle_date):
                    last_candle_date = updated_at
                for signal in signals:
                    results.append({
                        "symbol":           symbol,
                        "signal_type":      signal.get("type", ""),
                        "direction":        signal.get("direction", ""),
                        "ltp":              snap.get("ltp", 0),
                        "prev_close":       snap.get("prev_close", 0),
                        "supertrend_dir":   snap.get("supertrend_dir", ""),
                        "choppiness_class": snap.get("choppiness_class", ""),
                        "rsi14":            snap.get("rsi14", 0),
                        "ema9":             snap.get("ema9", 0),
                        "pp":               snap.get("pp", 0),
                        "r1":               snap.get("r1", 0),
                        "s1":               snap.get("s1", 0),
                        "sector":           snap.get("sector", ""),
                    })
        except Exception:
            continue

    grouped = {}
    for r in results:
        grouped.setdefault(r["signal_type"], []).append(r)

    return {
        "scanned": len(symbols),
        "signals_found": len(results),
        "last_candle_date": last_candle_date,
        "grouped": grouped,
        "flat": results,
    }


@app.get("/api/debug/snapshot-scan")
async def snapshot_scan():
    from core.universe_builder import get_symbols
    redis = await get_redis()
    symbols = await get_symbols()
    results = {
        "supertrend_bull": [], "supertrend_bear": [],
        "above_r1": [], "above_vwap": [],
        "rsi_momentum": [], "choppiness_trending": [],
        "supertrend_flip_candidates": [], "summary": {}
    }
    for symbol in symbols:
        try:
            snap = await redis.hgetall(f"snapshot:{symbol}")
            if not snap:
                continue
            ltp        = float(snap.get("ltp") or snap.get("prev_close") or 0)
            prev_close = float(snap.get("prev_close") or 0)
            r1         = float(snap.get("r1") or 0)
            pp         = float(snap.get("pp") or 0)
            s1         = float(snap.get("s1") or 0)
            vwap       = float(snap.get("vwap") or 0)
            rsi14      = float(snap.get("rsi14") or 0)
            ema9       = float(snap.get("ema9") or 0)
            ema200     = float(snap.get("ema200") or 0)
            st_dir     = snap.get("supertrend_dir", "")
            chop_class = snap.get("choppiness_class", "")
            vwap_slope = float(snap.get("vwap_slope") or 0)
            st_band    = float(snap.get("supertrend_band") or 0)
            atr14      = float(snap.get("atr14") or 1)
            price = ltp if ltp > 0 else prev_close
            if price == 0:
                continue
            row = {
                "symbol": symbol, "price": round(price, 2),
                "rsi14": round(rsi14, 1), "st_dir": st_dir,
                "chop": chop_class, "r1": round(r1, 2),
                "pp": round(pp, 2), "s1": round(s1, 2),
                "vwap": round(vwap, 2), "ema9": round(ema9, 2),
                "ema200": round(ema200, 2), "st_band": round(st_band, 2),
            }
            if st_dir == "BULL":
                results["supertrend_bull"].append(row)
            if st_dir == "BEAR":
                results["supertrend_bear"].append(row)
            if r1 > 0 and price > r1:
                results["above_r1"].append(row)
            if vwap > 0 and price > vwap:
                results["above_vwap"].append(row)
            if 55 <= rsi14 <= 68 and st_dir == "BULL":
                results["rsi_momentum"].append({**row, "direction": "LONG"})
            elif 32 <= rsi14 <= 45 and st_dir == "BEAR":
                results["rsi_momentum"].append({**row, "direction": "SHORT"})
            if chop_class == "TRENDING":
                results["choppiness_trending"].append(row)
            if st_band > 0 and atr14 > 0:
                if st_dir == "BULL":
                    dist = price - st_band
                else:
                    dist = st_band - price
                if 0 < dist < atr14 * 0.5:
                    results["supertrend_flip_candidates"].append({
                        **row, "dist_to_flip": round(dist, 2)
                    })
        except Exception:
            continue
    for key in results:
        if key != "summary" and isinstance(results[key], list):
            results[key].sort(key=lambda x: x.get("rsi14", 0), reverse=True)
    results["summary"] = {
        "total_scanned": len(symbols),
        "supertrend_bull": len(results["supertrend_bull"]),
        "supertrend_bear": len(results["supertrend_bear"]),
        "above_r1": len(results["above_r1"]),
        "above_vwap": len(results["above_vwap"]),
        "rsi_momentum_long": len([x for x in results["rsi_momentum"] if x.get("direction") == "LONG"]),
        "rsi_momentum_short": len([x for x in results["rsi_momentum"] if x.get("direction") == "SHORT"]),
        "choppiness_trending": len(results["choppiness_trending"]),
        "supertrend_flip_watch": len(results["supertrend_flip_candidates"]),
    }
    return results
