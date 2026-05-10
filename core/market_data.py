"""
core/market_data.py
====================
Shared market intelligence: PCR (NIFTY + BANKNIFTY) and India VIX from NSE.

Used by:
  scripts/morning_seeder.py  → 8:30 AM before AI pipeline (Friday data)
  strategy_brain/brain.py    → every 60 min during live market hours

Fetch functions are synchronous (requests).
Call write_market_intelligence() from async context — it handles threading.
"""

import asyncio
import json
import logging
import time

import requests

log = logging.getLogger(__name__)

_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/option-chain",
}


def _make_nse_session() -> requests.Session:
    """Create a requests session with NSE cookies. Required before API calls."""
    s = requests.Session()
    s.headers.update(_NSE_HEADERS)
    s.get("https://www.nseindia.com/", timeout=10)
    return s


def fetch_pcr(symbol: str, session: requests.Session) -> float:
    """
    Fetch PCR for NIFTY or BANKNIFTY from NSE option chain API.
    Works on weekends — returns Friday's closing data.

    PCR interpretation (contrarian, standard for Indian F&O intraday):
      High PCR = smart money hedging longs with puts → bullish underlying
      Low PCR  = aggressive call buying → overextended, potential reversal
    """
    try:
        url  = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
        resp = session.get(url, timeout=15)
        data = resp.json()
        records = data.get("records", {}).get("data", [])
        ce_oi = sum(r.get("CE", {}).get("openInterest", 0) for r in records if "CE" in r)
        pe_oi = sum(r.get("PE", {}).get("openInterest", 0) for r in records if "PE" in r)
        return round(pe_oi / ce_oi, 3) if ce_oi > 0 else 0.0
    except Exception as exc:
        log.warning("[market_data] PCR fetch failed for %s: %s", symbol, exc)
        return 0.0


def fetch_vix(session: requests.Session) -> float:
    """
    Fetch India VIX from NSE allIndices API.
    Works on weekends — returns Friday's closing VIX.
    """
    try:
        resp = session.get(
            "https://www.nseindia.com/api/allIndices", timeout=15
        )
        data = resp.json()
        for item in data.get("data", []):
            if "VIX" in str(item.get("symbol", "")).upper():
                return round(float(item.get("last") or 0), 2)
        return 0.0
    except Exception as exc:
        log.warning("[market_data] VIX fetch failed: %s", exc)
        return 0.0


def combined_sentiment(pcr: float, prev_pcr: float, vix: float) -> str:
    """
    Combined PCR + VIX market sentiment for NIFTY intraday F&O.

    PCR contrarian interpretation (used by Sensibull, NiftyTrader, Indian F&O desks):
      High PCR = smart money hedging longs → bullish
      Low PCR  = call speculation → overextended

    VIX thresholds (India VIX):
      < 13  = Low fear, calm market
      13-18 = Normal range
      > 18  = Elevated uncertainty
    """
    if pcr <= 0:
        return "NEUTRAL"
    rising  = pcr > prev_pcr + 0.03
    falling = pcr < prev_pcr - 0.03
    high_vix = vix > 18
    low_vix  = 0 < vix < 13

    if pcr > 1.5:                          return "EXTREME_BULLISH"
    if pcr < 0.5:                          return "EXTREME_BEARISH"
    if pcr > 1.2 and low_vix:             return "BULLISH"
    if pcr > 1.2 and high_vix:            return "REVERSAL_WATCH"
    if pcr < 0.7 and high_vix:            return "BEARISH"
    if pcr < 0.8 and low_vix:             return "NEUTRAL_CAUTION"
    if pcr > 1.0 and rising:              return "BULLISH_LEAN"
    if pcr < 0.9 and falling:             return "BEARISH_LEAN"
    return "NEUTRAL"


def _fetch_all_sync() -> dict:
    """
    Fetch PCR (NIFTY + BANKNIFTY) and VIX synchronously.
    Runs in a thread via asyncio.to_thread() — never call directly from async.

    Returns dict with nifty_pcr, banknifty_pcr, vix, sentiment.
    Returns zeros on failure — caller should check before writing.
    """
    result = {"nifty_pcr": 0.0, "banknifty_pcr": 0.0, "vix": 0.0, "sentiment": "NEUTRAL"}
    try:
        session = _make_nse_session()

        nifty_pcr = fetch_pcr("NIFTY", session)
        log.info("[market_data] NIFTY PCR=%.3f", nifty_pcr)
        time.sleep(1.5)  # polite delay between NSE API calls

        bank_pcr = fetch_pcr("BANKNIFTY", session)
        log.info("[market_data] BANKNIFTY PCR=%.3f", bank_pcr)
        time.sleep(1.5)

        vix = fetch_vix(session)
        log.info("[market_data] India VIX=%.2f", vix)

        pcr_for_sentiment = nifty_pcr if nifty_pcr > 0 else 0.0
        sentiment = combined_sentiment(pcr_for_sentiment, 0.0, vix)

        result = {
            "nifty_pcr":     nifty_pcr,
            "banknifty_pcr": bank_pcr,
            "vix":           vix,
            "sentiment":     sentiment,
        }
    except Exception as exc:
        log.error("[market_data] _fetch_all_sync failed: %s", exc)
    return result


async def write_market_intelligence(redis_client) -> dict:
    """
    Fetch PCR + VIX and write to Redis. Async-safe.

    Fetching runs in a thread (sync requests).
    Redis writes are async.

    Call from seeder: await write_market_intelligence(redis)
    Call from brain:  await write_market_intelligence(redis)

    Redis keys written (no TTL — brain overwrites with 7200s TTL later):
        options:pcr:NIFTY
        options:pcr_prev:NIFTY
        options:pcr:BANKNIFTY
        options:pcr_prev:BANKNIFTY
        market:vix
        market:pcr_sentiment
        market:intelligence   ← JSON consumed by AI context engine
    """
    data = await asyncio.to_thread(_fetch_all_sync)

    try:
        nifty_prev_raw = await redis_client.get("options:pcr:NIFTY")
        bank_prev_raw  = await redis_client.get("options:pcr:BANKNIFTY")
        nifty_prev = float(nifty_prev_raw) if nifty_prev_raw else 0.0
        bank_prev  = float(bank_prev_raw)  if bank_prev_raw  else 0.0

        # Recompute sentiment with prev values for direction awareness
        sentiment = combined_sentiment(data["nifty_pcr"], nifty_prev, data["vix"])
        data["sentiment"] = sentiment

        if data["nifty_pcr"] > 0:
            await redis_client.set("options:pcr_prev:NIFTY", str(nifty_prev))
            await redis_client.set("options:pcr:NIFTY",      str(data["nifty_pcr"]))
        if data["banknifty_pcr"] > 0:
            await redis_client.set("options:pcr_prev:BANKNIFTY", str(bank_prev))
            await redis_client.set("options:pcr:BANKNIFTY",      str(data["banknifty_pcr"]))
        if data["vix"] > 0:
            await redis_client.set("market:vix", str(data["vix"]))

        await redis_client.set("market:pcr_sentiment", sentiment)
        await redis_client.set("market:intelligence", json.dumps({
            "nifty_pcr":     data["nifty_pcr"],
            "banknifty_pcr": data["banknifty_pcr"],
            "vix":           data["vix"],
            "sentiment":     sentiment,
        }))

        log.info(
            "[market_data] ✅ Written — NIFTY PCR=%.3f BN PCR=%.3f VIX=%.1f → %s",
            data["nifty_pcr"], data["banknifty_pcr"], data["vix"], sentiment,
        )
    except Exception as exc:
        log.error("[market_data] Redis write failed: %s", exc)

    return data
