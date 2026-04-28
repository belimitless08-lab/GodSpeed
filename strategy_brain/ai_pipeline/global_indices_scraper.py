"""
global_indices_scraper.py
─────────────────────────────────────────────────────────────────────────────
Scrapes Groww's /indices/global-indices page via __NEXT_DATA__ JSON.

Confirmed key path (verified from live page source 27-Apr-2026):
  props
    └─ pageProps
         └─ data
              └─ aggregatedGlobalInstrumentDto   ← list of index objects
                   └─ each item:
                        ├─ instrumentDetailDto.name          (display name)
                        ├─ instrumentDetailDto.symbol        (ticker)
                        └─ livePriceDto.value / dayChange / dayChangePerc

CFD disclaimer (Groww's own note):
  Values are Contract for Difference prices from market makers,
  NOT direct exchange feeds. Use as macro context only.

Indices available from Groww (verified 27-Apr-2026):
  GIFT NIFTY, Dow, Dow Futures, S&P, Nikkei, Hang Seng, DAX, CAC, KOSPI, FTSE 100
─────────────────────────────────────────────────────────────────────────────
"""

import re
import json
import time
import logging
import requests

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

URL = "https://groww.in/indices/global-indices"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://groww.in/",
}

# Groww internal name (uppercased + stripped) → clean frontend display name
# Source: instrumentDetailDto.name field from verified __NEXT_DATA__
NAME_MAP = {
    "GIFT NIFTY":  "GIFT Nifty",
    "DOW":         "Dow Jones",
    "DOW FUTURES": "Dow Futures",
    "S&P":         "S&P 500",
    "NIKKEI":      "Nikkei 225",   # Groww sends "NIKKEI " with trailing space — strip() handles it
    "HANG SENG":   "Hang Seng",
    "DAX":         "DAX",
    "CAC":         "CAC 40",
    "KOSPI":       "KOSPI",
    "FTSE 100":    "FTSE 100",
}

# Display order for the frontend strip
PINNED = [
    "GIFT Nifty",
    "Dow Jones",
    "S&P 500",
    "Nikkei 225",
    "Hang Seng",
    "DAX",
    "FTSE 100",
]

REDIS_KEY      = "global:indices"
REDIS_KEY_TS   = "global:indices:ts"
REDIS_TTL      = 310        # 5-min refresh cadence + 10s buffer
REDIS_TTL_SEED = 3600       # morning seeder: 1-hour TTL at 8:30 AM


# ── Core scrape ──────────────────────────────────────────────────────────────

def scrape_global_indices() -> list[dict]:
    """
    Fetch Groww global indices page and return parsed list.

    Returns:
        [
            {
                "name":   "GIFT Nifty",
                "symbol": "SGX NIFTY",
                "ltp":    23954.0,
                "change": 0.0,
                "pct":    0.0,
                "trend":  "up" | "down" | "flat"
            },
            ...
        ]

    Raises:
        requests.HTTPError  on non-200 HTTP response
        ValueError          if __NEXT_DATA__ tag or key path is missing
    """
    # Dedicated session — no cookie bleed from stock-news Groww scraper
    session = requests.Session()
    session.headers.update(HEADERS)

    resp = session.get(URL, timeout=15)
    resp.raise_for_status()

    # Extract __NEXT_DATA__ JSON blob
    m = re.search(
        r'<script id="__NEXT_DATA__" type="application/json"[^>]*>(.*?)</script>',
        resp.text,
        re.DOTALL,
    )
    if not m:
        raise ValueError(
            "__NEXT_DATA__ tag not found — Groww may have changed page structure"
        )

    raw = json.loads(m.group(1))

    # Walk the confirmed key path — no guessing
    try:
        instruments = raw["props"]["pageProps"]["data"]["aggregatedGlobalInstrumentDto"]
    except KeyError as e:
        raise ValueError(
            f"Key path broken at {e}. "
            f"Top-level keys: {list(raw.get('props', {}).get('pageProps', {}).keys())}"
        )

    results = []
    for item in instruments:
        try:
            detail = item["instrumentDetailDto"]
            price  = item["livePriceDto"]

            raw_name = (detail.get("name") or "").strip().upper()
            display  = NAME_MAP.get(raw_name, detail.get("name", raw_name).title())
            symbol   = detail.get("symbol", "")

            ltp    = float(price.get("value")         or 0)
            change = float(price.get("dayChange")     or 0)
            pct    = float(price.get("dayChangePerc") or 0)

            # Round: most values like 0.97387 mean 0.97% — already correct scale
            # Very small values like -0.00277958 round to 4dp for readability
            pct_rounded = round(pct, 4) if abs(pct) < 0.01 else round(pct, 2)

            trend = "up" if pct > 0 else ("down" if pct < 0 else "flat")

            results.append({
                "name":   display,
                "symbol": symbol,
                "ltp":    round(ltp, 2),
                "change": round(change, 2),
                "pct":    pct_rounded,
                "trend":  trend,
            })

        except Exception as e:
            logger.warning(f"[global_indices] skipping malformed entry: {e} | raw={item}")
            continue

    logger.info(f"[global_indices] fetched {len(results)} indices successfully")
    return results


# ── Redis writer ─────────────────────────────────────────────────────────────

def scrape_and_store(ttl: int = REDIS_TTL) -> bool:
    """
    Scrape Groww and write result to Redis using its own sync connection.

    Args:
        ttl: key TTL in seconds
             REDIS_TTL (310)       → hourly background refresh
             REDIS_TTL_SEED (3600) → morning seeder at 8:30 AM

    Returns:
        True on success, False on any failure (error is logged).
    """
    import os
    import redis as _redis_sync
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    r = _redis_sync.from_url(redis_url)
    try:
        data    = scrape_global_indices()
        payload = json.dumps(data)
        r.setex(REDIS_KEY,    ttl, payload)
        r.setex(REDIS_KEY_TS, ttl, str(int(time.time())))
        return True
    except Exception as e:
        logger.error(f"[global_indices] scrape_and_store failed: {e}")
        return False
    finally:
        r.close()
