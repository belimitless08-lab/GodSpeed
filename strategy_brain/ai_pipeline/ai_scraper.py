"""
strategy_brain/ai_pipeline/ai_scraper.py
==========================================
News scraper for Market Pulse Pro v2 AI pipeline.

Two data sources:
  1. Groww stock pages — extracts newsData from __NEXT_DATA__ JSON (proven SSR)
     URL: https://groww.in/stocks/{search_id}
     Key path: props.pageProps.stockData.newsData
     
  2. Google News RSS — 4 feeds for broad market/macro headlines

Flow (called by morning_seeder.py at 8:00 AM):
  Step 0: Resolve search_ids for all 213 symbols via Groww search API
          → cache Redis: ai:search_id:{symbol} TTL 7 days
          → only resolves symbols with missing/expired cache (day 1: ~4min, subsequent: ~30s)
  Step 1: Scrape market headlines (4 Google News RSS feeds) → ai:news:market
  Step 2: Scrape per-stock news (213 Groww pages) → ai:news:stock:{symbol}

Key design decisions:
  - Dedicated cookie-free session per Groww request (prevents cookie poisoning)
  - Rotating 20-UA pool (same as proven old project)
  - 1s delay between stock requests (anti-blocking)
  - recency filter: discard headlines > 48 hours old
  - word-overlap dedup for market headlines
  - Google Finance fallback when Groww returns nothing
  - partial save to Redis every 30 stocks
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Optional

import requests

from core.redis_client import get_redis as _get_redis_async
from strategy_brain.ai_pipeline.ai_config import (
    UNIVERSE, get_company_names, is_headline_useful,
    MIN_HEADLINE_LENGTH, MAX_HEADLINE_AGE_HOURS,
    REDIS_KEYS, REDIS_TTL,
)

logger = logging.getLogger(__name__)

# ===========================================================================
# Constants
# ===========================================================================

IST = timezone(timedelta(hours=5, minutes=30))

# Groww search API — dynamic search_id resolution
GROWW_SEARCH_URL = (
    "https://groww.in/v1/api/search/v1/entity"
    "?page=0&query={symbol}&size=1&web=true"
)

# Groww stock page — __NEXT_DATA__ extraction
GROWW_STOCK_URL = "https://groww.in/stocks/{search_id}/market-news"

# Google Finance fallback (SSR, news in HTML)
GOOGLE_FINANCE_URL = "https://www.google.com/finance/quote/{symbol}:NSE?hl=en"

# Google News RSS feeds for market-level headlines
MARKET_RSS_FEEDS = [
    {
        "name": "google_market",
        "url": "https://news.google.com/rss/search?q=indian+stock+market+today+nifty+sensex&hl=en-IN&gl=IN&ceid=IN:en",
    },
    {
        "name": "google_economy",
        "url": "https://news.google.com/rss/search?q=rbi+policy+india+economy+inflation&hl=en-IN&gl=IN&ceid=IN:en",
    },
    {
        "name": "google_global",
        "url": "https://news.google.com/rss/search?q=global+markets+asia+fed+oil+crude&hl=en-IN&gl=IN&ceid=IN:en",
    },
    {
        "name": "google_fii",
        "url": "https://news.google.com/rss/search?q=foreign+institutional+investors+india+FII+DII&hl=en-IN&gl=IN&ceid=IN:en",
    },
]

MAX_HEADLINES_PER_STOCK = 5
MAX_MARKET_HEADLINES    = 25
SAVE_PARTIAL_EVERY      = 30    # write to Redis every N stocks
SESSION_ROTATE_EVERY    = 50    # rotate GF/RSS session every N stocks
REQUEST_DELAY_S         = 1.0   # between stock page requests
STALE_HOURS             = MAX_HEADLINE_AGE_HOURS

# ===========================================================================
# User Agent Pool (20 realistic UAs from old project)
# ===========================================================================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (iPad; CPU OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]


# ===========================================================================
# Session Factories
# ===========================================================================

def _make_groww_session() -> requests.Session:
    """
    Dedicated cookie-free session for Groww.
    Never reuse this for Google Finance — prevents cookie poisoning.
    """
    s = requests.Session()
    s.cookies.clear()
    adapter = requests.adapters.HTTPAdapter(pool_connections=2, pool_maxsize=4)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _make_gf_session() -> requests.Session:
    """General session for Google Finance / RSS."""
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _groww_headers() -> dict:
    return {
        "User-Agent":        random.choice(USER_AGENTS),
        "Accept":            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":   "en-IN,en;q=0.9,hi;q=0.8",
        "Accept-Encoding":   "gzip, deflate",
        "Connection":        "keep-alive",
        "Referer":           "https://groww.in/stocks/",
        "DNT":               "1",
    }


def _json_headers() -> dict:
    return {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "application/json",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer":         "https://groww.in/",
    }


def _rss_headers() -> dict:
    return {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
    }


def _gf_headers() -> dict:
    return {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer":         "https://www.google.com/finance/",
        "DNT":             "1",
    }


# ===========================================================================
# Timestamp Parsing & Recency
# ===========================================================================

_RELATIVE_RE = re.compile(
    r"(\d+)\s+(second|minute|hour|day|week|month)s?\s+ago", re.IGNORECASE
)


def _parse_iso_age_hours(iso_str: str) -> Optional[float]:
    """Parse ISO 8601 datetime string → age in hours."""
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
        return max(age, 0.0)
    except Exception:
        return None


def _parse_rss_age_hours(pubdate: str) -> Optional[float]:
    """Parse RFC 822 RSS pubDate → age in hours."""
    if not pubdate:
        return None
    try:
        dt = parsedate_to_datetime(pubdate)
        age = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
        return max(age, 0.0)
    except Exception:
        return None


def _is_stale(age_hours: Optional[float]) -> bool:
    if age_hours is None:
        return False    # no parseable date → keep (assume recent)
    return age_hours > STALE_HOURS


# ===========================================================================
# Step 0 — Resolve search_ids from Groww search API
# ===========================================================================

def resolve_search_id(symbol: str, session: requests.Session) -> Optional[str]:
    """
    Call Groww search API to get the search_id for a symbol.
    Returns e.g. "coal-india-ltd" or None on failure.
    """
    url = GROWW_SEARCH_URL.format(symbol=symbol)
    try:
        resp = session.get(url, headers=_json_headers(), timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        items = data.get("content") or []
        for item in items:
            sid = item.get("search_id") or item.get("searchId")
            if sid:
                return sid
        return None
    except Exception as e:
        logger.debug("[ai_scraper] search_id resolution failed for %s: %s", symbol, e)
        return None


def resolve_all_search_ids(redis_client, symbols: list[str]) -> dict[str, str]:
    """
    Resolve search_ids for all symbols.
    Uses Redis cache (TTL 7 days) — only fetches missing/expired ones.
    Returns {symbol: search_id}.
    """
    result: dict[str, str] = {}
    missing: list[str] = []

    # Check cache first
    for symbol in symbols:
        key = REDIS_KEYS["search_id"].format(symbol=symbol)
        cached = redis_client.get(key)
        if cached:
            sid = cached if isinstance(cached, str) else cached.decode()
            result[symbol] = sid
        else:
            missing.append(symbol)

    if not missing:
        logger.info("[ai_scraper] All %d search_ids from cache", len(result))
        return result

    logger.info(
        "[ai_scraper] Resolving %d missing search_ids (cached: %d)...",
        len(missing), len(result)
    )

    session = _make_groww_session()
    resolved = 0
    failed   = 0

    for i, symbol in enumerate(missing):
        sid = resolve_search_id(symbol, session)
        if sid:
            result[symbol] = sid
            key = REDIS_KEYS["search_id"].format(symbol=symbol)
            redis_client.setex(key, REDIS_TTL["search_id"], sid)
            resolved += 1
        else:
            failed += 1

        if (i + 1) % 20 == 0:
            logger.info(
                "[ai_scraper] search_id progress: %d/%d (ok=%d, failed=%d)",
                i + 1, len(missing), resolved, failed
            )

        time.sleep(0.3)   # light delay — search API is fast

    logger.info(
        "[ai_scraper] search_id resolution complete: %d ok, %d failed",
        resolved, failed
    )
    return result


# ===========================================================================
# Step 1 — Market Headlines (Google News RSS)
# ===========================================================================

def _word_overlap(a: str, b: str) -> float:
    wa = set(re.findall(r"\w+", a.lower()))
    wb = set(re.findall(r"\w+", b.lower()))
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


def _dedup_market_headlines(headlines: list[dict]) -> list[dict]:
    kept: list[dict] = []
    for candidate in headlines:
        h = candidate["headline"]
        is_dup = any(_word_overlap(h, k["headline"]) >= 0.60 for k in kept)
        if not is_dup:
            kept.append(candidate)
    return kept


def scrape_market_headlines(session: requests.Session) -> list[dict]:
    """
    Scrape 4 Google News RSS feeds for market-level headlines.
    Returns list of {headline, source, pubdate, age_hours}.
    """
    all_items: list[dict] = []

    for feed in MARKET_RSS_FEEDS:
        try:
            resp = session.get(feed["url"], headers=_rss_headers(), timeout=12)
            if resp.status_code != 200:
                continue

            root = ET.fromstring(resp.content)
            channel = root.find("channel")
            if channel is None:
                continue

            for item in channel.findall("item"):
                title_el = item.find("title")
                pubdate_el = item.find("pubDate")

                headline = (title_el.text or "").strip() if title_el is not None else ""
                pubdate  = (pubdate_el.text or "").strip() if pubdate_el is not None else ""

                if len(headline) < MIN_HEADLINE_LENGTH:
                    continue

                # Strip publisher suffix "Headline text - Publisher Name"
                if " - " in headline:
                    parts = headline.rsplit(" - ", 1)
                    if len(parts[1]) < 60:
                        headline = parts[0].strip()

                # Strip leading "- " garbage
                headline = headline.lstrip("- ").strip()
                if len(headline.split()) < 6:
                    continue

                age = _parse_rss_age_hours(pubdate)
                if _is_stale(age):
                    continue

                all_items.append({
                    "headline":  headline[:300],
                    "source":    feed["name"],
                    "pubdate":   pubdate,
                    "age_hours": round(age, 2) if age is not None else None,
                })

            logger.info("[ai_scraper] RSS %s: %d items", feed["name"], len(all_items))

        except Exception as e:
            logger.warning("[ai_scraper] RSS %s failed: %s", feed["name"], e)
            continue

    deduped = _dedup_market_headlines(all_items)
    return deduped[:MAX_MARKET_HEADLINES]


# ===========================================================================
# Step 2A — Groww __NEXT_DATA__ news extraction (primary)
# ===========================================================================

_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json"[^>]*>(.*?)</script>',
    re.DOTALL,
)


def scrape_groww_stock_news(symbol: str, search_id: str) -> list[dict]:
    """
    Fetch https://groww.in/stocks/{search_id} and extract newsData from __NEXT_DATA__.
    
    Confirmed key path (verified 27-Apr-2026):
      props → pageProps → stockData → newsData → [{id, title, summary, url, pubDate, source}]
    
    Returns filtered list of {headline, source, url, pubdate, age_hours}.
    Uses a fresh cookie-free session per call.
    """
    url = GROWW_STOCK_URL.format(search_id=search_id)
    session = _make_groww_session()

    try:
        resp = session.get(url, headers=_groww_headers(), timeout=12)

        if resp.status_code == 429:
            logger.warning("[ai_scraper] Groww 429 for %s — rate limited", symbol)
            return []
        if resp.status_code == 403:
            logger.warning("[ai_scraper] Groww 403 for %s — blocked", symbol)
            return []
        if resp.status_code != 200:
            logger.debug("[ai_scraper] Groww %d for %s", resp.status_code, symbol)
            return []

        m = _NEXT_DATA_RE.search(resp.text)
        if not m:
            logger.debug("[ai_scraper] No __NEXT_DATA__ for %s", symbol)
            return []

        raw = json.loads(m.group(1))
        news_list = (
            raw.get("props", {})
               .get("pageProps", {})
               .get("stockData", {})
               .get("newsData", [])
        )

        if not news_list:
            logger.debug("[ai_scraper] Empty newsData for %s", symbol)
            return []

        results = []
        for article in news_list:
            headline = (article.get("title") or "").strip()
            if not headline:
                continue

            # Apply quality filter
            if not is_headline_useful(headline, symbol):
                continue

            pub_date = article.get("pubDate", "")
            age = _parse_iso_age_hours(pub_date)
            if _is_stale(age):
                continue

            results.append({
                "headline":  headline[:300],
                "source":    article.get("source", "Groww"),
                "url":       article.get("url", ""),
                "pubdate":   pub_date,
                "age_hours": round(age, 2) if age is not None else None,
            })

            if len(results) >= MAX_HEADLINES_PER_STOCK:
                break

        return results

    except Exception as e:
        logger.debug("[ai_scraper] Groww scrape error for %s: %s", symbol, e)
        return []


# ===========================================================================
# Step 2B — Google Finance fallback
# ===========================================================================

_RELATIVE_TS_RE = re.compile(
    r"(\d+)\s+(second|minute|hour|day|week|month)s?\s+ago", re.IGNORECASE
)


def _parse_relative_age(text: str) -> Optional[float]:
    m = _RELATIVE_TS_RE.search(text)
    if not m:
        return None
    value = int(m.group(1))
    unit  = m.group(2).lower()
    hours = {"second": 1/3600, "minute": 1/60, "hour": 1.0,
             "day": 24.0, "week": 168.0, "month": 720.0}
    return value * hours.get(unit, 0)


def scrape_google_finance_stock(
    symbol: str, session: requests.Session
) -> list[dict]:
    """
    Fallback: scrape Google Finance stock page for news headlines.
    SSR page — news links are in the raw HTML.
    """
    url = GOOGLE_FINANCE_URL.format(symbol=symbol)
    try:
        resp = session.get(url, headers=_gf_headers(), timeout=12)
        if resp.status_code in (429, 403) or resp.status_code != 200:
            return []

        # Find anchor tags pointing to news articles
        results = []
        seen: set[str] = set()

        # Pattern: find links with /articles/ (Google Finance news structure)
        all_anchors = re.findall(
            r'href="([^"]*articles[^"]*)"[^>]*>([^<]{20,})</a>',
            resp.text,
        )

        for href, raw_text in all_anchors:
            headline = re.sub(
                r"^.{0,80}?\d+\s+(?:second|minute|hour|day|week|month)s?\s+ago",
                "", raw_text, flags=re.IGNORECASE
            ).strip() or raw_text.strip()

            if len(headline) < MIN_HEADLINE_LENGTH:
                continue
            if not is_headline_useful(headline, symbol):
                continue
            key = headline.lower()[:80]
            if key in seen:
                continue
            seen.add(key)

            results.append({
                "headline":  headline[:300],
                "source":    "google_finance",
                "url":       href,
                "pubdate":   "",
                "age_hours": None,
            })

            if len(results) >= MAX_HEADLINES_PER_STOCK:
                break

        return results

    except Exception as e:
        logger.debug("[ai_scraper] GF fallback error for %s: %s", symbol, e)
        return []


# ===========================================================================
# Main Orchestrator — run_full_scrape
# ===========================================================================

def run_full_scrape(redis_client) -> dict:
    """
    Full scraping pipeline. Called synchronously by morning_seeder.py.

    Step 0: Resolve search_ids (cache-aware)
    Step 1: Scrape market headlines (RSS)
    Step 2: Scrape per-stock news (Groww primary, GF fallback)

    Returns stats dict.
    """
    t_start = time.monotonic()
    now_ist = datetime.now(IST).isoformat()

    stats = {
        "started_at":    now_ist,
        "symbols_total": len(UNIVERSE),
        "groww_ok":      0,
        "gf_ok":         0,
        "no_news":       0,
        "blocked":       0,
        "market_headlines": 0,
    }

    # ── Step 0: Resolve search_ids ────────────────────────────────────────
    logger.info("[ai_scraper] Step 0: resolving search_ids for %d symbols...", len(UNIVERSE))
    search_ids = resolve_all_search_ids(redis_client, UNIVERSE)
    logger.info("[ai_scraper] search_ids available: %d/%d", len(search_ids), len(UNIVERSE))

    # ── Step 1: Market headlines ──────────────────────────────────────────
    logger.info("[ai_scraper] Step 1: scraping market headlines (4 RSS feeds)...")
    gf_session = _make_gf_session()
    market_headlines = scrape_market_headlines(gf_session)
    stats["market_headlines"] = len(market_headlines)

    market_key = REDIS_KEYS["market_news"]
    redis_client.setex(
        market_key,
        REDIS_TTL["news"],
        json.dumps({
            "headlines":  market_headlines,
            "scraped_at": now_ist,
            "count":      len(market_headlines),
        }),
    )
    logger.info("[ai_scraper] Market headlines: %d written to Redis", len(market_headlines))

    # ── Step 2: Per-stock news ────────────────────────────────────────────
    logger.info("[ai_scraper] Step 2: scraping news for %d symbols...", len(UNIVERSE))

    session_stock_count = 0

    for idx, symbol in enumerate(UNIVERSE):
        search_id = search_ids.get(symbol)
        headlines: list[dict] = []
        source_used = None

        # Primary: Groww __NEXT_DATA__
        if search_id:
            try:
                headlines = scrape_groww_stock_news(symbol, search_id)
                if headlines:
                    source_used = "groww"
                    stats["groww_ok"] += 1
            except Exception as e:
                logger.debug("[ai_scraper] Groww error for %s: %s", symbol, e)

        # Fallback: Google Finance
        if not headlines:
            try:
                headlines = scrape_google_finance_stock(symbol, gf_session)
                if headlines:
                    source_used = "google_finance"
                    stats["gf_ok"] += 1
            except Exception as e:
                logger.debug("[ai_scraper] GF fallback error for %s: %s", symbol, e)

        if not headlines:
            stats["no_news"] += 1

        # Write to Redis
        stock_key = REDIS_KEYS["stock_news"].format(symbol=symbol)
        redis_client.setex(
            stock_key,
            REDIS_TTL["news"],
            json.dumps({
                "symbol":     symbol,
                "headlines":  headlines,
                "source":     source_used,
                "count":      len(headlines),
                "scraped_at": now_ist,
            }),
        )

        session_stock_count += 1

        # Progress log
        if (idx + 1) % 30 == 0:
            elapsed = round(time.monotonic() - t_start, 1)
            logger.info(
                "[ai_scraper] Progress: %d/%d | groww=%d gf=%d no_news=%d | %.0fs elapsed",
                idx + 1, len(UNIVERSE),
                stats["groww_ok"], stats["gf_ok"], stats["no_news"],
                elapsed,
            )

        # Rotate GF/RSS session every 50 stocks
        if session_stock_count >= SESSION_ROTATE_EVERY:
            gf_session = _make_gf_session()
            session_stock_count = 0

        time.sleep(REQUEST_DELAY_S)

    elapsed_total = round(time.monotonic() - t_start, 1)
    stats["elapsed_seconds"] = elapsed_total
    stats["completed_at"] = datetime.now(IST).isoformat()

    logger.info(
        "[ai_scraper] Scraping complete in %.0fs | "
        "groww=%d gf=%d no_news=%d market=%d",
        elapsed_total,
        stats["groww_ok"], stats["gf_ok"],
        stats["no_news"], stats["market_headlines"],
    )

    return stats


# ===========================================================================
# Redis Reader Helpers (used by ai_engine.py)
# ===========================================================================

def get_market_headlines(redis_client) -> list[dict]:
    """Read market headlines from Redis."""
    raw = redis_client.get(REDIS_KEYS["market_news"])
    if not raw:
        return []
    try:
        data = json.loads(raw if isinstance(raw, str) else raw.decode())
        return data.get("headlines", [])
    except Exception:
        return []


def get_stock_news(redis_client, symbol: str) -> list[dict]:
    """Read per-stock headlines from Redis."""
    key = REDIS_KEYS["stock_news"].format(symbol=symbol)
    raw = redis_client.get(key)
    if not raw:
        return []
    try:
        data = json.loads(raw if isinstance(raw, str) else raw.decode())
        return data.get("headlines", [])
    except Exception:
        return []


def get_stocks_with_news(redis_client) -> list[str]:
    """Return list of symbols that have news in Redis."""
    result = []
    for symbol in UNIVERSE:
        key = REDIS_KEYS["stock_news"].format(symbol=symbol)
        raw = redis_client.get(key)
        if not raw:
            continue
        try:
            data = json.loads(raw if isinstance(raw, str) else raw.decode())
            if data.get("headlines"):
                result.append(symbol)
        except Exception:
            continue
    return result
