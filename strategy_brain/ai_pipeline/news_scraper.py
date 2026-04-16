"""
strategy_brain/ai_pipeline/news_scraper.py
============================================
Lightweight async news scraper — no Playwright, no Selenium.

Schedules
---------
1. Pre-market 7:30 AM IST — broad market / macro headlines
2. Every 20 minutes during market hours — HOT_WATCHLIST symbols only

Sources
-------
- Moneycontrol RSS (market news)
- Economic Times Markets RSS
- Groww search API (per-symbol)

Redis keys written
------------------
    news:premarket          → JSON list of {headline, source, url, scraped_at}
    news:stock:{symbol}     → JSON list (last 5 stories)

Usage
-----
    from strategy_brain.ai_pipeline.news_scraper import (
        scrape_premarket_news,
        scrape_stock_news,
    )
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from xml.etree import ElementTree as ET

import httpx

from core.redis_client import get_redis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_IST = timezone(timedelta(hours=5, minutes=30))

_HTTP_TIMEOUT = 15.0
_MAX_PREMARKET_STORIES = 30
_MAX_STOCK_STORIES     = 5

# RSS feeds for broad market news
_RSS_FEEDS: list[dict] = [
    {
        "source": "Moneycontrol",
        "url":    "https://www.moneycontrol.com/rss/marketreports.xml",
    },
    {
        "source": "Economic Times",
        "url":    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    },
]

# Groww search endpoint (same pattern as existing ai_scraper.py)
_GROWW_SEARCH_URL = "https://groww.in/v1/api/search/query?q={symbol}&entity_type=stock&page=0&size=5"
_GROWW_NEWS_URL   = "https://groww.in/v1/api/stocks_data/v3/news/{search_id}?page=0&size=5"

# Garbage filter — skip these in RSS
_GARBAGE_PATTERNS = re.compile(
    r"(sponsored|advertisement|advertorial|click here|subscribe now|"
    r"free trial|download app|follow us|watch now|live tv)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# HTTP client factory (per-call, not shared — avoids cookie leakage)
# ---------------------------------------------------------------------------

def _make_client(extra_headers: Optional[dict] = None) -> httpx.AsyncClient:
    headers = {
        "User-Agent":      "Mozilla/5.0 (compatible; MarketPulsePro/2.0)",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
    }
    if extra_headers:
        headers.update(extra_headers)
    return httpx.AsyncClient(
        timeout=_HTTP_TIMEOUT,
        follow_redirects=True,
        headers=headers,
    )


# ---------------------------------------------------------------------------
# RSS parser
# ---------------------------------------------------------------------------

def _parse_rss(xml_text: str, source: str) -> list[dict]:
    """Parse an RSS XML string and return headline dicts."""
    results = []
    now_ist = datetime.now(_IST).isoformat()

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.warning("[news_scraper] RSS parse error for %s: %s", source, exc)
        return []

    # Handle both <rss><channel><item> and <feed><entry> (Atom)
    items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")

    for item in items:
        title_el = item.find("title") or item.find("{http://www.w3.org/2005/Atom}title")
        link_el  = item.find("link")  or item.find("{http://www.w3.org/2005/Atom}link")

        title = (title_el.text or "").strip() if title_el is not None else ""
        url   = (link_el.text  or link_el.get("href", "")).strip() if link_el is not None else ""

        if not title:
            continue
        if _GARBAGE_PATTERNS.search(title):
            continue
        if len(title) < 20:
            continue

        results.append({
            "headline":   title,
            "source":     source,
            "url":        url,
            "scraped_at": now_ist,
        })

    return results


# ---------------------------------------------------------------------------
# Public API — pre-market
# ---------------------------------------------------------------------------

async def scrape_premarket_news() -> list[dict]:
    """
    Scrape broad market headlines from Moneycontrol and Economic Times RSS.

    Returns
    -------
    list of {headline, source, url, scraped_at}

    Also writes result to Redis: news:premarket
    """
    all_headlines: list[dict] = []

    for feed in _RSS_FEEDS:
        try:
            async with _make_client() as client:
                resp = await client.get(feed["url"])
                resp.raise_for_status()

            parsed = _parse_rss(resp.text, feed["source"])
            all_headlines.extend(parsed)
            logger.info("[news_scraper] %s — fetched %d headlines", feed["source"], len(parsed))

        except httpx.HTTPStatusError as exc:
            logger.warning("[news_scraper] HTTP %d for %s: %s",
                           exc.response.status_code, feed["source"], feed["url"])
        except Exception as exc:  # noqa: BLE001
            logger.warning("[news_scraper] Error fetching %s: %s", feed["source"], exc)

    # Deduplicate by headline text
    seen: set[str] = set()
    deduped: list[dict] = []
    for item in all_headlines:
        key = item["headline"].lower()[:80]
        if key not in seen:
            seen.add(key)
            deduped.append(item)

    trimmed = deduped[:_MAX_PREMARKET_STORIES]

    # Write to Redis
    try:
        redis = await get_redis()
        await redis.set("news:premarket", json.dumps(trimmed), ex=3600)  # 1-hour TTL
        logger.info("[news_scraper] Wrote %d premarket headlines to Redis", len(trimmed))
    except Exception as exc:  # noqa: BLE001
        logger.error("[news_scraper] Redis write failed: %s", exc)

    return trimmed


# ---------------------------------------------------------------------------
# Public API — per-symbol
# ---------------------------------------------------------------------------

async def scrape_stock_news(symbol: str) -> list[dict]:
    """
    Scrape recent news for a specific symbol via Groww search API.

    Called for HOT_WATCHLIST stocks every 20 minutes during market hours.

    Parameters
    ----------
    symbol : NSE underlying symbol (e.g. "RELIANCE")

    Returns
    -------
    list of {headline, source, url, scraped_at} — last 5 stories

    Also writes to Redis: news:stock:{symbol}
    """
    now_ist = datetime.now(_IST).isoformat()
    results: list[dict] = []

    try:
        # Step 1 — resolve search_id from Groww
        search_url = _GROWW_SEARCH_URL.format(symbol=symbol)
        async with _make_client({"Accept": "application/json"}) as client:
            resp = await client.get(search_url)
            resp.raise_for_status()
            search_data = resp.json()

        search_results = (
            search_data.get("data", {}).get("content", [])
            or search_data.get("content", [])
        )
        search_id = None
        for item in search_results:
            if isinstance(item, dict):
                search_id = item.get("search_id") or item.get("searchId")
                if search_id:
                    break

        if not search_id:
            logger.debug("[news_scraper] No Groww search_id for %s", symbol)
            return []

        # Step 2 — fetch news via search_id
        news_url = _GROWW_NEWS_URL.format(search_id=search_id)
        async with _make_client({"Accept": "application/json"}) as client:
            resp = await client.get(news_url)
            resp.raise_for_status()
            news_data = resp.json()

        articles = (
            news_data.get("data", {}).get("news", [])
            or news_data.get("news", [])
            or news_data.get("articles", [])
        )

        for article in articles[:_MAX_STOCK_STORIES]:
            headline = _clean_groww_headline(
                article.get("headline") or article.get("title") or ""
            )
            if not headline or len(headline) < 15:
                continue
            results.append({
                "headline":   headline,
                "source":     "Groww",
                "url":        article.get("url") or article.get("link") or "",
                "scraped_at": now_ist,
            })

        logger.info("[news_scraper] %s — %d Groww articles fetched", symbol, len(results))

    except httpx.HTTPStatusError as exc:
        logger.warning("[news_scraper] HTTP %d for %s news: %s",
                       exc.response.status_code, symbol, exc)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[news_scraper] Error fetching news for %s: %s", symbol, exc)

    # Write to Redis
    try:
        redis = await get_redis()
        await redis.set(f"news:stock:{symbol}", json.dumps(results), ex=1800)  # 30-min TTL
    except Exception as exc:  # noqa: BLE001
        logger.error("[news_scraper] Redis write failed for %s: %s", symbol, exc)

    return results


# ---------------------------------------------------------------------------
# Headline cleaner
# ---------------------------------------------------------------------------

_GROWW_GARBAGE_RE = re.compile(
    r"[\|\[\]{}]|"                        # pipe / bracket noise
    r"(read more|click here|view more|"
    r"exclusive:|breaking:|alert:)",
    re.IGNORECASE,
)


def _clean_groww_headline(raw: str) -> str:
    cleaned = _GROWW_GARBAGE_RE.sub(" ", raw)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned
