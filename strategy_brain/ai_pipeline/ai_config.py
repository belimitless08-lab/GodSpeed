"""
strategy_brain/ai_pipeline/ai_config.py
========================================
AI Pipeline configuration — models, prompts, thresholds, sector map, helpers.
Redis-native version for Market Pulse Pro v2.

No external dependencies beyond standard library.
No API calls, no scraping, no FastAPI.
"""

from typing import Optional

# ===========================================================================
# SECTION 1 — LLM Model Configuration
# ===========================================================================

# All calls go to OpenRouter (https://openrouter.ai/api/v1)
# which hosts both Groq-served llama and gpt-oss models under one endpoint.
OPENROUTER_API_BASE = "https://openrouter.ai/api/v1"

MODELS = {
    "context": {
        "id":          "meta-llama/llama-3.3-70b-instruct",
        "purpose":     "Market context — 1 call/day, macro + sector bias",
        "max_tokens":  1024,
        "temperature": 0.3,
    },
    "sentiment": {
        "id":               "openai/gpt-4o-mini",   # fast, cheap, good JSON
        "purpose":          "Per-stock sentiment — ~60-80 calls/day",
        "max_tokens":       512,
        "temperature":      0.4,
        "reasoning_effort": "low",
    },
    "decision": {
        "id":               "openai/gpt-4o-mini",
        "purpose":          "Final trade decision — ~15-20 calls/day",
        "max_tokens":       1024,
        "temperature":      0.3,
        "reasoning_effort": "medium",
    },
}

# ===========================================================================
# SECTION 2 — Rate Limits
# ===========================================================================

RATE_LIMITS = {
    "delay_between_sentiment_calls": 2.0,   # seconds — ~30 calls/min
    "delay_between_decision_calls":  2.0,
    "max_retries":   3,
    "retry_delay":   15,    # seconds on 429
}

# ===========================================================================
# SECTION 3 — Redis Key Schema
# ===========================================================================

# All keys written by the AI pipeline
REDIS_KEYS = {
    # Scraper outputs (written at 8:00 AM)
    "search_id":         "ai:search_id:{symbol}",       # Groww search_id, TTL 7d
    "stock_news":        "ai:news:stock:{symbol}",       # list of headlines, TTL 12hr
    "market_news":       "ai:news:market",               # market headlines, TTL 12hr

    # Engine outputs (written 8:10–8:20 AM)
    "context":           "ai:context",                   # market bias + sector bias, TTL 12hr
    "sentiment":         "ai:sentiment:{symbol}",        # score + confidence, TTL 12hr
    "trade_list":        "ai:trade_list",                # top 10 bull + bear, TTL 12hr
    "pipeline_status":   "ai:pipeline:status",           # run metadata, TTL 24hr
}

REDIS_TTL = {
    "search_id":   7 * 24 * 3600,   # 7 days — stable, rarely changes
    "news":        12 * 3600,        # 12 hours
    "engine":      12 * 3600,        # 12 hours
    "pipeline":    24 * 3600,        # 24 hours
}

# ===========================================================================
# SECTION 4 — Sector Map
# ===========================================================================

SECTOR_MAP: dict[str, str] = {
    # ── BANKING ─────────────────────────────────────────────────────────────
    "HDFCBANK": "BANKING", "ICICIBANK": "BANKING", "AXISBANK": "BANKING",
    "KOTAKBANK": "BANKING", "SBIN": "BANKING", "INDUSINDBK": "BANKING",
    "BANDHANBNK": "BANKING", "BANKBARODA": "BANKING", "CANBK": "BANKING",
    "FEDERALBNK": "BANKING", "IDFCFIRSTB": "BANKING", "PNB": "BANKING",
    "RBLBANK": "BANKING", "YESBANK": "BANKING", "UNIONBANK": "BANKING",
    "AUBANK": "BANKING", "BANKINDIA": "BANKING", "INDIANB": "BANKING",

    # ── IT ──────────────────────────────────────────────────────────────────
    "TCS": "IT", "INFY": "IT", "HCLTECH": "IT", "WIPRO": "IT",
    "TECHM": "IT", "MPHASIS": "IT", "COFORGE": "IT", "PERSISTENT": "IT",
    "OFSS": "IT", "KPITTECH": "IT", "TATAELXSI": "IT", "NAUKRI": "IT",
    "PAYTM": "IT", "TATATECH": "IT",

    # ── PHARMA ──────────────────────────────────────────────────────────────
    "SUNPHARMA": "PHARMA", "DRREDDY": "PHARMA", "CIPLA": "PHARMA",
    "DIVISLAB": "PHARMA", "APOLLOHOSP": "PHARMA", "AUROPHARMA": "PHARMA",
    "LUPIN": "PHARMA", "ALKEM": "PHARMA", "BIOCON": "PHARMA",
    "GLENMARK": "PHARMA", "TORNTPHARM": "PHARMA", "ZYDUSLIFE": "PHARMA",
    "LAURUSLABS": "PHARMA", "SYNGENE": "PHARMA", "MAXHEALTH": "PHARMA",
    "MANKIND": "PHARMA", "FORTIS": "PHARMA", "PPLPHARMA": "PHARMA",

    # ── AUTO ────────────────────────────────────────────────────────────────
    "MARUTI": "AUTO", "M&M": "AUTO", "BAJAJ-AUTO": "AUTO",
    "HEROMOTOCO": "AUTO", "EICHERMOT": "AUTO", "TVSMOTOR": "AUTO",
    "ASHOKLEY": "AUTO", "MOTHERSON": "AUTO", "MRF": "AUTO",
    "HYUNDAI": "AUTO", "SONACOMS": "AUTO", "UNOMINDA": "AUTO",
    "TIINDIA": "AUTO", "BOSCHLTD": "AUTO", "EXIDEIND": "AUTO",

    # ── OIL_GAS ─────────────────────────────────────────────────────────────
    "RELIANCE": "OIL_GAS", "ONGC": "OIL_GAS", "BPCL": "OIL_GAS",
    "IOC": "OIL_GAS", "GAIL": "OIL_GAS", "PETRONET": "OIL_GAS",
    "IGL": "OIL_GAS", "ATGL": "OIL_GAS", "HINDPETRO": "OIL_GAS",
    "OIL": "OIL_GAS",

    # ── METALS ──────────────────────────────────────────────────────────────
    "TATASTEEL": "METALS", "JSWSTEEL": "METALS", "HINDALCO": "METALS",
    "VEDL": "METALS", "SAIL": "METALS", "NMDC": "METALS",
    "JINDALSTEL": "METALS", "HINDZINC": "METALS", "COALINDIA": "METALS",
    "APLAPOLLO": "METALS", "NATIONALUM": "METALS",

    # ── FMCG ────────────────────────────────────────────────────────────────
    "HINDUNILVR": "FMCG", "ITC": "FMCG", "NESTLEIND": "FMCG",
    "BRITANNIA": "FMCG", "DABUR": "FMCG", "MARICO": "FMCG",
    "COLPAL": "FMCG", "GODREJCP": "FMCG", "TATACONSUM": "FMCG",
    "VBL": "FMCG", "GODFRYPHLP": "FMCG", "UNITDSPR": "FMCG",
    "PATANJALI": "FMCG",

    # ── TELECOM ─────────────────────────────────────────────────────────────
    "BHARTIARTL": "TELECOM", "INDUSTOWER": "TELECOM", "BHARTIHEXA": "TELECOM",
    "IDEA": "TELECOM", "TATACOMM": "TELECOM",

    # ── INFRA ───────────────────────────────────────────────────────────────
    "LT": "INFRA", "GMRAIRPORT": "INFRA", "CONCOR": "INFRA",
    "IRCTC": "INFRA", "HUDCO": "INFRA", "IRB": "INFRA",
    "RVNL": "INFRA", "IRFC": "INFRA", "ADANIPORTS": "INFRA",
    "NBCC": "INFRA",

    # ── REALTY ──────────────────────────────────────────────────────────────
    "DLF": "REALTY", "GODREJPROP": "REALTY", "OBEROIRLTY": "REALTY",
    "PRESTIGE": "REALTY", "PHOENIXLTD": "REALTY", "LODHA": "REALTY",

    # ── POWER ───────────────────────────────────────────────────────────────
    "NTPC": "POWER", "POWERGRID": "POWER", "TATAPOWER": "POWER",
    "TORNTPOWER": "POWER", "ADANIGREEN": "POWER", "JSWENERGY": "POWER",
    "NHPC": "POWER", "SUZLON": "POWER", "CGPOWER": "POWER",
    "INOXWIND": "POWER", "WAAREEENER": "POWER", "ADANIENSOL": "POWER",
    "ADANIPOWER": "POWER", "NTPCGREEN": "POWER", "PREMIERENE": "POWER",
    "IREDA": "POWER",

    # ── CEMENT ──────────────────────────────────────────────────────────────
    "ULTRACEMCO": "CEMENT", "GRASIM": "CEMENT", "ACC": "CEMENT",
    "AMBUJACEM": "CEMENT", "DALBHARAT": "CEMENT", "SHREECEM": "CEMENT",

    # ── FINANCE_NBFC ────────────────────────────────────────────────────────
    "BAJFINANCE": "FINANCE_NBFC", "BAJAJFINSV": "FINANCE_NBFC",
    "HDFCLIFE": "FINANCE_NBFC", "SBILIFE": "FINANCE_NBFC",
    "ICICIGI": "FINANCE_NBFC", "ICICIPRULI": "FINANCE_NBFC",
    "CHOLAFIN": "FINANCE_NBFC", "MUTHOOTFIN": "FINANCE_NBFC",
    "MANAPPURAM": "FINANCE_NBFC", "ABCAPITAL": "FINANCE_NBFC",
    "LICHSGFIN": "FINANCE_NBFC", "LICI": "FINANCE_NBFC",
    "M&MFIN": "FINANCE_NBFC", "SHRIRAMFIN": "FINANCE_NBFC",
    "PFC": "FINANCE_NBFC", "RECLTD": "FINANCE_NBFC",
    "MFSL": "FINANCE_NBFC", "SBICARD": "FINANCE_NBFC",
    "POLICYBZR": "FINANCE_NBFC", "360ONE": "FINANCE_NBFC",
    "BAJAJHFL": "FINANCE_NBFC", "BAJAJHLDNG": "FINANCE_NBFC",
    "CAMS": "FINANCE_NBFC", "HDFCAMC": "FINANCE_NBFC",
    "JIOFIN": "FINANCE_NBFC", "KFINTECH": "FINANCE_NBFC",
    "LTF": "FINANCE_NBFC", "MOTILALOFS": "FINANCE_NBFC",
    "NUVAMA": "FINANCE_NBFC", "PNBHOUSING": "FINANCE_NBFC",
    "SAMMAANCAP": "FINANCE_NBFC", "ANGELONE": "FINANCE_NBFC",
    "MCX": "FINANCE_NBFC", "BSE": "FINANCE_NBFC",
    "IEX": "FINANCE_NBFC", "CDSL": "FINANCE_NBFC",

    # ── CAPITAL_GOODS ───────────────────────────────────────────────────────
    "ABB": "CAPITAL_GOODS", "SIEMENS": "CAPITAL_GOODS", "BHEL": "CAPITAL_GOODS",
    "CUMMINSIND": "CAPITAL_GOODS", "HAVELLS": "CAPITAL_GOODS",
    "POLYCAB": "CAPITAL_GOODS", "KEI": "CAPITAL_GOODS", "DIXON": "CAPITAL_GOODS",
    "SUPREMEIND": "CAPITAL_GOODS", "ASTRAL": "CAPITAL_GOODS",
    "BHARATFORG": "CAPITAL_GOODS", "BLUESTARCO": "CAPITAL_GOODS",
    "AMBER": "CAPITAL_GOODS", "KAYNES": "CAPITAL_GOODS",
    "PGEL": "CAPITAL_GOODS", "POWERINDIA": "CAPITAL_GOODS",
    "PIDILITIND": "CAPITAL_GOODS", "SRF": "CHEMICALS",
    "PIIND": "CHEMICALS", "SOLARINDS": "CHEMICALS",

    # ── DEFENCE ─────────────────────────────────────────────────────────────
    "HAL": "DEFENCE", "BEL": "DEFENCE", "BDL": "DEFENCE",
    "MAZDOCK": "DEFENCE", "COCHINSHIP": "DEFENCE",

    # ── CONSUMER_DURABLES ───────────────────────────────────────────────────
    "TITAN": "CONSUMER_DURABLES", "VOLTAS": "CONSUMER_DURABLES",
    "ASIANPAINT": "CONSUMER_DURABLES", "KALYANKJIL": "CONSUMER_DURABLES",
    "CROMPTON": "CONSUMER_DURABLES",

    # ── AGRI ────────────────────────────────────────────────────────────────
    "UPL": "AGRI", "COROMANDEL": "AGRI",

    # ── OTHER ───────────────────────────────────────────────────────────────
    "DELHIVERY": "OTHER", "ETERNAL": "OTHER", "SWIGGY": "OTHER",
    "TRENT": "OTHER", "DMART": "OTHER", "JUBLFOOD": "OTHER",
    "INDHOTEL": "OTHER", "ADANIENT": "OTHER", "NYKAA": "OTHER",
    "ITCHOTELS": "OTHER", "PAGEIND": "TEXTILES", "INDIGO": "AVIATION",
    "ENRIN": "POWER", "LTM": "INFRA", "TMPV": "AUTO",
    "VMM": "INFRA",
}

# Auto-build reverse lookup: sector → [symbols]
SECTOR_STOCKS: dict[str, list[str]] = {}
for _sym, _sec in SECTOR_MAP.items():
    SECTOR_STOCKS.setdefault(_sec, []).append(_sym)

UNIVERSE: list[str] = sorted(SECTOR_MAP.keys())

# ===========================================================================
# SECTION 5 — Prompts
# ===========================================================================

# 5A — Context Engine (1 llama call at 8:10 AM)
# Reads market headlines → produces market_bias + sector_bias for ALL sectors
CONTEXT_PROMPT = """\
You are a macro market analyst for Indian equity markets (NSE/BSE).

Analyze today's market headlines and return the macro environment that will affect Indian stocks.

Headlines:
{headlines}

Real-time Market Intelligence (PCR + VIX):
{market_intel}

Respond ONLY with valid JSON, no markdown, no extra text:
{{
    "market_bias": "bullish",
    "volatility_expectation": "medium",
    "themes": ["theme1", "theme2", "theme3"],
    "sector_bias": {{
        "BANKING": "bullish",
        "IT": "neutral",
        "OIL_GAS": "bearish",
        "PHARMA": "neutral",
        "AUTO": "bullish",
        "METALS": "neutral",
        "FMCG": "neutral",
        "TELECOM": "neutral",
        "INFRA": "bullish",
        "REALTY": "neutral",
        "POWER": "bullish",
        "CEMENT": "neutral",
        "FINANCE_NBFC": "neutral",
        "CAPITAL_GOODS": "neutral",
        "DEFENCE": "neutral",
        "CONSUMER_DURABLES": "neutral",
        "CHEMICALS": "neutral",
        "AGRI": "neutral",
        "AVIATION": "neutral",
        "OTHER": "neutral"
    }},
    "key_events": ["event1", "event2"],
    "global_macro": "One sentence on global macro context affecting Indian markets today."
}}

Rules:
- market_bias: "bullish" / "bearish" / "neutral"
- volatility_expectation: "low" / "medium" / "high"
- sector_bias values: "bullish" / "bearish" / "neutral"
- themes: max 5 short phrases
- key_events: max 3 specific events (e.g. "RBI policy meet", "US CPI data")
- global_macro: one sentence, specific (e.g. "Nikkei up 1.2%, crude oil flat at $82")
- Use market_intel (PCR/VIX) to refine market_bias: high PCR is bullish (smart hedging), high VIX adds uncertainty"""


# 5B — Sentiment Engine (~60-80 gpt-oss calls)
# Gets: stock headlines + macro context → score + conviction
SENTIMENT_PROMPT = """\
You are a stock sentiment analyst for NSE intraday trading.

Analyze {symbol} ({company_name}) using BOTH its specific news AND the macro context.

Sector: {sector}

Stock News (today):
{stock_news}

Macro Context:
- Market bias: {market_bias}
- Sector bias for {sector}: {sector_bias}
- Key themes: {themes}
- Global macro: {global_macro}

Score from -5 (extremely bearish) to +5 (extremely bullish). 0 = neutral.

Scoring weights:
- Earnings beats/misses, order wins, capex cuts → HIGH weight
- Policy changes, regulatory actions, management changes → HIGH weight  
- Analyst upgrades/downgrades, price targets → MEDIUM weight
- Vague commentary, market roundups → LOW weight
- If macro/sector is bullish AND stock news is positive → amplify score
- If macro/sector is bearish AND stock news is negative → amplify score
- If macro contradicts stock news → reduce confidence, moderate score

Respond ONLY with valid JSON, no markdown, no extra text:
{{
    "score": 2.5,
    "confidence": "high",
    "driver": "Q4 earnings beat + sector tailwind from bullish macro",
    "sector_alignment": "positive",
    "news_quality": "strong"
}}

Fields:
- score: float -5 to +5
- confidence: "low" / "medium" / "high"
- driver: max 1 line, specific
- sector_alignment: "positive" / "negative" / "neutral"
- news_quality: "strong" / "moderate" / "weak" (quality of the news itself, not score)"""


# 5C — Decision Engine (~15-20 gpt-oss calls, reasoning model)
# Gets: sentiment + macro + snapshot technicals → final trade decision
DECISION_PROMPT = """\
You are a professional NSE intraday F&O trader making a pre-market decision.

Stock: {symbol} | Sector: {sector}
Sentiment Score: {sentiment_score}/5 | Driver: {sentiment_driver}
Confidence: {sentiment_confidence} | News Quality: {news_quality}

Macro:
- Market bias: {market_bias} | Volatility: {volatility}
- Sector bias ({sector}): {sector_bias}
- Key themes: {themes}

Technical Context (from Friday's close, morning seeder):
- Supertrend: {supertrend_dir}
- RSI14: {rsi14}
- Price vs EMA9: {ema9_position}
- Price vs EMA200: {ema200_position}
- Choppiness: {choppiness_class}
- Supertrend band distance: {st_band_dist}

Decision rules:
- Positive sentiment + bullish technicals + bullish macro → BUY CE, high conviction
- Positive sentiment + bearish technicals → BUY CE, reduce conviction
- Negative sentiment + bearish technicals + bearish macro → BUY PE, high conviction
- Sentiment strong but sector contradicts → keep direction, reduce conviction
- AVOID only if: sentiment score between -1 and +1 AND no clear technical direction
- BUY CE = bullish trade. BUY PE = bearish trade. NEVER use AVOID for strong signals.

Respond ONLY with valid JSON, no markdown, no extra text:
{{
    "final_score": 3.2,
    "action": "BUY CE",
    "conviction": "high",
    "reason": "Q4 beat + bullish supertrend + sector tailwind. Strong alignment.",
    "risk_note": "Watch for profit booking above R1."
}}

Fields:
- final_score: float -5 to +5 (your adjusted score after technical context)
- action: "BUY CE" / "BUY PE" / "AVOID"
- conviction: "high" / "medium" / "low"
- reason: max 2 lines, specific trade thesis
- risk_note: 1 line, specific risk"""

# ===========================================================================
# SECTION 6 — Filter Thresholds
# ===========================================================================

FILTER_THRESHOLDS = {
    "bullish_min_score":    1.5,    # pass to decision engine if score >= +1.5
    "bearish_min_score":   -1.5,    # pass to decision engine if score <= -1.5
    "min_confidence":      "low",   # any confidence passes — LLM decides
    "min_news_quality":    "weak",  # any quality passes — scraper already filtered
    "max_filtered_stocks":  40,     # max stocks going to decision engine
}

RANKING_CONFIG = {
    "top_n":         10,    # top 10 total (5 bull + 5 bear, or best 10)
    "top_bullish":   10,
    "top_bearish":   10,
    "sort_by":       "final_score",
}

# ===========================================================================
# SECTION 7 — News Quality Filters
# ===========================================================================

USELESS_HEADLINE_PATTERNS: list[str] = [
    "share price today", "stock price target", "buy or sell",
    "multibagger", "penny stock", "hot stock pick", "intraday tip",
    "stock market live", "nifty prediction", "should you invest",
    "returns in last", "top gainers", "top losers", "52 week high",
    "52 week low", "circuit filter", "upper circuit", "lower circuit",
]

REQUIRE_STOCK_NAME_IN_HEADLINE: bool = True
MIN_HEADLINE_LENGTH: int = 25
MAX_HEADLINE_AGE_HOURS: int = 48

# ===========================================================================
# SECTION 8 — Company Name Map (for headline relevance filtering)
# ===========================================================================

COMPANY_NAMES: dict[str, list[str]] = {
    "ADANIENT": ["Adani Enterprises", "AEL"],
    "ADANIPORTS": ["Adani Ports", "APSEZ"],
    "APOLLOHOSP": ["Apollo Hospitals", "Apollo Hospital"],
    "ASIANPAINT": ["Asian Paints"],
    "AXISBANK": ["Axis Bank"],
    "BAJAJ-AUTO": ["Bajaj Auto"],
    "BAJFINANCE": ["Bajaj Finance"],
    "BAJAJFINSV": ["Bajaj Finserv"],
    "BPCL": ["BPCL", "Bharat Petroleum"],
    "BHARTIARTL": ["Bharti Airtel", "Airtel"],
    "BRITANNIA": ["Britannia"],
    "CIPLA": ["Cipla"],
    "COALINDIA": ["Coal India", "CIL"],
    "DIVISLAB": ["Divi's Laboratories", "Divis Lab"],
    "DRREDDY": ["Dr. Reddy's", "Dr Reddy", "DRL"],
    "EICHERMOT": ["Eicher Motors", "Royal Enfield"],
    "GRASIM": ["Grasim Industries"],
    "HCLTECH": ["HCL Technologies", "HCL Tech"],
    "HDFCBANK": ["HDFC Bank"],
    "HDFCLIFE": ["HDFC Life"],
    "HEROMOTOCO": ["Hero MotoCorp", "Hero Moto"],
    "HINDALCO": ["Hindalco", "Novelis"],
    "HINDUNILVR": ["Hindustan Unilever", "HUL"],
    "ICICIBANK": ["ICICI Bank"],
    "INDUSINDBK": ["IndusInd Bank"],
    "INFY": ["Infosys"],
    "ITC": ["ITC"],
    "JSWSTEEL": ["JSW Steel"],
    "KOTAKBANK": ["Kotak Bank", "Kotak Mahindra Bank"],
    "LT": ["Larsen & Toubro", "L&T"],
    "M&M": ["Mahindra & Mahindra", "M&M", "Mahindra"],
    "MARUTI": ["Maruti Suzuki", "Maruti", "MSIL"],
    "NESTLEIND": ["Nestle India", "Nestle"],
    "NTPC": ["NTPC"],
    "ONGC": ["ONGC", "Oil and Natural Gas"],
    "POWERGRID": ["Power Grid", "PGCIL"],
    "RELIANCE": ["Reliance Industries", "Reliance", "RIL", "Jio"],
    "SBILIFE": ["SBI Life"],
    "SBIN": ["State Bank of India", "SBI"],
    "SUNPHARMA": ["Sun Pharma", "Sun Pharmaceutical"],
    "TCS": ["TCS", "Tata Consultancy"],
    "TATACONSUM": ["Tata Consumer", "TCPL"],
    "TATASTEEL": ["Tata Steel"],
    "TECHM": ["Tech Mahindra"],
    "TITAN": ["Titan", "Tanishq"],
    "ULTRACEMCO": ["UltraTech Cement", "UltraTech"],
    "WIPRO": ["Wipro"],
    "TRENT": ["Trent", "Zudio"],
    "SHRIRAMFIN": ["Shriram Finance"],
    "ABB": ["ABB India"],
    "APLAPOLLO": ["APL Apollo Tubes"],
    "ASTRAL": ["Astral Pipes", "Astral Limited"],
    "BOSCHLTD": ["Bosch India"],
    "CROMPTON": ["Crompton Greaves Consumer"],
    "DELHIVERY": ["Delhivery"],
    "ETERNAL": ["Eternal", "Eternal Limited", "Zomato"],
    "EXIDEIND": ["Exide Industries"],
    "HINDPETRO": ["Hindustan Petroleum", "HPCL"],
    "IDEA": ["Vodafone Idea", "Vi"],
    "NATIONALUM": ["National Aluminium", "NALCO"],
    "OIL": ["Oil India"],
    "PAGEIND": ["Page Industries", "Jockey India"],
    "PATANJALI": ["Patanjali Foods"],
    "SWIGGY": ["Swiggy"],
    "TATACOMM": ["Tata Communications"],
    "ACC": ["ACC Cement"],
    "ABCAPITAL": ["Aditya Birla Capital", "AB Capital"],
    "ADANIENSOL": ["Adani Energy Solutions"],
    "ADANIGREEN": ["Adani Green Energy", "AGEL"],
    "ADANIPOWER": ["Adani Power"],
    "ALKEM": ["Alkem Laboratories"],
    "AMBUJACEM": ["Ambuja Cements"],
    "AMBER": ["Amber Enterprises"],
    "ANGELONE": ["Angel One"],
    "ASHOKLEY": ["Ashok Leyland"],
    "ATGL": ["Adani Total Gas"],
    "AUBANK": ["AU Small Finance Bank", "AU Bank"],
    "AUROPHARMA": ["Aurobindo Pharma"],
    "BAJAJHFL": ["Bajaj Housing Finance"],
    "BAJAJHLDNG": ["Bajaj Holdings"],
    "BANDHANBNK": ["Bandhan Bank"],
    "BANKBARODA": ["Bank of Baroda"],
    "BANKINDIA": ["Bank of India"],
    "BDL": ["Bharat Dynamics"],
    "BEL": ["Bharat Electronics"],
    "BHARATFORG": ["Bharat Forge"],
    "BHARTIHEXA": ["Bharti Hexacom"],
    "BHEL": ["BHEL", "Bharat Heavy Electricals"],
    "BIOCON": ["Biocon"],
    "BLUESTARCO": ["Blue Star"],
    "BSE": ["BSE Limited", "Bombay Stock Exchange"],
    "CAMS": ["CAMS", "Computer Age Management"],
    "CANBK": ["Canara Bank"],
    "CDSL": ["CDSL", "Central Depository"],
    "CGPOWER": ["CG Power", "Crompton Greaves Power"],
    "CHOLAFIN": ["Chola Finance", "Cholamandalam"],
    "COCHINSHIP": ["Cochin Shipyard"],
    "COFORGE": ["Coforge"],
    "COLPAL": ["Colgate", "Colgate-Palmolive"],
    "CONCOR": ["Container Corporation", "CONCOR"],
    "COROMANDEL": ["Coromandel International"],
    "CUMMINSIND": ["Cummins India"],
    "DABUR": ["Dabur India"],
    "DALBHARAT": ["Dalmia Bharat"],
    "DIXON": ["Dixon Technologies"],
    "DLF": ["DLF"],
    "DMART": ["DMart", "Avenue Supermarts"],
    "ENRIN": ["Enviro Infra Engineers"],
    "FEDERALBNK": ["Federal Bank"],
    "FORTIS": ["Fortis Healthcare"],
    "GAIL": ["GAIL India"],
    "GLENMARK": ["Glenmark Pharmaceuticals"],
    "GMRAIRPORT": ["GMR Airports"],
    "GODREJCP": ["Godrej Consumer Products", "GCPL"],
    "GODREJPROP": ["Godrej Properties"],
    "GODFRYPHLP": ["Godfrey Phillips"],
    "HAL": ["HAL", "Hindustan Aeronautics"],
    "HAVELLS": ["Havells India"],
    "HDFCAMC": ["HDFC AMC"],
    "HINDZINC": ["Hindustan Zinc"],
    "HUDCO": ["HUDCO"],
    "HYUNDAI": ["Hyundai Motor India"],
    "ICICIPRULI": ["ICICI Prudential Life"],
    "ICICIGI": ["ICICI Lombard"],
    "IDFCFIRSTB": ["IDFC First Bank"],
    "IEX": ["Indian Energy Exchange"],
    "IGL": ["Indraprastha Gas"],
    "INDIANB": ["Indian Bank"],
    "INDHOTEL": ["Indian Hotels", "Taj Hotels", "IHCL"],
    "INDIGO": ["IndiGo", "InterGlobe Aviation"],
    "INDUSTOWER": ["Indus Towers"],
    "INOXWIND": ["Inox Wind"],
    "IOC": ["Indian Oil", "IOCL"],
    "IRB": ["IRB Infrastructure"],
    "IRCTC": ["IRCTC"],
    "IREDA": ["IREDA"],
    "IRFC": ["IRFC"],
    "ITCHOTELS": ["ITC Hotels"],
    "JIOFIN": ["Jio Financial Services", "JFS"],
    "JINDALSTEL": ["Jindal Steel", "JSPL"],
    "JSWENERGY": ["JSW Energy"],
    "JUBLFOOD": ["Jubilant FoodWorks", "Domino's India"],
    "KALYANKJIL": ["Kalyan Jewellers"],
    "KAYNES": ["Kaynes Technology"],
    "KEI": ["KEI Industries"],
    "KFINTECH": ["KFin Technologies"],
    "KPITTECH": ["KPIT Technologies"],
    "LAURUSLABS": ["Laurus Labs"],
    "LICHSGFIN": ["LIC Housing Finance"],
    "LICI": ["LIC", "Life Insurance Corporation"],
    "LODHA": ["Lodha", "Macrotech Developers"],
    "LTF": ["L&T Finance"],
    "LTM": ["L&T Metro"],
    "LUPIN": ["Lupin"],
    "M&MFIN": ["Mahindra Finance"],
    "MANKIND": ["Mankind Pharma"],
    "MANAPPURAM": ["Manappuram Finance"],
    "MARICO": ["Marico", "Parachute"],
    "MAXHEALTH": ["Max Healthcare"],
    "MAZDOCK": ["Mazagon Dock"],
    "MCX": ["MCX", "Multi Commodity Exchange"],
    "MOTHERSON": ["Samvardhana Motherson", "Motherson"],
    "MOTILALOFS": ["Motilal Oswal"],
    "MPHASIS": ["Mphasis"],
    "MRF": ["MRF Tyres"],
    "MUTHOOTFIN": ["Muthoot Finance"],
    "NAUKRI": ["Info Edge", "Naukri"],
    "NBCC": ["NBCC India"],
    "NHPC": ["NHPC"],
    "NMDC": ["NMDC"],
    "NTPCGREEN": ["NTPC Green Energy"],
    "NUVAMA": ["Nuvama Wealth"],
    "NYKAA": ["Nykaa", "FSN E-Commerce"],
    "OBEROIRLTY": ["Oberoi Realty"],
    "OFSS": ["Oracle Financial Services"],
    "PAYTM": ["Paytm", "One97 Communications"],
    "PERSISTENT": ["Persistent Systems"],
    "PETRONET": ["Petronet LNG"],
    "PFC": ["Power Finance Corporation"],
    "PGEL": ["PG Electroplast"],
    "PHOENIXLTD": ["Phoenix Mills"],
    "PIIND": ["PI Industries"],
    "PIDILITIND": ["Pidilite", "Fevicol"],
    "PNB": ["Punjab National Bank"],
    "PNBHOUSING": ["PNB Housing Finance"],
    "POLICYBZR": ["Policybazaar", "PB Fintech"],
    "POLYCAB": ["Polycab India"],
    "POWERINDIA": ["Hitachi Energy India"],
    "PPLPHARMA": ["Piramal Pharma"],
    "PREMIERENE": ["Premier Energies"],
    "PRESTIGE": ["Prestige Estates"],
    "RBLBANK": ["RBL Bank"],
    "RECLTD": ["REC Limited", "Rural Electrification"],
    "RVNL": ["RVNL", "Rail Vikas Nigam"],
    "SAIL": ["SAIL", "Steel Authority of India"],
    "SAMMAANCAP": ["Sammaan Capital"],
    "SBICARD": ["SBI Cards"],
    "SHREECEM": ["Shree Cement"],
    "SIEMENS": ["Siemens India"],
    "SOLARINDS": ["Solar Industries"],
    "SONACOMS": ["Sona BLW", "Sona Comstar"],
    "SRF": ["SRF Limited"],
    "SUPREMEIND": ["Supreme Industries"],
    "SUZLON": ["Suzlon Energy"],
    "SYNGENE": ["Syngene International"],
    "TATAELXSI": ["Tata Elxsi"],
    "TATAPOWER": ["Tata Power"],
    "TATATECH": ["Tata Technologies"],
    "TIINDIA": ["Tube Investments of India"],
    "TMPV": ["Tata Motors DVR"],
    "TORNTPHARM": ["Torrent Pharmaceuticals"],
    "TORNTPOWER": ["Torrent Power"],
    "TVSMOTOR": ["TVS Motor"],
    "UNITDSPR": ["United Spirits", "Diageo India"],
    "UNOMINDA": ["Uno Minda", "Minda Industries"],
    "UNIONBANK": ["Union Bank of India"],
    "UPL": ["UPL Limited"],
    "VBL": ["Varun Beverages"],
    "VEDL": ["Vedanta"],
    "VMM": ["Vishnu Prakash R Punglia"],
    "VOLTAS": ["Voltas"],
    "WAAREEENER": ["Waaree Energies"],
    "YESBANK": ["Yes Bank"],
    "ZYDUSLIFE": ["Zydus Lifesciences", "Zydus Cadila"],
    "360ONE": ["360 One WAM"],
    "MFSL": ["Max Financial Services", "Max Life"],
}

# ===========================================================================
# SECTION 9 — Helper Functions
# ===========================================================================

def get_sector(symbol: str) -> str:
    return SECTOR_MAP.get(symbol.upper(), "OTHER")


def get_company_names(symbol: str) -> list[str]:
    return COMPANY_NAMES.get(symbol.upper(), [symbol])


def get_stocks_in_sector(sector: str) -> list[str]:
    return SECTOR_STOCKS.get(sector.upper(), [])


def is_headline_useful(headline: str, symbol: str) -> bool:
    """Return True if headline passes quality filters."""
    if not headline or len(headline) < MIN_HEADLINE_LENGTH:
        return False
    lower = headline.lower()
    for pattern in USELESS_HEADLINE_PATTERNS:
        if pattern.lower() in lower:
            return False
    if REQUIRE_STOCK_NAME_IN_HEADLINE:
        variants = get_company_names(symbol)
        sym_up = symbol.upper()
        hl_up = headline.upper()
        if not (sym_up in hl_up or any(v.lower() in lower for v in variants)):
            return False
    return True


def format_prompt(stage: str, **kwargs) -> str:
    """Format a prompt template for the given pipeline stage."""
    templates = {
        "context":   CONTEXT_PROMPT,
        "sentiment": SENTIMENT_PROMPT,
        "decision":  DECISION_PROMPT,
    }
    if stage not in templates:
        raise ValueError(f"Unknown stage '{stage}'")
    try:
        return templates[stage].format(**kwargs)
    except KeyError as e:
        raise ValueError(f"Missing prompt variable for stage '{stage}': {e}") from e
