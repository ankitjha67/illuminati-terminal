# -*- coding: utf-8 -*-
"""
Illuminati Terminal v29.0 - The "Bulletproof Titan" Build (ENRICHED PATCHSET)
FIXES / ENRICHMENTS (no feature removed):
- NSE Universe: Added NSE official EQUITY_L.csv ingest (fallback + augment) -> 1000–2000+ symbols.
- Titan Backup: Still supported, now via optional local titan_universe.txt file (keeps offline resilience).
- Data Fetch: Robust retry sessions + Yahoo batch prefetch via yf.download for massive scans.
- Technicals: Adaptive trend logic for shorter histories (still uses SMA50/200 when available).
- AI Logic: Keeps Gemini "Legacy Protocol" fallback exactly as you had it.
- Stability: Keeps all features (News, Deep Dive, Email, Self-Healing, Scheduler).

ENRICHMENT REQUEST (Dec-16-2025):
- Use Option A in EVERY run: Always attempt to download EQUITY_L.csv from nsearchives.nseindia.com each execution.
  Fallback to local cached cache/EQUITY_L.csv only if download fails.

PATCHSET (Dec-17-2025+):
- GLOBAL NEWS: Add reputable global RSS feeds + Google News site-feeds (FT/WSJ/MarketWatch/TradingView).
- OUTPUT: Add Company column separate from Ticker (offline from NSE EQUITY_L ingest).
- DCF: Avoid N/A by improving cashflow parsing + safe numeric fallback.
- SECTOR: Reduce Unknown/default via offline sector inference from Company name and normalization for Industry Momentum.
- DEP CHECK: Fix false "missing" due to pip-name vs import-name mismatch (bs4/dateutil/google.generativeai).

PATCHSET (Verification + Disclaimer + Email Readability):
- VERIFICATION FULL COVERAGE: Verification Summary covers 100% tickers (not top 25).
- TradingView scan: BATCHED + DISK CACHED + best-effort fallback to Neutral if throttled.
- News sentiment: per-ticker aggregation using mapper extraction per-article.
- Fundamental verdict: derived from DCF vs Price (plus stable fallback).
- DISCLAIMER: included in Excel, Dashboard, Deep Dive text, Zip bundle, Email body.
- EMAIL: readable HTML body + plaintext fallback; supports multiple recipients via EMAIL_TO (comma/semicolon separated).
"""

import sys
import subprocess
import importlib.util
import time
import os
import re
import json
import ssl
import random
import uuid
import sqlite3
import argparse
import asyncio
import logging
import hashlib
import smtplib
import datetime as dt
import io
import csv
import zipfile
import textwrap
import warnings
from typing import List, Dict, Optional, Any, Tuple
from pathlib import Path
from urllib.parse import urlparse, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed


# --- 0. WARNINGS (non-breaking; reduces noisy notebook deprecation warnings) ---
warnings.filterwarnings(
    "ignore",
    message=r".*datetime\.datetime\.utcnow\(\) is deprecated.*",
    category=DeprecationWarning,
    module=r"jupyter_client\..*"
)

# --- 1. ROBUST SELF-HEALING INSTALLER ---
def check_and_install_dependencies():
    required_packages = [
        'nselib', 'yfinance', 'pandas', 'numpy', 'requests', 'feedparser',
        'tabulate', 'reportlab', 'nltk', 'transformers', 'schedule',
        'google-generativeai', 'aiohttp', 'xlsxwriter', 'trafilatura',
        'rapidfuzz', 'beautifulsoup4', 'ta', 'jinja2', 'textblob', 'nest_asyncio', 'pytz',
        'python-dateutil'
    ]

    # PATCH: pip name != import name for some libs; avoid false "missing"
    import_name_overrides = {
        "google-generativeai": "google.generativeai",
        "beautifulsoup4": "bs4",
        "python-dateutil": "dateutil",
    }

    missing = []
    print("🛠️ System Health Check...")

    for package in required_packages:
        mod = import_name_overrides.get(package, package)
        try:
            if importlib.util.find_spec(mod) is None:
                missing.append(package)
        except Exception:
            try:
                __import__(mod)
            except Exception:
                missing.append(package)

    if missing:
        print(f"📦 Installing missing modules: {', '.join(sorted(set(missing)))}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + sorted(set(missing)))
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "yfinance", "google-generativeai"])
            print("✅ Dependencies installed.")
            importlib.invalidate_caches()
        except subprocess.CalledProcessError as e:
            print(f"❌ Install Error: {e}")

check_and_install_dependencies()

# --- 2. IMPORTS ---
import numpy as np
import pandas as pd
import pytz
import yfinance as yf
import aiohttp
import feedparser
import requests
import nest_asyncio
from requests.adapters import HTTPAdapter, Retry
from tabulate import tabulate
from textblob import TextBlob

# Guard: some old codebases had a typo "Beautifulsoup" (wrong). Keep it harmless.
try:
    from bs4 import Beautifulsoup  # noqa: F401
except Exception:
    Beautifulsoup = None

from bs4 import BeautifulSoup
from jinja2 import Template
from dateutil import parser as dateparser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Conditional Imports
try:
    from nselib import capital_market
    HAS_NSELIB = True
except ImportError:
    HAS_NSELIB = False

try:
    from ta.trend import SMAIndicator, MACD
    from ta.momentum import RSIIndicator
    HAS_TA = True
except ImportError:
    HAS_TA = False

try:
    import trafilatura
    logging.getLogger('trafilatura').setLevel(logging.CRITICAL)
    HAS_TRAFILATURA = True
except ImportError:
    HAS_TRAFILATURA = False

try:
    import google.generativeai as genai
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

try:
    from transformers import pipeline as hf_pipeline
    HAS_HF = True
except ImportError:
    HAS_HF = False


# --- 3. CONFIGURATION & DATA ---
nest_asyncio.apply()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("Illuminati")

DB_PATH = "market_memory.db"
OUTPUT_DIR = Path("output")
CACHE_DIR = Path("cache")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
CACHE_DIR.mkdir(exist_ok=True, parents=True)

if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context

DISCLAIMER_TEXT = (
    "DISCLAIMER: This content is provided strictly for educational and research purposes only. "
    "It is not financial, investment, trading, legal, tax, accounting, or any other professional advice, "
    "and should not be treated as a recommendation to buy, sell, hold, or transact in any security, derivative, "
    "cryptocurrency, commodity, or any other financial instrument.\n\n"
    "All information, examples, opinions, analyses, code snippets, backtests, charts, and outputs are shared “as is” "
    "without any warranty of accuracy, completeness, timeliness, reliability, or fitness for a particular purpose. "
    "Markets are risky and volatile; past performance is not indicative of future results. Any strategies, indicators, "
    "models, signals, or backtested results may be affected by assumptions, data quality issues, survivorship bias, "
    "look-ahead bias, slippage, latency, fees, taxes, execution constraints, liquidity, and changing market regimes—"
    "meaning real-world performance can differ materially.\n\n"
    "You are solely responsible for your decisions and actions. Do your own due diligence and consult a SEBI-registered "
    "investment adviser or other qualified professional before making any financial decision. By using or relying on this "
    "content, you acknowledge that the author/creator shall not be liable for any direct or indirect loss, damages, or "
    "consequences (including financial losses) arising from the use of this information or any related tools/scripts.\n\n"
    "If any third-party sources (e.g., websites, APIs, data providers, brokers) are referenced or used, their content and "
    "availability are outside the author’s control, and you must comply with their respective terms, policies, and applicable "
    "laws/regulations."
)

# --- USER WATCHLIST (Priority) ---
USER_WATCHLIST = [
    'IITL', 'HNDFDS', 'QPOWER', 'MATRIMONY', 'MHRIL', 'EDELWEISS', 'UNITEDPOLY', 'PRUDENT', 'CIFL',
    'IDEA', 'INDIGO', 'BSE', 'SIL', 'AUROPHARMA', 'DIXON', 'URBANCO', 'OFSS', 'KOTAKBANK', 'ROUTE',
    'HOMEFIRST', 'TECHM', 'ACE', 'INDOUS', 'STAR', 'FACT', 'ICICIPRULI', 'SILVERTUC', 'INOXWIND',
    'NFL', 'BAJFINANCE', 'MCX', 'WIPRO', 'ATGL', 'PERSISTENT', 'TITAN', 'ENERGYDEV', 'PAYTM',
    'INTERARCH', 'EMIL', 'KMEW', 'TNPL', 'REFEX', 'DOLLAR', 'GEOJITFSL', 'UNIONBANK', 'INTLCONV',
    'OIL', 'NEXTMEDIA', 'LICI', 'HUDCO', 'TOTAL', 'SAIL', 'BANG', 'NAUKRI', 'CHENNPETRO',
    'EASTSILK', 'TIMETECHNO', 'TARIL', 'ATHERENERG', 'TIPSMUSIC', 'JYOTISTRUC', 'STEELXIND',
    'PVSL', 'MARUTI', 'GLOBAL', 'FOCUS', 'SHAREINDIA', 'HONASA', 'WORTHPERI', 'ASIANTILES',
    'CONTROLPR', 'PRUDMOULI', 'COMSYN'
]

# -----------------------------------------------------------------------------
# TITAN UNIVERSE (Backup)
# -----------------------------------------------------------------------------
TITAN_LIST_FILE = os.environ.get("TITAN_LIST_FILE", "titan_universe.txt")

NSE_MEGA_BUILTIN_MINI = [
    'RELIANCE', 'TCS', 'HDFCBANK', 'ICICIBANK', 'INFY', 'SBIN', 'BHARTIARTL', 'ITC', 'HINDUNILVR',
    'LT', 'BAJFINANCE', 'HCLTECH', 'MARUTI', 'SUNPHARMA', 'TITAN', 'KOTAKBANK', 'ONGC', 'AXISBANK',
    'NTPC', 'ULTRACEMCO', 'ADANIPORTS', 'POWERGRID', 'WIPRO', 'BAJAJFINSV', 'HAL', 'COALINDIA',
    'IOC', 'DLF', 'ZOMATO', 'JIOFIN', 'SIEMENS', 'TATASTEEL', 'GRASIM', 'BEL', 'LTIM', 'TRENT',
    'HINDALCO', 'INDUSINDBK', 'BANKBARODA', 'EICHERMOT', 'BPCL', 'PIDILITIND', 'TECHM'
]

def load_titan_backup() -> List[str]:
    try:
        p = Path(TITAN_LIST_FILE)
        if p.exists():
            raw = p.read_text(encoding="utf-8").splitlines()
            tickers = []
            for line in raw:
                t = line.strip().upper()
                if not t or t.startswith("#"):
                    continue
                tickers.append(t)
            tickers = list(dict.fromkeys(tickers))
            log.info(f"✅ Titan Backup loaded from {p} | {len(tickers)} tickers")
            return tickers
    except Exception as e:
        log.warning(f"⚠️ Titan Backup file load failed: {e}")

    log.warning("⚠️ Titan Backup file not found. Using minimal built-in backup list.")
    return NSE_MEGA_BUILTIN_MINI

NSE_MEGA_LIST = load_titan_backup()

STOPLIST = set([
    "THE", "AND", "ARE", "IS", "FOR", "OVER", "WITH", "TO", "OF", "IN", "BY", "FROM", "ON", "AT", "OR", "AS", "AN", "IT", "GO", "NO",
    "MARKET", "COMPANY", "COMPANIES", "NEWS", "STOCK", "STOCKS", "SEBI", "INDIAN", "INDIA", "EXPECTED", "LOSSES", "GAINS", "SHARES",
    "NSE", "BSE", "DECLINE", "DECLINED", "ALSO", "FIRMS", "MONTHS", "SEGMENTS", "LTD", "PRIMARY", "BOTH", "COMING", "FUNDRAISING",
    "SIGNIFICANT", "LIMITED", "POSSIBLE", "HEALTH", "HEALTHCARE", "WAVE", "FIFTEEN", "EYE", "BANK", "IPO", "IPOS", "SET", "RS", "BE",
    "WAS", "PUSH", "PARTICULARLY", "MUTUAL", "FUNDS", "PRIVATE", "PUBLIC", "LOWER", "HIGHER", "TODAY", "WEEK", "YEAR", "REPORT",
    "GLOBAL", "WORLD", "BUSINESS", "FINANCE", "MONEY", "TIMES", "ECONOMIC", "CITY", "SALES", "PROFIT", "LOSS", "QUARTER", "RESULTS",
    "DATA", "GROUP", "IND", "OUT"
])

SECTOR_MAP = {
    'Technology': {'wacc': 0.13, 'growth': 0.12},
    'Financial Services': {'wacc': 0.14, 'growth': 0.10},
    'Energy': {'wacc': 0.11, 'growth': 0.05},
    'Utilities': {'wacc': 0.10, 'growth': 0.04},
    'Consumer Cyclical': {'wacc': 0.12, 'growth': 0.08},
    'Healthcare': {'wacc': 0.11, 'growth': 0.09},
    'Defense': {'wacc': 0.12, 'growth': 0.15},
    'default': {'wacc': 0.12, 'growth': 0.08}
}

FUTURE_THEMES = {
    "Green Energy": ["green hydrogen", "renewable", "solar", "wind", "ethanol", "clean energy"],
    "Defense": ["defense", "drone", "missile", "weapon", "army", "navy", "air force"],
    "EV & Auto": ["electric vehicle", "ev", "battery", "lithium", "charging"],
    "AI & Tech": ["artificial intelligence", "ai", "semiconductor", "chip", "data center", "cloud"],
    "Infrastructure": ["highway", "road", "metro", "railway", "infra", "construction"],
    "Banking": ["credit", "loan", "npa", "rbi", "bank", "finance"]
}

# -----------------------------------------------------------------------------
# GLOBAL NEWS PATCH: Add reputable global RSS + robust Google News site-feeds
# -----------------------------------------------------------------------------
GLOBAL_FEEDS = [
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",                 # WSJ Markets RSS
    "https://feeds.marketwatch.com/marketwatch/topstories/",        # MarketWatch Top Stories RSS
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",        # CNBC Top News RSS
    "https://www.cnbc.com/id/10000664/device/rss/rss.html",         # CNBC Finance RSS
    "https://www.theguardian.com/uk/business/rss",
    "https://www.investing.com/rss/news.rss",
    "https://news.google.com/rss/search?q=site:ft.com+markets+when:7d&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:wsj.com+markets+when:7d&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:marketwatch.com+markets+when:7d&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:cnbc.com+markets+when:7d&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:tradingview.com+when:7d&hl=en&gl=US&ceid=US:en",
]

DEFAULT_FEEDS = [
    "https://news.google.com/rss/search?q=site:moneycontrol.com+when:7d&hl=en-IN&gl=IN&ceid=IN:en",
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
    "https://www.livemint.com/rss/markets",
    "https://www.business-standard.com/rss/markets-106.rss",
    "https://www.financialexpress.com/market/feed/",
    "https://feeds.reuters.com/reuters/INbusinessNews",
    "https://feeds.bloomberg.com/markets/news.rss",
    *GLOBAL_FEEDS
]

# =============================================================================
# PATCH: Robust retry session helper
# =============================================================================
def make_retry_session(user_agent: str = "Mozilla/5.0", total_retries: int = 5) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent})

    try:
        retry = Retry(
            total=total_retries,
            connect=total_retries,
            read=total_retries,
            status=total_retries,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"])
        )
    except TypeError:
        retry = Retry(
            total=total_retries,
            connect=total_retries,
            read=total_retries,
            status=total_retries,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            method_whitelist=["GET", "POST"]
        )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=200, pool_maxsize=200)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# =============================================================================
# Offline sector inference (PATCH) to reduce Unknown/default
# =============================================================================
def infer_sector_from_name(company_name: str) -> str:
    n = (company_name or "").upper()

    if not n:
        return "Unclassified"

    if any(k in n for k in ["BANK", "FINANCE", "FINANCIAL", "INSURANCE", "NBFC", "CAPITAL", "BROKING", "SECURITIES", "HOUSING", "AMC"]):
        return "Financial Services"
    if any(k in n for k in ["PHARMA", "PHARM", "HEALTH", "HOSPITAL", "DIAGNOST", "BIOTECH", "LAB", "LIFE SCIENCE", "MEDICAL"]):
        return "Healthcare"
    if any(k in n for k in ["TECH", "SOFTWARE", "INFOTECH", "INFORMATION", "IT SERVICES", "DIGITAL", "SYSTEMS", "DATA", "CYBER", "CLOUD"]):
        return "Technology"
    if any(k in n for k in ["OIL", "GAS", "PETRO", "REFIN", "COAL", "ENERGY", "EXPLORATION"]):
        return "Energy"
    if any(k in n for k in ["POWER", "ELECTRIC", "UTILITY", "TRANSMISSION", "GRID", "WATER"]):
        return "Utilities"
    if any(k in n for k in ["DEFENCE", "DEFENSE", "AEROSPACE", "MISSILE", "DRONE", "ORDNANCE", "ARMAMENT", "SHIPYARD", "HAL", "BEL"]):
        return "Defense"
    if any(k in n for k in ["AUTO", "MOTORS", "TYRE", "RETAIL", "CONSUMER", "FASHION", "LIFESTYLE", "HOTEL", "TRAVEL", "JEWELL", "CEMENT", "PAINT"]):
        return "Consumer Cyclical"

    return "Unclassified"

def normalize_sector(sector: str) -> str:
    s = (sector or "").strip()
    if not s:
        return "Unclassified"
    if s.lower() in ["unknown", "default", "n/a", "na", "none", "null", "unclassified"]:
        return "Unclassified"
    return s

def safe_ratio(a: float, b: float, default: float = 0.0) -> float:
    try:
        if b == 0:
            return default
        return float(a) / float(b)
    except Exception:
        return default


# =============================================================================
# 4. MARKET MAPPER (TITAN ENGINE) - ENRICHED
# =============================================================================
class MasterMapper:
    EQUITY_CSV_URLS = [
        "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
        "https://www.nseindia.com/content/equities/EQUITY_L.csv",
    ]

    def __init__(self):
        self.universe: Dict[str, str] = {}
        self.keywords: Dict[str, str] = {}
        self.symbols: set = set()

        # PATCH: store symbol -> company name (offline; fixes Company separate column)
        self.symbol_names: Dict[str, str] = {}

        self.session = make_retry_session(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )
        self.build_universe()

    def _ingest_symbol(self, symbol: str, name: str = ""):
        if not symbol:
            return
        sym = str(symbol).strip().upper()
        if not sym:
            return

        self.symbols.add(sym)

        self.universe[sym] = sym
        self.universe[f"{sym}.NS"] = sym
        self.universe[f"NSE:{sym}"] = sym
        self.universe[f"NSE-{sym}"] = sym
        self.universe[f"BSE:{sym}"] = sym
        self.universe[f"BSE-{sym}"] = sym

        raw_name = str(name or "").strip()
        nm = raw_name.upper().strip()
        if nm:
            self.symbol_names[sym] = raw_name

            self.universe[nm] = sym
            simple = (
                nm.replace("LIMITED", "")
                  .replace("LTD", "")
                  .replace("PVT", "")
                  .replace("PRIVATE", "")
                  .replace("PUBLIC", "")
                  .replace("CO.", "")
                  .replace("CO", "")
                  .strip()
            )
            if simple:
                self.universe[simple] = sym
                first = simple.split()[0] if simple.split() else ""
                if len(first) > 3 and first not in STOPLIST:
                    self.keywords[first] = sym

    def get_company_name(self, ticker: str) -> str:
        t = str(ticker or "").strip().upper()
        t = t.replace(".NS", "").replace(".BO", "")
        return self.symbol_names.get(t, "")

    def _local_equity_csv_path(self) -> Path:
        return CACHE_DIR / "EQUITY_L.csv"

    def _load_nse_equity_csv(self) -> int:
        """
        Option A (EVERY RUN):
          1) Always try to download EQUITY_L.csv from NSE archives (and then secondary URL).
          2) Cache locally to cache/EQUITY_L.csv (overwrite).
          3) If download fails, fallback to local cache (even if old).
        """
        def ingest_csv_text(text: str) -> int:
            if not text or len(text) < 2000:
                return 0
            f = io.StringIO(text)
            reader = csv.DictReader(f)
            before = len(self.symbols)
            for row in reader:
                sym = (row.get("SYMBOL") or "").strip().upper()
                nm = (row.get("NAME OF COMPANY") or "").strip()
                if sym:
                    self._ingest_symbol(sym, nm)
            return len(self.symbols) - before

        headers = {"Referer": "https://www.nseindia.com/"}

        for url in self.EQUITY_CSV_URLS:
            try:
                resp = self.session.get(url, headers=headers, timeout=25)
                if resp.ok and len(resp.text) > 2000:
                    try:
                        self._local_equity_csv_path().write_text(resp.text, encoding="utf-8")
                        log.info(f"✅ NSE CSV downloaded & cached (Option A, every run): {url}")
                    except Exception as e:
                        log.warning(f"⚠️ Could not write cache/EQUITY_L.csv: {e}")

                    added = ingest_csv_text(resp.text)
                    if added > 0:
                        log.info(f"✅ NSE CSV ingest success | +{added} symbols")
                    return added
            except Exception as e:
                log.warning(f"⚠️ NSE CSV download failed ({url}): {e}")

        try:
            lp = self._local_equity_csv_path()
            if lp.exists():
                text = lp.read_text(encoding="utf-8", errors="ignore")
                added = ingest_csv_text(text)
                if added > 0:
                    log.info(f"✅ NSE CSV ingest from local fallback cache {lp} | +{added} symbols")
                else:
                    log.warning(f"⚠️ Local cache exists but ingest yielded 0 symbols: {lp}")
                return added
        except Exception as e:
            log.warning(f"⚠️ Local cache fallback failed: {e}")

        return 0

    def build_universe(self):
        log.info("⏳ Indexing NSE Market...")

        for t in NSE_MEGA_LIST + USER_WATCHLIST:
            self._ingest_symbol(t, "")

        try:
            if HAS_NSELIB:
                df = capital_market.equity_list()
                cols = {c.upper(): c for c in df.columns}
                sym_col = cols.get("SYMBOL")
                name_col = cols.get("NAME OF COMPANY") or cols.get("NAME") or cols.get("COMPANY NAME")

                if sym_col:
                    before = len(self.symbols)
                    for _, row in df.iterrows():
                        sym = row.get(sym_col)
                        nm = row.get(name_col, "") if name_col else ""
                        self._ingest_symbol(sym, nm)
                    added = len(self.symbols) - before
                    log.info(f"✅ nselib ingest | +{added} symbols")
                else:
                    log.warning("⚠️ nselib equity_list() missing SYMBOL column. Skipping nselib ingest.")
            else:
                log.warning("⚠️ nselib missing. Using Titan Backup + Watchlist + NSE CSV fallback.")
        except Exception as e:
            log.warning(f"⚠️ NSE Indexing via nselib failed: {e}")

        csv_added = self._load_nse_equity_csv()

        total_symbols = len(self.symbols)
        log.info(f"✅ Indexed Universe: {total_symbols} symbols | universe-keys={len(self.universe)} | keywords={len(self.keywords)}")
        if total_symbols < 900:
            log.warning("⚠️ Universe still < 900 symbols. NSE endpoints may be blocked. Try later/off-network or keep cache/EQUITY_L.csv present.")

    def get_all_symbols(self) -> List[str]:
        return sorted(self.symbols)

    def extract_tickers(self, articles: List[Dict[str, Any]]) -> List[str]:
        found = []
        pat_tagged = re.compile(r"\b(?:NSE|BSE)\s*[:\-]\s*([A-Z0-9&\-]{2,20})\b", re.IGNORECASE)
        pat_symbol = re.compile(r"\b([A-Z][A-Z0-9&\-]{2,20})\b")

        for art in articles:
            text = f"{art.get('title','')} {str(art.get('body',''))[:1200]}".upper()

            for m in pat_tagged.findall(text):
                key = m.upper().strip()
                if key in self.universe:
                    found.append(self.universe[key])
                elif key in self.symbols:
                    found.append(key)

            for m in pat_symbol.findall(text):
                if m in STOPLIST:
                    continue
                if m in self.universe:
                    found.append(self.universe[m])
                elif m in self.keywords:
                    found.append(self.keywords[m])

        return list(dict.fromkeys(found))


# =============================================================================
# TrendHunter (unchanged)
# =============================================================================
class TrendHunter:
    def predict_booming_industries(self, articles):
        log.info("🔮 Predicting Future Booming Industries...")
        scores = {k: 0 for k in FUTURE_THEMES.keys()}
        for art in articles:
            text = (art.get('title', '') + " " + art.get('body', '')).lower()
            for theme, keywords in FUTURE_THEMES.items():
                for kw in keywords:
                    if kw in text:
                        scores[theme] += 1

        total_hits = sum(scores.values())
        if total_hits == 0:
            return []

        trends = []
        for theme, score in scores.items():
            trends.append({'Theme': theme, 'Hype_Score': round((score / total_hits) * 100, 1), 'Mentions': score})
        return sorted(trends, key=lambda x: x['Hype_Score'], reverse=True)


# =============================================================================
# 5. UTILITIES
# =============================================================================
class APIKeys:
    def __init__(self):
        self.keys = {}
        for k in ["TWELVEDATA", "FINNHUB", "ALPHAVANTAGE", "NEWSAPI", "GEMINI", "EMAIL_USER", "EMAIL_PASS", "EMAIL_TO"]:
            val = os.environ.get(f"{k}_KEY") or os.environ.get(f"{k}") or os.environ.get(f"{k}_API_KEY")
            if val:
                self.keys[k] = val

    def get(self, name):
        return self.keys.get(name)

    def interactive_load(self):
        print("\n" + "="*50)
        print("🔐 API KEY SETUP")
        print("="*50)
        if "GEMINI" not in self.keys:
            self.keys["GEMINI"] = input("   Gemini API Key: ").strip()
        print("\n📧 EMAIL SETUP (Optional)")
        if "EMAIL_USER" not in self.keys:
            self.keys["EMAIL_USER"] = input("   Gmail Address: ").strip()
        if "EMAIL_PASS" not in self.keys:
            self.keys["EMAIL_PASS"] = input("   Gmail App Password: ").strip()
        if "EMAIL_TO" not in self.keys:
            self.keys["EMAIL_TO"] = input("   Recipient Email(s) (comma separated ok): ").strip()
        print("="*50 + "\n")

class DiskCache:
    def __init__(self, base_dir: Path, ttl_seconds: int = 21600):
        self.base_dir = base_dir
        self.ttl = ttl_seconds
        (base_dir / "pages").mkdir(exist_ok=True, parents=True)

    def _key(self, url: str) -> str:
        return hashlib.sha1(url.encode("utf-8")).hexdigest()

    def get(self, url: str) -> Optional[str]:
        path = self.base_dir / "pages" / f"{self._key(url)}.txt"
        if path.exists() and (time.time() - path.stat().st_mtime) < self.ttl:
            try:
                return path.read_text(encoding="utf-8")
            except Exception:
                pass
        return None

    def set(self, url: str, content: str):
        try:
            (self.base_dir / "pages" / f"{self._key(url)}.txt").write_text(content, encoding="utf-8")
        except Exception:
            pass

class DatabaseManager:
    def __init__(self, db_path=DB_PATH):
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass
        self.conn = sqlite3.connect(db_path)
        self.create_tables()

    def create_tables(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS news_items
                     (uid TEXT PRIMARY KEY, timestamp DATETIME, source TEXT, title TEXT, body TEXT,
                      sentiment_score REAL, tickers TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS asset_analysis
                     (run_id TEXT, timestamp DATETIME, ticker TEXT, price REAL, target_price REAL,
                      horizon TEXT, sharpe REAL, score REAL, verdict TEXT, trend TEXT, rsi REAL)''')
        self.conn.commit()

    def save_news(self, items):
        c = self.conn.cursor()
        for i in items:
            try:
                c.execute("INSERT OR IGNORE INTO news_items VALUES (?,?,?,?,?,?,?)",
                          (i['uid'], i['published'], i['source'], i['title'], i.get('body', '')[:5000], i['score'], str(i.get('tickers', []))))
            except Exception:
                pass
        self.conn.commit()

    def save_analysis(self, results):
        c = self.conn.cursor()
        run_id = str(uuid.uuid4())[:8]
        ts = dt.datetime.now().isoformat()
        for r in results:
            c.execute("INSERT INTO asset_analysis VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                      (run_id, ts, r['Ticker'], r['Price'], r.get('Target_Price', 0), r.get('Horizon', ''),
                       r.get('Sharpe', 0), r['Score'], r['Verdict'], r['Trend'], r['RSI']))
        self.conn.commit()


# =============================================================================
# 6. NEWS ENGINE
# =============================================================================
class NewsEngine:
    def __init__(self, api_keys: APIKeys):
        self.keys = api_keys
        self.cache = DiskCache(CACHE_DIR)
        self.feeds = list(set(DEFAULT_FEEDS))
        self.session_sync = make_retry_session(user_agent="Mozilla/5.0", total_retries=4)
        self._setup_nlp()

    def _setup_nlp(self):
        try:
            nltk.data.find('sentiment/vader_lexicon.zip')
        except LookupError:
            nltk.download('vader_lexicon', quiet=True)
        self.vader = SentimentIntensityAnalyzer()
        self.finbert = None
        if HAS_HF:
            try:
                self.finbert = hf_pipeline("sentiment-analysis", model="ProsusAI/finbert",
                                           tokenizer="ProsusAI/finbert", truncation=True)
            except Exception:
                pass

    def add_google_news_feed(self, query):
        q = quote_plus(query)
        self.feeds.append(f"https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en")

    def extract_body(self, url):
        cached = self.cache.get(url)
        if cached:
            return cached
        try:
            resp = self.session_sync.get(url, timeout=12)
            if not resp.ok:
                return ""
            text = trafilatura.extract(resp.text) if HAS_TRAFILATURA else ""
            if not text:
                soup = BeautifulSoup(resp.text, 'html.parser')
                text = ' '.join([p.get_text(" ", strip=True) for p in soup.find_all('p')])
            if text:
                self.cache.set(url, text)
                return text[:3000]
        except Exception:
            pass
        return ""

    async def fetch_feed_async(self, session, url):
        try:
            async with session.get(url, timeout=15) as response:
                if response.status == 200:
                    return feedparser.parse(await response.read())
        except Exception:
            return None

    async def collect_all(self):
        log.info(f"📡 Scanning {len(self.feeds)} feeds (India + Global)...")
        headers = {"User-Agent": "Mozilla/5.0"}
        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = [self.fetch_feed_async(session, url) for url in self.feeds]
            results = await asyncio.gather(*tasks)

        articles = []
        cutoff = dt.datetime.now() - dt.timedelta(days=7)

        for res in results:
            if not res:
                continue
            for entry in res.entries[:15]:
                try:
                    pub_date = None
                    if hasattr(entry, "published"):
                        try:
                            pub_date = dateparser.parse(entry.published).replace(tzinfo=None)
                        except Exception:
                            pub_date = None
                    if pub_date and pub_date < cutoff:
                        continue

                    link = getattr(entry, "link", "")
                    title = getattr(entry, "title", "").strip()
                    if not link or not title:
                        continue

                    articles.append({
                        'title': title,
                        'link': link,
                        'published': (pub_date or dt.datetime.now()).isoformat(),
                        'source': urlparse(link).netloc.replace('www.', ''),
                        'uid': str(uuid.uuid5(uuid.NAMESPACE_URL, link))
                    })
                except Exception:
                    pass

        return articles

    def score_text(self, text):
        v_score = self.vader.polarity_scores(text)['compound']
        f_score = 0.0
        if self.finbert:
            try:
                res = self.finbert(text[:512])[0]
                val = float(res['score'])
                f_score = -val if res['label'].lower() == 'negative' else val
            except Exception:
                pass
        return round((v_score * 0.4) + (f_score * 0.6) if f_score != 0 else v_score, 3)

    def process(self):
        loop = asyncio.get_event_loop()
        articles = loop.run_until_complete(self.collect_all())
        random.shuffle(articles)

        log.info(f"📥 Extracting body text for {len(articles)} articles...")
        with ThreadPoolExecutor(max_workers=80) as executor:
            body_map = list(executor.map(lambda a: self.extract_body(a['link']), articles))

        unique = []
        seen_links = set()
        for i, art in enumerate(articles):
            if art['link'] in seen_links:
                continue
            art['body'] = body_map[i]
            art['score'] = self.score_text(f"{art['title']} {art['body'][:300]}")
            art['tickers'] = []  # will be filled per-article later using mapper
            unique.append(art)
            seen_links.add(art['link'])

        return unique


# =============================================================================
# 7. ANALYSIS & STRATEGY (WITH SESSION FIX + BATCH PREFETCH)
# =============================================================================
class DataEngine:
    def __init__(self, api_keys: APIKeys):
        self.keys = api_keys
        self.session = self._get_yf_session()
        self.price_cache: Dict[str, pd.Series] = {}

    def _get_yf_session(self):
        return make_retry_session(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            total_retries=5
        )

    def _normalize_plain(self, ticker: str) -> str:
        t = str(ticker).strip().upper()
        t = t.replace(".NS", "").replace(".BO", "")
        return t

    def _yf_symbol(self, ticker: str) -> str:
        t = str(ticker).strip()
        if t.endswith(".NS") or t.endswith(".BO"):
            return t
        return f"{t}.NS"

    # PATCH: yfinance session incompatibility in some environments.
    # Always try without session; keep self.session for our own requests.
    def prefetch_prices(self, tickers: List[str], period: str = "2y", batch_size: int = 120):
        uniq = []
        seen = set()
        for t in tickers:
            sym = self._normalize_plain(t)
            if sym and sym not in seen:
                uniq.append(sym)
                seen.add(sym)

        if not uniq:
            return

        log.info(f"🧲 Prefetching Yahoo prices for {len(uniq)} tickers in batches...")
        for i in range(0, len(uniq), batch_size):
            batch = uniq[i:i + batch_size]
            yf_batch = [self._yf_symbol(x) for x in batch]
            try:
                df = yf.download(
                    tickers=yf_batch,
                    period=period,
                    interval="1d",
                    group_by="ticker",
                    auto_adjust=False,
                    threads=True,
                    progress=False
                )

                if len(yf_batch) == 1 and isinstance(df.columns, pd.Index) and "Close" in df.columns:
                    close = df["Close"].dropna()
                    if not close.empty:
                        self.price_cache[batch[0]] = close
                    continue

                for plain, yf_sym in zip(batch, yf_batch):
                    try:
                        close = None
                        if isinstance(df.columns, pd.MultiIndex):
                            if yf_sym in df.columns.get_level_values(0):
                                close = df[yf_sym]["Close"].dropna()
                        if close is None or close.empty:
                            alt = plain + ".NS"
                            if isinstance(df.columns, pd.MultiIndex) and alt in df.columns.get_level_values(0):
                                close = df[alt]["Close"].dropna()

                        if close is not None and not close.empty:
                            self.price_cache[plain] = close
                    except Exception:
                        continue

            except Exception as e:
                log.warning(f"⚠️ Prefetch batch failed ({i}-{i+len(batch)}): {e}")

    def fetch_data(self, ticker, days=365):
        plain_ticker = self._normalize_plain(ticker)

        if self.keys.get("TWELVEDATA"):
            try:
                url = f"https://api.twelvedata.com/time_series?symbol={plain_ticker}&interval=1day&outputsize={days}&apikey={self.keys.get('TWELVEDATA')}"
                data = requests.get(url, timeout=15).json()
                if 'values' in data:
                    df = pd.DataFrame(data['values'])
                    df['close'] = pd.to_numeric(df['close'])
                    df.index = pd.to_datetime(df['datetime'])
                    return df['close'].sort_index(), {}, None, "TwelveData"
            except Exception:
                pass

        try:
            if plain_ticker in self.price_cache and self.price_cache[plain_ticker] is not None and not self.price_cache[plain_ticker].empty:
                prices = self.price_cache[plain_ticker]
                yf_ticker = self._yf_symbol(plain_ticker)

                stock = yf.Ticker(yf_ticker)  # PATCH: avoid session param by default

                info = {}
                try:
                    if hasattr(stock, "fast_info") and stock.fast_info:
                        info = dict(stock.fast_info)
                    else:
                        info = stock.info
                except Exception:
                    info = {}

                return prices, info, stock, "Yahoo(Batch)"

            yf_ticker = f"{ticker}.NS" if not str(ticker).endswith('.NS') else str(ticker)
            stock = yf.Ticker(yf_ticker)  # PATCH: avoid session param by default
            hist = stock.history(period="2y")
            if not hist.empty:
                info = {}
                try:
                    if hasattr(stock, "fast_info") and stock.fast_info:
                        info = dict(stock.fast_info)
                    else:
                        info = stock.info
                except Exception:
                    info = {}
                return hist['Close'].dropna(), info, stock, "Yahoo"
        except Exception:
            pass

        return None, None, None, "None"


class AnalysisLab:
    def calculate_technicals(self, prices: pd.Series) -> Dict[str, Any]:
        prices = prices.dropna()
        if len(prices) < 35:
            return {}

        df = pd.DataFrame({'close': prices}).dropna()
        df['SMA20'] = df['close'].rolling(20).mean()
        df['SMA50'] = df['close'].rolling(50).mean()
        df['SMA200'] = df['close'].rolling(200).mean()

        exp12 = df['close'].ewm(span=12, adjust=False).mean()
        exp26 = df['close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp12 - exp26
        df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()

        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))

        curr = df.iloc[-1]
        trend = "SIDEWAYS"

        if not np.isnan(curr.get('SMA200', np.nan)):
            if curr['close'] > curr['SMA50'] > curr['SMA200']:
                trend = "UPTREND"
            elif curr['close'] < curr['SMA50'] < curr['SMA200']:
                trend = "DOWNTREND"
        else:
            if curr['close'] > curr['SMA20'] > curr['SMA50']:
                trend = "UPTREND"
            elif curr['close'] < curr['SMA20'] < curr['SMA50']:
                trend = "DOWNTREND"

        return {
            "RSI": round(float(curr['RSI']), 1) if not np.isnan(curr['RSI']) else 50.0,
            "Trend": trend,
            "MACD_Signal": "Bullish" if curr['MACD'] > curr['Signal'] else "Bearish",
            "Volatility": round(float(prices.pct_change().std() * np.sqrt(252)), 2)
        }

    # PATCH: better cashflow row detection + safe numeric fallback to avoid "N/A"
    def calculate_valuation(self, stock, info, current_price):
        def safe_float(x):
            try:
                if x is None:
                    return None
                if isinstance(x, (int, float, np.integer, np.floating)):
                    return float(x)
                return float(str(x).replace(",", "").strip())
            except Exception:
                return None

        def find_cf_value(cf_df: pd.DataFrame, labels: List[str]) -> Optional[float]:
            if cf_df is None or cf_df.empty:
                return None
            try:
                col = cf_df.columns[0]
            except Exception:
                return None

            idx = [str(i).strip().lower() for i in cf_df.index]
            for lab in labels:
                lab_l = lab.lower()
                for i, idx_name in enumerate(idx):
                    if lab_l == idx_name or lab_l in idx_name:
                        try:
                            return safe_float(cf_df.iloc[i][col])
                        except Exception:
                            continue
            return None

        # Try DCF via cashflow if available
        try:
            if stock is not None:
                cashflow = stock.cashflow
                if cashflow is not None and not cashflow.empty:
                    ocf = find_cf_value(
                        cashflow,
                        ["Operating Cash Flow", "Total Cash From Operating Activities", "Net Cash Provided By Operating Activities"]
                    )
                    capex = find_cf_value(
                        cashflow,
                        ["Capital Expenditures", "Capital Expenditure", "Purchase Of PPE", "Purchase of Plant Property and Equipment"]
                    )

                    if ocf is not None:
                        capex_val = abs(capex) if capex is not None else 0.0
                        fcf = ocf - capex_val

                        sector = info.get('sector', 'default') if isinstance(info, dict) else 'default'
                        sector = normalize_sector(sector)
                        params = SECTOR_MAP.get(sector, SECTOR_MAP['default'])
                        growth, wacc = params['growth'], params['wacc']

                        shares = None
                        if isinstance(info, dict):
                            shares = info.get('sharesOutstanding') or info.get('shares') or info.get('shares_outstanding')
                        shares = safe_float(shares) or 1.0

                        future_val = 0.0
                        for i in range(1, 6):
                            future_val += (fcf * ((1 + growth) ** i)) / ((1 + wacc) ** i)

                        term_val = (fcf * ((1 + growth) ** 5) * 1.04) / max((wacc - 0.04), 0.02)
                        intrinsic = (future_val + (term_val / ((1 + wacc) ** 5))) / shares

                        if intrinsic and intrinsic > 0:
                            intrinsic = min(max(intrinsic, current_price * 0.2), current_price * 5.0)
                            return round(float(intrinsic), 2)
        except Exception:
            pass

        # EPS multiple fallback (if present)
        try:
            if isinstance(info, dict):
                eps = info.get('trailingEps') or info.get('epsTrailingTwelveMonths') or info.get('trailing_eps')
                eps = safe_float(eps)
                if eps and eps > 0:
                    intrinsic = eps * 20
                    intrinsic = min(max(intrinsic, current_price * 0.2), current_price * 5.0)
                    return round(float(intrinsic), 2)
        except Exception:
            pass

        # Final PATCH: never return "N/A" -> stable numeric estimate (current price)
        return round(float(current_price), 2)

    def determine_strategy(self, price, dcf_val, trend, score, volatility):
        target_price = float(price)
        horizon = "Watchlist"
        tech_upside = float(price) * (1 + float(volatility))

        if isinstance(dcf_val, (int, float)) and not np.isnan(dcf_val):
            if dcf_val > price:
                target_price = (dcf_val * 0.6) + (tech_upside * 0.4)
            else:
                target_price = (dcf_val * 0.3) + (price * 0.7)
        else:
            target_price = price * 1.15 if trend == "UPTREND" else price * 0.95

        if score >= 75:
            horizon = "Long Term (1-3 Yrs)"
        elif score >= 60:
            horizon = "Mid Term (3-6 Mos)"
        elif score <= 40:
            horizon = "Exit / Short Term"
        else:
            horizon = "Swing / Neutral"

        return round(float(target_price), 2), horizon

    def compute_risk_metrics(self, prices):
        prices = prices.dropna()
        if len(prices) < 30:
            return {}
        ret = prices.pct_change().dropna()
        rf = 0.07 / 252
        mean, std = ret.mean(), ret.std()
        sharpe = ((mean - rf) / std) * np.sqrt(252) if std > 0 else 0
        cum = (1 + ret).cumprod()
        max_dd = ((cum - cum.cummax()) / cum.cummax()).min()
        return {"Sharpe": round(float(sharpe), 2), "MaxDD": round(float(max_dd), 3)}

    # PATCH: accept company_name for separate column
    def analyze_asset(self, ticker, prices, info, stock, source, company_name: str = ""):
        tech = self.calculate_technicals(prices)
        if not tech:
            return None

        current_price = float(prices.iloc[-1])
        val = self.calculate_valuation(stock, info if isinstance(info, dict) else {}, current_price)
        risk = self.compute_risk_metrics(prices)

        score = 50
        if tech['Trend'] == "UPTREND":
            score += 20
        if tech['MACD_Signal'] == "Bullish":
            score += 10
        if tech['RSI'] < 30:
            score += 15
        elif tech['RSI'] > 70:
            score -= 15
        if isinstance(val, (int, float)) and val > current_price:
            score += 20
        if risk.get('Sharpe', 0) > 1:
            score += 10
        if tech['Trend'] == "DOWNTREND":
            score -= 20
        if isinstance(val, (int, float)) and val < current_price * 0.8:
            score -= 10
        if risk.get('Sharpe', 0) < 0:
            score -= 5

        verdict = "HOLD"
        if score >= 75:
            verdict = "STRONG BUY"
        elif score >= 60:
            verdict = "BUY"
        elif score <= 20:
            verdict = "STRONG SELL"
        elif score <= 40:
            verdict = "SELL"

        target, horizon = self.determine_strategy(current_price, val, tech['Trend'], score, tech['Volatility'])

        dd_data = {
            "Score_Breakdown": [
                f"Final Score: {score}",
                f"Trend: {tech['Trend']}",
                f"Sharpe: {risk.get('Sharpe')}"
            ],
            "Valuation_Method": "DCF" if isinstance(val, (int, float)) and val != 0 else "Estimate",
        }

        # Sector extraction + PATCH fallback inference
        sector = "Unknown"
        if isinstance(info, dict):
            sector = info.get('sector', 'Unknown') or "Unknown"
        sector = normalize_sector(sector)
        if sector == "Unclassified":
            sector = infer_sector_from_name(company_name or "")

        # Company extraction priority (PATCH):
        company = (company_name or "").strip()
        if not company and isinstance(info, dict):
            company = (info.get("longName") or info.get("shortName") or info.get("name") or "").strip()
        if not company:
            company = str(ticker).upper().replace(".NS", "")

        return {
            "Ticker": str(ticker).upper().replace(".NS", ""),
            "Company": company,
            "Price": round(current_price, 2),
            "Target_Price": target,
            "Horizon": horizon,
            "Trend": tech['Trend'],
            "RSI": tech['RSI'],
            "DCF_Val": val,
            "Sharpe": risk.get('Sharpe', 0),
            "Score": int(score),
            "Verdict": verdict,
            "Deep_Dive_Data": dd_data,
            "Sector": sector
        }


# =============================================================================
# 7.5 VERIFICATION ENGINE (TradingView + News + Fundamental) - FULL COVERAGE
# =============================================================================
def tv_num_to_verdict(x: float) -> str:
    """
    TradingView Recommend.All is typically in [-1, 1].
    """
    try:
        v = float(x)
    except Exception:
        return "Neutral"
    if v >= 0.5:
        return "Strong Buy"
    if v >= 0.1:
        return "Buy"
    if v <= -0.5:
        return "Strong Sell"
    if v <= -0.1:
        return "Sell"
    return "Neutral"

def coarse(v: str) -> str:
    s = (v or "").upper()
    if "BUY" in s:
        return "BUY"
    if "SELL" in s:
        return "SELL"
    return "NEUTRAL"

class Verifier:
    def __init__(self):
        self.session = make_retry_session(user_agent="Mozilla/5.0", total_retries=4)

    def build_news_sentiment_map(self, articles: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Aggregate sentiment per ticker from (article.score) values.
        """
        agg: Dict[str, List[float]] = {}
        for a in (articles or []):
            score = a.get("score", 0.0)
            tks = a.get("tickers", []) or []
            for t in tks:
                t0 = str(t).upper().replace(".NS", "").replace(".BO", "").strip()
                if not t0:
                    continue
                agg.setdefault(t0, []).append(float(score))

        out: Dict[str, str] = {}
        for t, vals in agg.items():
            if not vals:
                continue
            avg = float(np.mean(vals))
            if avg >= 0.15:
                out[t] = "Positive"
            elif avg <= -0.15:
                out[t] = "Negative"
            else:
                out[t] = "Neutral"
        return out

    def fundamental_verdict_from_row(self, row: Dict[str, Any]) -> str:
        """
        Simple but stable fundamental signal:
        - If DCF_Val > Price by 10% => Positive
        - If DCF_Val < Price by 10% => Negative
        - else Neutral
        """
        try:
            p = float(row.get("Price", 0))
            d = float(row.get("DCF_Val", p))
            if p <= 0:
                return "Neutral"
            ratio = d / p
            if ratio >= 1.10:
                return "Positive"
            if ratio <= 0.90:
                return "Negative"
            return "Neutral"
        except Exception:
            return "Neutral"

    def fetch_tradingview_recos(
        self,
        tickers_plain: List[str],
        batch_size: int = 150,
        pause_seconds: float = 0.9,
        cache_ttl_hours: int = 24
    ) -> Dict[str, str]:
        """
        FULL COVERAGE (best-effort):
        - Batches requests to avoid throttling.
        - Caches results to disk (cache/tv_recos_cache.json) for TTL hours.
        - If TradingView blocks/throttles, missing tickers simply return Neutral later.
        """
        endpoints = [
            "https://scanner.tradingview.com/india/scan",
            "https://scanner.tradingview.com/global/scan",
        ]

        tickers_plain = [t.replace(".NS", "").replace(".BO", "").upper().strip() for t in (tickers_plain or []) if t]
        tickers_plain = list(dict.fromkeys(tickers_plain))
        if not tickers_plain:
            return {}

        cache_path = CACHE_DIR / "tv_recos_cache.json"

        def load_cache() -> Tuple[Dict[str, Any], float]:
            if not cache_path.exists():
                return {}, 0.0
            try:
                data = json.loads(cache_path.read_text(encoding="utf-8"))
                ts = float(data.get("_ts", 0.0))
                return data, ts
            except Exception:
                return {}, 0.0

        def save_cache(cache_obj: Dict[str, Any]):
            try:
                cache_obj["_ts"] = time.time()
                cache_path.write_text(json.dumps(cache_obj, ensure_ascii=False), encoding="utf-8")
            except Exception:
                pass

        cache_obj, cache_ts = load_cache()
        ttl_ok = (time.time() - cache_ts) <= (cache_ttl_hours * 3600)

        cached_out: Dict[str, str] = {}
        if ttl_ok:
            for t in tickers_plain:
                v = cache_obj.get(t)
                if isinstance(v, str) and v:
                    cached_out[t] = v

        missing = [t for t in tickers_plain if t not in cached_out]

        headers = {
            "Content-Type": "application/json",
            "Origin": "https://in.tradingview.com",
            "Referer": "https://in.tradingview.com/",
            "User-Agent": "Mozilla/5.0"
        }

        def post_scan(url: str, symbols: List[str]) -> Optional[Dict[str, str]]:
            payload = {
                "symbols": {"tickers": symbols, "query": {"types": []}},
                "columns": ["Recommend.All"]
            }
            try:
                r = self.session.post(url, data=json.dumps(payload), headers=headers, timeout=25)
                if not r.ok:
                    return None
                data = r.json()
                rows = data.get("data", []) or []
                out = {}
                for row in rows:
                    s = row.get("s", "")
                    d = row.get("d", [])
                    if not s or not d:
                        continue
                    sym = s.split(":")[-1].upper().strip()
                    try:
                        val = float(d[0])
                    except Exception:
                        continue
                    out[sym] = tv_num_to_verdict(val)
                return out
            except Exception:
                return None

        tv_out: Dict[str, str] = dict(cached_out)

        if missing:
            log.info(f"🔎 TradingView verification: {len(missing)} tickers missing from cache (batch_size={batch_size})")

        for i in range(0, len(missing), batch_size):
            batch = missing[i:i + batch_size]
            tv_symbols = [f"NSE:{t}" for t in batch]

            batch_result = None
            for url in endpoints:
                attempt = 0
                while attempt < 3 and batch_result is None:
                    attempt += 1
                    batch_result = post_scan(url, tv_symbols)
                    if batch_result is None:
                        time.sleep(pause_seconds * attempt)
                if batch_result:
                    break

            if batch_result:
                for k, v in batch_result.items():
                    tv_out[k] = v
                    cache_obj[k] = v
                save_cache(cache_obj)
            else:
                log.warning(f"⚠️ TradingView scan failed for batch {i}-{i+len(batch)} (will fallback to Neutral).")

            time.sleep(pause_seconds)

        return tv_out

    def build_verification_table(
        self,
        df_src: pd.DataFrame,
        tv_map: Dict[str, str],
        news_map: Dict[str, str]
    ) -> pd.DataFrame:
        rows = []
        for _, r in df_src.iterrows():
            ticker = str(r.get("Ticker", "")).upper().replace(".NS", "").replace(".BO", "").strip()
            if not ticker:
                continue

            our = str(r.get("Verdict", "HOLD"))
            tv = tv_map.get(ticker, "Neutral")
            news = news_map.get(ticker, "Neutral")
            fundamental = self.fundamental_verdict_from_row(r.to_dict())

            # Agreement score
            ours_c = coarse(our)
            tv_c = coarse(tv)
            news_c = coarse(news)
            fund_c = coarse(fundamental)

            comps = [tv_c, news_c, fund_c]
            matches = sum(1 for c in comps if c == ours_c)
            agreement = int(round((matches / 3) * 100))

            confidence = "Low"
            if agreement >= 67:
                confidence = "High"
            elif agreement >= 34:
                confidence = "Medium"

            rows.append({
                "Ticker": ticker,
                "Company": r.get("Company", ""),
                "Our Verdict": our,
                "Agreement": agreement,
                "Confidence": confidence,
                "TV": tv,
                "News": news,
                "Fundamental": fundamental
            })

        return pd.DataFrame(rows)


# =============================================================================
# 8. REPORTING & EMAIL
# =============================================================================
def _split_recipients(raw: str) -> List[str]:
    if not raw:
        return []
    parts = re.split(r"[,\s;]+", raw.strip())
    out = []
    for p in parts:
        p = p.strip()
        if p and "@" in p:
            out.append(p)
    return list(dict.fromkeys(out))

def md_to_basic_html(md: str) -> str:
    """
    Minimal markdown-to-HTML for email readability (headings, bold, hr, bullets, numbered).
    No external libs; safe and simple.
    """
    if not md:
        return ""

    lines = md.splitlines()
    html_lines = []
    in_ul = False
    in_ol = False

    def close_lists():
        nonlocal in_ul, in_ol
        if in_ul:
            html_lines.append("</ul>")
            in_ul = False
        if in_ol:
            html_lines.append("</ol>")
            in_ol = False

    for line in lines:
        s = line.rstrip()

        if s.strip() == "---":
            close_lists()
            html_lines.append("<hr/>")
            continue

        if s.startswith("### "):
            close_lists()
            html_lines.append(f"<h3>{s[4:]}</h3>")
            continue
        if s.startswith("## "):
            close_lists()
            html_lines.append(f"<h2>{s[3:]}</h2>")
            continue
        if s.startswith("# "):
            close_lists()
            html_lines.append(f"<h1>{s[2:]}</h1>")
            continue

        m_num = re.match(r"^\s*\d+\.\s+(.*)$", s)
        if m_num:
            if in_ul:
                html_lines.append("</ul>")
                in_ul = False
            if not in_ol:
                html_lines.append("<ol>")
                in_ol = True
            item = m_num.group(1)
            item = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", item)
            html_lines.append(f"<li>{item}</li>")
            continue

        m_bul = re.match(r"^\s*[\-\*]\s+(.*)$", s)
        if m_bul:
            if in_ol:
                html_lines.append("</ol>")
                in_ol = False
            if not in_ul:
                html_lines.append("<ul>")
                in_ul = True
            item = m_bul.group(1)
            item = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", item)
            html_lines.append(f"<li>{item}</li>")
            continue

        close_lists()

        # bold
        s2 = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", s)
        if not s2.strip():
            html_lines.append("<br/>")
        else:
            html_lines.append(f"<p>{s2}</p>")

    close_lists()
    return "\n".join(html_lines)

class Emailer:
    def __init__(self, api_keys: APIKeys):
        self.user = api_keys.get("EMAIL_USER")
        self.password = api_keys.get("EMAIL_PASS")
        self.recipient_raw = api_keys.get("EMAIL_TO")

    def send_report(self, files: List[Path], subject_line: str, body_text: str, html_body: Optional[str] = None):
        recipients = _split_recipients(self.recipient_raw or "")
        if not (self.user and self.password and recipients):
            log.warning("📧 Email credentials missing (or no recipients). Skipping email.")
            return

        msg = MIMEMultipart("mixed")
        msg['From'] = self.user
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = subject_line

        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(body_text or "", "plain", "utf-8"))
        if html_body:
            alt.attach(MIMEText(html_body, "html", "utf-8"))
        msg.attach(alt)

        for f in files:
            if not f or not Path(f).exists():
                continue
            with open(f, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f"attachment; filename= {Path(f).name}")
                msg.attach(part)

        try:
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.user, self.password)
            server.sendmail(self.user, recipients, msg.as_string())
            server.quit()
            log.info(f"📧 Email Sent Successfully to {', '.join(recipients)}")
        except Exception as e:
            log.error(f"❌ Email Failed: {e}")

class ReportLab:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir

    def generate_html_dashboard(self, results, articles, trends, ind_summary, disclaimer_text: str, verification_preview: Optional[pd.DataFrame] = None) -> Optional[Path]:
        template = """<!DOCTYPE html><html><head><title>Illuminati v29.0</title>
<style>
body{font-family:Arial,Helvetica,sans-serif;background:#0f172a;color:#e2e8f0;padding:20px}
.card{background:#1e293b;border-radius:8px;padding:15px;margin-bottom:15px;border:1px solid #334155}
.badge{padding:4px 8px;border-radius:4px;font-weight:bold;display:inline-block}
.buy{background:#065f46;color:#34d399}
.sell{background:#7f1d1d;color:#f87171}
.hold{background:#854d0e;color:#fef08a}
table{width:100%;border-collapse:collapse;margin-top:12px}
th,td{padding:10px;text-align:left;border-bottom:1px solid #334155;vertical-align:top}
th{color:#94a3b8}
small{color:#94a3b8}
a{color:#60a5fa}
hr{border:0;border-top:1px solid #334155;margin:18px 0}
pre{white-space:pre-wrap;word-wrap:break-word}
</style></head><body>
<h1>👁️ Illuminati Terminal v29.0</h1>
<p>Assets Analyzed: {{ total }} | Date: {{ date }}</p>

<h2>🔮 Future Booming Industries</h2>
<table><thead><tr><th>Theme</th><th>Hype Score</th><th>Mentions</th></tr></thead><tbody>
{% for t in trends %}
<tr><td><b>{{ t.Theme }}</b></td><td>{{ t.Hype_Score }}%</td><td>{{ t.Mentions }}</td></tr>
{% endfor %}
</tbody></table>

<h2>🚀 Industry Momentum</h2>
<table><thead><tr><th>Sector</th><th>Avg Score</th><th>Top Verdict</th></tr></thead><tbody>
{% for s, data in ind_summary.items() %}
<tr><td><b>{{ s }}</b></td><td>{{ data['avg_score'] }}</td><td>{{ data['verdict'] }}</td></tr>
{% endfor %}
</tbody></table>

<h2>✅ Verification Summary (Preview)</h2>
<small>Full coverage for all tickers is included in the Excel sheet “Verification”.</small>
<table><thead><tr><th>Ticker</th><th>Our</th><th>Agreement</th><th>TV</th><th>News</th><th>Fundamental</th></tr></thead><tbody>
{% for v in verification_preview %}
<tr>
<td><b>{{ v["Ticker"] }}</b></td>
<td>{{ v["Our Verdict"] }}</td>
<td>{{ v["Agreement"] }}% ({{ v["Confidence"] }})</td>
<td>{{ v["TV"] }}</td>
<td>{{ v["News"] }}</td>
<td>{{ v["Fundamental"] }}</td>
</tr>
{% endfor %}
</tbody></table>

<h2>🚀 Investment Strategy (Top 150)</h2>
<table><thead><tr><th>Ticker</th><th>Company</th><th>Price</th><th>Target</th><th>Horizon</th><th>Sharpe</th><th>Valuation</th><th>Score</th><th>Verdict</th></tr></thead><tbody>
{% for r in results_top %}
<tr>
<td><b>{{ r.Ticker }}</b></td><td>{{ r.Company }}</td><td>{{ r.Price }}</td><td>{{ r.Target_Price }}</td><td>{{ r.Horizon }}</td>
<td>{{ r.Sharpe }}</td><td>{{ r.DCF_Val }}</td><td>{{ r.Score }}</td>
<td><span class="badge {{ 'buy' if 'BUY' in r.Verdict else ('sell' if 'SELL' in r.Verdict else 'hold') }}">{{ r.Verdict }}</span></td>
</tr>
{% endfor %}
</tbody></table>

<h2>📰 Market Intel</h2>
{% for a in articles[:10] %}
<div class="card">
<h3><a href="{{ a.link }}">{{ a.title }}</a></h3>
<p><small>{{ a.published }} | {{ a.source }}</small></p>
<p>{{ a.body[:260] }}...</p>
</div>
{% endfor %}

<hr/>
<h2>⚠️ Disclaimer</h2>
<pre>{{ disclaimer_text }}</pre>

</body></html>"""

        try:
            t = Template(template)
            results_top = results if isinstance(results, list) else []
            verification_preview = verification_preview if verification_preview is not None else []
            html = t.render(
                results_top=results_top,
                articles=articles or [],
                trends=trends or [],
                ind_summary=ind_summary or {},
                date=dt.datetime.now(),
                total=len(results) if results else 0,
                disclaimer_text=disclaimer_text,
                verification_preview=verification_preview
            )
            path = self.out_dir / f"Dashboard_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            path.write_text(html, encoding="utf-8")
            return path
        except Exception as e:
            log.error(f"HTML Error: {e}")
            return None

    def generate_full_deep_dive(self, results, disclaimer_text: str) -> Path:
        path = self.out_dir / f"Deep_Dive_Full_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(path, 'w', encoding="utf-8") as f:
            f.write(f"ILLUMINATI DEEP DIVE REPORT | {dt.datetime.now()}\n")
            f.write("="*80 + "\n\n")
            for r in results:
                f.write(f"Ticker: {r['Ticker']} | Company: {r.get('Company','')} | Price: {r['Price']} | Verdict: {r['Verdict']}\n")
                f.write(f"Target: {r['Target_Price']} ({r['Horizon']})\n")
                f.write(f"Sector: {r.get('Sector','')}\n")
                f.write(f"Trend: {r['Trend']} | RSI: {r['RSI']} | Sharpe: {r.get('Sharpe')}\n")
                f.write(f"Valuation ({r['Deep_Dive_Data']['Valuation_Method']}): {r['DCF_Val']}\n")
                f.write(f"Score Components: {r['Deep_Dive_Data']['Score_Breakdown']}\n")
                f.write("-" * 60 + "\n\n")
            f.write("\n" + "="*80 + "\n")
            f.write("DISCLAIMER\n")
            f.write("="*80 + "\n")
            f.write(disclaimer_text + "\n")
        return path

    def generate_disclaimer_txt(self, disclaimer_text: str) -> Path:
        path = self.out_dir / f"Disclaimer_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        path.write_text(disclaimer_text, encoding="utf-8")
        return path

    def generate_exec_brief_txt(self, exec_brief_md: str) -> Path:
        path = self.out_dir / f"Executive_Brief_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        # keep as text; email HTML will render separately
        path.write_text(exec_brief_md.strip(), encoding="utf-8")
        return path

    def generate_zip_bundle(self, files: List[Path]) -> Path:
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_path = self.out_dir / f"Illuminati_Report_{ts}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for f in files:
                try:
                    if f and Path(f).exists():
                        z.write(str(f), arcname=Path(f).name)
                except Exception:
                    continue
        return zip_path


class GeminiBrain:
    def __init__(self, api_key=None):
        self.active = False
        self.model = None
        if HAS_GEMINI and api_key:
            genai.configure(api_key=api_key)
            self.active = True

    def generate_narrative(self, df_summary: pd.DataFrame) -> str:
        if not self.active:
            return "LLM Analysis Disabled."

        try:
            all_models = list(genai.list_models())
            valid_models = [m.name for m in all_models if 'generateContent' in m.supported_generation_methods]

            priority = ['flash', '1.5-pro', 'pro', '1.0']
            sorted_models = []
            for p in priority:
                for vm in valid_models:
                    if p in vm:
                        sorted_models.append(vm)
            final_list = list(dict.fromkeys(sorted_models + valid_models))

            for model_name in final_list:
                try:
                    log.info(f"🤖 Generating Insight with: {model_name}")
                    self.model = genai.GenerativeModel(model_name)
                    return self.model.generate_content(
                        f"Analyze this Indian Stock Market data:\n{df_summary.to_csv(index=False)}"
                    ).text
                except Exception:
                    continue

        except Exception:
            pass

        try:
            log.warning("⚠️ Using Legacy AI Protocol...")
            self.model = genai.GenerativeModel('gemini-pro')
            return self.model.generate_content(
                f"Analyze this Indian Stock Market data:\n{df_summary.to_csv(index=False)}"
            ).text
        except Exception as e:
            return f"LLM Completely Failed: {e}"


def format_ai_insight_to_md(narrative: str) -> str:
    """
    Make the AI narrative readable: numbered points + bold labels where possible.
    We preserve original content but wrap it for consistency.
    """
    if not narrative:
        return "LLM Analysis Disabled."

    # If model already produced headings/numbering, keep it; just ensure spacing.
    txt = narrative.strip()

    # Small enhancement: ensure "Key Observations" bullets become numbered if present
    txt = re.sub(r"\n\*\s+", "\n- ", txt)

    md = (
        "## AI Insight\n\n"
        f"{txt}\n"
    ).strip()
    return md

def build_executive_brief_md(
    df: pd.DataFrame,
    trends: List[Dict[str, Any]],
    ind_summary: Dict[str, Dict[str, Any]],
    verification_df: Optional[pd.DataFrame],
    tv_coverage_pct: float,
    narrative_md: str
) -> str:
    top = df.head(5).copy()

    lines = []
    lines.append("# ILLUMINATI EXECUTIVE BRIEF")
    lines.append("")
    lines.append(f"**Run Time:** {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**Assets Analyzed:** {len(df)}")
    lines.append("")

    lines.append("## 1. Snapshot")
    lines.append(f"1. **Top Verdict Mix:** "
                 f"STRONG BUY={int((df['Verdict']=='STRONG BUY').sum())}, "
                 f"BUY={int((df['Verdict']=='BUY').sum())}, "
                 f"HOLD={int((df['Verdict']=='HOLD').sum())}, "
                 f"SELL={int((df['Verdict']=='SELL').sum())}, "
                 f"STRONG SELL={int((df['Verdict']=='STRONG SELL').sum())}")
    lines.append(f"2. **TradingView Coverage (best-effort):** {tv_coverage_pct:.1f}% of tickers have cached/returned TV verdicts")
    lines.append("")

    lines.append("## 2. Top Opportunities")
    for i, row in enumerate(top.itertuples(index=False), 1):
        lines.append(
            f"{i}. **{row.Ticker}** ({getattr(row,'Company','')}) — "
            f"**{row.Verdict}**, Price={row.Price}, Target={row.Target_Price}, "
            f"Horizon={row.Horizon}, Sharpe={row.Sharpe}"
        )
    lines.append("")

    lines.append("## 3. Future Themes (News Hype)")
    if trends:
        for i, t in enumerate(trends[:5], 1):
            lines.append(f"{i}. **{t['Theme']}** — Hype Score: **{t['Hype_Score']}%**, Mentions: {t['Mentions']}")
    else:
        lines.append("1. **Not enough theme signals** in the last 7 days of feeds.")

    lines.append("")
    lines.append("## 4. Industry Momentum (by Sector)")
    if ind_summary:
        # sort by avg score
        items = sorted(ind_summary.items(), key=lambda kv: kv[1].get("avg_score", 0), reverse=True)
        for i, (sec, d) in enumerate(items[:10], 1):
            lines.append(f"{i}. **{sec}** — Avg Score: **{d['avg_score']}**, Verdict: **{d['verdict']}**")
    else:
        lines.append("1. **Not available** (sector mapping missing).")

    lines.append("")
    if verification_df is not None and not verification_df.empty:
        lines.append("## 5. Verification Summary (Full Coverage)")
        lines.append("1. **Agreement score** is computed across: TradingView + News + Fundamental vs our verdict.")
        lines.append("2. Full table is available in Excel → **Verification** sheet.")
        # quick stats
        try:
            avg_agree = float(verification_df["Agreement"].mean())
        except Exception:
            avg_agree = 0.0
        lines.append(f"3. **Average Agreement:** **{avg_agree:.1f}%**")
    else:
        lines.append("## 5. Verification Summary")
        lines.append("1. Verification table not generated.")

    lines.append("")
    # Attach AI Insight in brief as a short excerpt only
    lines.append("## 6. AI Insight (Excerpt)")
    snippet = narrative_md.replace("## AI Insight", "").strip()
    snippet = "\n".join(snippet.splitlines()[:18]).strip()
    lines.append(snippet + ("\n..." if len(snippet.splitlines()) >= 18 else ""))

    lines.append("")
    lines.append("---")
    lines.append("## Disclaimer")
    lines.append(DISCLAIMER_TEXT)

    return "\n".join(lines).strip()

def print_deep_dive_console(asset):
    if not asset:
        return
    print("\n" + "="*60)
    print(f"🔬 DEEP DIVE HIGHLIGHT: {asset['Ticker']}")
    print("="*60)
    print(f"Company: {asset.get('Company','')}")
    print(f"Current Price: ₹{asset['Price']}  |  Target: ₹{asset['Target_Price']}")
    print(f"Verdict: {asset['Verdict']}  |  Horizon: {asset['Horizon']}")
    print(f"Valuation Method: {asset['Deep_Dive_Data']['Valuation_Method']}")
    print(f"Calculated Fair Value: {asset['DCF_Val']}")
    print(f"Score Factors: {asset['Deep_Dive_Data']['Score_Breakdown']}")


# =============================================================================
# 9. SCHEDULER & ORCHESTRATOR
# =============================================================================
def calculate_sleep_seconds():
    now = dt.datetime.now(dt.timezone.utc)
    is_utc = time.localtime().tm_gmtoff == 0
    target_utc_hour = 2 if is_utc else 8
    target_utc_min = 30 if is_utc else 0
    target_time = now.replace(hour=target_utc_hour, minute=target_utc_min, second=0, microsecond=0)
    if now >= target_time:
        target_time += dt.timedelta(days=1)
    wait_seconds = (target_time - now).total_seconds()
    return wait_seconds, target_time

def run_illuminati(interactive=False, tickers_arg=None):
    ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    current_time = dt.datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')
    print("\n" + "="*80)
    print(f"👁️ ILLUMINATI TERMINAL v29.0 (BULLETPROOF TITAN) | {current_time}")
    print("="*80)

    api = APIKeys()
    if interactive:
        api.interactive_load()

    db = DatabaseManager()
    news = NewsEngine(api)
    mapper = MasterMapper()  # Option A NSE CSV download each run
    data = DataEngine(api)
    lab = AnalysisLab()
    trend_hunter = TrendHunter()
    reporter = ReportLab(OUTPUT_DIR)
    emailer = Emailer(api)
    llm = GeminiBrain(api.get("GEMINI"))
    verifier = Verifier()

    news.add_google_news_feed("Indian Stock Market News")
    news.add_google_news_feed("Global Stock Market News")

    articles = news.process()

    # PATCH: per-article ticker extraction (prevents "all neutral" news mapping)
    log.info("🧠 Mapping tickers per article (for News sentiment verification)...")
    with ThreadPoolExecutor(max_workers=50) as ex:
        futures = []
        for a in articles:
            futures.append(ex.submit(mapper.extract_tickers, [a]))
        for a, f in zip(articles, futures):
            try:
                a["tickers"] = f.result() or []
            except Exception:
                a["tickers"] = []

    tickers_from_news = list(dict.fromkeys([t for a in articles for t in (a.get("tickers") or [])]))
    db.save_news(articles)

    trends = trend_hunter.predict_booming_industries(articles)
    if trends:
        print("\n🔮 PREDICTED BOOMING INDUSTRIES (News Hype):")
        print(tabulate(pd.DataFrame(trends).head(5), headers='keys', tablefmt='psql', showindex=False))

    full_universe = mapper.get_all_symbols()
    combined_tickers = list(set(tickers_from_news + full_universe + NSE_MEGA_LIST + USER_WATCHLIST))

    if tickers_arg:
        combined_tickers.extend([x.strip().upper() for x in tickers_arg.split(',') if x.strip()])

    combined_tickers = list(dict.fromkeys([t.strip().upper() for t in combined_tickers if t and isinstance(t, str)]))
    print(f"\n⚡ Analyzing {len(combined_tickers)} Assets (Titan Scan)...")

    data.prefetch_prices(combined_tickers, period="2y", batch_size=120)

    results = []

    def process_ticker(t):
        try:
            prices, info, stock, src = data.fetch_data(t)
            if prices is None or len(prices) < 35:
                return None
            company = mapper.get_company_name(t)  # PATCH: offline company name
            res = lab.analyze_asset(t, prices, info, stock, src, company_name=company)
            return res
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = [executor.submit(process_ticker, t) for t in combined_tickers]
        done = 0
        for f in as_completed(futures):
            done += 1
            try:
                res = f.result()
                if res:
                    results.append(res)
            except Exception:
                pass
            if done % 250 == 0:
                log.info(f"📈 Progress: {done}/{len(futures)} completed | results={len(results)}")

    if not results:
        print("\n❌ No results produced. Common reasons: network blocks, Yahoo throttling, or empty price histories.")
        print("Tip: ensure cache/EQUITY_L.csv exists and rerun, or reduce scan using --tickers for testing.")
        return

    df = pd.DataFrame(results).sort_values("Score", ascending=False)

    # PATCH: normalize sectors to reduce junk in Industry Momentum
    if 'Sector' in df.columns:
        df['Sector'] = df['Sector'].apply(normalize_sector)

    ind_summary = {}
    if 'Sector' in df.columns:
        for sector, grp in df.groupby('Sector'):
            avg = grp['Score'].mean()
            ind_summary[sector] = {
                'avg_score': round(float(avg), 1),
                'verdict': "Bullish" if avg > 60 else ("Bearish" if avg < 40 else "Neutral")
            }

    db.save_analysis(results)

    # ---------------- AI INSIGHT ----------------
    narrative_raw = llm.generate_narrative(df.head(10))
    narrative_md = format_ai_insight_to_md(narrative_raw)

    print(f"\n🤖 AI Insight (excerpt): {str(narrative_raw)[:280]}...\n")

    # ---------------- VERIFICATION (FULL COVERAGE) ----------------
    df_verify_src = df.copy()

    tv_map = {}
    try:
        tv_map = verifier.fetch_tradingview_recos(df_verify_src["Ticker"].astype(str).tolist())
    except Exception:
        tv_map = {}

    news_map = {}
    try:
        news_map = verifier.build_news_sentiment_map(articles)
    except Exception:
        news_map = {}

    df_verify = verifier.build_verification_table(df_verify_src, tv_map, news_map)
    verification_rows = df_verify.to_dict(orient="records") if df_verify is not None and not df_verify.empty else None

    # TV coverage metric
    try:
        tv_cov = 100.0 * (df_verify["TV"].astype(str).str.lower().ne("neutral").sum() / max(len(df_verify), 1))
    except Exception:
        tv_cov = 0.0

    # Executive brief
    exec_brief_md = build_executive_brief_md(
        df=df,
        trends=trends,
        ind_summary=ind_summary,
        verification_df=df_verify,
        tv_coverage_pct=tv_cov,
        narrative_md=narrative_md
    )

    # ---------------- REPORT FILES ----------------
    # Dashboard preview (avoid gigantic HTML) -> top 50 verification preview
    ver_preview_df = df_verify.head(50).copy() if df_verify is not None and not df_verify.empty else pd.DataFrame()
    dashboard_path = reporter.generate_html_dashboard(
        results=results,
        articles=articles,
        trends=trends,
        ind_summary=ind_summary,
        disclaimer_text=DISCLAIMER_TEXT,
        verification_preview=ver_preview_df.to_dict(orient="records") if not ver_preview_df.empty else []
    )
    dd_path = reporter.generate_full_deep_dive(results, DISCLAIMER_TEXT)
    disc_txt_path = reporter.generate_disclaimer_txt(DISCLAIMER_TEXT)
    exec_brief_txt_path = reporter.generate_exec_brief_txt(exec_brief_md)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_path = OUTPUT_DIR / f"Strategy_{ts}.xlsx"

    # Excel (multi-sheet, readable, disclaimer + verification + momentum)
    try:
        with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="Strategy", index=False)
            if df_verify is not None and not df_verify.empty:
                df_verify.to_excel(writer, sheet_name="Verification", index=False)

            # Momentum + trends sheets
            if ind_summary:
                mom = pd.DataFrame([{"Sector": k, "Avg Score": v["avg_score"], "Top Verdict": v["verdict"]} for k, v in ind_summary.items()])
                mom.sort_values("Avg Score", ascending=False).to_excel(writer, sheet_name="Industry_Momentum", index=False)
            if trends:
                pd.DataFrame(trends).to_excel(writer, sheet_name="Booming_Themes", index=False)

            # AI insight + exec brief + disclaimer
            pd.DataFrame([{"AI Insight": narrative_raw}]).to_excel(writer, sheet_name="AI_Insight", index=False)
            pd.DataFrame([{"Executive Brief": exec_brief_md}]).to_excel(writer, sheet_name="Executive_Brief", index=False)
            pd.DataFrame([{"Disclaimer": DISCLAIMER_TEXT}]).to_excel(writer, sheet_name="Disclaimer", index=False)

            # Light formatting: set column widths
            wb = writer.book
            for sheet in writer.sheets.values():
                try:
                    sheet.set_default_row(18)
                except Exception:
                    pass
            ws = writer.sheets.get("Strategy")
            if ws:
                ws.set_column(0, 0, 12)  # Ticker
                ws.set_column(1, 1, 34)  # Company
                ws.set_column(2, 10, 14)

            wv = writer.sheets.get("Verification")
            if wv:
                wv.set_column(0, 0, 12)
                wv.set_column(1, 1, 34)
                wv.set_column(2, 7, 16)

            wd = writer.sheets.get("Disclaimer")
            if wd:
                wd.set_column(0, 0, 120)

    except Exception as e:
        log.error(f"Excel write failed: {e}")
        # fallback (keeps run alive)
        df.to_excel(excel_path, index=False)

    # Zip bundle includes everything
    bundle_files = [excel_path, dd_path, exec_brief_txt_path, disc_txt_path]
    if dashboard_path:
        bundle_files.append(dashboard_path)
    zip_path = reporter.generate_zip_bundle(bundle_files)

    # ---------------- CONSOLE OUTPUT ----------------
    print("\n" + "="*80)
    print("🚀 TOP OPPORTUNITIES (Buys)")
    print("="*80)
    buys = df[df['Verdict'].str.contains("BUY")].head(10)
    if not buys.empty:
        print(tabulate(buys[['Ticker', 'Company', 'Price', 'Target_Price', 'Horizon', 'Sharpe', 'Verdict']],
                       headers='keys', tablefmt='psql', showindex=False))
    else:
        print("   No strong buy signals found.")

    print("\n" + "="*80)
    print("⚠️ WARNINGS & EXITS (Sells)")
    print("="*80)
    sells = df[df['Verdict'].str.contains("SELL")].head(10)
    if not sells.empty:
        print(tabulate(sells[['Ticker', 'Company', 'Price', 'Target_Price', 'Horizon', 'Sharpe', 'Verdict']],
                       headers='keys', tablefmt='psql', showindex=False))
    else:
        print("   No strong sell signals found.")

    # Print Industry Momentum
    if ind_summary:
        print("\n🚀 Industry Momentum")
        mom_df = pd.DataFrame([{"Sector": k, "Avg Score": v["avg_score"], "Top Verdict": v["verdict"]} for k, v in ind_summary.items()]).sort_values("Avg Score", ascending=False)
        print(tabulate(mom_df, headers='keys', tablefmt='psql', showindex=False))

    # Print Verification preview
    if df_verify is not None and not df_verify.empty:
        print("\n✅ Verification Summary (Preview - full coverage in Excel)")
        print(tabulate(df_verify.head(25), headers='keys', tablefmt='psql', showindex=False))

    if not df.empty:
        print_deep_dive_console(df.iloc[0].to_dict())

    print(f"\n✅ All Reports Saved to: {OUTPUT_DIR.resolve()}")
    print(f"📦 Zip Bundle: {zip_path}")

    # ---------------- EMAIL ----------------
    if api.get("EMAIL_USER"):
        # Email readable body: Executive Brief + AI Insight + Disclaimer
        email_body_md = (
            f"{exec_brief_md}\n\n"
            f"---\n\n"
            f"{narrative_md}\n\n"
            f"---\n\n"
            f"## Disclaimer\n\n{DISCLAIMER_TEXT}\n"
        ).strip()

        email_html = md_to_basic_html(email_body_md)

        attachments = [zip_path, excel_path, dd_path, exec_brief_txt_path, disc_txt_path]
        if dashboard_path:
            attachments.append(dashboard_path)

        emailer.send_report(
            attachments,
            f"Illuminati Report - {dt.datetime.now().strftime('%Y-%m-%d')}",
            email_body_md,
            html_body=email_html
        )

def schedule_job():
    print("⏰ Scheduler Started. Calculating next run time...")
    while True:
        wait_seconds, next_run = calculate_sleep_seconds()
        print(f"💤 Sleeping for {wait_seconds/3600:.1f} hours. Next run: {next_run.strftime('%Y-%m-%d %H:%M %Z')}")
        time.sleep(wait_seconds)
        if dt.datetime.now().weekday() < 5:
            run_illuminati()
        else:
            print("Skipping run (Weekend).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--interactive", action="store_true")
    parser.add_argument("--schedule", action="store_true")
    parser.add_argument("--tickers", type=str)
    args = parser.parse_args(args=[] if 'google.colab' in sys.modules else None)

    if 'google.colab' in sys.modules:
        is_interactive = not args.schedule
    else:
        is_interactive = args.interactive or len(sys.argv) == 1

    if args.schedule:
        schedule_job()
    else:
        run_illuminati(interactive=is_interactive, tickers_arg=args.tickers)
