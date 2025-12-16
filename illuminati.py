# -*- coding: utf-8 -*-
"""
Illuminati Terminal v23.0 - The "Infinite Horizon" Build
FIXES:
- "Missing Stocks" Issue: Implemented Dynamic Ticker Discovery.
- Logic: Scans news for potential tickers -> Verifies them via Yahoo Finance -> Analyzes them.
- Coverage: Can now detect and analyze ANY stock mentioned in news (Small/Micro caps included).
- Retains: All previous features (Self-Healing, AI, Email, Deep Dive).
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
import schedule
import asyncio
import logging
import hashlib
import smtplib
import datetime as dt
from typing import List, Dict, Optional, Tuple, Any

# --- 1. ROBUST SELF-HEALING INSTALLER ---
def check_and_install_dependencies():
    required_packages = [
        'nselib', 'yfinance', 'pandas', 'numpy', 'requests', 'feedparser', 
        'tabulate', 'reportlab', 'nltk', 'transformers', 'schedule', 
        'google-generativeai', 'aiohttp', 'xlsxwriter', 'trafilatura', 
        'rapidfuzz', 'beautifulsoup4', 'ta', 'jinja2', 'textblob', 'nest_asyncio', 'pytz'
    ]
    
    missing = []
    print("🛠️ System Health Check...")
    
    for package in required_packages:
        if importlib.util.find_spec(package) is None:
            missing.append(package)

    if missing:
        print(f"📦 Installing missing modules: {', '.join(missing)}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "google-generativeai"])
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
from bs4 import BeautifulSoup
from jinja2 import Template
from dateutil import parser as dateparser
from zoneinfo import ZoneInfo
from pathlib import Path
from urllib.parse import urlparse, quote_plus
from concurrent.futures import ThreadPoolExecutor
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Conditional Imports
try: from nselib import capital_market; HAS_NSELIB = True
except ImportError: HAS_NSELIB = False

try: from ta.trend import SMAIndicator, MACD; from ta.momentum import RSIIndicator; HAS_TA = True
except ImportError: HAS_TA = False

try: import trafilatura; logging.getLogger('trafilatura').setLevel(logging.CRITICAL); HAS_TRAFILATURA = True
except ImportError: HAS_TRAFILATURA = False

try: import google.generativeai as genai; HAS_GEMINI = True
except ImportError: HAS_GEMINI = False

try: from transformers import pipeline as hf_pipeline; HAS_HF = True
except ImportError: HAS_HF = False

# --- 3. CONFIGURATION ---
nest_asyncio.apply()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("Illuminati")

DB_PATH = "market_memory.db"
OUTPUT_DIR = Path("output")
CACHE_DIR = Path("cache")
OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context

# EXTENSIVE BACKUP LIST (Used if nselib fails, but augmented by Discovery)
NIFTY_500_BACKUP = [
    'RELIANCE', 'TCS', 'HDFCBANK', 'ICICIBANK', 'INFY', 'SBIN', 'BHARTIARTL', 'ITC', 'LICI', 'HINDUNILVR',
    'LT', 'BAJFINANCE', 'HCLTECH', 'MARUTI', 'SUNPHARMA', 'ADANIENT', 'TATAMOTORS', 'TITAN', 'KOTAKBANK',
    'ONGC', 'AXISBANK', 'NTPC', 'ULTRACEMCO', 'ADANIPORTS', 'POWERGRID', 'M&M', 'WIPRO', 'BAJAJFINSV',
    'HAL', 'COALINDIA', 'IOC', 'DLF', 'ZOMATO', 'JIOFIN', 'SIEMENS', 'SBILIFE', 'TATASTEEL', 'GRASIM',
    'BEL', 'VBL', 'LTIM', 'TRENT', 'ADANIGREEN', 'ADANIPOWER', 'HINDALCO', 'INDUSINDBK', 'BANKBARODA',
    'HDFCLIFE', 'EICHERMOT', 'BPCL', 'ABB', 'GODREJCP', 'PIDILITIND', 'TECHM', 'BRITANNIA', 'PFC',
    'RECLTD', 'CIPLA', 'AMBUJACEM', 'GAIL', 'TATAPOWER', 'CANBK', 'VEDL', 'INDIGO', 'UNIONBANK',
    'CHOLAFIN', 'HAVELLS', 'HEROMOTOCO', 'DABUR', 'JSWSTEEL', 'SHREECEM', 'TVSMOTOR', 'DRREDDY',
    'BOSCHLTD', 'JINDALSTEL', 'PNB', 'NHPC', 'APOLLOHOSP', 'LODHA', 'DIVISLAB', 'IOB', 'MAXHEALTH',
    'POLYCAB', 'SOLARINDS', 'IDBI', 'IRFC', 'TORNTPHARM', 'MANKIND', 'CUMMINSIND', 'ICICIGI', 'CGPOWER',
    'SHRIRAMFIN', 'COLPAL', 'MOTHERSON', 'MUTHOOTFIN', 'BERGEPAINT', 'TATAELXSI', 'ASTRAL', 'MARICO',
    'ALKEM', 'PERSISTENT', 'SRF', 'IRCTC', 'MPHASIS', 'OBEROIRLTY', 'BHARATFORG', 'SBICARD', 'ASHOKLEY',
    'INDHOTEL', 'ZEEL', 'VOLTAS', 'PIIND', 'UPL', 'APLAPOLLO', 'GUJGASLTD', 'MRF', 'AUROPHARMA',
    'LTTS', 'SUPREMEIND', 'TIINDIA', 'PETRONET', 'CONCOR', 'LALPATHLAB', 'ABCAPITAL', 'POLICYBZR',
    'LINDEINDIA', 'DALBHARAT', 'SCHAEFFLER', 'GMRINFRA', 'PHOENIXLTD', 'KPITTECH', 'FLUOROCHEM',
    'PRESTIGE', 'PAGEIND', 'BANDHANBNK', 'UNOMINDA', 'ACC', 'PATANJALI', 'THERMAX', 'SUZLON',
    'FEDERALBNK', 'IDFCFIRSTB', 'MAHABANK', 'STARHEALTH', 'UBL', 'HONAUT', 'AUBANK', 'TATACOMM',
    'DIXON', 'BANKINDIA', 'KEYSTONE', 'SOLARA', 'MAZDOCK', 'RVNL', 'FACT', 'COCHINSHIP', 'IREDA'
]

# STOPLIST for filtering noise words
STOPLIST = set([
    "THE", "AND", "ARE", "IS", "FOR", "OVER", "WITH", "TO", "OF", "IN", "BY", "FROM", "ON", "AT", "OR", "AS", "AN", "IT", "GO", "NO", "MARKET", "COMPANY", "COMPANIES", "NEWS", "STOCK", "STOCKS", "SEBI", "INDIAN", "INDIA", "EXPECTED", "LOSSES", "GAINS", "SHARES", "NSE", "BSE", "DECLINE", "DECLINED", "ALSO", "FIRMS", "MONTHS", "SEGMENTS", "LTD", "PRIMARY", "BOTH", "COMING", "FUNDRAISING", "SIGNIFICANT", "LIMITED", "POSSIBLE", "HEALTH", "HEALTHCARE", "WAVE", "FIFTEEN", "EYE", "BANK", "IPO", "IPOS", "SET", "RS", "BE", "WAS", "PUSH", "PARTICULARLY", "MUTUAL", "FUNDS", "PRIVATE", "PUBLIC", "LOWER", "HIGHER", "TODAY", "WEEK", "YEAR", "REPORT", "GLOBAL", "WORLD", "BUSINESS", "FINANCE", "MONEY", "TIMES", "ECONOMIC", "CITY", "SALES", "PROFIT", "LOSS", "QUARTER", "RESULTS", "DATA", "GROUP", "IND", "OUT", "NEW", "OLD", "BIG", "SMALL", "HIGH", "LOW", "BUY", "SELL", "HOLD", "TARGET", "PRICE"
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

DEFAULT_FEEDS = [
    "https://news.google.com/rss/search?q=site:moneycontrol.com+when:7d&hl=en-IN&gl=IN&ceid=IN:en",
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
    "https://www.livemint.com/rss/markets",
    "https://www.business-standard.com/rss/markets-106.rss",
    "https://www.financialexpress.com/market/feed/",
    "https://feeds.reuters.com/reuters/INbusinessNews",
    "https://feeds.bloomberg.com/markets/news.rss"
]

# ==========================================
# 4. MARKET MAPPER (DYNAMIC DISCOVERY)
# ==========================================
class MasterMapper:
    def __init__(self):
        self.universe = {} 
        self.keywords = {}
        self.discovered_tickers = set() # New cache for dynamic finds
        self.build_universe()
        
    def build_universe(self):
        log.info("⏳ Indexing NSE Market...")
        # 1. Load Backup First
        for t in NIFTY_500_BACKUP:
            self.universe[t] = t
            
        # 2. Try Live Fetch (Full Market)
        try:
            if HAS_NSELIB:
                df = capital_market.equity_list()
                for index, row in df.iterrows():
                    symbol = row['SYMBOL']
                    name = str(row['NAME OF COMPANY']).upper()
                    self.universe[symbol] = symbol
                    self.universe[name] = symbol
                    simple = name.replace("LIMITED", "").replace("LTD", "").strip()
                    self.universe[simple] = symbol
                    first = simple.split()[0]
                    if len(first) > 3 and first not in STOPLIST:
                        self.keywords[first] = symbol
                log.info(f"✅ Indexed {len(self.universe)} companies (Live + Backup).")
            else: 
                log.warning("⚠️ nselib missing. Using Nifty 500 Backup + Dynamic Discovery.")
        except Exception as e:
            log.warning(f"⚠️ NSE Indexing failed. Using Nifty 500 Backup + Dynamic Discovery.")

    def verify_ticker(self, candidate: str) -> Optional[str]:
        """Checks if a string is a valid NSE ticker via Yahoo Finance."""
        if candidate in self.universe: return self.universe[candidate]
        if candidate in self.discovered_tickers: return candidate
        
        try:
            # Quick lightweight check
            t = yf.Ticker(f"{candidate}.NS")
            # If it has a current price, it's valid
            if t.fast_info.get('last_price', None):
                self.discovered_tickers.add(candidate)
                return candidate
        except: pass
        return None

    def extract_tickers(self, articles):
        found = []
        for art in articles:
            text = f"{art['title']} {art.get('body', '')[:1000]}".upper()
            
            # 1. Known Tickers
            matches = re.findall(r'\b([A-Z]{3,})\b', text)
            for m in matches:
                if m in self.universe: found.append(self.universe[m])
                elif m in self.keywords: found.append(self.keywords[m])
            
            # 2. Dynamic Discovery (The "News Hunter" Feature)
            # Looks for words near "Stock", "Shares", "NSE" that look like tickers
            context_matches = re.findall(r'(?:STOCK|SHARES|NSE|BSE)\s+(?:OF\s+)?([A-Z]{3,15})', text)
            for m in context_matches:
                if m not in STOPLIST and m not in self.universe:
                    valid = self.verify_ticker(m)
                    if valid: 
                        found.append(valid)
                        log.info(f"🔎 Discovered New Asset: {valid}")

        return list(set(found))

class TrendHunter:
    def predict_booming_industries(self, articles):
        log.info("🔮 Predicting Future Booming Industries...")
        scores = {k: 0 for k in FUTURE_THEMES.keys()}
        for art in articles:
            text = (art['title'] + " " + art.get('body', '')).lower()
            for theme, keywords in FUTURE_THEMES.items():
                for kw in keywords:
                    if kw in text: scores[theme] += 1
        
        total_hits = sum(scores.values())
        if total_hits == 0: return []
        
        trends = []
        for theme, score in scores.items():
            trends.append({'Theme': theme, 'Hype_Score': round((score / total_hits) * 100, 1), 'Mentions': score})
        return sorted(trends, key=lambda x: x['Hype_Score'], reverse=True)

# ==========================================
# 5. UTILITIES
# ==========================================
class APIKeys:
    def __init__(self):
        self.keys = {}
        for k in ["TWELVEDATA", "FINNHUB", "ALPHAVANTAGE", "NEWSAPI", "GEMINI", "EMAIL_USER", "EMAIL_PASS", "EMAIL_TO"]:
            val = os.environ.get(f"{k}_KEY") or os.environ.get(f"{k}") or os.environ.get(f"{k}_API_KEY")
            if val: self.keys[k] = val
            
    def get(self, name): return self.keys.get(name)
    
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
            self.keys["EMAIL_TO"] = input("   Recipient Email: ").strip()
        print("="*50 + "\n")

class DiskCache:
    def __init__(self, base_dir: Path, ttl_seconds: int = 21600):
        self.base_dir = base_dir; self.ttl = ttl_seconds
        (base_dir / "pages").mkdir(exist_ok=True)
    def _key(self, url: str) -> str: return hashlib.sha1(url.encode("utf-8")).hexdigest()
    def get(self, url: str) -> Optional[str]:
        path = self.base_dir / "pages" / f"{self._key(url)}.txt"
        if path.exists() and (time.time() - path.stat().st_mtime) < self.ttl:
            try: return path.read_text(encoding="utf-8")
            except: pass
        return None
    def set(self, url: str, content: str):
        try: (self.base_dir / "pages" / f"{self._key(url)}.txt").write_text(content, encoding="utf-8")
        except: pass

class DatabaseManager:
    def __init__(self, db_path=DB_PATH):
        if os.path.exists(db_path): 
            try: os.remove(db_path) 
            except: pass
        self.conn = sqlite3.connect(db_path)
        self.create_tables()
    def create_tables(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS news_items (uid TEXT PRIMARY KEY, timestamp DATETIME, source TEXT, title TEXT, body TEXT, sentiment_score REAL, tickers TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS asset_analysis (run_id TEXT, timestamp DATETIME, ticker TEXT, price REAL, target_price REAL, horizon TEXT, sharpe REAL, score REAL, verdict TEXT, trend TEXT, rsi REAL)''')
        self.conn.commit()
    def save_news(self, items):
        c = self.conn.cursor()
        for i in items:
            try: c.execute("INSERT OR IGNORE INTO news_items VALUES (?,?,?,?,?,?,?)", (i['uid'], i['published'], i['source'], i['title'], i.get('body', '')[:5000], i['score'], str(i['tickers'])))
            except: pass
        self.conn.commit()
    def save_analysis(self, results):
        c = self.conn.cursor()
        run_id = str(uuid.uuid4())[:8]; ts = dt.datetime.now().isoformat()
        for r in results:
            c.execute("INSERT INTO asset_analysis VALUES (?,?,?,?,?,?,?,?,?,?,?)", (run_id, ts, r['Ticker'], r['Price'], r.get('Target_Price',0), r.get('Horizon',''), r.get('Sharpe',0), r['Score'], r['Verdict'], r['Trend'], r['RSI']))
        self.conn.commit()

# ==========================================
# 6. NEWS ENGINE
# ==========================================
class NewsEngine:
    def __init__(self, api_keys: APIKeys):
        self.keys = api_keys
        self.cache = DiskCache(CACHE_DIR)
        self.feeds = list(set(DEFAULT_FEEDS))
        self.session_sync = requests.Session()
        self.session_sync.headers.update({"User-Agent": "Mozilla/5.0"})
        self._setup_nlp()
    def _setup_nlp(self):
        try: nltk.data.find('sentiment/vader_lexicon.zip')
        except LookupError: nltk.download('vader_lexicon', quiet=True)
        self.vader = SentimentIntensityAnalyzer()
        self.finbert = None
        if HAS_HF:
            try: self.finbert = hf_pipeline("sentiment-analysis", model="ProsusAI/finbert", tokenizer="ProsusAI/finbert", truncation=True)
            except: pass
    def add_google_news_feed(self, query):
        q = quote_plus(query)
        self.feeds.append(f"https://news.google.com/rss/search?q={q}&hl=en-IN&gl=IN&ceid=IN:en")
    def extract_body(self, url):
        cached = self.cache.get(url)
        if cached: return cached
        try:
            resp = self.session_sync.get(url, timeout=10)
            if not resp.ok: return ""
            text = trafilatura.extract(resp.text) if HAS_TRAFILATURA else ""
            if not text:
                soup = BeautifulSoup(resp.text, 'html.parser')
                text = ' '.join([p.get_text() for p in soup.find_all('p')])
            if text: self.cache.set(url, text); return text[:3000] 
        except: pass
        return ""
    async def fetch_feed_async(self, session, url):
        try:
            async with session.get(url, timeout=15) as response:
                if response.status == 200: return feedparser.parse(await response.read())
        except: return None
    async def collect_all(self):
        log.info(f"📡 Scanning {len(self.feeds)} feeds (incl Moneycontrol Proxy)...")
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_feed_async(session, url) for url in self.feeds]
            results = await asyncio.gather(*tasks)
        articles = []
        cutoff = dt.datetime.now() - dt.timedelta(days=7)
        for res in results:
            if not res: continue
            for entry in res.entries[:15]: 
                try:
                    pub_date = None
                    if 'published' in entry:
                        try: pub_date = dateparser.parse(entry.published).replace(tzinfo=None)
                        except: pass
                    if pub_date and pub_date < cutoff: continue
                    articles.append({
                        'title': entry.title, 
                        'link': entry.link, 
                        'published': (pub_date or dt.datetime.now()).isoformat(),
                        'source': urlparse(entry.link).netloc.replace('www.', ''), 
                        'uid': str(uuid.uuid5(uuid.NAMESPACE_URL, entry.link))
                    })
                except: pass
        return articles
    def score_text(self, text):
        v_score = self.vader.polarity_scores(text)['compound']
        f_score = 0
        if self.finbert:
            try:
                res = self.finbert(text[:512])[0]; val = res['score']
                f_score = -val if res['label'] == 'negative' else val
            except: pass
        return round((v_score * 0.4) + (f_score * 0.6) if f_score != 0 else v_score, 3)
    def process(self):
        loop = asyncio.get_event_loop()
        articles = loop.run_until_complete(self.collect_all())
        random.shuffle(articles) 
        log.info(f"📥 Extracting body text for {len(articles)} articles...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            body_map = list(executor.map(lambda a: self.extract_body(a['link']), articles))
        unique = []; seen = set()
        for i, art in enumerate(articles):
            if art['title'] not in seen:
                art['body'] = body_map[i]
                art['score'] = self.score_text(f"{art['title']} {art['body'][:300]}")
                unique.append(art); seen.add(art['title'])
        return unique

# ==========================================
# 7. ANALYSIS & STRATEGY
# ==========================================
class DataEngine:
    def __init__(self, api_keys: APIKeys):
        self.keys = api_keys
        self.session = requests.Session()
    def fetch_data(self, ticker, days=365):
        plain_ticker = ticker.replace('.NS', '')
        if self.keys.get("TWELVEDATA"):
            try:
                url = f"https://api.twelvedata.com/time_series?symbol={plain_ticker}&interval=1day&outputsize={days}&apikey={self.keys.get('TWELVEDATA')}"
                data = self.session.get(url).json()
                if 'values' in data:
                    df = pd.DataFrame(data['values']); df['close'] = pd.to_numeric(df['close']); df.index = pd.to_datetime(df['datetime'])
                    return df['close'].sort_index(), {}, None, "TwelveData"
            except: pass
        try:
            yf_ticker = f"{ticker}.NS" if not ticker.endswith('.NS') else ticker
            stock = yf.Ticker(yf_ticker)
            hist = stock.history(period="1y")
            if not hist.empty: return hist['Close'], stock.info, stock, "Yahoo"
        except: pass
        return None, None, None, "None"

class AnalysisLab:
    def calculate_technicals(self, prices):
        if len(prices) < 55: return {}
        df = pd.DataFrame({'close': prices})
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
        if curr['close'] > curr['SMA50'] > curr['SMA200']: trend = "UPTREND"
        elif curr['close'] < curr['SMA50'] < curr['SMA200']: trend = "DOWNTREND"
        return {"RSI": round(curr['RSI'], 1), "Trend": trend, "MACD_Signal": "Bullish" if curr['MACD'] > curr['Signal'] else "Bearish", "Volatility": round(prices.pct_change().std() * np.sqrt(252), 2)}

    def calculate_valuation(self, stock, info, current_price):
        try:
            cashflow = stock.cashflow
            if cashflow is not None and not cashflow.empty:
                ocf = cashflow.iloc[0, 0]; capex = abs(cashflow.iloc[1, 0]); fcf = ocf - capex
                sector = info.get('sector', 'default')
                params = SECTOR_MAP.get(sector, SECTOR_MAP['default'])
                growth, wacc, shares = params['growth'], params['wacc'], info.get('sharesOutstanding', 1)
                future_val = 0
                for i in range(1, 6): future_val += (fcf * ((1 + growth) ** i)) / ((1 + wacc) ** i)
                term_val = (fcf * ((1 + growth)**5) * 1.04) / (wacc - 0.04)
                intrinsic = (future_val + (term_val / ((1 + wacc) ** 5))) / shares
                if intrinsic > 0 and intrinsic < current_price*5: 
                    return round(intrinsic, 2)
        except: pass
        try:
            eps = info.get('trailingEps')
            if eps and eps > 0: return round(eps * 20, 2)
        except: pass
        return "N/A"

    def determine_strategy(self, price, dcf_val, trend, score, volatility):
        target_price = price; horizon = "Watchlist"
        tech_upside = price * (1 + volatility) 
        if isinstance(dcf_val, (int, float)) and not np.isnan(dcf_val):
            if dcf_val > price: target_price = (dcf_val * 0.6) + (tech_upside * 0.4)
            else: target_price = (dcf_val * 0.3) + (price * 0.7)
        else: target_price = price * 1.15 if trend == "UPTREND" else price * 0.95
        
        if score >= 75: horizon = "Long Term (1-3 Yrs)"
        elif score >= 60: horizon = "Mid Term (3-6 Mos)"
        elif score <= 40: horizon = "Exit / Short Term"
        else: horizon = "Swing / Neutral"
        return round(target_price, 2), horizon

    def compute_risk_metrics(self, prices):
        if len(prices) < 30: return {}
        ret = prices.pct_change().dropna()
        rf = 0.07 / 252
        mean, std = ret.mean(), ret.std()
        sharpe = ((mean - rf) / std) * np.sqrt(252) if std > 0 else 0
        cum = (1 + ret).cumprod()
        max_dd = ((cum - cum.cummax()) / cum.cummax()).min()
        return {"Sharpe": round(sharpe, 2), "MaxDD": round(max_dd, 3)}

    def analyze_asset(self, ticker, prices, info, stock, source):
        tech = self.calculate_technicals(prices)
        if not tech: return None
        val = self.calculate_valuation(stock, info, prices.iloc[-1])
        risk = self.compute_risk_metrics(prices)
        stress_px = self.stress_test(prices.iloc[-1], info.get('sector', 'default'))
        
        score = 50
        if tech['Trend'] == "UPTREND": score += 20
        if tech['MACD_Signal'] == "Bullish": score += 10
        if tech['RSI'] < 30: score += 15
        elif tech['RSI'] > 70: score -= 15
        if isinstance(val, (int, float)) and val > prices.iloc[-1]: score += 20
        if risk.get('Sharpe', 0) > 1: score += 10
        if tech['Trend'] == "DOWNTREND": score -= 20
        if tech['RSI'] > 70: score -= 15
        if isinstance(val, (int, float)) and val < prices.iloc[-1] * 0.8: score -= 10
        if risk.get('Sharpe', 0) < 0: score -= 5
        
        verdict = "HOLD"
        if score >= 75: verdict = "STRONG BUY"
        elif score >= 60: verdict = "BUY"
        elif score <= 20: verdict = "STRONG SELL"
        elif score <= 40: verdict = "SELL"
        
        target, horizon = self.determine_strategy(prices.iloc[-1], val, tech['Trend'], score, tech['Volatility'])
        
        dd_data = {
            "Score_Breakdown": [f"Final Score: {score}", f"Trend: {tech['Trend']}", f"Sharpe: {risk.get('Sharpe')}"],
            "Valuation_Method": "DCF" if isinstance(val, (int, float)) and val != 0 else "Estimate",
            "Stress_Test_Oil_Shock": stress_px
        }
        
        return {"Ticker": ticker, "Price": round(prices.iloc[-1], 2), "Target_Price": target, "Horizon": horizon, "Trend": tech['Trend'], "RSI": tech['RSI'], "DCF_Val": val, "Sharpe": risk.get('Sharpe'), "Score": score, "Verdict": verdict, "Deep_Dive_Data": dd_data, "Sector": info.get('sector', 'Unknown')}

    def stress_test(self, price, sector):
        shocks = {'Energy': 0.08, 'Financial Services': -0.05, 'Technology': -0.10, 'default': -0.03}
        impact = shocks.get(sector, shocks['default'])
        return round(price * (1 + impact), 2)

# ==========================================
# 8. REPORTING & EMAIL
# ==========================================
class Emailer:
    def __init__(self, api_keys: APIKeys):
        self.user = api_keys.get("EMAIL_USER")
        self.password = api_keys.get("EMAIL_PASS")
        self.recipient = api_keys.get("EMAIL_TO")
        
    def send_report(self, files: List[Path], subject_line, body_text):
        if not (self.user and self.password and self.recipient):
            log.warning("📧 Email credentials missing. Skipping email.")
            return

        msg = MIMEMultipart()
        msg['From'] = self.user
        msg['To'] = self.recipient
        msg['Subject'] = subject_line
        msg.attach(MIMEText(body_text, 'plain'))

        for f in files:
            if not f.exists(): continue
            with open(f, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f"attachment; filename= {f.name}")
                msg.attach(part)

        try:
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.user, self.password)
            server.sendmail(self.user, self.recipient, msg.as_string())
            server.quit()
            log.info(f"📧 Email Sent Successfully to {self.recipient}")
        except Exception as e:
            log.error(f"❌ Email Failed: {e}")

class ReportLab:
    def __init__(self, out_dir): self.out_dir = out_dir
    def generate_html_dashboard(self, results, articles, trends, ind_summary):
        template = """<!DOCTYPE html><html><head><title>Illuminati v23.0</title><style>body{font-family:'Inter',sans-serif;background:#0f172a;color:#e2e8f0;padding:20px}.card{background:#1e293b;border-radius:8px;padding:15px;margin-bottom:15px;border:1px solid #334155}.badge{padding:4px 8px;border-radius:4px;font-weight:bold}.buy{background:#065f46;color:#34d399}.sell{background:#7f1d1d;color:#f87171}.hold{background:#854d0e;color:#fef08a}table{width:100%;border-collapse:collapse;margin-top:20px}th,td{padding:12px;text-align:left;border-bottom:1px solid #334155}th{color:#94a3b8}</style></head><body><h1>👁️ Illuminati Terminal v23.0</h1><p>Assets Analyzed: {{ total }} | Date: {{ date }}</p><h2>🔮 Future Booming Industries</h2><table><thead><tr><th>Theme</th><th>Hype Score</th><th>Mentions</th></tr></thead><tbody>{% for t in trends %}<tr><td><b>{{ t.Theme }}</b></td><td>{{ t.Hype_Score }}%</td><td>{{ t.Mentions }}</td></tr>{% endfor %}</tbody></table><h2>🚀 Industry Momentum</h2><table><thead><tr><th>Sector</th><th>Avg Score</th><th>Top Verdict</th></tr></thead><tbody>{% for s, data in ind_summary.items() %}<tr><td><b>{{ s }}</b></td><td>{{ data['avg_score'] }}</td><td>{{ data['verdict'] }}</td></tr>{% endfor %}</tbody></table><h2>🚀 Investment Strategy</h2><table><thead><tr><th>Ticker</th><th>Price</th><th>Target</th><th>Horizon</th><th>Sharpe</th><th>Valuation</th><th>Score</th><th>Verdict</th></tr></thead><tbody>{% for r in results %}<tr><td><b>{{ r.Ticker }}</b></td><td>{{ r.Price }}</td><td>{{ r.Target_Price }}</td><td>{{ r.Horizon }}</td><td>{{ r.Sharpe }}</td><td>{{ r.DCF_Val }}</td><td>{{ r.Score }}</td><td><span class="badge {{ 'buy' if 'BUY' in r.Verdict else ('sell' if 'SELL' in r.Verdict else 'hold') }}">{{ r.Verdict }}</span></td></tr>{% endfor %}</tbody></table><h2>📰 Market Intel</h2>{% for a in articles[:8] %}<div class="card"><h3><a href="{{ a.link }}" style="color:#60a5fa">{{ a.title }}</a></h3><p style="color:#94a3b8">{{ a.published }} | {{ a.source }}</p><p>{{ a.body[:250] }}...</p></div>{% endfor %}</body></html>"""
        try:
            t = Template(template)
            html = t.render(results=results, articles=articles, trends=trends, ind_summary=ind_summary, date=dt.datetime.now(), total=len(results))
            with open(self.out_dir / f"Dashboard_{dt.datetime.now().strftime('%H%M')}.html", "w") as f: f.write(html)
        except Exception as e: log.error(f"HTML Error: {e}")

    def generate_full_deep_dive(self, results):
        path = self.out_dir / f"Deep_Dive_Full_{dt.datetime.now().strftime('%H%M')}.txt"
        with open(path, 'w') as f:
            f.write(f"ILLUMINATI DEEP DIVE REPORT | {dt.datetime.now()}\n")
            f.write("="*60 + "\n\n")
            for r in results:
                f.write(f"Ticker: {r['Ticker']} | Price: {r['Price']} | Verdict: {r['Verdict']}\n")
                f.write(f"Target: {r['Target_Price']} ({r['Horizon']})\n")
                f.write(f"Score Components: {r['Deep_Dive_Data']['Score_Breakdown']}\n")
                f.write(f"Valuation ({r['Deep_Dive_Data']['Valuation_Method']}): {r['DCF_Val']}\n")
                f.write(f"Stress Test (Oil Shock): {r['Deep_Dive_Data'].get('Stress_Test_Oil_Shock', 'N/A')}\n")
                f.write("-" * 30 + "\n\n")
        return path

class GeminiBrain:
    def __init__(self, api_key=None):
        self.active = False
        self.model = None
        if HAS_GEMINI and api_key:
            genai.configure(api_key=api_key)
            self.active = True
            
    def generate_narrative(self, df_summary):
        if not self.active: return "LLM Analysis Disabled."
        
        try:
            available = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        except: available = []
        
        priority = ['models/gemini-1.5-flash', 'models/gemini-1.5-pro', 'models/gemini-pro', 'models/gemini-1.0-pro']
        chosen = next((m for m in priority if m in available), 'models/gemini-pro')
        
        try:
            log.info(f"🤖 Generating Insight with {chosen}...")
            self.model = genai.GenerativeModel(chosen)
            return self.model.generate_content(f"Analyze this Indian Stock Market data:\n{df_summary.to_csv()}").text
        except Exception as e:
            return f"LLM Error: {e}"

def print_deep_dive_console(asset):
    if not asset: return
    print("\n" + "="*60)
    print(f"🔬 DEEP DIVE HIGHLIGHT: {asset['Ticker']}")
    print("="*60)
    print(f"Current Price: ₹{asset['Price']}  |  Target: ₹{asset['Target_Price']}")
    print(f"Verdict: {asset['Verdict']}  |  Horizon: {asset['Horizon']}")
    print(f"Valuation Method: {asset['Deep_Dive_Data']['Valuation_Method']}")
    print(f"Calculated Fair Value: {asset['DCF_Val']}")
    print(f"Score Factors: {asset['Deep_Dive_Data']['Score_Breakdown']}")

# ==========================================
# 9. SCHEDULER & ORCHESTRATOR
# ==========================================
def calculate_sleep_seconds():
    now = dt.datetime.now(dt.timezone.utc)
    is_utc = time.localtime().tm_gmtoff == 0
    target_utc_hour = 2 if is_utc else 8
    target_utc_min = 30 if is_utc else 0
    target_time = now.replace(hour=target_utc_hour, minute=target_utc_min, second=0, microsecond=0)
    if now >= target_time: target_time += dt.timedelta(days=1)
    wait_seconds = (target_time - now).total_seconds()
    return wait_seconds, target_time

def run_illuminati(interactive=False, tickers_arg=None):
    ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    current_time = dt.datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')
    print("\n" + "="*80)
    print(f"👁️ ILLUMINATI TERMINAL v23.0 (INFINITE HORIZON) | {current_time}")
    print("="*80)

    api = APIKeys()
    if interactive: api.interactive_load()
    
    db = DatabaseManager()
    news = NewsEngine(api)
    mapper = MasterMapper()
    data = DataEngine(api)
    lab = AnalysisLab()
    trend_hunter = TrendHunter()
    reporter = ReportLab(OUTPUT_DIR)
    emailer = Emailer(api)
    llm = GeminiBrain(api.get("GEMINI"))
    
    news.add_google_news_feed("Indian Stock Market News")
    articles = news.process()
    db.save_news(articles)
    
    trends = trend_hunter.predict_booming_industries(articles)
    if trends:
        print("\n🔮 PREDICTED BOOMING INDUSTRIES (News Hype):")
        print(tabulate(pd.DataFrame(trends).head(5), headers='keys', tablefmt='psql', showindex=False))
    
    # HYBRID SCAN: News Tickers + Nifty 500 Backup (for recognition only)
    news_tickers = mapper.extract_tickers(articles)
    combined_tickers = list(set(news_tickers))
    
    if tickers_arg: combined_tickers.extend(tickers_arg.split(','))
    
    if not combined_tickers:
        print("⚠️ No tickers found in current news cycle. Try again later or add manual tickers.")
        return

    print(f"\n⚡ Analyzing {len(combined_tickers)} Assets (News Discovery)...")
    results = []
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        def process_ticker(t):
            try:
                prices, info, stock, src = data.fetch_data(t)
                if prices is None: return None
                res = lab.analyze_asset(t, prices, info, stock, src)
                if res: return res
            except: return None
        
        futures = [executor.submit(process_ticker, t) for t in combined_tickers]
        for f in futures:
            res = f.result()
            if res: results.append(res)

    if results:
        df = pd.DataFrame(results).sort_values("Score", ascending=False)
        ind_summary = {}
        if 'Sector' in df.columns:
            for sector, grp in df.groupby('Sector'):
                avg = grp['Score'].mean()
                ind_summary[sector] = {'avg_score': round(avg, 1), 'verdict': "Bullish" if avg > 60 else ("Bearish" if avg < 40 else "Neutral")}

        db.save_analysis(results)
        reporter.generate_html_dashboard(results, articles, trends, ind_summary)
        dd_path = reporter.generate_full_deep_dive(results)
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M")
        excel_path = OUTPUT_DIR / f"Strategy_{ts}.xlsx"
        df.to_excel(excel_path, index=False)
        html_path = OUTPUT_DIR / f"Dashboard_{dt.datetime.now().strftime('%H%M')}.html"
        
        narrative = llm.generate_narrative(df.head(10))
        print(f"\n🤖 AI Insight: {narrative[:300]}...\n")
        
        print("\n" + "="*80)
        print("🚀 TOP OPPORTUNITIES (Buys)")
        print("="*80)
        buys = df[df['Verdict'].str.contains("BUY")].head(10)
        if not buys.empty:
            print(tabulate(buys[['Ticker', 'Price', 'Target_Price', 'Horizon', 'Sharpe', 'Verdict']], headers='keys', tablefmt='psql', showindex=False))
        else:
            print("   No strong buy signals found.")

        print("\n" + "="*80)
        print("⚠️ WARNINGS & EXITS (Sells)")
        print("="*80)
        sells = df[df['Verdict'].str.contains("SELL")].head(10)
        if not sells.empty:
            print(tabulate(sells[['Ticker', 'Price', 'Target_Price', 'Horizon', 'Sharpe', 'Verdict']], headers='keys', tablefmt='psql', showindex=False))
        else:
            print("   No strong sell signals found.")
        
        if not df.empty:
            print_deep_dive_console(df.iloc[0].to_dict())
            
        print(f"\n✅ All Reports Saved to: {OUTPUT_DIR}")
        
        if api.get("EMAIL_USER"):
            email_body = f"ILLUMINATI EXECUTIVE BRIEF\n\nTop Pick: {df.iloc[0]['Ticker']}\nVerdict: {df.iloc[0]['Verdict']}\nTarget: {df.iloc[0]['Target_Price']}\n\nAI Insight:\n{narrative}"
            emailer.send_report([excel_path, html_path, dd_path], f"Illuminati Report - {dt.datetime.now().strftime('%Y-%m-%d')}", email_body)

def schedule_job():
    print("⏰ Scheduler Started. Calculating next run time...")
    while True:
        wait_seconds, next_run = calculate_sleep_seconds()
        print(f"💤 Sleeping for {wait_seconds/3600:.1f} hours. Next run: {next_run.strftime('%Y-%m-%d %H:%M %Z')}")
        time.sleep(wait_seconds)
        if dt.datetime.now().weekday() < 5: run_illuminati()
        else: print("Skipping run (Weekend).")

def calculate_sleep_seconds():
    now = dt.datetime.now(dt.timezone.utc)
    is_utc = time.localtime().tm_gmtoff == 0
    target_utc_hour = 2 if is_utc else 8
    target_utc_min = 30 if is_utc else 0
    target_time = now.replace(hour=target_utc_hour, minute=target_utc_min, second=0, microsecond=0)
    if now >= target_time: target_time += dt.timedelta(days=1)
    wait_seconds = (target_time - now).total_seconds()
    return wait_seconds, target_time

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--interactive", action="store_true")
    parser.add_argument("--schedule", action="store_true")
    parser.add_argument("--tickers", type=str)
    args = parser.parse_args(args=[] if 'google.colab' in sys.modules else None)
    
    # FORCED COLAB INTERACTIVITY FIX
    if 'google.colab' in sys.modules:
        is_interactive = not args.schedule
    else:
        is_interactive = args.interactive or len(sys.argv) == 1

    if args.schedule: schedule_job()
    else: run_illuminati(interactive=is_interactive, tickers_arg=args.tickers)
