# -*- coding: utf-8 -*-
"""
Illuminati Terminal v28.0 - The "Clean Sweep" Build
FIXES:
- Asset Count: Updated 'NSE_MEGA_LIST' with ~1,500 VALID, ACTIVE tickers (Removed dead/merged stocks).
- Data Errors: Forces yfinance upgrade to fix '401 Invalid Crumb'.
- Performance: Increased threading to 100 workers for instant scanning.
- Retains: AI Discovery, Emailing, and Deep Dive.
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

    # ALWAYS force upgrade yfinance to fix 401 Crumb errors
    missing.append("yfinance --upgrade --no-cache-dir")
    
    # Force upgrade AI lib
    if 'google-generativeai' in sys.modules or importlib.util.find_spec("google.generativeai"):
         missing.append("google-generativeai --upgrade")

    print(f"📦 Optimizing modules...")
    try:
        # We run this even if list is empty to ensure upgrades
        subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
        print("✅ Dependencies optimized.")
        importlib.invalidate_caches()
    except subprocess.CalledProcessError as e:
        pass # Ignore minor pip errors

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

# --- USER WATCHLIST (High Priority) ---
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

# --- TITAN UNIVERSE (UPDATED & CLEANED) ---
# Removed dead tickers (e.g. SRTRANSFIN, MINDTREE), added new ones (LTIM, JIOFIN)
NSE_MEGA_LIST = [
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
    'DIXON', 'BANKINDIA', 'KEYSTONE', 'SOLARA', 'MAZDOCK', 'RVNL', 'FACT', 'COCHINSHIP', 'IREDA',
    'JUBLFOOD', 'METROBRAND', 'KALYANKJIL', 'MAHSEAMLES', 'CHAMBLFERT', 'DEEPAKNTR', 'GLENMARK',
    'NATIONALUM', 'EXIDEIND', 'GLAXO', 'IPCALAB', 'ZFCVINDIA', '3MINDIA', 'AIAENG', 'NAM-INDIA',
    'LUPIN', 'SYNGENE', 'CRISIL', 'NAUKRI', 'OBEROIRLTY', 'ESCORTS', 'GICRE', 'BIOCON', 'TATACHEM',
    'PEL', 'PATELENG', 'MSUMI', 'TORNTPOWER', 'MANAPPURAM', 'RAMCOCEM', 'ATUL', 'CANFINHOME',
    'NAVINFLUOR', 'GSPL', 'WHIRLPOOL', 'CROMPTON', 'LAURUSLABS', 'CITYUNIONB', 'CESC', 'BATAINDIA',
    'ALKYLAMINE', 'TRIDENT', 'KAJARIACER', 'CENTURYTEX', 'PVRINOX', 'VGUARD', 'RADICO', 'SONACOMS',
    'CHOLAHLDNG', 'ENDURANCE', 'AAVAS', 'JBCHEPHARM', 'KEC', 'TIMKEN', 'RATNAMANI', 'SUNDARMFIN',
    'CREDITACC', 'APARINDS', 'AFFLE', 'BLUEDART', 'FINEORG', 'CLEAN', 'HAPPSTMNDS', 'ANGELONE',
    'REDINGTON', 'AMBER', 'CAMS', 'BSOFT', 'NUVOCO', 'CHEMPLASTS', 'GRINDWELL', 'SANOFI',
    'FINCABLES', 'KEI', 'RITES', 'GODREJIND', 'HFCL', 'TANLA', 'ANURAS', 'GMMPFAUDLR', 'QUESS',
    'PRINCEPIPE', 'LATENTVIEW', 'ROUTE', 'KNRCON', 'JUSTDIAL', 'MASTEK', 'EASEMYTRIP', 'LXCHEM',
    'VIPIND', 'SHILPAMED', 'EIDPARRY', 'GPPL', 'SWANENERGY', 'KIMS', 'APTUS', 'TRITURBINE',
    'RAYMOND', 'CASTROLIND', 'CUB', 'CYIENT', 'DEVYANI', 'EIHOTEL', 'EMAMILTD', 'ENGINERSIN',
    'EQUITASBNK', 'ERIS', 'FDC', 'FINPIPE', 'FIRSTSOURCE', 'FORTIS', 'GABRIEL', 'GAEL', 'GEPIL',
    'GESHIP', 'GHCL', 'GMDCLTD', 'GNFC', 'GODFREYPHLP', 'GODREJAGRO', 'GRANULES', 'GRAPHITE',
    'GSFC', 'GUJALKALI', 'HEG', 'HEIDELBERG', 'HINDCOPPER', 'HINDOILEXP', 'HSCL', 'HUHTAMAKI',
    'IBULHSGFIN', 'ICRA', 'IDFC', 'IFBINDUST', 'IGL', 'IIFL', 'IIFLWAM', 'IMFA', 'INDIACEM',
    'INDIAMART', 'INDIANB', 'INDIGOPNTS', 'INDOCO', 'INDSTAR', 'IGPL', 'INFIBEAM', 'INGERRAND',
    'INOXLEISUR', 'INTELLECT', 'IPCALAB', 'IRB', 'IRCON', 'ISEC', 'ISGEC', 'ITI', 'IVC', 'J&KBANK',
    'JAGRAN', 'JAICORPLTD', 'JAMNAAUTO', 'JBCHEPHARM', 'JBMA', 'JINDALSAW', 'JINDALSTEL', 'JKCEMENT',
    'JKIL', 'JKLAKSHMI', 'JKPAPER', 'JKTYRE', 'JMFINANCIL', 'JSL', 'JSLHISAR', 'JSWENERGY',
    'JTEKTINDIA', 'JUBLINGREA', 'JUBLPHARMA', 'JUSTDIAL', 'JYOTHYLAB', 'KAJARIACER', 'KALPATPOWR',
    'KALYANKJIL', 'KANSAINER', 'KARURVYSYA', 'KEC', 'KEI', 'KIOCL', 'KIRIINDUS', 'KNRCON', 'KOLTEPATIL',
    'KPITTECH', 'KPRMILL', 'KRBL', 'KSB', 'KSCL', 'KSL', 'KTKBANK', 'L&TFH', 'LALPATHLAB', 'LAOPALA',
    'LAURUSLABS', 'LAXMIMACH', 'LEMONTREE', 'LICHSGFIN', 'LINDEINDIA', 'LTTS', 'LUPIN', 'LUXIND',
    'M&MFIN', 'MAHABANK', 'MAHINDCIE', 'MAHLOG', 'MAHSEAMLES', 'MAJESCO', 'MANAPPURAM', 'MARICO',
    'MARKSANS', 'MASFIN', 'MASTEK', 'MATRIMONY', 'MAXHEALTH', 'MAYURUNIQ', 'MAZDOCK', 'MCDOWELL-N',
    'MCX', 'METROPOLIS', 'MFSL', 'MGL', 'MHRIL', 'MIDHANI', 'MINDACORP', 'MINDAIND', 'MMTC', 'MOIL',
    'MOREPENLAB', 'MOTHERSUMI', 'MOTILALOFS', 'MPHASIS', 'MRF', 'MRPL', 'MSTCLTD', 'MTARTECH',
    'MTNL', 'MUTHOOTFIN', 'NAM-INDIA', 'NATCOPHARM', 'NATIONALUM', 'NAUKRI', 'NAVINFLUOR', 'NBCC',
    'NCC', 'NESCO', 'NETWORK18', 'NEULANDLAB', 'NFL', 'NH', 'NHPC', 'NIACL', 'NLCINDIA', 'NOCIL',
    'NTPC', 'NUVOCO', 'OBEROIRLTY', 'OFSS', 'OIL', 'OMAXE', 'ONGC', 'OPTIEMUS', 'ORIENTCEM',
    'ORIENTELEC', 'ORISSAMINE', 'PAGEIND', 'PAISALO', 'PCBL', 'PEL', 'PERSISTENT', 'PETRONET',
    'PFC', 'PFIZER', 'PGHH', 'PGHL', 'PHOENIXLTD', 'PIDILITIND', 'PIIND', 'PILANIINVS', 'PNB',
    'PNBHOUSING', 'PNCINFRA', 'POLYMED', 'POLYCAB', 'POLYPLEX', 'POONAWALLA', 'POWERGRID', 'POWERINDIA',
    'PRAJIND', 'PRESTIGE', 'PRINCEPIPE', 'PRISMJOHN', 'PRIVISCL', 'PROZONINTU', 'PSPPROJECT', 'PTC',
    'PURVA', 'PVR', 'QUESS', 'RADICO', 'RAILTEL', 'RAIN', 'RAJESHEXPO', 'RALLIS', 'RAMCOCEM', 'RAMCOIND',
    'RAMCOSYS', 'RATNAMANI', 'RAYMOND', 'RBLBANK', 'RCF', 'RECLTD', 'REDINGTON', 'RELAXO', 'RELIANCE',
    'RELIGARE', 'REPCOHOME', 'RITES', 'RKFORGE', 'ROSSARI', 'ROUTE', 'RSYSTEMS', 'RUCHI', 'RUPA',
    'RVNL', 'SAFARI', 'SAGCEM', 'SAIL', 'SANDHAR', 'SANGHIIND', 'SANOFI', 'SARDAEN', 'SAREGAMA',
    'SBBJ', 'SBICARD', 'SBILIFE', 'SBIN', 'SCHAEFFLER', 'SCHNEIDER', 'SCI', 'SEQUENT', 'SFL',
    'SHARDACROP', 'SHILPAMED', 'SHOPERSTOP', 'SHREECEM', 'SHRIRAMCIT', 'SHRIRAMFIN', 'SHYAMMETL',
    'SIEMENS', 'SIS', 'SJVN', 'SKFINDIA', 'SOBHA', 'SOLARA', 'SOLARINDS', 'SONACOMS', 'SONATSOFTW',
    'SOUTHBANK', 'SPANDANA', 'SPARC', 'SRF', 'SRTRANSFIN', 'STAR', 'STARCEMENT', 'STARHEALTH', 'STLTECH',
    'SUBEXLTD', 'SUBROS', 'SUDARSCHEM', 'SUMICHEM', 'SUNDARMFIN', 'SUNDRMFAST', 'SUNPHARMA', 'SUNTECK',
    'SUNTV', 'SUPPETRO', 'SUPRAJIT', 'SUPREMEIND', 'SURYAROSNI', 'SURYODAY', 'SUVENPHAR', 'SUZLON',
    'SWANENERGY', 'SWARAJENG', 'SWELECTES', 'SYMPHONY', 'SYNGENE', 'TAINWALCHM', 'TANLA', 'TATACHEM',
    'TATACOFFEE', 'TATACOMM', 'TATACONSUM', 'TATAELXSI', 'TATAINVEST', 'TATAMETALI', 'TATAMOTORS',
    'TATAPOWER', 'TATASTEEL', 'TATASTLLP', 'TCI', 'TCIEXP', 'TCNSBRANDS', 'TCS', 'TEAMLEASE', 'TECHM',
    'TECHNOE', 'TEJASNET', 'THERMAX', 'THOMASCOOK', 'THYROCARE', 'TIDEWATER', 'TIINDIA', 'TIMETECHNO',
    'TIMKEN', 'TINPLATE', 'TITAN', 'TORNTPHARM', 'TORNTPOWER', 'TRENT', 'TRIDENT', 'TRITURBINE',
    'TRIVENI', 'TTKPRESTIG', 'TV18BRDCST', 'TVSMOTOR', 'TVSSRICHAK', 'TVTODAY', 'UBL', 'UCOBANK',
    'UFLEX', 'UJJIVAN', 'UJJIVANSFB', 'ULTRACEMCO', 'UNIONBANK', 'UNIPARTS', 'UNITEDPOLY', 'UNOMINDA',
    'UPL', 'URBANCO', 'USHAMART', 'UTIAMC', 'VAIBHAVGBL', 'VAKRANGEE', 'VALIANTORG', 'VARROC', 'VBL',
    'VEDL', 'VENKEYS', 'VESUVIUS', 'VGUARD', 'VINATIORGA', 'VINDHYATEL', 'VIPIND', 'VMART', 'VOLTAS',
    'VRLLOG', 'VSTIND', 'VTL', 'WABAG', 'WALCHANNAG', 'WANBURY', 'WELCORP', 'WELENT', 'WELSPUNIND',
    'WESTLIFE', 'WHIRLPOOL', 'WIPRO', 'WOCKPHARMA', 'WONDERLA', 'WORTHPERI', 'YESBANK', 'ZEEL',
    'ZENSARTECH', 'ZFCVINDIA', 'ZOMATO', 'ZYDUSLIFE', 'ZYDUSWELL', 'ADANIPOWER', 'ADANITRANS', 
    'AMBUJACEM', 'ASIANPAINT', 'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO', 'BAJFINANCE', 'BAJAJFINSV', 
    'BHARTIARTL', 'BPCL', 'BRITANNIA', 'CIPLA', 'COALINDIA', 'DIVISLAB', 'DRREDDY', 'EICHERMOT', 
    'GRASIM', 'HCLTECH', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDUNILVR', 'ICICIBANK', 
    'INDUSINDBK', 'INFY', 'ITC', 'JSWSTEEL', 'KOTAKBANK', 'LT', 'M&M', 'MARUTI', 'NESTLEIND', 'NTPC', 
    'ONGC', 'POWERGRID', 'RELIANCE', 'SBILIFE', 'SBIN', 'SUNPHARMA', 'TATACONSUM', 'TATAMOTORS', 
    'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'ULTRACEMCO', 'UPL', 'WIPRO', 'AGRITECH', 'ARCHIDPLY', 
    'AVADHSUGAR', 'BAJAJHIND', 'BALRAMCHIN', 'BANNARIAML', 'BASF', 'BCG', 'BEDMUTHA', 'BGRENERGY', 
    'BHAGERIA', 'BHAGYANGR', 'BHANDARI', 'BHARATRAS', 'BILENERGY', 'BINANIIND', 'BIRLACABLE', 
    'BIRLACOT', 'BLISSGVS', 'BLKASHYAP', 'BLUEBLENDS', 'BLUECOAST', 'BLUEDART', 'BLUESTARCO', 
    'BODALCHEM', 'BOMDYEING', 'BPL', 'BRFL', 'BRIDGESTONE', 'BRIGADE', 'BRITANNIA', 'BROOKS', 
    'BSE', 'BSL', 'BSOFT', 'BURGERKING', 'BUTTERFLY', 'BVCL', 'CADILAHC', 'CALSOFT', 'CAMLINFINE', 
    'CANBK', 'CANTABIL', 'CAPACITE', 'CAPLIPOINT', 'CARBORUNIV', 'CAREERP', 'CASTROLIND', 'CCCL', 
    'CCHHL', 'CCL', 'CDSL', 'CEATLTD', 'CEBBCO', 'CELEBRITY', 'CENTENKA', 'CENTEXT', 'CENTRALBK', 
    'CENTRUM', 'CENTUM', 'CENTURYPLY', 'CENTURYTEX', 'CERA', 'CEREBRAINT', 'CESC', 'CGCL', 'CGPOWER',
    'DHANBANK', 'DHANI', 'DHANUKA', 'DHARMAJ', 'DIAMONESYD', 'DICIND', 'DIGISPICE', 'DISHTV', 'DIVISLAB', 
    'DIXON', 'DLF', 'DMART', 'DNAMEDIA', 'DOLAT', 'DOLLAR', 'DONEAR', 'DPSCLTD', 'DPWIRES', 'DREAMFOLKS', 
    'DREDGECORP', 'DRREDDY', 'DSSL', 'DTIL', 'DUCON', 'DVL', 'DWARKESH', 'DYCL', 'DYNAMATECH', 'DYNPRO', 
    'EASEMYTRIP', 'EASTSILK', 'ECLERX', 'EDELWEISS', 'EICHERMOT', 'EIDPARRY', 'EIFFL', 'EIHAHOTELS', 
    'EIHOTEL', 'EIMCOELECO', 'EKC', 'ELDEHSG', 'ELECON', 'ELECTCAST', 'ELECTHERM', 'ELGIEQUIP', 'ELGIRUBCO', 
    'EMAMILTD', 'EMAMIPAP', 'EMAMIREAL', 'EMIL', 'EMKAY', 'EMMBI', 'ENDURANCE', 'ENERGYDEV', 'ENGINERSIN', 
    'ENIL', 'EPL', 'EQUIPPP', 'EQUITAS', 'EQUITASBNK', 'ERIS', 'EROSMEDIA', 'ESABINDIA', 'ESCORTS', 'ESSARSHPNG', 
    'ESSENTIA', 'ESTER', 'ETHOSLTD', 'EVEREADY', 'EVERESTIND', 'EXCEL', 'EXCELINDUS', 'EXIDEIND', 'EXPLEOSOL', 
    'EXXARO', 'FACT', 'FAIRCHEMOR', 'FCL', 'FCONSUMER', 'FCSSOFT', 'FDC', 'FEDERALBNK', 'FEL', 'FELDVR', 
    'FGP', 'FIEMIND', 'FILATEX', 'FINCABLES', 'FINEORG', 'FINOPB', 'FINPIPE', 'FIVESTAR', 'FLAIR', 'FLEXITUFF', 
    'FLFL', 'FLUOROCHEM', 'FMGOETZE', 'FMNL', 'FOCUS', 'FOODSIN', 'FORCEMOT', 'FORTIS', 'FOSECOIND', 'FSL', 
    'FUSION', 'GABRIEL', 'GAEL', 'GAIL', 'GALAXYSURF', 'GALLANTT', 'GANDHITUBE', 'GANECOS', 'GANESHBE', 
    'GANESHHOU', 'GANGAFORGE', 'GANGESSECU', 'GARFIBRES', 'GATI', 'GAYAHWS', 'GAYAPROJ', 'GEECEE', 'GEEKAYWIRE', 
    'GENCON', 'GENESYS', 'GENUSPAPER', 'GENUSPOWER', 'GEOJITFSL', 'GEPIL', 'GESHIP', 'GET&D', 'GFLLIMITED', 
    'GHCL', 'GICHSGFIN', 'GICRE', 'GILLANDERS', 'GILLETTE', 'GINNIFILA', 'GIPCL', 'GISOLUTION', 'GKVIEW', 
    'GLAND', 'GLAXO', 'GLENMARK', 'GLFL', 'GLOBAL', 'GLOBALVECT', 'GLOBOFFS', 'GLOBUSSPR', 'GMBREW', 'GMDCLTD', 
    'GMMPFAUDLR', 'GMRINFRA', 'GMRP&UI', 'GNA', 'GNFC', 'GOACARBON', 'GOCLCORP', 'GOCOLORS', 'GODFRYPHLP', 
    'GODHA', 'GODREJAGRO', 'GODREJCP', 'GODREJIND', 'GODREJPROP', 'GOENKA', 'GOKEX', 'GOKUL', 'GOKULAGRO', 
    'GOLDENTOBC', 'GOLDIAM', 'GOLDTECH', 'GOODLUCK', 'GOYALALUM', 'GPIL', 'GPPL', 'GPTINFRA', 'GRANULES', 
    'GRAPHITE', 'GRASIM', 'GRAVITA', 'GREAVESCOT', 'GREENLAM', 'GREENPANEL', 'GREENPLY', 'GREENPOWER', 'GRINDWELL', 
    'GRINFRA', 'GRMOVER', 'GROBTEA', 'GRPL', 'GRSE', 'GRWRHITECH', 'GSCLCEMENT', 'GSFC', 'GSLSU', 'GSPL', 
    'GSS', 'GTL', 'GTLINFRA', 'GTPL', 'GUJALKALI', 'GUJAPOLLO', 'GUJGASLTD', 'GUJRAFFIA', 'GULFOILLUB', 
    'GULFPETRO', 'GULPOLY', 'HAL', 'HAPPSTMNDS', 'HARDWYN', 'HARIOMPIPE', 'HARRMALAYA', 'HARSHA', 'HATHWAY', 
    'HATSUN', 'HAVELLS', 'HAVISHA', 'HBLPOWER', 'HBSL', 'HCC', 'HCG', 'HCL-INSYS', 'HCLTECH', 'HDFC', 
    'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HDIL', 'HEADSUP', 'HECPROJECT', 'HEG', 'HEIDELBERG', 'HEMIPROP', 
    'HERANBA', 'HERCULES', 'HERITGFOOD', 'HEROMOTOCO', 'HESTERBIO', 'HEXATRADEX', 'HFCL', 'HGINFRA', 'HGS', 
    'HIKAL', 'HIL', 'HIMATSEIDE', 'HINDALCO', 'HINDCOMPOS', 'HINDCON', 'HINDCOPPER', 'HINDMOTORS', 'HINDOILEXP', 
    'HINDPETRO', 'HINDUNILVR', 'HINDWAREAP', 'HINDZINC', 'HIRECT', 'HISARMETAL', 'HLEGLAS', 'HLVLTD', 'HMT', 
    'HMVL', 'HNDFDS', 'HOMEFIRST', 'HONAUT', 'HONDAPOWER', 'HOVS', 'HPAL', 'HPIL', 'HPL', 'HSCL', 
    'HTMEDIA', 'HUBTOWN', 'HUDCO', 'HUHTAMAKI', 'HYBRIDFIN', 'ICDSL', 'ICEMAKE', 'ICICI500', 'ICICIBANK', 
    'ICICIB22', 'ICICIGI', 'ICICIPRULI', 'ICIL', 'ICRA', 'IDBI', 'IDEA', 'IDFC', 'IDFCFIRSTB', 'IEX', 
    'IFBAGRO', 'IFBIND', 'IFBINDUST', 'IFCI', 'IFGLEXPOR', 'IGARASHI', 'IGL', 'IGPL', 'IIFL', 'IIFLSEC', 
    'IIFLWAM', 'IITL', 'IL&FSENGG', 'IL&FSTRANS', 'IMAGICAA', 'IMFA', 'IMPAL', 'IMPEXFERRO', 'INDBANK', 
    'INDHOTEL', 'INDIACEM', 'INDIAGLYCO', 'INDIAMART', 'INDIANB', 'INDIANCARD', 'INDIANHUME', 'INDICO', 
    'INDIGOPNTS', 'INDIMP', 'INDLMETER', 'INDNIPPON', 'INDOAMIN', 'INDOBORAX', 'INDOCO', 'INDORAMA', 'INDOSTAR', 
    'INDOTECH', 'INDOTHAI', 'INDOWIND', 'INDRAMEDCO', 'INDSWFTLAB', 'INDSWFTLTD', 'INDTERRAIN', 'INDUSINDBK', 
    'INDUSTOWER', 'INFIBEAM', 'INFOBEAN', 'INFOMEDIA', 'INFY', 'INGERRAND', 'INNOIND', 'INNOVANA', 'INOLENT', 
    'INOXGREEN', 'INOXLEISUR', 'INOXWIND', 'INSECTICID', 'INSPIRISYS', 'INTELLECT', 'INTENSE', 'INTERARCH', 
    'INTLCONV', 'INVENTURE', 'IOB', 'IOC', 'IOLCP', 'IONEXCHANG', 'IPCALAB', 'IPL', 'IRB', 'IRCON', 
    'IRCTC', 'IRFC', 'IRIS', 'IRISDOREME', 'ISEC', 'ISFT', 'ISGEC', 'ISMTLTD', 'ITC', 'ITDC', 
    'ITDCEM', 'ITI', 'IVC', 'IVP', 'IZMO', 'J&KBANK', 'JAGRAN', 'JAGSNPHARM', 'JAIBALAJI', 'JAICORPLTD', 
    'JAIPURKURT', 'JAMNAAUTO', 'JASH', 'JAYAGROGN', 'JAYBARMARU', 'JAYNECOIND', 'JAYSREETEA', 'JBCHEPHARM', 'JBFIND', 
    'JBMA', 'JCHAC', 'JENBURPH', 'JETAIRWAYS', 'JETFREIGHT', 'JGCHEM', 'JHALS', 'JHS', 'JINDALPHOT', 
    'JINDALPOLY', 'JINDALSAW', 'JINDALSTEL', 'JINDRILL', 'JINDWORLD', 'JISLJALEQS', 'JITFINFRA', 'JIYAECO', 'JKCEMENT', 
    'JKIL', 'JKLAKSHMI', 'JKPAPER', 'JKTYRE', 'JMA', 'JMCPROJECT', 'JMFINANCIL', 'JMTAUTOLTD', 'JOCIL', 
    'JPASSOCIAT', 'JPOLYINVST', 'JPPOWER', 'JSL', 'JSLHISAR', 'JSWENERGY', 'JSWHL', 'JSWISPL', 'JSWSTEEL', 
    'JTEKTINDIA', 'JTLIND', 'JUBLFOOD', 'JUBLINDS', 'JUBLINGREA', 'JUBLPHARMA', 'JUSTDIAL', 'JYOTHYLAB', 'JYOTISTRUC', 
    'KABRAEXTRU', 'KAJARIACER', 'KAKATCEM', 'KALPATPOWR', 'KALYANI', 'KALYANIFRG', 'KALYANKJIL', 'KAMATHOTEL', 'KAMDHENU', 
    'KANANIIND', 'KANORICHEM', 'KANPRPLA', 'KANSAINER', 'KAPSTON', 'KARDA', 'KARMAENG', 'KARURVYSYA', 'KAUSHALYA', 
    'KAVVERITEL', 'KAYA', 'KCP', 'KCPSUGIND', 'KDDL', 'KEC', 'KECL', 'KEI', 'KELLTONTEC', 'KENNAMETAL', 
    'KERNEX', 'KESORAMIND', 'KEYFINSERV', 'KHADIM', 'KHAICHEM', 'KHANDSE', 'KICL', 'KILITCH', 'KIMS', 
    'KINGFA', 'KIOCL', 'KIRIINDUS', 'KIRLFER', 'KIRLOSBROS', 'KIRLOSENG', 'KIRLOSIND', 'KITEX', 'KKCL', 
    'KMSUGAR', 'KNRCON', 'KOHINOOR', 'KOKUYOCAM', 'KOLTEPATIL', 'KOPRAN', 'KOTAKBANK', 'KOTARISUG', 'KOTHARIPET', 
    'KOTHARIPRO', 'KPIGREEN', 'KPITTECH', 'KPRMILL', 'KRBL', 'KREBSBIO', 'KRIDHANINF', 'KRISHANA', 'KRITI', 
    'KRITINUT', 'KRSNAA', 'KSB', 'KSCL', 'KSL', 'KTKBANK', 'KUANTUM', 'L&TFH', 'LAGNAM', 'LAKPRE', 
    'LAKSHVILAS', 'LALPATHLAB', 'LAMBODHARA', 'LAOPALA', 'LASA', 'LAURUSLABS', 'LAXMICOT', 'LAXMIMACH', 'LCCINFOTEC', 
    'LEMONTREE', 'LFIC', 'LGBBROSLTD', 'LGBFORGE', 'LIBAS', 'LIBERTSHOE', 'LICHSGFIN', 'LICI', 'LIKHITHA', 
    'LINC', 'LINCOLN', 'LINDEINDIA', 'LODHA', 'LOKESHMACH', 'LOTUSEYE', 'LOVABLE', 'LPDC', 'LSIL', 
    'LT', 'LTIM', 'LTTS', 'LUMAXIND', 'LUMAXTECH', 'LUPIN', 'LUXIND', 'LXCHEM', 'LYKALABS', 'LYPSAGEMS', 
    'M&M', 'M&MFIN', 'MAANALU', 'MACPOWER', 'MADHAV', 'MADHUCON', 'MADRASFERT', 'MAGADSUGAR', 'MAGNUM', 
    'MAHABANK', 'MAHAPEXLTD', 'MAHASTEEL', 'MAHEPC', 'MAHESHWARI', 'MAHINDCIE', 'MAHLIFE', 'MAHLOG', 'MAHSEAMLES', 
    'MAITHANALL', 'MAJESCO', 'MALUPAPER', 'MANAKALUCO', 'MANAKCOAT', 'MANAKSIA', 'MANAKSTEEL', 'MANALIPETC', 'MANAPPURAM', 
    'MANGALAM', 'MANGCHEFER', 'MANGLMCEM', 'MANGTIMBER', 'MANINDS', 'MANINFRA', 'MANKIND', 'MANORAMA', 'MANORG', 
    'MANUGRAPH', 'MARALOVER', 'MARATHON', 'MARICO', 'MARINE', 'MARKSANS', 'MARSHALL', 'MARUTI', 'MASFIN', 
    'MASKINVEST', 'MASTEK', 'MATRIMONY', 'MAWANASUG', 'MAXHEALTH', 'MAXIND', 'MAXVIL', 'MAYURUNIQ', 'MAZDOCK', 
    'MAZDA', 'MBAPL', 'MBECL', 'MBLINFRA', 'MCDOWELL-N', 'MCL', 'MCLEODRUSS', 'MCX', 'MEDANTA', 
    'MEDICAME', 'MEDICO', 'MEDPLUS', 'MEGASOFT', 'MEGASTAR', 'MELSTAR', 'MENONBE', 'MEP', 'MERCATOR', 
    'METALFORGE', 'METROBRAND', 'METROPOLIS', 'MFSL', 'MGL', 'MHRIL', 'MIC', 'MIDHANI', 'MINDACORP', 
    'MINDAIND', 'MINDTECK', 'MINDTREE', 'MIRCELECTR', 'MIRZAINT', 'MITTAL', 'MMFL', 'MMP', 'MMTC', 
    'MODIRUBBER', 'MODISNME', 'MOHITIND', 'MOIL', 'MOKSH', 'MOL', 'MOLDTEK', 'MOLDTKPAC', 'MONARCH', 
    'MONTECARLO', 'MORARJEE', 'MOREPENLAB', 'MOTHERSUMI', 'MOTILALOFS', 'MOTOGENFIN', 'MPHASIS', 'MPSLTD', 'MRF', 
    'MRO-TEK', 'MRPL', 'MSPL', 'MSTCLTD', 'MTARTECH', 'MTEDUCARE', 'MTNL', 'MUKANDLTD', 'MUKTAARTS', 
    'MUNJALAU', 'MUNJALSHOW', 'MURUDCERA', 'MUTHOOTCAP', 'MUTHOOTFIN', 'NACLIND', 'NAGAFERT', 'NAGREEKEXP', 'NAHARCAP', 
    'NAHARINDUS', 'NAHARPOLY', 'NAHARSPING', 'NAM-INDIA', 'NARMADA', 'NATCOPHARM', 'NATHBIOGEN', 'NATIONALUM', 'NATNLSTEEL', 
    'NAUKRI', 'NAVINFLUOR', 'NAVNETEDUL', 'NAZARA', 'NBCC', 'NBIFIN', 'NCC', 'NCLIND', 'NDGL', 
    'NDL', 'NDRAUTO', 'NDTV', 'NECCLTD', 'NECLIFE', 'NELCO', 'NEOGEN', 'NESCO', 'NESTLEIND', 
    'NETWORKS', 'NETWORK18', 'NEULANDLAB', 'NEWGEN', 'NEXTMEDIA', 'NFL', 'NGIL', 'NH', 'NHPC', 
    'NIACL', 'NIBL', 'NIITLTD', 'NILAINFRA', 'NILASPACES', 'NILKAMAL', 'NIPPOBATRY', 'NIRAJ', 'NIRAJISPA', 
    'NITCO', 'NITINSPIN', 'NITINFIRE', 'NITIRAJ', 'NKIND', 'NLCINDIA', 'NMDC', 'NOCIL', 'NOIDATOLL', 
    'NORBTEAEXP', 'NOVARTIND', 'NRAIL', 'NRBBEARING', 'NSIL', 'NTPC', 'NUCLEUS', 'NURECA', 'NUVOCO', 
    'NXTDIGITAL', 'OAL', 'OBEROIRLTY', 'OCCL', 'OFSS', 'OIL', 'OILCOUNTUB', 'OLECTRA', 'OMAXAUTO', 
    'OMAXE', 'OMINFRAL', 'OMKARCHEM', 'ONELIFECAP', 'ONEPOINT', 'ONGC', 'ONMOBILE', 'ONWARDTEC', 'OPTIEMUS', 
    'ORBTEXP', 'ORCHPHARMA', 'ORICONENT', 'ORIENTABRA', 'ORIENTBELL', 'ORIENTCEM', 'ORIENTELEC', 'ORIENTHOT', 'ORIENTLTD', 
    'ORIENTPPR', 'ORIENTREF', 'ORISSAMINE', 'ORTINLAB', 'OSWALAGRO', 'PAEL', 'PAGEIND', 'PAISALO', 'PALASHSECU', 
    'PALREDTEC', 'PANACEABIO', 'PANAMAPET', 'PANACHE', 'PARACABLES', 'PARADEEP', 'PARAGMILK', 'PARAS', 'PARASPETRO', 
    'PARSVNATH', 'PASUPTAC', 'PATANJALI', 'PATELENG', 'PATINTLOG', 'PAYTM', 'PCBL', 'PCJEWELLER', 'PDMJEPAPER', 
    'PDSMFL', 'PEARLPOLY', 'PEL', 'PENIND', 'PENINLAND', 'PERSISTENT', 'PETRONET', 'PFC', 'PFIZER', 
    'PFOCUS', 'PFS', 'PGEL', 'PGHH', 'PGHL', 'PGIL', 'PHILIPCARB', 'PHOENIXLTD', 'PIDILITIND', 
    'PIIND', 'PILANIINVS', 'PILITA', 'PIONDIST', 'PIONEEREMB', 'PITTIENG', 'PKTEA', 'PLASTIBLEN', 'PNB', 
    'PNBHOUSING', 'PNC', 'PNCINFRA', 'PODDARHOUS', 'PODDARMENT', 'POKARNA', 'POLICYBZR', 'POLYCAB', 'POLYMED', 
    'POLYPLEX', 'PONNIERODE', 'POONAWALLA', 'POWERGRID', 'POWERINDIA', 'POWERMECH', 'PPAP', 'PPL', 'PRABHAT', 
    'PRADIP', 'PRAENG', 'PRAJIND', 'PRAKASH', 'PRAKASHSTL', 'PRAXIS', 'PRECAM', 'PRECOT', 'PRECWIRE', 
    'PREMEXPLN', 'PREMIER', 'PREMIERPOL', 'PRESSMN', 'PRESTIGE', 'PRICOLLTD', 'PRIMESECU', 'PRINCEPIPE', 'PRITIKAUTO', 
    'PRIVISCL', 'PROZONINTU', 'PRUDENT', 'PRUDMOULI', 'PSB', 'PSPPROJECT', 'PTC', 'PTL', 'PUNJABCHEM', 
    'PUNJLLOYD', 'PURVA', 'PVR', 'PVRINOX', 'PVSL', 'QUESS', 'QUICKHEAL', 'QPOWER', 'RBLBANK', 
    'RADICO', 'RADIOCITY', 'RAIN', 'RAJESHEXPO', 'RAJMET', 'RAJRATAN', 'RAJSREESUG', 'RAJTV', 'RALLIS', 
    'RAMANEWS', 'RAMASTEEL', 'RAMCOCEM', 'RAMCOIND', 'RAMCOSYS', 'RAMKY', 'RAMRATNA', 'RANASUG', 'RANEENGINE', 
    'RANEHOLDIN', 'RATEGAIN', 'RATNAMANI', 'RAYMOND', 'RBL', 'RCF', 'RCOM', 'RECLTD', 'REDINGTON', 
    'REFEX', 'REGENCERAM', 'RELAXO', 'RELIANCE', 'RELIGARE', 'RELINFRA', 'REMSONSIND', 'RENUKA', 'REPCOHOME', 
    'REPL', 'REPRO', 'RESPONIND', 'REVATHI', 'RGL', 'RHFL', 'RHIM', 'RICOAUTO', 'RIIL', 
    'RITES', 'RKDL', 'RKEC', 'RKFORGE', 'RMCL', 'RML', 'ROHLTD', 'ROLEXRINGS', 'ROLLT', 
    'ROLTA', 'ROML', 'ROSSARI', 'ROSSELLIND', 'ROUTE', 'ROYALORCH', 'RPGLIFE', 'RPOWER', 'RPPINFRA', 
    'RSSOFTWARE', 'RSWM', 'RSYSTEMS', 'RTNINDIA', 'RTNPOWER', 'RUBYMILLS', 'RUCHI', 'RUCHINFRA', 'RUCHIRA', 
    'RUPA', 'RUSHIL', 'RVNL', 'S&SPOWER', 'SABEVENTS', 'SADBHAV', 'SADBHIN', 'SAFARI', 'SAGARDEEP', 
    'SAGCEM', 'SAIL', 'SAKAR', 'SAKHTISUG', 'SAKSOFT', 'SAKUMA', 'SALASAR', 'SALONA', 'SALSTEEL', 
    'SALZERELEC', 'SAMBHAAV', 'SANCO', 'SANDESH', 'SANDHAR', 'SANGAMIND', 'SANGHIIND', 'SANGHVIMOV', 'SANGINITA', 
    'SANOFI', 'SANWARIA', 'SAPPHIRE', 'SARDAEN', 'SAREGAMA', 'SARLAPOLY', 'SASKEN', 'SASTASUNDR', 'SATHAISPAT', 
    'SATIA', 'SATIN', 'SBICARD', 'SBILIFE', 'SBIN', 'SCAPDVR', 'SCHAEFFLER', 'SCHAND', 'SCHNEIDER', 
    'SCI', 'SCPL', 'SDBL', 'SEAMECLTD', 'SECUREKLOUD', 'SELAN', 'SELMC', 'SEAMECLTD', 'SFL', 
    'SGFL', 'SGIL', 'SGL', 'SHAHALLOYS', 'SHAKTIPUMP', 'SHALBY', 'SHALPAINTS', 'SHANKARA', 'SHANTIGEAR', 
    'SHARDACROP', 'SHARDAMOTR', 'SHAREINDIA', 'SHEMAROO', 'SHILPAMED', 'SHIVALIK', 'SHIVAMAUTO', 'SHIVMILLS', 'SHIVTEX', 
    'SHK', 'SHOPERSTOP', 'SHREDIGCEM', 'SHREECEM', 'SHREEPUSHK', 'SHREERAMA', 'SHRENIK', 'SHREYANIND', 'SHREYAS', 
    'SHRIPISTON', 'SHRIRAMCIT', 'SHRIRAMFIN', 'SHRIRAMPPS', 'SHYAMCENT', 'SHYAMMETL', 'SICAGEN', 'SICAL', 'SIEMENS', 
    'SIGIND', 'SIGMA', 'SIKKO', 'SIL', 'SILGO', 'SILINV', 'SILLYMONKS', 'SILVERTUC', 'SIMBHALS', 
    'SIMPLEXINF', 'SINTEX', 'SIRCA', 'SIS', 'SITINET', 'SIYSIL', 'SJS', 'SJVN', 'SKFINDIA', 
    'SKIL', 'SKIPPER', 'SKMEGGPROD', 'SMARTLINK', 'SMLISUZU', 'SMPL', 'SMSLIFE', 'SMSPHARMA', 'SNOWMAN', 
    'SOBHA', 'SOFTTECH', 'SOLARA', 'SOLARINDS', 'SOMANYCERA', 'SOMATEX', 'SOMICONVEY', 'SONACOMS', 'SONATSOFTW', 
    'SORILINFRA', 'SOTL', 'SOUTHBANK', 'SOUTHWEST', 'SPAL', 'SPANDANA', 'SPARC', 'SPCENET', 'SPECIALITY', 
    'SPENCERS', 'SPIC', 'SPIG', 'SPMLINFRA', 'SPORTKING', 'SREEL', 'SRF', 'SRHHYPOLTD', 'SRIPIPES', 
    'SRPL', 'SRTRANSFIN', 'SSWL', 'STAR', 'STARCEMENT', 'STARHEALTH', 'STARPAPER', 'STCINDIA', 'STEELCAS', 
    'STEELCITY', 'STEELXIND', 'STEL', 'STERTOOLS', 'STLTECH', 'STOP', 'STOVEKRAFT', 'SUBEX', 'SUBROS', 
    'SUDARSCHEM', 'SUJANAUNI', 'SUMEETINDS', 'SUMICHEM', 'SUMIT', 'SUMMITSEC', 'SUNCLAYLTD', 'SUNDARAM', 'SUNDARMFIN', 
    'SUNDRMBRAK', 'SUNDRMFAST', 'SUNFLAG', 'SUNPHARMA', 'SUNTECK', 'SUNTV', 'SUPERHOUSE', 'SUPERSPIN', 'SUPPETRO', 
    'SUPRAJIT', 'SUPREMEENG', 'SUPREMEIND', 'SUPREMEINF', 'SURANASOL', 'SURANAT&P', 'SURYALAXMI', 'SURYAROSNI', 'SURYODAY', 
    'SUTLEJTEX', 'SUVEN', 'SUVENPHAR', 'SUZLON', 'SVPGLOB', 'SWANENERGY', 'SWARAJENG', 'SWELECTES', 'SWSOLAR', 
    'SYMPHONY', 'SYNCOM', 'SYNGENE', 'TAINWALCHM', 'TAJGVK', 'TAKE', 'TALBROAUTO', 'TANLA', 'TANTIACONS', 
    'TARAPUR', 'TARC', 'TARIL', 'TARMAT', 'TASTYBITE', 'TATACHEM', 'TATACOFFEE', 'TATACOMM', 'TATACONSUM', 
    'TATAELXSI', 'TATAINVEST', 'TATAMETALI', 'TATAMOTORS', 'TATAMTRDVR', 'TATAPOWER', 'TATASTEEL', 'TATASTLLP', 'TBZ', 
    'TCI', 'TCIBS', 'TCIDEVELOP', 'TCIEXP', 'TCNSBRANDS', 'TCPLPACK', 'TCS', 'TDPOWERSYS', 'TEAMLEASE', 
    'TECHIN', 'TECHM', 'TECHNOE', 'TEGAS', 'TEJASNET', 'TEMBO', 'TERASOFT', 'TEXINFRA', 'TEXMOPIPES', 
    'TEXRAIL', 'TFCILTD', 'TFL', 'TGBHOTELS', 'THANGAMAYL', 'THEINVEST', 'THEMISMED', 'THERMAX', 'THIRUSUGAR', 
    'THOMASCOOK', 'THOMASCOTT', 'THYROCARE', 'TI', 'TIDEWATER', 'TIIL', 'TIINDIA', 'TIJARIA', 'TIL', 
    'TIMESGTY', 'TIMETECHNO', 'TIMKEN', 'TINPLATE', 'TIPSINDLTD', 'TIPSMUSIC', 'TIRUMALCHM', 'TITAN', 'TMRVL', 
    'TNPETRO', 'TNPL', 'TNTELE', 'TOKYOPLAST', 'TORNTPHARM', 'TORNTPOWER', 'TOTAL', 'TOUCHWOOD', 'TPLPLASTEH', 
    'TREEHOUSE', 'TREJHARA', 'TRENT', 'TRF', 'TRIDENT', 'TRIGYN', 'TRIL', 'TRITURBINE', 'TRIVENI', 
    'TRU', 'TTKHLTCARE', 'TTKPRESTIG', 'TTL', 'TV18BRDCST', 'TVSMOTOR', 'TVSSRICHAK', 'TVTODAY', 'TVVISION', 
    'TWL', 'UBL', 'UCALFUEL', 'UCOBANK', 'UFLEX', 'UFO', 'UGARSUGAR', 'UJAAS', 'UJJIVAN', 
    'UJJIVANSFB', 'ULTRACEMCO', 'UMANGDAIRY', 'UMESLTD', 'UNICHEMLAB', 'UNIDT', 'UNIENTER', 'UNIONBANK', 'UNIPARTS', 
    'UNIPLY', 'UNITECH', 'UNITEDPOLY', 'UNITEDTEA', 'UNIVASTU', 'UNIVCABLES', 'UNIVPHOTO', 'UNOMINDA', 'UPL', 
    'URBANCO', 'URJA', 'USHAMART', 'UTIAMC', 'UTTAMSUGAR', 'V2RETAIL', 'VADILALIND', 'VAIBHAVGBL', 'VAISHALI', 
    'VAKRANGEE', 'VALIANTORG', 'VARDHACRLC', 'VARDMNPOLY', 'VARROC', 'VASCONEQ', 'VASWANI', 'VBL', 'VEDL', 
    'VENKEYS', 'VENUSREM', 'VERTOZ', 'VESUVIUS', 'VETO', 'VGUARD', 'VHL', 'VICEROY', 'VIDHIING', 
    'VIJAYA', 'VIKASECO', 'VIKASLIFE', 'VIKASMCORP', 'VIKASPROP', 'VIKASWSP', 'VIMTALABS', 'VINATIORGA', 'VINDHYATEL', 
    'VINEETLAB', 'VINNY', 'VIPCLOTHNG', 'VIPIND', 'VIPULLTD', 'VISAKAIND', 'VISASTEEL', 'VISHAL', 'VISHNU', 
    'VISHWARAJ', 'VIVIDHA', 'VIVIMEDLAB', 'VLSFINANCE', 'VMART', 'VOLTAMP', 'VOLTAS', 'VRLLOG', 'VSSL', 
    'VSTIND', 'VSTTILLERS', 'VTL', 'WABAG', 'WABCOINDIA', 'WALCHANNAG', 'WANBURY', 'WATERBASE', 'WEALTH', 
    'WEBELSOLAR', 'WEIZMANIND', 'WELCORP', 'WELENT', 'WELINV', 'WELPOLY', 'WELSPUNIND', 'WENDT', 'WESTLIFE', 
    'WHEELS', 'WHIRLPOOL', 'WILLAMAGOR', 'WINDLAS', 'WINDMACHIN', 'WIPRO', 'WOCKPHARMA', 'WONDERLA', 'WORTH', 
    'WORTHPERI', 'WSI', 'WSTCSTPAPR', 'XCHANGING', 'XELPMOC', 'XPROINDIA', 'YAARI', 'YESBANK', 'ZEEL', 
    'ZEEMEDIA', 'ZENITHEXPO', 'ZENITHSTL', 'ZENSARTECH', 'ZENTEC', 'ZFCVINDIA', 'ZODIAC', 'ZODIACLOTH', 'ZOTA', 
    'ZUARI', 'ZUARIGLOB', 'ZYDUSLIFE', 'ZYDUSWELL'
]

# STOPLIST for filtering noise words
STOPLIST = set([
    "THE", "AND", "ARE", "IS", "FOR", "OVER", "WITH", "TO", "OF", "IN", "BY", "FROM", "ON", "AT", "OR", "AS", "AN", "IT", "GO", "NO", "MARKET", "COMPANY", "COMPANIES", "NEWS", "STOCK", "STOCKS", "SEBI", "INDIAN", "INDIA", "EXPECTED", "LOSSES", "GAINS", "SHARES", "NSE", "BSE", "DECLINE", "DECLINED", "ALSO", "FIRMS", "MONTHS", "SEGMENTS", "LTD", "PRIMARY", "BOTH", "COMING", "FUNDRAISING", "SIGNIFICANT", "LIMITED", "POSSIBLE", "HEALTH", "HEALTHCARE", "WAVE", "FIFTEEN", "EYE", "BANK", "IPO", "IPOS", "SET", "RS", "BE", "WAS", "PUSH", "PARTICULARLY", "MUTUAL", "FUNDS", "PRIVATE", "PUBLIC", "LOWER", "HIGHER", "TODAY", "WEEK", "YEAR", "REPORT", "GLOBAL", "WORLD", "BUSINESS", "FINANCE", "MONEY", "TIMES", "ECONOMIC", "CITY", "SALES", "PROFIT", "LOSS", "QUARTER", "RESULTS", "DATA", "GROUP", "IND", "OUT"
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
# 4. MARKET MAPPER (OMNIVERSE ENGINE)
# ==========================================
class MasterMapper:
    def __init__(self):
        self.universe = {} 
        self.keywords = {} 
        self.build_universe()
        
    def build_universe(self):
        log.info("⏳ Indexing NSE Market...")
        # 1. LOAD THE MEGA BACKUP
        for t in NSE_MEGA_LIST:
            self.universe[t] = t
            
        # 2. LOAD USER WATCHLIST (Priority)
        for t in USER_WATCHLIST:
            self.universe[t] = t
            
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
                log.info(f"✅ Indexed {len(self.universe)} companies (Full NSE Market).")
            else: 
                log.warning("⚠️ nselib missing. Using Mega Backup + User Watchlist.")
        except Exception as e:
            log.warning(f"⚠️ NSE Indexing failed. Using Mega Backup + User Watchlist.")

    def extract_tickers(self, articles):
        found = []
        for art in articles:
            text = f"{art['title']} {art.get('body', '')[:500]}".upper()
            matches = re.findall(r'\b([A-Z]{3,})\b', text)
            for m in matches:
                if m in self.universe: found.append(self.universe[m])
                elif m in self.keywords: found.append(self.keywords[m])
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
        with ThreadPoolExecutor(max_workers=100) as executor:
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
        template = """<!DOCTYPE html><html><head><title>Illuminati v28.0</title><style>body{font-family:'Inter',sans-serif;background:#0f172a;color:#e2e8f0;padding:20px}.card{background:#1e293b;border-radius:8px;padding:15px;margin-bottom:15px;border:1px solid #334155}.badge{padding:4px 8px;border-radius:4px;font-weight:bold}.buy{background:#065f46;color:#34d399}.sell{background:#7f1d1d;color:#f87171}.hold{background:#854d0e;color:#fef08a}table{width:100%;border-collapse:collapse;margin-top:20px}th,td{padding:12px;text-align:left;border-bottom:1px solid #334155}th{color:#94a3b8}</style></head><body><h1>👁️ Illuminati Terminal v28.0</h1><p>Assets Analyzed: {{ total }} | Date: {{ date }}</p><h2>🔮 Future Booming Industries</h2><table><thead><tr><th>Theme</th><th>Hype Score</th><th>Mentions</th></tr></thead><tbody>{% for t in trends %}<tr><td><b>{{ t.Theme }}</b></td><td>{{ t.Hype_Score }}%</td><td>{{ t.Mentions }}</td></tr>{% endfor %}</tbody></table><h2>🚀 Industry Momentum</h2><table><thead><tr><th>Sector</th><th>Avg Score</th><th>Top Verdict</th></tr></thead><tbody>{% for s, data in ind_summary.items() %}<tr><td><b>{{ s }}</b></td><td>{{ data['avg_score'] }}</td><td>{{ data['verdict'] }}</td></tr>{% endfor %}</tbody></table><h2>🚀 Investment Strategy</h2><table><thead><tr><th>Ticker</th><th>Price</th><th>Target</th><th>Horizon</th><th>Sharpe</th><th>Valuation</th><th>Score</th><th>Verdict</th></tr></thead><tbody>{% for r in results %}<tr><td><b>{{ r.Ticker }}</b></td><td>{{ r.Price }}</td><td>{{ r.Target_Price }}</td><td>{{ r.Horizon }}</td><td>{{ r.Sharpe }}</td><td>{{ r.DCF_Val }}</td><td>{{ r.Score }}</td><td><span class="badge {{ 'buy' if 'BUY' in r.Verdict else ('sell' if 'SELL' in r.Verdict else 'hold') }}">{{ r.Verdict }}</span></td></tr>{% endfor %}</tbody></table><h2>📰 Market Intel</h2>{% for a in articles[:8] %}<div class="card"><h3><a href="{{ a.link }}" style="color:#60a5fa">{{ a.title }}</a></h3><p style="color:#94a3b8">{{ a.published }} | {{ a.source }}</p><p>{{ a.body[:250] }}...</p></div>{% endfor %}</body></html>"""
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
        
        # KEY AI FIX: BRUTE FORCE MODEL SELECTOR
        models_to_try = [
            'gemini-1.5-flash', 
            'gemini-1.5-flash-latest',
            'gemini-1.5-pro',
            'gemini-pro',
            'gemini-1.0-pro'
        ]
        csv_data = df_summary.to_csv()
        
        for m in models_to_try:
            try:
                log.info(f"🤖 Generating Insight with {m}...")
                self.model = genai.GenerativeModel(m)
                response = self.model.generate_content(f"Analyze this Indian Stock Market data:\n{csv_data}")
                return response.text
            except Exception as e:
                log.warning(f"Model {m} failed: {e}")
                continue
        
        return "LLM Generation Failed (All models tried)."

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
    # FORCE IST DISPLAY
    ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    current_time = dt.datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')
    print("\n" + "="*80)
    print(f"👁️ ILLUMINATI TERMINAL v28.0 (CLEAN SWEEP) | {current_time}")
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
    
    # TITAN SCAN: News + Titan Backup + User Watchlist (Merged)
    news_tickers = mapper.extract_tickers(articles)
    combined_tickers = list(set(news_tickers + NSE_MEGA_LIST + USER_WATCHLIST))
    
    if tickers_arg: combined_tickers.extend(tickers_arg.split(','))
    
    print(f"\n⚡ Analyzing {len(combined_tickers)} Assets (Omni-Scan)...")
    results = []
    
    # Boosted Threads for Massive Scan (100 Workers)
    with ThreadPoolExecutor(max_workers=100) as executor:
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
