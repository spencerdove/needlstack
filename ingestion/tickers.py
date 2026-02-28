"""
Fetch and cache the S&P 500 ticker list from Wikipedia.

Cache lives at data/sp500_tickers.csv and is refreshed if >7 days old.
"""
import logging
from datetime import datetime, timedelta
from pathlib import Path

import io

import pandas as pd
import requests
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
CACHE_FILE = DATA_DIR / "sp500_tickers.csv"
CACHE_TTL_DAYS = 7
WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def _cache_is_fresh() -> bool:
    if not CACHE_FILE.exists():
        return False
    mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=CACHE_TTL_DAYS)


def _fetch_from_wikipedia() -> pd.DataFrame:
    logger.info("Fetching S&P 500 list from Wikipedia...")
    headers = {"User-Agent": "Mozilla/5.0 (compatible; needlstack/1.0)"}
    resp = requests.get(WIKIPEDIA_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text), attrs={"id": "constituents"})
    df = tables[0]
    df = df.rename(columns={
        "Symbol": "ticker",
        "Security": "company_name",
        "GICS Sector": "sector",
        "GICS Sub-Industry": "industry",
        "Date added": "added_date",
    })
    df = df[["ticker", "company_name", "sector", "industry", "added_date"]].copy()
    # yfinance uses hyphens, not dots (BRK.B → BRK-B)
    df["ticker"] = df["ticker"].str.replace(".", "-", regex=False)
    return df


def get_sp500_tickers(force_refresh: bool = False) -> list[str]:
    """
    Return a list of S&P 500 ticker strings, suitable for yfinance.

    Uses a local CSV cache; re-fetches from Wikipedia if the cache is stale.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not force_refresh and _cache_is_fresh():
        logger.info("Loading S&P 500 tickers from cache.")
        df = pd.read_csv(CACHE_FILE)
    else:
        df = _fetch_from_wikipedia()
        df.to_csv(CACHE_FILE, index=False)
        logger.info(f"Cached {len(df)} tickers to {CACHE_FILE}")

    return df["ticker"].tolist()


def get_sp500_dataframe(force_refresh: bool = False) -> pd.DataFrame:
    """
    Return the full S&P 500 DataFrame (ticker, company_name, sector, industry, added_date).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not force_refresh and _cache_is_fresh():
        return pd.read_csv(CACHE_FILE)

    df = _fetch_from_wikipedia()
    df.to_csv(CACHE_FILE, index=False)
    logger.info(f"Cached {len(df)} tickers to {CACHE_FILE}")
    return df
