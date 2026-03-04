"""
Fetch index constituents from Wikipedia and upsert into the
index_constituents table.

Supports: NDX100, DOW30, SP400, SP600.
Uses a 7-day TTL file cache per index.
"""
import io
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import sqlalchemy as sa
from dotenv import load_dotenv

from db.schema import get_engine

load_dotenv()

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
CACHE_TTL_DAYS = 7

INDEX_CONFIGS: dict[str, dict] = {
    "NDX100": {
        "url": "https://en.wikipedia.org/wiki/Nasdaq-100",
        "table_id": "constituents",
        "ticker_col": "Ticker",
        "cache_file": "data/ndx100_constituents.csv",
    },
    "DOW30": {
        "url": "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average",
        "table_id": "constituents",
        "ticker_col": "Symbol",
        "cache_file": "data/dow30_constituents.csv",
    },
    "SP400": {
        "url": "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
        "table_id": "constituents",
        "ticker_col": "Symbol",
        "cache_file": "data/sp400_constituents.csv",
    },
    "SP600": {
        "url": "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies",
        "table_id": "constituents",
        "ticker_col": "Symbol",
        "cache_file": "data/sp600_constituents.csv",
    },
}


def _cache_is_fresh(cache_path: Path) -> bool:
    if not cache_path.exists():
        return False
    mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=CACHE_TTL_DAYS)


def _fetch_index_tickers(index_id: str, config: dict, force_refresh: bool = False) -> list[str]:
    """
    Fetch tickers for a single index from Wikipedia, using a TTL cache.

    Returns a list of ticker strings suitable for yfinance.
    """
    cache_path = Path(config["cache_file"])
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if not force_refresh and _cache_is_fresh(cache_path):
        logger.info(f"{index_id}: loading constituents from cache ({cache_path})")
        df = pd.read_csv(cache_path)
        return df["ticker"].tolist()

    logger.info(f"{index_id}: fetching constituents from Wikipedia...")
    headers = {"User-Agent": "Mozilla/5.0 (compatible; needlstack/1.0)"}
    resp = requests.get(config["url"], headers=headers, timeout=15)
    resp.raise_for_status()

    tables = pd.read_html(io.StringIO(resp.text), attrs={"id": config["table_id"]})
    df = tables[0]

    ticker_col = config["ticker_col"]
    if ticker_col not in df.columns:
        raise ValueError(
            f"{index_id}: expected column '{ticker_col}' not found. "
            f"Available columns: {list(df.columns)}"
        )

    tickers = (
        df[ticker_col]
        .dropna()
        .astype(str)
        .str.strip()
        .str.replace(".", "-", regex=False)
        .tolist()
    )

    out_df = pd.DataFrame({"ticker": tickers})
    out_df.to_csv(cache_path, index=False)
    logger.info(f"{index_id}: cached {len(tickers)} tickers to {cache_path}")

    return tickers


def get_index_tickers(index_id: str, force_refresh: bool = False) -> list[str]:
    """
    Return a list of ticker strings for the given index_id.

    Supported index IDs: NDX100, DOW30, SP400, SP600.
    Uses a local CSV cache with a 7-day TTL.
    """
    index_id = index_id.upper()
    if index_id not in INDEX_CONFIGS:
        raise ValueError(
            f"Unknown index_id '{index_id}'. "
            f"Supported: {list(INDEX_CONFIGS.keys())}"
        )
    config = INDEX_CONFIGS[index_id]
    return _fetch_index_tickers(index_id, config, force_refresh=force_refresh)


def upsert_index_constituents(
    index_id: str,
    tickers: list[str],
    engine: Optional[sa.Engine] = None,
) -> int:
    """
    Upsert tickers into the index_constituents table for the given index_id.

    Sets added_date = today and removed_date = NULL for all provided tickers.
    Returns the number of rows upserted.
    """
    if engine is None:
        engine = get_engine()

    if not tickers:
        return 0

    today = date.today().isoformat()
    rows = [
        {
            "index_id": index_id,
            "ticker": ticker,
            "added_date": today,
            "removed_date": None,
            "weight": None,
        }
        for ticker in tickers
    ]

    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO index_constituents
                    (index_id, ticker, added_date, removed_date, weight)
                VALUES
                    (:index_id, :ticker, :added_date, :removed_date, :weight)
                """
            ),
            rows,
        )

    logger.info(f"{index_id}: upserted {len(rows)} constituent rows")
    return len(rows)
