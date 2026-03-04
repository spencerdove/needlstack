"""
Unified ticker universe source — replaces tickers.py as source of truth for
expanded coverage beyond S&P 500.

Sources:
1. NASDAQ Trader pipe-delimited files (nasdaqlisted.txt, otherlisted.txt)
2. Hardcoded macro instruments (~16 yfinance symbols)

Cache: data/universe.csv with a 7-day TTL.
"""
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import sqlalchemy as sa

from db.schema import get_engine

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
CACHE_FILE = DATA_DIR / "universe.csv"
CACHE_TTL_DAYS = 7

HEADERS = {"User-Agent": "needlstack/1.0 (contact@needlstack.com)"}

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
NASDAQ_OTHER_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

# Regex to exclude warrants (W suffix), rights (R suffix), preferred shares, and
# any symbol containing a dot (typically preferred/unit shares in NASDAQ files).
_EXCLUDE_PATTERN = re.compile(r"[A-Z]+[WR]$|.*\..*")

MACRO_SYMBOLS = [
    "^GSPC",
    "^NDX",
    "^DJI",
    "^VIX",
    "^TNX",
    "DX-Y.NYB",
    "GC=F",
    "CL=F",
    "NG=F",
    "SI=F",
    "EURUSD=X",
    "GBPUSD=X",
    "JPYUSD=X",
    "USDJPY=X",
    "BTC-USD",
    "ETH-USD",
]


def _cache_is_fresh() -> bool:
    if not CACHE_FILE.exists():
        return False
    mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=CACHE_TTL_DAYS)


def _macro_asset_type(symbol: str) -> str:
    """Infer asset_type from macro symbol conventions."""
    if symbol.startswith("^"):
        return "index"
    if symbol.endswith("=X"):
        return "fx"
    if symbol.endswith("=F"):
        return "commodity"
    # BTC-USD, ETH-USD fall through as 'equity' per spec (could be 'crypto' later)
    return "equity"


def _fetch_nasdaq_files() -> pd.DataFrame:
    """Download and parse both NASDAQ Trader pipe-delimited files."""
    frames: list[pd.DataFrame] = []

    # -- nasdaqlisted.txt --
    logger.info("Fetching nasdaqlisted.txt...")
    resp = requests.get(NASDAQ_LISTED_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    lines = resp.text.splitlines()
    # Last line is a file-creation timestamp footer — drop it
    data_lines = [ln for ln in lines if not ln.startswith("File Creation Time")]
    nasdaq_df = pd.read_csv(
        pd.io.common.StringIO("\n".join(data_lines)),
        sep="|",
        dtype=str,
    )
    # Columns: Symbol | Security Name | Market Category | Test Issue | Financial Status | Round Lot Size | ETF | NextShares
    nasdaq_df = nasdaq_df.rename(columns={
        "Symbol": "ticker",
        "Security Name": "company_name",
        "ETF": "etf_flag",
    })
    nasdaq_df["exchange"] = "NASDAQ"
    # Filter test issues
    if "Test Issue" in nasdaq_df.columns:
        nasdaq_df = nasdaq_df[nasdaq_df["Test Issue"] != "Y"]
    frames.append(nasdaq_df[["ticker", "company_name", "exchange", "etf_flag"]].copy())

    # -- otherlisted.txt --
    logger.info("Fetching otherlisted.txt...")
    resp2 = requests.get(NASDAQ_OTHER_URL, headers=HEADERS, timeout=30)
    resp2.raise_for_status()
    lines2 = resp2.text.splitlines()
    data_lines2 = [ln for ln in lines2 if not ln.startswith("File Creation Time")]
    other_df = pd.read_csv(
        pd.io.common.StringIO("\n".join(data_lines2)),
        sep="|",
        dtype=str,
    )
    # Columns: ACT Symbol | Security Name | Exchange | CQS Symbol | ETF | Round Lot Size | Test Issue | NASDAQ Symbol
    other_df = other_df.rename(columns={
        "ACT Symbol": "ticker",
        "Security Name": "company_name",
        "Exchange": "exchange",
        "ETF": "etf_flag",
    })
    if "Test Issue" in other_df.columns:
        other_df = other_df[other_df["Test Issue"] != "Y"]
    frames.append(other_df[["ticker", "company_name", "exchange", "etf_flag"]].copy())

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["ticker"])
    combined["ticker"] = combined["ticker"].str.strip()
    combined = combined[combined["ticker"] != ""]

    # Exclude warrants, rights, preferred, dotted symbols
    combined = combined[~combined["ticker"].str.match(_EXCLUDE_PATTERN)]

    # Determine asset_type
    combined["asset_type"] = combined["etf_flag"].apply(
        lambda v: "etf" if str(v).strip().upper() == "Y" else "equity"
    )

    return combined[["ticker", "company_name", "exchange", "asset_type"]].copy()


def _build_macro_df() -> pd.DataFrame:
    rows = []
    for sym in MACRO_SYMBOLS:
        rows.append({
            "ticker": sym,
            "company_name": None,
            "exchange": None,
            "asset_type": _macro_asset_type(sym),
        })
    return pd.DataFrame(rows)


def refresh_universe(engine: Optional[sa.Engine] = None) -> tuple[int, list[str]]:
    """
    Download NASDAQ Trader files, merge with hardcoded macro list, and upsert
    everything into the tickers table.

    Returns (rows_upserted, failed_tickers).
    """
    if engine is None:
        engine = get_engine()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    failed: list[str] = []

    try:
        equity_df = _fetch_nasdaq_files()
    except Exception as exc:
        logger.error(f"Failed to fetch NASDAQ universe files: {exc}")
        equity_df = pd.DataFrame(columns=["ticker", "company_name", "exchange", "asset_type"])
        failed.append("__nasdaq_fetch__")

    macro_df = _build_macro_df()

    # Combine; macro symbols take precedence via drop_duplicates keep='last'
    combined = pd.concat([equity_df, macro_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["ticker"], keep="last")

    # Cache to CSV
    combined.to_csv(CACHE_FILE, index=False)
    logger.info(f"Cached {len(combined)} universe rows to {CACHE_FILE}")

    today = datetime.utcnow().date().isoformat()

    rows = []
    for _, row in combined.iterrows():
        rows.append({
            "ticker": row["ticker"],
            "company_name": row["company_name"] if pd.notna(row.get("company_name")) else None,
            "exchange": row["exchange"] if pd.notna(row.get("exchange")) else None,
            "asset_type": row["asset_type"],
            "is_active": 1,
            "first_seen_date": today,
        })

    if not rows:
        logger.warning("No rows to upsert into tickers.")
        return 0, failed

    upserted = 0
    with engine.begin() as conn:
        for row in rows:
            try:
                conn.execute(
                    sa.text(
                        """
                        INSERT INTO tickers (ticker, company_name, exchange, asset_type, is_active, first_seen_date)
                        VALUES (:ticker, :company_name, :exchange, :asset_type, :is_active, :first_seen_date)
                        ON CONFLICT(ticker) DO UPDATE SET
                            company_name  = COALESCE(excluded.company_name, tickers.company_name),
                            exchange      = COALESCE(excluded.exchange, tickers.exchange),
                            asset_type    = excluded.asset_type,
                            is_active     = 1
                        """
                    ),
                    row,
                )
                upserted += 1
            except Exception as exc:
                logger.warning(f"Upsert failed for {row['ticker']}: {exc}")
                failed.append(row["ticker"])

    logger.info(f"refresh_universe: upserted {upserted} rows, {len(failed)} failures")
    return upserted, failed


def get_active_tickers(asset_types: list[str] | None = None, engine: Optional[sa.Engine] = None) -> list[str]:
    """
    Return active tickers from the DB tickers table (is_active=1).

    Falls back to the universe.csv cache if the DB table is empty.

    Parameters
    ----------
    asset_types : list of str or None
        Filter by asset_type values e.g. ['equity', 'etf']. None = all.
    engine : SQLAlchemy engine (optional).
    """
    if engine is None:
        engine = get_engine()

    try:
        if asset_types is not None:
            placeholders = ", ".join(f"'{t}'" for t in asset_types)
            query = sa.text(
                f"SELECT ticker FROM tickers WHERE is_active = 1 AND asset_type IN ({placeholders})"
            )
        else:
            query = sa.text("SELECT ticker FROM tickers WHERE is_active = 1")

        with engine.connect() as conn:
            rows = conn.execute(query).fetchall()

        tickers = [r[0] for r in rows]
        if tickers:
            return tickers
        logger.info("DB tickers table empty — falling back to universe.csv cache")
    except Exception as exc:
        logger.warning(f"DB query failed ({exc}), falling back to universe.csv")

    # Fallback: read from CSV cache
    if not CACHE_FILE.exists():
        logger.warning("universe.csv not found — returning empty list")
        return []

    df = pd.read_csv(CACHE_FILE, dtype=str)
    if asset_types is not None:
        df = df[df["asset_type"].isin(asset_types)]
    return df["ticker"].dropna().tolist()
