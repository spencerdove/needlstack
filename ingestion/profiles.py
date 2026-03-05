"""
Fetch company profile data from yfinance Ticker.info and upsert into the
company_profiles table.

Keys pulled:
    longBusinessSummary  → description
    fullTimeEmployees    → employees
    website
    country
    city
    state
"""
import concurrent.futures
import logging
import threading
import time
from datetime import datetime
from typing import Optional

import pandas as pd
import sqlalchemy as sa
import yfinance as yf

from db.schema import get_engine

logger = logging.getLogger(__name__)

_SEMAPHORE = threading.BoundedSemaphore(8)


def _safe_str(val) -> Optional[str]:
    """Return stripped string or None for missing/NaN values."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    return s if s else None


def _safe_int(val) -> Optional[int]:
    """Return int or None for missing/NaN values."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _upsert_profiles(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO company_profiles
                    (ticker, description, employees, website, country, city, state, updated_at)
                VALUES
                    (:ticker, :description, :employees, :website, :country, :city, :state, :updated_at)
                """
            ),
            rows,
        )
    return len(rows)


def _fetch_one_profile(ticker: str, engine: sa.Engine, delay: float) -> tuple[str, bool]:
    with _SEMAPHORE:
        time.sleep(delay)
        try:
            yf_ticker = yf.Ticker(ticker)
            info = yf_ticker.info

            if not info:
                logger.debug(f"{ticker}: no info data returned")
                return ticker, True

            row = {
                "ticker": ticker,
                "description": _safe_str(info.get("longBusinessSummary")),
                "employees": _safe_int(info.get("fullTimeEmployees")),
                "website": _safe_str(info.get("website")),
                "country": _safe_str(info.get("country")),
                "city": _safe_str(info.get("city")),
                "state": _safe_str(info.get("state")),
                "updated_at": datetime.utcnow().isoformat(),
            }

            _upsert_profiles(engine, [row])
            logger.debug(f"{ticker}: company_profiles upserted")
            return ticker, True

        except Exception as exc:
            logger.error(f"Failed to process company profile for {ticker}: {exc}")
            return ticker, False


def download_company_profiles(
    tickers: list[str],
    engine: Optional[sa.Engine] = None,
    delay: float = 0.1,
) -> tuple[int, list[str]]:
    """
    Fetch company profile information from yfinance Ticker.info for each
    ticker and upsert into the company_profiles table.

    Returns (total_rows_upserted, failed_tickers).
    """
    if engine is None:
        engine = get_engine()

    failed: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_one_profile, t, engine, delay): t for t in tickers}
        for fut in concurrent.futures.as_completed(futures):
            _, ok = fut.result()
            if not ok:
                failed.append(futures[fut])

    total_rows = len(tickers) - len(failed)
    return total_rows, failed
