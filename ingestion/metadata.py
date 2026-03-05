"""
Fetch security metadata from yfinance Ticker.info and upsert into
the security_metadata table. Also computes avg_volume_30d and
avg_dollar_vol_30d via SQL over stock_prices.
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


def _safe(val) -> Optional[float]:
    """Return float(val) or None for missing/NaN values."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _upsert_metadata(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO security_metadata
                    (ticker, shares_outstanding, float_shares, market_cap,
                     enterprise_value, avg_volume_30d, avg_dollar_vol_30d, updated_at)
                VALUES
                    (:ticker, :shares_outstanding, :float_shares, :market_cap,
                     :enterprise_value, :avg_volume_30d, :avg_dollar_vol_30d, :updated_at)
                """
            ),
            rows,
        )
    return len(rows)


def _compute_volume_averages(engine: sa.Engine, ticker: str) -> tuple[Optional[float], Optional[float]]:
    """Query stock_prices for the 30-day avg volume and avg dollar volume."""
    with engine.connect() as conn:
        row = conn.execute(
            sa.text(
                """
                SELECT AVG(volume), AVG(dollar_volume)
                FROM stock_prices
                WHERE ticker = :ticker
                  AND date >= date('now', '-30 days')
                """
            ),
            {"ticker": ticker},
        ).fetchone()
    if row is None:
        return None, None
    avg_vol = float(row[0]) if row[0] is not None else None
    avg_dollar_vol = float(row[1]) if row[1] is not None else None
    return avg_vol, avg_dollar_vol


def _fetch_one_metadata(ticker: str, engine: sa.Engine, delay: float) -> tuple[str, bool]:
    with _SEMAPHORE:
        time.sleep(delay)
        try:
            yf_ticker = yf.Ticker(ticker)
            info = yf_ticker.info

            if not info:
                logger.debug(f"{ticker}: no info data returned")
                return ticker, True

            avg_vol, avg_dollar_vol = _compute_volume_averages(engine, ticker)

            row = {
                "ticker": ticker,
                "shares_outstanding": _safe(info.get("sharesOutstanding")),
                "float_shares": _safe(info.get("floatShares")),
                "market_cap": _safe(info.get("marketCap")),
                "enterprise_value": _safe(info.get("enterpriseValue")),
                "avg_volume_30d": avg_vol,
                "avg_dollar_vol_30d": avg_dollar_vol,
                "updated_at": datetime.utcnow().isoformat(),
            }

            _upsert_metadata(engine, [row])
            logger.debug(f"{ticker}: security_metadata upserted")
            return ticker, True

        except Exception as exc:
            logger.error(f"Failed to process security metadata for {ticker}: {exc}")
            return ticker, False


def download_security_metadata(
    tickers: list[str],
    engine: Optional[sa.Engine] = None,
    delay: float = 0.1,
) -> tuple[int, list[str]]:
    """
    Fetch security metadata from yfinance Ticker.info for each ticker and
    upsert into the security_metadata table. Computes avg_volume_30d and
    avg_dollar_vol_30d from stock_prices.

    Returns (total_rows_upserted, failed_tickers).
    """
    if engine is None:
        engine = get_engine()

    failed: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_fetch_one_metadata, t, engine, delay): t for t in tickers}
        for fut in concurrent.futures.as_completed(futures):
            _, ok = fut.result()
            if not ok:
                failed.append(futures[fut])

    total_rows = len(tickers) - len(failed)
    return total_rows, failed
