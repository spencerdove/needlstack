"""
Download historical earnings surprises from yfinance and upsert into
the earnings_surprises table.
"""
import logging
import time
from typing import Optional

import pandas as pd
import sqlalchemy as sa
import yfinance as yf

from db.schema import get_engine

logger = logging.getLogger(__name__)


def _upsert_earnings(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO earnings_surprises
                    (ticker, earnings_date, eps_estimate, eps_actual, eps_surprise_pct)
                VALUES
                    (:ticker, :earnings_date, :eps_estimate, :eps_actual, :eps_surprise_pct)
                """
            ),
            rows,
        )
    return len(rows)


def download_earnings_surprises(
    tickers: list[str],
    engine: Optional[sa.Engine] = None,
    delay: float = 0.25,
) -> tuple[int, list[str]]:
    """
    Fetch earnings_dates from yfinance for each ticker and upsert into
    earnings_surprises table.

    Returns (total_rows_inserted, failed_tickers).
    """
    if engine is None:
        engine = get_engine()

    total_rows = 0
    failed: list[str] = []

    for ticker in tickers:
        try:
            yf_ticker = yf.Ticker(ticker)
            df = yf_ticker.earnings_dates

            if df is None or df.empty:
                logger.debug(f"{ticker}: no earnings_dates data")
                time.sleep(delay)
                continue

            # Filter rows where at least one of estimate or actual is non-null
            df = df[~(df["EPS Estimate"].isna() & df["Reported EPS"].isna())]
            if df.empty:
                time.sleep(delay)
                continue

            rows = []
            for idx_date, row in df.iterrows():
                def _safe(val):
                    if pd.isna(val):
                        return None
                    return float(val)

                earnings_date = pd.Timestamp(idx_date).date().isoformat()
                rows.append({
                    "ticker": ticker,
                    "earnings_date": earnings_date,
                    "eps_estimate": _safe(row.get("EPS Estimate")),
                    "eps_actual": _safe(row.get("Reported EPS")),
                    "eps_surprise_pct": _safe(row.get("Surprise(%)")),
                })

            inserted = _upsert_earnings(engine, rows)
            total_rows += inserted
            logger.debug(f"{ticker}: {inserted} earnings rows inserted")

        except Exception as exc:
            logger.error(f"Failed to process earnings for {ticker}: {exc}")
            failed.append(ticker)
        finally:
            time.sleep(delay)

    return total_rows, failed
