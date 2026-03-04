"""
Fetch stock splits and dividends from yfinance and upsert into
the corporate_actions table.
"""
import logging
import time
from typing import Optional

import pandas as pd
import sqlalchemy as sa
import yfinance as yf

from db.schema import get_engine

logger = logging.getLogger(__name__)


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


def _upsert_corporate_actions(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO corporate_actions
                    (ticker, action_date, action_type, ratio, amount, notes)
                VALUES
                    (:ticker, :action_date, :action_type, :ratio, :amount, :notes)
                """
            ),
            rows,
        )
    return len(rows)


def download_corporate_actions(
    tickers: list[str],
    engine: Optional[sa.Engine] = None,
    delay: float = 0.3,
) -> tuple[int, list[str]]:
    """
    Fetch splits and dividends from yfinance for each ticker and upsert
    into the corporate_actions table.

    Splits are stored with action_type='split' and ratio=float(val).
    Dividends are stored with action_type='dividend' and amount=float(val).

    Returns (total_rows_upserted, failed_tickers).
    """
    if engine is None:
        engine = get_engine()

    total_rows = 0
    failed: list[str] = []

    for ticker in tickers:
        try:
            yf_ticker = yf.Ticker(ticker)
            rows: list[dict] = []

            # --- Splits ---
            splits = yf_ticker.splits
            if splits is not None and not splits.empty:
                for idx_date, val in splits.items():
                    ratio = _safe(val)
                    if ratio is None:
                        continue
                    action_date = pd.Timestamp(idx_date).date().isoformat()
                    rows.append({
                        "ticker": ticker,
                        "action_date": action_date,
                        "action_type": "split",
                        "ratio": ratio,
                        "amount": None,
                        "notes": None,
                    })

            # --- Dividends ---
            dividends = yf_ticker.dividends
            if dividends is not None and not dividends.empty:
                for idx_date, val in dividends.items():
                    amount = _safe(val)
                    if amount is None:
                        continue
                    action_date = pd.Timestamp(idx_date).date().isoformat()
                    rows.append({
                        "ticker": ticker,
                        "action_date": action_date,
                        "action_type": "dividend",
                        "ratio": None,
                        "amount": amount,
                        "notes": None,
                    })

            if rows:
                inserted = _upsert_corporate_actions(engine, rows)
                total_rows += inserted
                logger.debug(f"{ticker}: {inserted} corporate action rows upserted")
            else:
                logger.debug(f"{ticker}: no corporate actions found")

        except Exception as exc:
            logger.error(f"Failed to process corporate actions for {ticker}: {exc}")
            failed.append(ticker)
        finally:
            time.sleep(delay)

    return total_rows, failed
