"""
Download OHLCV price data from yfinance and upsert into stock_prices table.
"""
import logging
from datetime import date, datetime
from typing import Optional

import pandas as pd
import sqlalchemy as sa
import yfinance as yf

from db.schema import get_engine, stock_prices_table, tickers_table

logger = logging.getLogger(__name__)

BATCH_SIZE = 100


def _upsert_prices(engine: sa.Engine, rows: list[dict]) -> int:
    """Insert or replace rows into stock_prices. Returns count inserted."""
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO stock_prices
                    (ticker, date, open, high, low, close, adj_close, volume)
                VALUES
                    (:ticker, :date, :open, :high, :low, :close, :adj_close, :volume)
                """
            ),
            rows,
        )
    return len(rows)


def _upsert_tickers(engine: sa.Engine, tickers_df: pd.DataFrame) -> None:
    """Insert or ignore ticker metadata rows."""
    rows = tickers_df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR IGNORE INTO tickers
                    (ticker, company_name, sector, industry, added_date)
                VALUES
                    (:ticker, :company_name, :sector, :industry, :added_date)
                """
            ),
            rows,
        )


def download_prices(
    tickers: list[str],
    start: str,
    end: Optional[str] = None,
    engine: Optional[sa.Engine] = None,
) -> tuple[int, list[str]]:
    """
    Download OHLCV data for *tickers* from *start* to *end* and upsert into DB.

    Returns (total_rows_inserted, failed_tickers).
    """
    if engine is None:
        engine = get_engine()

    if end is None:
        end = date.today().isoformat()

    total_inserted = 0
    failed: list[str] = []

    for batch_start in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[batch_start : batch_start + BATCH_SIZE]
        logger.info(
            f"Downloading batch {batch_start // BATCH_SIZE + 1} "
            f"({len(batch)} tickers, {start} → {end})"
        )

        try:
            raw = yf.download(
                batch,
                start=start,
                end=end,
                auto_adjust=False,
                progress=False,
                threads=True,
            )
        except Exception as exc:
            logger.error(f"Batch download failed: {exc}")
            failed.extend(batch)
            continue

        if raw.empty:
            logger.warning(f"No data returned for batch starting at index {batch_start}")
            continue

        rows = _flatten_download(raw, batch)
        inserted = _upsert_prices(engine, rows)
        total_inserted += inserted
        logger.info(f"  Inserted {inserted} rows from this batch.")

    return total_inserted, failed


def _flatten_download(raw: pd.DataFrame, tickers: list[str]) -> list[dict]:
    """
    Convert the multi-level yfinance DataFrame to a flat list of row dicts.
    Handles both single-ticker (flat columns) and multi-ticker (MultiIndex) output.
    """
    rows: list[dict] = []

    # yfinance returns MultiIndex columns when multiple tickers requested
    if isinstance(raw.columns, pd.MultiIndex):
        # columns are (field, ticker)
        for ticker in tickers:
            try:
                df = raw.xs(ticker, axis=1, level=1).copy()
            except KeyError:
                logger.warning(f"No data for ticker {ticker}")
                continue
            df = df.dropna(how="all")
            for idx_date, row in df.iterrows():
                rows.append(_make_row(ticker, idx_date, row))
    else:
        # Single ticker — columns are field names directly
        ticker = tickers[0]
        raw = raw.dropna(how="all")
        for idx_date, row in raw.iterrows():
            rows.append(_make_row(ticker, idx_date, row))

    return rows


def _make_row(ticker: str, idx_date, row: pd.Series) -> dict:
    def _safe(val):
        if pd.isna(val):
            return None
        return float(val)

    def _safe_int(val):
        if pd.isna(val):
            return None
        return int(val)

    return {
        "ticker": ticker,
        "date": pd.Timestamp(idx_date).date().isoformat(),
        "open": _safe(row.get("Open")),
        "high": _safe(row.get("High")),
        "low": _safe(row.get("Low")),
        "close": _safe(row.get("Close")),
        "adj_close": _safe(row.get("Adj Close")),
        "volume": _safe_int(row.get("Volume")),
    }
