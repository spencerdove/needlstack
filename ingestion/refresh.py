"""
Incremental refresh logic: for each ticker, find the last stored date and
download only new data since then.
"""
import logging
from datetime import date, timedelta
from typing import Optional

import sqlalchemy as sa

from db.schema import get_engine, stock_prices_table
from ingestion.prices import download_prices

logger = logging.getLogger(__name__)


def get_last_dates(engine: sa.Engine, tickers: list[str]) -> dict[str, Optional[date]]:
    """
    Return a mapping of ticker → most recent date in stock_prices, or None
    if the ticker has no rows yet.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(
                stock_prices_table.c.ticker,
                sa.func.max(stock_prices_table.c.date).label("max_date"),
            ).group_by(stock_prices_table.c.ticker)
        ).fetchall()

    last: dict[str, Optional[date]] = {t: None for t in tickers}
    for row in rows:
        if row.ticker in last:
            last[row.ticker] = date.fromisoformat(row.max_date)
    return last


def run_incremental_refresh(
    start_fallback: str = "2020-01-01",
    engine: Optional[sa.Engine] = None,
    tickers: Optional[list[str]] = None,
) -> dict:
    """
    For each ticker, download data from (last_date + 1 day) to today.
    Tickers with no history fall back to *start_fallback*.

    If *tickers* is None, falls back to get_active_tickers(['equity'])
    or, if that returns nothing, get_sp500_tickers().

    Returns a summary dict with keys: tickers_updated, rows_added, failures.
    """
    if engine is None:
        engine = get_engine()

    today = date.today()
    if tickers is None:
        try:
            from ingestion.universe import get_active_tickers
            tickers = get_active_tickers(asset_types=["equity"], engine=engine)
        except Exception:
            pass
        if not tickers:
            from ingestion.tickers import get_sp500_tickers
            tickers = get_sp500_tickers()
    last_dates = get_last_dates(engine, tickers)

    to_refresh: dict[str, str] = {}
    skipped = 0

    for ticker in tickers:
        last = last_dates.get(ticker)
        if last is None:
            start = start_fallback
        else:
            next_day = last + timedelta(days=1)
            if next_day > today:
                skipped += 1
                continue
            start = next_day.isoformat()
        to_refresh[ticker] = start

    logger.info(
        f"Refreshing {len(to_refresh)} tickers "
        f"(skipped {skipped} already up-to-date)."
    )

    if not to_refresh:
        logger.info("All tickers are up to date. Nothing to do.")
        return {"tickers_updated": 0, "rows_added": 0, "failures": []}

    # Group by start date to batch tickers with the same start into one download
    # For simplicity, use the earliest start across all and let upsert handle dupes
    all_tickers = list(to_refresh.keys())
    earliest_start = min(to_refresh.values())

    total_rows, failures = download_prices(
        tickers=all_tickers,
        start=earliest_start,
        end=today.isoformat(),
        engine=engine,
    )

    summary = {
        "tickers_updated": len(all_tickers) - len(failures),
        "rows_added": total_rows,
        "failures": failures,
    }

    logger.info(
        f"Refresh complete. "
        f"Tickers updated: {summary['tickers_updated']}, "
        f"Rows added: {summary['rows_added']}, "
        f"Failures: {len(failures)}"
    )
    if failures:
        logger.warning(f"Failed tickers: {failures}")

    return summary
