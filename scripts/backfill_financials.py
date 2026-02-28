"""
One-time historical backfill: load all financial statements and earnings
surprises for S&P 500 companies from SEC EDGAR and yfinance.

Usage:
    python scripts/backfill_financials.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from tqdm import tqdm

from db.schema import init_db
from ingestion.cik_lookup import update_tickers_cik
from ingestion.financials import download_financials
from ingestion.earnings import download_earnings_surprises
import sqlalchemy as sa


def main() -> None:
    logger.info("=== Financial backfill started ===")

    engine = init_db()
    logger.info("Database initialized (tables created, migrations applied).")

    # Populate CIK column from SEC map
    mapped = update_tickers_cik(engine)
    logger.info(f"CIK mapping complete: {mapped} tickers mapped.")

    # Load tickers that have a CIK
    with engine.connect() as conn:
        rows = conn.execute(
            sa.text("SELECT ticker, cik FROM tickers WHERE cik IS NOT NULL")
        ).fetchall()

    tickers_with_cik = [(row[0], int(row[1])) for row in rows]
    tickers = [t for t, _ in tickers_with_cik]
    logger.info(f"Downloading financials for {len(tickers_with_cik)} tickers...")

    # Download financial statements with progress bar
    total_financial_rows = 0
    financial_failures: list[str] = []

    with tqdm(total=len(tickers_with_cik), desc="Financials", unit="ticker") as pbar:
        for ticker, cik in tickers_with_cik:
            rows_inserted, failures = download_financials(
                [(ticker, cik)], engine=engine
            )
            total_financial_rows += rows_inserted
            financial_failures.extend(failures)
            pbar.update(1)
            pbar.set_postfix(rows=total_financial_rows, failures=len(financial_failures))

    logger.info(f"Financials complete: {total_financial_rows} rows inserted.")
    if financial_failures:
        logger.warning(f"Failed financials ({len(financial_failures)}): {financial_failures}")

    # Download earnings surprises
    logger.info(f"Downloading earnings surprises for {len(tickers)} tickers...")
    total_earnings_rows = 0
    earnings_failures: list[str] = []

    with tqdm(total=len(tickers), desc="Earnings", unit="ticker") as pbar:
        for ticker in tickers:
            rows_inserted, failures = download_earnings_surprises(
                [ticker], engine=engine
            )
            total_earnings_rows += rows_inserted
            earnings_failures.extend(failures)
            pbar.update(1)
            pbar.set_postfix(rows=total_earnings_rows, failures=len(earnings_failures))

    logger.info(f"Earnings complete: {total_earnings_rows} rows inserted.")
    if earnings_failures:
        logger.warning(f"Failed earnings ({len(earnings_failures)}): {earnings_failures}")

    logger.info(
        f"=== Backfill complete | "
        f"Financial rows: {total_financial_rows} | "
        f"Earnings rows: {total_earnings_rows} | "
        f"Financial failures: {len(financial_failures)} | "
        f"Earnings failures: {len(earnings_failures)} ==="
    )


if __name__ == "__main__":
    main()
