"""
One-time historical backfill: load all S&P 500 OHLCV data from 2020-01-01.

Usage:
    python scripts/backfill.py [--start 2020-01-01]
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from tqdm import tqdm

from db.schema import init_db, get_engine
from ingestion.tickers import get_sp500_dataframe, get_sp500_tickers
from ingestion.prices import download_prices, _upsert_tickers, BATCH_SIZE


def main(start: str = "2020-01-01") -> None:
    engine = init_db()

    # Upsert ticker metadata first
    logger.info("Loading S&P 500 ticker metadata...")
    tickers_df = get_sp500_dataframe()
    _upsert_tickers(engine, tickers_df)
    logger.info(f"Upserted {len(tickers_df)} ticker rows.")

    tickers = tickers_df["ticker"].tolist()
    logger.info(f"Starting backfill for {len(tickers)} tickers from {start}...")

    total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE
    total_rows = 0
    all_failures: list[str] = []

    with tqdm(total=total_batches, desc="Batches", unit="batch") as pbar:
        for batch_start in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[batch_start : batch_start + BATCH_SIZE]
            rows, failures = download_prices(
                tickers=batch,
                start=start,
                engine=engine,
            )
            total_rows += rows
            all_failures.extend(failures)
            pbar.update(1)
            pbar.set_postfix(rows=total_rows, failures=len(all_failures))

    logger.info(f"Backfill complete. Total rows inserted: {total_rows}")
    if all_failures:
        logger.warning(f"Failed tickers ({len(all_failures)}): {all_failures}")
    else:
        logger.info("No failures.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill S&P 500 price history.")
    parser.add_argument(
        "--start",
        default="2020-01-01",
        help="Start date for historical data (default: 2020-01-01)",
    )
    args = parser.parse_args()
    main(start=args.start)
