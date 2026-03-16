"""
One-time backfill: load key ETFs into the tickers table and download price history.

Usage:
    python scripts/backfill_etfs.py [--start 2020-01-01]
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import logging

import sqlalchemy as sa

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from db.schema import init_db
from ingestion.prices import download_prices

ETF_SYMBOLS = [
    "SPY", "QQQ", "VTI", "IWM", "DIA", "VOO", "VEA", "VWO", "EFA",
    "AGG", "BND", "TLT", "GLD", "SLV",
    "XLE", "XLF", "XLK", "XLV", "XLI", "XLC", "XLY", "XLP", "XLB", "XLU", "XLRE",
    "SMH", "ARKK", "SOXX", "IBB", "KRE", "XBI", "HYG", "LQD", "IEF", "VNQ",
]


def _upsert_etf_tickers(engine: sa.Engine) -> None:
    """Insert or update ETF tickers with asset_type='etf'."""
    with engine.begin() as conn:
        for sym in ETF_SYMBOLS:
            conn.execute(
                sa.text(
                    """
                    INSERT INTO tickers (ticker, company_name, asset_type, is_active, added_date)
                    VALUES (:ticker, :name, 'etf', 1, date('now'))
                    ON CONFLICT(ticker) DO UPDATE SET
                        asset_type = 'etf',
                        is_active = 1
                    """
                ),
                {"ticker": sym, "name": sym},
            )
    logger.info(f"Upserted {len(ETF_SYMBOLS)} ETF tickers")


def main(start: str = "2020-01-01") -> None:
    engine = init_db()

    _upsert_etf_tickers(engine)

    logger.info(f"Downloading prices for {len(ETF_SYMBOLS)} ETFs from {start}...")
    total_rows, failures = download_prices(
        tickers=ETF_SYMBOLS, start=start, engine=engine
    )

    logger.info(
        f"ETF backfill complete: {total_rows} rows inserted, "
        f"{len(failures)} failures: {failures}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill ETF price data")
    parser.add_argument("--start", default="2020-01-01", help="Start date (YYYY-MM-DD)")
    args = parser.parse_args()
    main(start=args.start)
