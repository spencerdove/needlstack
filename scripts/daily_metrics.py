"""
Compute derived financial metrics for all active equity tickers and upsert into derived_metrics.

Cron example (runs at 1 AM UTC Mon-Fri):
    0 1 * * 1-5 cd /path/to/needlstack && python scripts/daily_metrics.py

Usage:
    python scripts/daily_metrics.py
    python scripts/daily_metrics.py --tickers AAPL MSFT NVDA
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import logging
import os

from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "daily_metrics.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

import sqlalchemy as sa

from db.schema import init_db
from analysis.compute_metrics import compute_derived_metrics


def _get_active_equity_tickers(engine: sa.Engine) -> list[str]:
    """Return all active equity tickers from the tickers table."""
    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT ticker FROM tickers
                WHERE (asset_type = 'equity' OR asset_type IS NULL)
                  AND (is_active = 1 OR is_active IS NULL)
                ORDER BY ticker
                """
            )
        ).fetchall()
    return [row[0] for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute derived financial metrics")
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="Specific tickers to process (default: all active equity tickers)",
    )
    args = parser.parse_args()

    logger.info("=== Daily metrics computation started ===")

    engine = init_db()

    if args.tickers:
        tickers = [t.upper() for t in args.tickers]
        logger.info(f"Processing {len(tickers)} specified tickers.")
    else:
        tickers = _get_active_equity_tickers(engine)
        logger.info(f"Found {len(tickers)} active equity tickers.")

    if not tickers:
        logger.info("No tickers to process. Exiting.")
        return

    upserted, failures = compute_derived_metrics(tickers, engine=engine)

    logger.info(
        f"=== Daily metrics complete | "
        f"Rows upserted: {upserted} | "
        f"Failures: {len(failures)} ==="
    )

    if failures:
        logger.warning(f"Failed tickers: {failures}")


if __name__ == "__main__":
    main()
