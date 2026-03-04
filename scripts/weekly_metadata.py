"""
Weekly metadata refresh script — refreshes security_metadata and
corporate_actions for all active S&P 500 tickers.

Cron: 0 7 * * 1  (every Monday at 7 AM)

Usage:
    python scripts/weekly_metadata.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
import os
from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "weekly_metadata.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

from db.schema import init_db
from ingestion.tickers import get_sp500_tickers
from ingestion.metadata import download_security_metadata
from ingestion.corporate_actions import download_corporate_actions


def main() -> None:
    logger.info("=== Weekly metadata refresh started ===")

    engine = init_db()

    tickers = get_sp500_tickers()
    logger.info(f"Processing {len(tickers)} S&P 500 tickers")

    # Security metadata (shares outstanding, float, market cap, EV, avg volumes)
    meta_rows, meta_failed = download_security_metadata(tickers, engine)
    logger.info(
        f"security_metadata: {meta_rows} rows upserted, "
        f"{len(meta_failed)} failures"
    )
    if meta_failed:
        logger.warning(f"security_metadata failures: {meta_failed}")

    # Corporate actions (splits + dividends)
    ca_rows, ca_failed = download_corporate_actions(tickers, engine)
    logger.info(
        f"corporate_actions: {ca_rows} rows upserted, "
        f"{len(ca_failed)} failures"
    )
    if ca_failed:
        logger.warning(f"corporate_actions failures: {ca_failed}")

    logger.info(
        f"=== Weekly metadata refresh complete | "
        f"metadata rows: {meta_rows} | "
        f"corporate action rows: {ca_rows} | "
        f"total failures: {len(meta_failed) + len(ca_failed)} ==="
    )


if __name__ == "__main__":
    main()
