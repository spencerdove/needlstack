"""
Cron: 0 0 * * 1-5
Compute valuation_snapshots for all active equity tickers.

Example cron entry (runs at midnight UTC Mon-Fri):
    0 0 * * 1-5 cd /path/to/needlstack && python scripts/daily_valuations.py

Usage:
    python scripts/daily_valuations.py
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
LOG_FILE = LOG_DIR / "daily_valuations.log"

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
from ingestion.universe import get_active_tickers
from ingestion.valuations import compute_valuations


def main() -> None:
    logger.info("=== Daily valuations computation started ===")

    engine = init_db()

    tickers = get_active_tickers(asset_types=["equity"], engine=engine)
    logger.info(f"Found {len(tickers)} active equity tickers")

    rows_upserted, failed = compute_valuations(tickers, engine=engine)

    if failed:
        logger.warning(f"Failed tickers ({len(failed)}): {failed[:20]}{'...' if len(failed) > 20 else ''}")

    logger.info(
        f"=== Daily valuations complete | "
        f"Snapshots upserted: {rows_upserted} | "
        f"Failures: {len(failed)} ==="
    )


if __name__ == "__main__":
    main()
