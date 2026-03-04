"""
One-time script: download NASDAQ universe files and upsert all tickers into
the tickers table.

Does NOT backfill price history; new tickers collect prices from today forward.

Usage:
    python scripts/backfill_universe.py
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
LOG_FILE = LOG_DIR / "backfill_universe.log"

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
from ingestion.universe import refresh_universe


def main() -> None:
    logger.info("=== Backfill universe started ===")

    engine = init_db()

    rows_upserted, failed = refresh_universe(engine=engine)

    if failed:
        logger.warning(f"Failures during universe refresh: {failed}")

    logger.info(
        f"=== Backfill universe complete | "
        f"Tickers upserted: {rows_upserted} | "
        f"Failures: {len(failed)} ==="
    )


if __name__ == "__main__":
    main()
