"""
One-time backfill script — populates index_constituents for all 5 indexes:
SP500, NDX100, DOW30, SP400, SP600.

SP500 uses get_sp500_tickers() from ingestion.tickers.
All others use get_index_tickers() + upsert_index_constituents() from
ingestion.indexes.

Usage:
    python scripts/backfill_indexes.py
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
LOG_FILE = LOG_DIR / "backfill_indexes.log"

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
from ingestion.indexes import get_index_tickers, upsert_index_constituents, INDEX_CONFIGS


def main() -> None:
    logger.info("=== Index constituent backfill started ===")

    engine = init_db()

    total_rows = 0
    failed_indexes: list[str] = []

    # --- SP500 ---
    try:
        logger.info("Fetching SP500 constituents...")
        sp500_tickers = get_sp500_tickers()
        rows = upsert_index_constituents("SP500", sp500_tickers, engine)
        total_rows += rows
        logger.info(f"SP500: {rows} rows upserted ({len(sp500_tickers)} tickers)")
    except Exception as exc:
        logger.error(f"Failed to process SP500: {exc}")
        failed_indexes.append("SP500")

    # --- NDX100, DOW30, SP400, SP600 ---
    for index_id in INDEX_CONFIGS:
        try:
            logger.info(f"Fetching {index_id} constituents...")
            tickers = get_index_tickers(index_id)
            rows = upsert_index_constituents(index_id, tickers, engine)
            total_rows += rows
            logger.info(f"{index_id}: {rows} rows upserted ({len(tickers)} tickers)")
        except Exception as exc:
            logger.error(f"Failed to process {index_id}: {exc}")
            failed_indexes.append(index_id)

    logger.info(
        f"=== Index backfill complete | "
        f"total rows upserted: {total_rows} | "
        f"failed indexes: {failed_indexes} ==="
    )


if __name__ == "__main__":
    main()
