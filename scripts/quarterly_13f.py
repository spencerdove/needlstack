"""
Cron: 0 9 1 1,4,7,10 *
Fetch 13F-HR filings for all institutions in TOP_INSTITUTION_CIKS,
upsert into institutional_holdings, then compute institutional_summary.

Example cron entry (runs at 9 AM UTC on the 1st of Jan/Apr/Jul/Oct):
    0 9 1 1,4,7,10 * cd /path/to/needlstack && python scripts/quarterly_13f.py

Usage:
    python scripts/quarterly_13f.py
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
LOG_FILE = LOG_DIR / "quarterly_13f.log"

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
from ingestion.sec_13f import download_13f_holdings, compute_institutional_summary


def main() -> None:
    logger.info("=== Quarterly 13F fetch started ===")

    engine = init_db()

    # Step 1: Download 13F holdings from EDGAR
    rows_upserted, failed = download_13f_holdings(engine=engine)

    if failed:
        logger.warning(f"Failed institutions ({len(failed)}): {failed}")

    logger.info(
        f"13F download complete | "
        f"Holdings upserted: {rows_upserted} | "
        f"Institution failures: {len(failed)}"
    )

    # Step 2: Aggregate into institutional_summary
    logger.info("Computing institutional summary...")
    summary_rows = compute_institutional_summary(engine=engine)
    logger.info(f"institutional_summary: {summary_rows} rows upserted")

    logger.info(
        f"=== Quarterly 13F complete | "
        f"Holdings: {rows_upserted} | "
        f"Summary rows: {summary_rows} | "
        f"Failures: {len(failed)} ==="
    )


if __name__ == "__main__":
    main()
