"""
Daily incremental refresh script — run this via cron after market close.

Cron example (runs at 6 PM ET / 23:00 UTC Mon-Fri):
    0 23 * * 1-5 cd /path/to/needlstack && python scripts/daily_refresh.py

Usage:
    python scripts/daily_refresh.py
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
LOG_FILE = LOG_DIR / "refresh.log"

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
from ingestion.refresh import run_incremental_refresh
from ingestion.tickers import get_sp500_dataframe
from ingestion.prices import _upsert_tickers


def main() -> None:
    logger.info("=== Daily refresh started ===")

    engine = init_db()

    # Keep ticker metadata current
    tickers_df = get_sp500_dataframe()
    _upsert_tickers(engine, tickers_df)

    summary = run_incremental_refresh(engine=engine)

    logger.info(
        f"=== Daily refresh complete | "
        f"Updated: {summary['tickers_updated']} tickers | "
        f"Rows added: {summary['rows_added']} | "
        f"Failures: {len(summary['failures'])} ==="
    )


if __name__ == "__main__":
    main()
