"""
Cron: 0 23 * * 1-5
Refresh macro instruments (~16 symbols) using existing download_prices().

Example cron entry (runs at 11 PM UTC Mon-Fri):
    0 23 * * 1-5 cd /path/to/needlstack && python scripts/daily_macro.py

Usage:
    python scripts/daily_macro.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
import os
from datetime import date, timedelta

import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "daily_macro.log"

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
from ingestion.prices import download_prices
from ingestion.universe import MACRO_SYMBOLS


def _get_last_known_date(engine: sa.Engine, tickers: list[str]) -> str:
    """
    Return the most recent date present in stock_prices for any of the given
    macro tickers. Falls back to 30 days ago if no data exists.
    """
    placeholders = ", ".join(f"'{t}'" for t in tickers)
    fallback = (date.today() - timedelta(days=30)).isoformat()
    try:
        with engine.connect() as conn:
            row = conn.execute(
                sa.text(
                    f"SELECT MAX(date) FROM stock_prices WHERE ticker IN ({placeholders})"
                )
            ).fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception as exc:
        logger.warning(f"Could not determine last known date: {exc}")
    return fallback


def main() -> None:
    logger.info("=== Daily macro refresh started ===")

    engine = init_db()

    start = _get_last_known_date(engine, MACRO_SYMBOLS)
    end = date.today().isoformat()

    logger.info(f"Refreshing {len(MACRO_SYMBOLS)} macro symbols from {start} to {end}")

    rows_inserted, failed = download_prices(
        tickers=MACRO_SYMBOLS,
        start=start,
        end=end,
        engine=engine,
    )

    if failed:
        logger.warning(f"Failed symbols: {failed}")

    logger.info(
        f"=== Daily macro refresh complete | "
        f"Rows inserted: {rows_inserted} | "
        f"Failures: {len(failed)} ==="
    )


if __name__ == "__main__":
    main()
