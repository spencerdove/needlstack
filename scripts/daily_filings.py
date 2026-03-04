"""
Cron: 0 23 * * 1-5
Check for new 8-K filings for all CIK-mapped active tickers.

Example cron entry (runs at 11 PM UTC Mon-Fri):
    0 23 * * 1-5 cd /path/to/needlstack && python scripts/daily_filings.py

Usage:
    python scripts/daily_filings.py
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
LOG_FILE = LOG_DIR / "daily_filings.log"

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
from ingestion.sec_filings import download_sec_filings


def _get_tickers_with_ciks(engine: sa.Engine) -> list[tuple[str, str]]:
    """Query active tickers that have a CIK assigned."""
    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT ticker, cik
                FROM tickers
                WHERE cik IS NOT NULL
                  AND is_active = 1
                ORDER BY ticker
                """
            )
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def main() -> None:
    logger.info("=== Daily filings check started ===")

    engine = init_db()

    tickers_with_ciks = _get_tickers_with_ciks(engine)
    logger.info(f"Found {len(tickers_with_ciks)} active tickers with CIKs")

    if not tickers_with_ciks:
        logger.warning("No tickers with CIKs found — nothing to fetch. Run cik_lookup.py first.")
        return

    rows_upserted, failed = download_sec_filings(
        tickers_with_ciks=tickers_with_ciks,
        engine=engine,
    )

    if failed:
        logger.warning(f"Failed tickers ({len(failed)}): {failed[:20]}{'...' if len(failed) > 20 else ''}")

    logger.info(
        f"=== Daily filings complete | "
        f"8-K rows upserted: {rows_upserted} | "
        f"Failures: {len(failed)} ==="
    )


if __name__ == "__main__":
    main()
