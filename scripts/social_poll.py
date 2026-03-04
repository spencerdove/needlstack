"""
Cron: */30 * * * *
Poll Reddit and StockTwits for financial content → upsert into content_items
with inline ticker mentions + sentiment scoring.

Usage:
    python scripts/social_poll.py
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
LOG_FILE = LOG_DIR / "social_poll.log"

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
from ingestion.reddit import poll_reddit
from ingestion.stocktwits import poll_stocktwits


def _get_active_tickers(engine: sa.Engine) -> list[str]:
    """Return list of active tickers for StockTwits polling."""
    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT ticker FROM tickers WHERE is_active = 1 ORDER BY ticker"
            )
        ).fetchall()
    return [row[0] for row in rows]


def main() -> None:
    logger.info("=== Social poll started ===")

    engine = init_db()

    # Reddit
    reddit_new, reddit_failed = poll_reddit(engine=engine, limit=100)
    logger.info(
        f"Reddit: new_items={reddit_new} failed_subreddits={reddit_failed}"
    )

    # StockTwits — poll all active tickers
    tickers = _get_active_tickers(engine)
    if tickers:
        stocktwits_new, stocktwits_failed = poll_stocktwits(
            tickers=tickers, engine=engine, delay=0.5
        )
        logger.info(
            f"StockTwits: new_items={stocktwits_new} "
            f"failed_tickers={len(stocktwits_failed)}"
        )
    else:
        logger.warning("No active tickers found for StockTwits polling.")

    logger.info("=== Social poll complete ===")


if __name__ == "__main__":
    main()
