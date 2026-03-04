"""
Cron: */30 * * * *
Poll all active RSS sources → upsert articles → run ticker mentions + VADER sentiment
on new articles.

Usage:
    python scripts/rss_poll.py
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
LOG_FILE = LOG_DIR / "rss_poll.log"

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
from ingestion.rss_feeds import poll_all_feeds
from ingestion.ticker_mentions import load_ticker_cache, process_pending_articles
from ingestion.sentiment import score_pending_articles


def main() -> None:
    logger.info("=== RSS poll started ===")

    engine = init_db()

    # 1. Poll all RSS feeds
    total_new, failed_sources = poll_all_feeds(engine=engine)
    logger.info(
        f"Feeds polled: total_new={total_new} failed_sources={failed_sources}"
    )

    if total_new > 0:
        # 2. Extract ticker mentions for new articles
        load_ticker_cache(engine)
        processed = process_pending_articles(engine=engine, batch_size=total_new + 50)
        logger.info(f"Ticker mentions processed: {processed} articles")

        # 3. Score sentiment for new articles
        scored = score_pending_articles(engine=engine, batch_size=total_new + 50)
        logger.info(f"Sentiment scored: {scored} articles")

    logger.info("=== RSS poll complete ===")


if __name__ == "__main__":
    main()
