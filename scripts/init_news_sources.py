"""
One-time setup: seed news_sources table with default RSS sources.

Usage:
    python scripts/init_news_sources.py
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
LOG_FILE = LOG_DIR / "init_news_sources.log"

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
from ingestion.rss_feeds import seed_news_sources


def main() -> None:
    logger.info("=== Seeding news sources ===")

    engine = init_db()
    seed_news_sources(engine=engine)

    logger.info("=== News sources seeded successfully ===")


if __name__ == "__main__":
    main()
