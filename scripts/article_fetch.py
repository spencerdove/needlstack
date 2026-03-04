"""
Cron: */15 * * * *
Extract full text for up to 50 articles per run where full_text IS NULL
AND is_paywalled = 0.

Usage:
    python scripts/article_fetch.py
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
LOG_FILE = LOG_DIR / "article_fetch.log"

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
from ingestion.article_extractor import extract_article_texts


def main() -> None:
    logger.info("=== Article full-text fetch started ===")

    engine = init_db()

    extracted, paywalled = extract_article_texts(engine=engine, batch_size=50)

    logger.info(
        f"=== Article fetch complete | "
        f"extracted={extracted} paywalled_detected={paywalled} ==="
    )


if __name__ == "__main__":
    main()
