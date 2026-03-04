"""
Cron: 0 8 * * 0
Refresh company_profiles and security_metadata for all active equity tickers.

Example cron entry (runs at 8 AM UTC every Sunday):
    0 8 * * 0 cd /path/to/needlstack && python scripts/weekly_profiles.py

Usage:
    python scripts/weekly_profiles.py
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
LOG_FILE = LOG_DIR / "weekly_profiles.log"

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
from ingestion.universe import get_active_tickers
from ingestion.profiles import download_company_profiles
from ingestion.metadata import download_security_metadata


def main() -> None:
    logger.info("=== Weekly profiles refresh started ===")

    engine = init_db()

    tickers = get_active_tickers(asset_types=["equity"], engine=engine)
    logger.info(f"Found {len(tickers)} active equity tickers")

    # Company profiles
    logger.info("Downloading company profiles...")
    profile_rows, profile_failed = download_company_profiles(tickers, engine=engine)
    logger.info(f"company_profiles: {profile_rows} rows upserted, {len(profile_failed)} failures")
    if profile_failed:
        logger.warning(f"Profile failures: {profile_failed[:20]}{'...' if len(profile_failed) > 20 else ''}")

    # Security metadata
    logger.info("Downloading security metadata...")
    meta_rows, meta_failed = download_security_metadata(tickers, engine=engine)
    logger.info(f"security_metadata: {meta_rows} rows upserted, {len(meta_failed)} failures")
    if meta_failed:
        logger.warning(f"Metadata failures: {meta_failed[:20]}{'...' if len(meta_failed) > 20 else ''}")

    logger.info(
        f"=== Weekly profiles refresh complete | "
        f"Profiles: {profile_rows} | "
        f"Metadata: {meta_rows} | "
        f"Total failures: {len(profile_failed) + len(meta_failed)} ==="
    )


if __name__ == "__main__":
    main()
