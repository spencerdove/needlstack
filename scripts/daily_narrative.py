"""
Cron: 0 2 * * *
Compute narrative_signals for all active narratives for the prior day.

Usage:
    python scripts/daily_narrative.py [--date YYYY-MM-DD]
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import logging
import os
from datetime import date, timedelta

from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "daily_narrative.log"

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
from ingestion.narratives import compute_narrative_signals


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute narrative signals for all active narratives."
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date YYYY-MM-DD (default: yesterday)",
    )
    args = parser.parse_args()

    if args.date:
        from datetime import date as _date
        target_date = _date.fromisoformat(args.date)
    else:
        target_date = date.today() - timedelta(days=1)

    logger.info(f"=== Daily narrative signal computation started for {target_date} ===")

    engine = init_db()

    upserted = compute_narrative_signals(target_date=target_date, engine=engine)

    logger.info(
        f"=== Daily narrative complete | "
        f"signals upserted={upserted} for {target_date} ==="
    )


if __name__ == "__main__":
    main()
