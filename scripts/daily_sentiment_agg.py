"""
Cron: 0 1 * * *
Compute ticker_sentiment_daily for the prior day and export sentiment JSON
to docs/data/sentiment/{TICKER}.json for the frontend.

Usage:
    python scripts/daily_sentiment_agg.py [--date YYYY-MM-DD]
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import argparse
import json
import logging
import os
from datetime import date, timedelta

from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "daily_sentiment_agg.log"

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
from ingestion.sentiment_aggregator import aggregate_sentiment

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SENTIMENT_DIR = _REPO_ROOT / "docs" / "data" / "sentiment"


def export_sentiment_json(engine: sa.Engine) -> int:
    """
    Export per-ticker sentiment data (last 30 days) to docs/data/sentiment/{TICKER}.json.

    Each file is a JSON array of:
        {date, mention_count, article_count, avg_sentiment,
         bullish_count, bearish_count, neutral_count}

    Returns count of ticker files written.
    """
    _SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)

    cutoff = (date.today() - timedelta(days=30)).isoformat()

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT ticker, date, mention_count, article_count,
                       avg_sentiment, bullish_count, bearish_count, neutral_count
                FROM ticker_sentiment_daily
                WHERE date >= :cutoff
                ORDER BY ticker, date
                """
            ),
            {"cutoff": cutoff},
        ).fetchall()

    if not rows:
        logger.info("No sentiment data found for the last 30 days.")
        return 0

    # Group by ticker
    ticker_data: dict[str, list[dict]] = {}
    for row in rows:
        ticker = row[0]
        record = {
            "date": str(row[1]),
            "mention_count": row[2],
            "article_count": row[3],
            "avg_sentiment": row[4],
            "bullish_count": row[5],
            "bearish_count": row[6],
            "neutral_count": row[7],
        }
        ticker_data.setdefault(ticker, []).append(record)

    written = 0
    for ticker, records in ticker_data.items():
        out_file = _SENTIMENT_DIR / f"{ticker}.json"
        try:
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(records, f)
            written += 1
        except Exception as exc:
            logger.error(f"Failed to write {out_file}: {exc}")

    logger.info(f"export_sentiment_json: wrote {written} ticker files to {_SENTIMENT_DIR}")
    return written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate and export daily ticker sentiment."
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

    logger.info(f"=== Daily sentiment aggregation started for {target_date} ===")

    engine = init_db()

    upserted = aggregate_sentiment(target_date=target_date, engine=engine)
    logger.info(f"Aggregated {upserted} ticker rows for {target_date}")

    written = export_sentiment_json(engine=engine)
    logger.info(f"Exported {written} ticker sentiment JSON files")

    logger.info("=== Daily sentiment aggregation complete ===")


if __name__ == "__main__":
    main()
