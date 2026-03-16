"""
One-time setup: load data/sic_codes.json into sic_lookup table,
then backfill tickers.sector, tickers.industry, and tickers.sic_description
from sic_lookup for any tickers that have a sic_code but are missing those fields.

Usage:
    python scripts/init_sic_lookup.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
import os

from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "init_sic_lookup.log"

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

from db.schema import init_db, sic_lookup_table, tickers_table


DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "sic_codes.json"


def load_sic_codes(engine: sa.Engine) -> int:
    """Load data/sic_codes.json into sic_lookup table. Returns row count."""
    if not DATA_FILE.exists():
        logger.error("SIC codes file not found: %s", DATA_FILE)
        sys.exit(1)

    with open(DATA_FILE) as f:
        records = json.load(f)

    if not records:
        logger.warning("sic_codes.json is empty — nothing to load")
        return 0

    logger.info("Loaded %d SIC codes from %s", len(records), DATA_FILE.name)

    with engine.begin() as conn:
        for rec in records:
            conn.execute(
                sa.text(
                    """
                    INSERT OR REPLACE INTO sic_lookup
                        (sic_code, sic_description, division, division_name,
                         major_group, major_group_name, sector, industry)
                    VALUES
                        (:sic_code, :sic_description, :division, :division_name,
                         :major_group, :major_group_name, :sector, :industry)
                    """
                ),
                {
                    "sic_code": rec.get("sic_code"),
                    "sic_description": rec.get("sic_description"),
                    "division": rec.get("division"),
                    "division_name": rec.get("division_name"),
                    "major_group": rec.get("major_group"),
                    "major_group_name": rec.get("major_group_name"),
                    "sector": rec.get("sector"),
                    "industry": rec.get("industry"),
                },
            )

    return len(records)


def backfill_tickers(engine: sa.Engine) -> int:
    """
    Update tickers.sector, tickers.industry, and tickers.sic_description
    from sic_lookup for rows that have a sic_code but are missing those fields.
    Returns the number of tickers updated.
    """
    update_sql = sa.text(
        """
        UPDATE tickers
        SET sector = (
                SELECT sl.sector FROM sic_lookup sl
                WHERE sl.sic_code = tickers.sic_code
            ),
            industry = (
                SELECT sl.industry FROM sic_lookup sl
                WHERE sl.sic_code = tickers.sic_code
            ),
            sic_description = (
                SELECT sl.sic_description FROM sic_lookup sl
                WHERE sl.sic_code = tickers.sic_code
            )
        WHERE tickers.sic_code IS NOT NULL
          AND tickers.sector IS NULL
          AND EXISTS (
                SELECT 1 FROM sic_lookup sl
                WHERE sl.sic_code = tickers.sic_code
          )
        """
    )

    with engine.begin() as conn:
        result = conn.execute(update_sql)
        return result.rowcount


def main() -> None:
    logger.info("=== Initializing SIC lookup table ===")

    engine = init_db()

    # Step 1: Load SIC codes into sic_lookup
    n_loaded = load_sic_codes(engine)
    logger.info("Inserted/replaced %d rows into sic_lookup", n_loaded)

    # Step 2: Backfill tickers
    n_updated = backfill_tickers(engine)
    logger.info("Updated sector/industry/sic_description for %d tickers", n_updated)

    # Summary stats
    with engine.connect() as conn:
        total_sic = conn.execute(
            sa.text("SELECT COUNT(*) FROM sic_lookup")
        ).scalar()
        total_tickers = conn.execute(
            sa.text("SELECT COUNT(*) FROM tickers")
        ).scalar()
        tickers_with_sic = conn.execute(
            sa.text("SELECT COUNT(*) FROM tickers WHERE sic_code IS NOT NULL")
        ).scalar()
        tickers_with_sector = conn.execute(
            sa.text("SELECT COUNT(*) FROM tickers WHERE sector IS NOT NULL")
        ).scalar()
        tickers_missing_sector = conn.execute(
            sa.text(
                "SELECT COUNT(*) FROM tickers "
                "WHERE sic_code IS NOT NULL AND sector IS NULL"
            )
        ).scalar()

    logger.info("--- Summary ---")
    logger.info("  sic_lookup rows:              %d", total_sic)
    logger.info("  Total tickers:                %d", total_tickers)
    logger.info("  Tickers with sic_code:        %d", tickers_with_sic)
    logger.info("  Tickers with sector filled:   %d", tickers_with_sector)
    logger.info("  Tickers still missing sector: %d", tickers_missing_sector)
    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
