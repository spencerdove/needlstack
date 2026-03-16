"""
One-time backfill script — fetches IPO dates and delisting dates for all
US-listed securities from the Alpha Vantage LISTING_STATUS endpoint.

Makes two API calls (active + delisted), then updates ipo_date and delist_date
on matching rows in the tickers table.

Requires ALPHA_VANTAGE_API_KEY in .env.

Usage:
    python scripts/backfill_ipo_dates.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import csv
import io
import logging
import os
from datetime import date
from typing import Optional

import requests
from dotenv import load_dotenv
import sqlalchemy as sa

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "backfill_ipo_dates.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

from db.schema import get_engine, tickers_table

BASE_URL = "https://www.alphavantage.co/query"


def fetch_listing_status(api_key: str, state: str = "active") -> list[dict]:
    """Fetch LISTING_STATUS CSV from Alpha Vantage and return list of dicts."""
    params = {
        "function": "LISTING_STATUS",
        "apikey": api_key,
    }
    if state == "delisted":
        params["state"] = "delisted"

    logger.info(f"Fetching LISTING_STATUS (state={state})...")
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    rows = list(reader)
    logger.info(f"Received {len(rows)} rows for state={state}")
    return rows


def parse_date(value: str) -> Optional[date]:
    """Parse YYYY-MM-DD string to date, returning None on empty/invalid."""
    if not value or value.strip() == "" or value == "null":
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def main() -> None:
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        logger.error("ALPHA_VANTAGE_API_KEY not set in environment. Exiting.")
        sys.exit(1)

    logger.info("=== IPO date backfill started ===")

    engine = get_engine()

    # Fetch active and delisted listings
    active_rows = fetch_listing_status(api_key, state="active")
    delisted_rows = fetch_listing_status(api_key, state="delisted")

    # Build lookup: symbol -> {ipo_date, delist_date}
    # Active listings get ipo_date only; delisted get both
    lookup: dict[str, dict] = {}

    for row in active_rows:
        symbol = row.get("symbol", "").strip()
        if not symbol:
            continue
        ipo = parse_date(row.get("ipoDate", ""))
        if ipo:
            lookup.setdefault(symbol, {})
            lookup[symbol]["ipo_date"] = ipo

    for row in delisted_rows:
        symbol = row.get("symbol", "").strip()
        if not symbol:
            continue
        ipo = parse_date(row.get("ipoDate", ""))
        delist = parse_date(row.get("delistingDate", ""))
        lookup.setdefault(symbol, {})
        if ipo:
            lookup[symbol]["ipo_date"] = ipo
        if delist:
            lookup[symbol]["delist_date"] = delist

    logger.info(f"Lookup built: {len(lookup)} unique symbols")

    # Get existing tickers from DB
    with engine.connect() as conn:
        existing = conn.execute(
            sa.select(tickers_table.c.ticker, tickers_table.c.ipo_date, tickers_table.c.delist_date)
        ).fetchall()

    db_tickers = {row[0]: {"ipo_date": row[1], "delist_date": row[2]} for row in existing}
    logger.info(f"DB has {len(db_tickers)} tickers")

    matched = 0
    ipo_set = 0
    delist_set = 0

    with engine.begin() as conn:
        for ticker, current in db_tickers.items():
            if ticker not in lookup:
                continue
            matched += 1
            av = lookup[ticker]

            # Set ipo_date only if currently NULL
            if current["ipo_date"] is None and "ipo_date" in av:
                conn.execute(
                    sa.update(tickers_table)
                    .where(tickers_table.c.ticker == ticker)
                    .values(ipo_date=av["ipo_date"])
                )
                ipo_set += 1

            # Set delist_date only if currently NULL
            if current["delist_date"] is None and "delist_date" in av:
                conn.execute(
                    sa.update(tickers_table)
                    .where(tickers_table.c.ticker == ticker)
                    .values(delist_date=av["delist_date"])
                )
                delist_set += 1

    logger.info(
        f"=== IPO date backfill complete | "
        f"matched: {matched} | "
        f"ipo_dates set: {ipo_set} | "
        f"delist_dates set: {delist_set} ==="
    )


if __name__ == "__main__":
    main()
