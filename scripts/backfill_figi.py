"""
One-time backfill script — batch-lookups OpenFIGI identifiers (FIGI,
COMPOSITE_FIGI, SHARE_CLASS_FIGI) and security_type for all tickers.

Free tier: no API key, max 100 items/request, 25 requests/minute.

Usage:
    python scripts/backfill_figi.py [--limit N]
"""
import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
import os
from typing import Optional

import requests
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "backfill_figi.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

from db.schema import get_engine, identifier_crosswalk_table, tickers_table, init_db

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
BATCH_SIZE = 10
REQUEST_INTERVAL = 2.5  # seconds between requests (25 req/min = 2.4s minimum)

# FIGI fields to store and their corresponding id_type values
FIGI_FIELDS = {
    "figi": "FIGI",
    "compositeFIGI": "COMPOSITE_FIGI",
    "shareClassFIGI": "SHARE_CLASS_FIGI",
}


def fetch_tickers(engine: sa.Engine, limit: Optional[int] = None) -> list[str]:
    """Return all equity tickers from the tickers table."""
    stmt = sa.select(tickers_table.c.ticker).where(
        sa.and_(
            tickers_table.c.asset_type == "equity",
            tickers_table.c.security_type.is_(None),
        )
    ).order_by(tickers_table.c.ticker)
    if limit is not None:
        stmt = stmt.limit(limit)
    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [r[0] for r in rows]


def batch_lookup(tickers: list[str]) -> list[Optional[dict]]:
    """POST a batch of tickers to OpenFIGI and return parsed results."""
    body = [
        {"idType": "TICKER", "idValue": t, "exchCode": "US"}
        for t in tickers
    ]
    resp = requests.post(
        OPENFIGI_URL,
        json=body,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def upsert_crosswalk(
    conn: sa.Connection,
    ticker: str,
    id_type: str,
    id_value: str,
    now: datetime,
) -> None:
    """INSERT OR REPLACE a single identifier_crosswalk row."""
    stmt = sa.text(
        """
        INSERT INTO identifier_crosswalk (ticker, id_type, id_value, source, updated_at)
        VALUES (:ticker, :id_type, :id_value, :source, :updated_at)
        ON CONFLICT (ticker, id_type, id_value)
        DO UPDATE SET source = :source, updated_at = :updated_at
        """
    )
    conn.execute(stmt, {
        "ticker": ticker,
        "id_type": id_type,
        "id_value": id_value,
        "source": "openfigi",
        "updated_at": now.isoformat(),
    })


def update_security_type(
    conn: sa.Connection,
    ticker: str,
    security_type: str,
) -> None:
    """SET tickers.security_type for a given ticker."""
    conn.execute(
        sa.update(tickers_table)
        .where(tickers_table.c.ticker == ticker)
        .values(security_type=security_type)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill OpenFIGI identifiers")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of tickers (for testing)")
    args = parser.parse_args()

    logger.info("=== OpenFIGI backfill started ===")

    engine = init_db()
    tickers = fetch_tickers(engine, limit=args.limit)
    logger.info(f"Found {len(tickers)} tickers to process")

    if not tickers:
        logger.info("No tickers found — exiting")
        return

    total_matched = 0
    total_errors = 0
    total_crosswalk_rows = 0
    total_security_types = 0

    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    logger.info(f"Processing {len(batches)} batches of up to {BATCH_SIZE} tickers")

    for batch_idx, batch in enumerate(batches):
        processed_so_far = batch_idx * BATCH_SIZE
        if processed_so_far > 0 and processed_so_far % 1000 == 0:
            logger.info(
                f"Progress: {processed_so_far}/{len(tickers)} tickers | "
                f"matched={total_matched} errors={total_errors} "
                f"crosswalk_rows={total_crosswalk_rows} security_types={total_security_types}"
            )

        try:
            results = batch_lookup(batch)
        except requests.RequestException as exc:
            logger.error(f"Batch {batch_idx} request failed: {exc}")
            total_errors += len(batch)
            time.sleep(REQUEST_INTERVAL)
            continue

        now = datetime.now(timezone.utc)

        with engine.begin() as conn:
            for ticker, result in zip(batch, results):
                if "error" in result:
                    logger.debug(f"{ticker}: {result['error']}")
                    total_errors += 1
                    continue

                data_list = result.get("data", [])
                if not data_list:
                    logger.debug(f"{ticker}: no matches returned")
                    total_errors += 1
                    continue

                match = data_list[0]
                total_matched += 1

                # Upsert FIGI identifiers
                for field, id_type in FIGI_FIELDS.items():
                    value = match.get(field)
                    if value:
                        upsert_crosswalk(conn, ticker, id_type, value, now)
                        total_crosswalk_rows += 1

                # Update security_type on tickers table
                sec_type = match.get("securityType")
                if sec_type:
                    update_security_type(conn, ticker, sec_type)
                    total_security_types += 1

                # Log additional fields for reference
                exch = match.get("exchCode", "")
                market_sector = match.get("marketSector", "")
                if market_sector:
                    logger.debug(f"{ticker}: securityType={sec_type} exchCode={exch} marketSector={market_sector}")

        # Rate-limit between requests
        if batch_idx < len(batches) - 1:
            time.sleep(REQUEST_INTERVAL)

    logger.info(
        f"=== OpenFIGI backfill complete | "
        f"tickers={len(tickers)} matched={total_matched} errors={total_errors} | "
        f"crosswalk_rows={total_crosswalk_rows} security_types_set={total_security_types} ==="
    )


if __name__ == "__main__":
    main()
