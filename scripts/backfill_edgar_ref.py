"""
Backfill SEC EDGAR reference data for all tickers with a non-null CIK.

Fetches entity metadata from the EDGAR submissions API and populates:
  - tickers: sic_code, sic_description, fiscal_year_end, state_of_incorporation, entity_type
  - ticker_history: former name changes
  - source_lineage: provenance records

After the sweep, fills sector/industry from sic_lookup for tickers missing them.

Usage:
    python3 scripts/backfill_edgar_ref.py [--limit N]
"""
import argparse
import concurrent.futures
import logging
import sys
import threading
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import requests
import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.schema import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EDGAR_BASE = "https://data.sec.gov/submissions/CIK{cik}.json"
HEADERS = {"User-Agent": "needlstack/1.0 (contact@needlstack.com)"}
RATE_LIMIT_DELAY = 0.12  # ~8 req/sec to stay safely under 10 req/sec

_rate_lock = threading.Lock()
_last_request_time = 0.0
_lock = threading.Lock()
_updated = 0
_failed = 0
_skipped = 0


def _parse_date(s: Optional[str]) -> Optional[date]:
    """Parse a YYYY-MM-DD string into a date, or return None."""
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _fetch_and_store(ticker: str, cik: str, engine: sa.Engine) -> None:
    """Fetch EDGAR submissions JSON for one CIK and update the database."""
    global _updated, _failed, _skipped

    # Enforce global rate limit: wait until RATE_LIMIT_DELAY since last request
    with _rate_lock:
        global _last_request_time
        now = time.monotonic()
        wait = RATE_LIMIT_DELAY - (now - _last_request_time)
        if wait > 0:
            time.sleep(wait)
        _last_request_time = time.monotonic()

    try:
        padded_cik = cik.zfill(10)
        url = EDGAR_BASE.format(cik=padded_cik)
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 404:
            with _lock:
                _skipped += 1
            logger.debug(f"CIK {cik} not found (404) for {ticker}")
            return
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        with _lock:
            _failed += 1
        logger.debug(f"Failed to fetch {ticker} (CIK {cik}): {exc}")
        return

    # Extract fields
    sic_code = str(data.get("sic", "")).strip() or None
    sic_description = (data.get("sicDescription") or "").strip() or None
    fiscal_year_end = (data.get("fiscalYearEnd") or "").strip() or None
    state_of_inc = (data.get("stateOfIncorporation") or "").strip() or None
    entity_type = (data.get("entityType") or "").strip() or None
    former_names = data.get("formerNames") or []
    edgar_tickers = data.get("tickers") or []
    edgar_exchanges = data.get("exchanges") or []

    # Log multi-listed info
    if len(edgar_tickers) > 1:
        listings = list(zip(edgar_exchanges, edgar_tickers))
        logger.debug(f"{ticker}: multi-listed on EDGAR: {listings}")

    now = datetime.utcnow()

    try:
        with engine.begin() as conn:
            # Update tickers table
            conn.execute(
                sa.text(
                    "UPDATE tickers SET "
                    "sic_code = COALESCE(:sic_code, sic_code), "
                    "sic_description = COALESCE(:sic_description, sic_description), "
                    "fiscal_year_end = COALESCE(:fiscal_year_end, fiscal_year_end), "
                    "state_of_incorporation = COALESCE(:state_of_inc, state_of_incorporation), "
                    "entity_type = COALESCE(:entity_type, entity_type) "
                    "WHERE ticker = :ticker"
                ),
                {
                    "ticker": ticker,
                    "sic_code": sic_code,
                    "sic_description": sic_description,
                    "fiscal_year_end": fiscal_year_end,
                    "state_of_inc": state_of_inc,
                    "entity_type": entity_type,
                },
            )

            # Insert former names into ticker_history
            for entry in former_names:
                name = (entry.get("name") or "").strip()
                date_from = _parse_date(entry.get("from"))
                date_to = _parse_date(entry.get("to"))
                if not name or not date_from:
                    continue
                notes = f"to={date_to.isoformat()}" if date_to else None
                conn.execute(
                    sa.text(
                        "INSERT OR REPLACE INTO ticker_history "
                        "(ticker, event_type, event_date, old_value, new_value, notes, source) "
                        "VALUES (:ticker, 'name_change', :event_date, :old_value, :new_value, :notes, 'sec_edgar')"
                    ),
                    {
                        "ticker": ticker,
                        "event_date": date_from.isoformat(),
                        "old_value": name,
                        "new_value": ticker,
                        "notes": notes,
                    },
                )

            # Record provenance in source_lineage
            for field in ["sic_code", "sic_description", "fiscal_year_end",
                          "state_of_incorporation", "entity_type"]:
                conn.execute(
                    sa.text(
                        "INSERT OR REPLACE INTO source_lineage "
                        "(table_name, ticker, field_name, source, fetched_at, confidence) "
                        "VALUES ('tickers', :ticker, :field, 'sec_edgar', :fetched_at, 1.0)"
                    ),
                    {"ticker": ticker, "field": field, "fetched_at": now.isoformat()},
                )

        with _lock:
            _updated += 1
            if _updated % 500 == 0:
                logger.info(f"Progress: {_updated} tickers updated, {_failed} failed, {_skipped} skipped")

    except Exception as exc:
        with _lock:
            _failed += 1
        logger.warning(f"DB error for {ticker}: {exc}")


def _backfill_sector_from_sic(engine: sa.Engine) -> int:
    """Fill sector/industry from sic_lookup for tickers that have sic_code but NULL sector."""
    with engine.begin() as conn:
        result = conn.execute(
            sa.text(
                "UPDATE tickers SET "
                "sector = (SELECT sl.sector FROM sic_lookup sl WHERE sl.sic_code = tickers.sic_code), "
                "industry = (SELECT sl.industry FROM sic_lookup sl WHERE sl.sic_code = tickers.sic_code) "
                "WHERE sic_code IS NOT NULL AND sector IS NULL"
            )
        )
        return result.rowcount


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill SEC EDGAR reference data")
    parser.add_argument("--limit", type=int, default=0, help="Process only first N tickers (for testing)")
    args = parser.parse_args()

    engine = get_engine()

    # Query tickers with non-null CIK
    with engine.connect() as conn:
        query = "SELECT ticker, cik FROM tickers WHERE cik IS NOT NULL AND sic_code IS NULL ORDER BY ticker"
        rows = conn.execute(sa.text(query)).fetchall()

    tickers = [(r[0], r[1]) for r in rows]
    if args.limit > 0:
        tickers = tickers[: args.limit]

    logger.info(f"Found {len(tickers)} tickers with CIK to process")

    if not tickers:
        logger.info("Nothing to backfill")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        futures = [
            pool.submit(_fetch_and_store, ticker, cik, engine)
            for ticker, cik in tickers
        ]
        concurrent.futures.wait(futures)

    logger.info(f"EDGAR sweep complete. Updated: {_updated}, Failed: {_failed}, Skipped: {_skipped}")

    # Backfill sector/industry from SIC lookup
    filled = _backfill_sector_from_sic(engine)
    logger.info(f"Filled sector/industry from sic_lookup for {filled} tickers")

    logger.info("Done.")


if __name__ == "__main__":
    main()
