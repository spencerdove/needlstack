"""
Weekly event calendar fetch — pulls upcoming earnings, dividends, and splits
from FMP (Financial Modeling Prep) free bulk calendar endpoints.

Cron: 0 8 * * 1  (every Monday at 8 AM)

Usage:
    python scripts/weekly_event_calendar.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import requests
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "weekly_event_calendar.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

from db.schema import get_engine, init_db, event_calendar_table, tickers_table

FMP_BASE = "https://financialmodelingprep.com/api/v3"


def _get_known_tickers(engine: sa.Engine) -> set[str]:
    """Return set of all tickers present in the tickers table."""
    with engine.connect() as conn:
        rows = conn.execute(sa.select(tickers_table.c.ticker)).fetchall()
    return {row[0] for row in rows}


def _upsert_event(
    conn: sa.Connection,
    ticker: str,
    event_type: str,
    event_date: str,
    details: dict,
    now: datetime,
) -> bool:
    """INSERT OR REPLACE a single event. Returns True if inserted."""
    if not event_date:
        return False
    try:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO event_calendar
                    (ticker, event_type, event_date, confirmed, details, source, updated_at)
                VALUES
                    (:ticker, :event_type, :event_date, 1, :details, 'fmp', :updated_at)
                """
            ),
            {
                "ticker": ticker,
                "event_type": event_type,
                "event_date": event_date,
                "details": json.dumps(details),
                "updated_at": now.isoformat(),
            },
        )
        return True
    except Exception as exc:
        logger.warning(f"Failed to upsert {event_type} for {ticker} on {event_date}: {exc}")
        return False


def _fetch_json(url: str) -> Optional[list]:
    """GET a URL and return parsed JSON, or None on failure."""
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        logger.warning(f"Unexpected response type from {url}: {type(data)}")
        return None
    except Exception as exc:
        logger.error(f"Failed to fetch {url}: {exc}")
        return None


def fetch_earnings_calendar(api_key: str, engine: sa.Engine, known: set[str]) -> int:
    """Fetch FMP earnings calendar and upsert into event_calendar."""
    url = f"{FMP_BASE}/earning_calendar?apikey={api_key}"
    data = _fetch_json(url)
    if data is None:
        return 0

    now = datetime.now(timezone.utc)
    count = 0
    with engine.begin() as conn:
        for row in data:
            symbol = row.get("symbol", "")
            if symbol not in known:
                continue
            event_date = row.get("date")
            details = {
                "eps_estimated": row.get("epsEstimated"),
                "eps_actual": row.get("eps"),
                "revenue_estimated": row.get("revenueEstimated"),
                "revenue_actual": row.get("revenue"),
                "time": row.get("time"),
                "fiscal_date_ending": row.get("fiscalDateEnding"),
            }
            if _upsert_event(conn, symbol, "earnings", event_date, details, now):
                count += 1

    return count


def fetch_dividend_calendar(api_key: str, engine: sa.Engine, known: set[str]) -> int:
    """Fetch FMP dividend calendar and upsert ex-dividend + payment events."""
    url = f"{FMP_BASE}/stock_dividend_calendar?apikey={api_key}"
    data = _fetch_json(url)
    if data is None:
        return 0

    now = datetime.now(timezone.utc)
    count = 0
    with engine.begin() as conn:
        for row in data:
            symbol = row.get("symbol", "")
            if symbol not in known:
                continue

            dividend_details = {
                "dividend": row.get("dividend"),
                "adj_dividend": row.get("adjDividend"),
                "label": row.get("label"),
                "record_date": row.get("recordDate"),
                "payment_date": row.get("paymentDate"),
                "declaration_date": row.get("declarationDate"),
            }

            # Ex-dividend date
            ex_date = row.get("date")
            if ex_date and _upsert_event(conn, symbol, "ex_dividend", ex_date, dividend_details, now):
                count += 1

            # Payment date (separate event)
            payment_date = row.get("paymentDate")
            if payment_date and _upsert_event(conn, symbol, "dividend_payment", payment_date, dividend_details, now):
                count += 1

    return count


def fetch_split_calendar(api_key: str, engine: sa.Engine, known: set[str]) -> int:
    """Fetch FMP stock split calendar and upsert into event_calendar."""
    url = f"{FMP_BASE}/stock_split_calendar?apikey={api_key}"
    data = _fetch_json(url)
    if data is None:
        return 0

    now = datetime.now(timezone.utc)
    count = 0
    with engine.begin() as conn:
        for row in data:
            symbol = row.get("symbol", "")
            if symbol not in known:
                continue
            event_date = row.get("date")
            numerator = row.get("numerator")
            denominator = row.get("denominator")
            details = {
                "numerator": numerator,
                "denominator": denominator,
                "ratio": f"{numerator}:{denominator}" if numerator and denominator else None,
                "label": row.get("label"),
            }
            if _upsert_event(conn, symbol, "split", event_date, details, now):
                count += 1

    return count


def main() -> None:
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        print("FMP_API_KEY not set in environment. Exiting.")
        sys.exit(0)

    logger.info("=== Weekly event calendar fetch started ===")

    engine = init_db()
    known = _get_known_tickers(engine)
    logger.info(f"Found {len(known)} tickers in DB")

    earnings_count = fetch_earnings_calendar(api_key, engine, known)
    logger.info(f"Earnings events upserted: {earnings_count}")

    dividend_count = fetch_dividend_calendar(api_key, engine, known)
    logger.info(f"Dividend events upserted: {dividend_count}")

    split_count = fetch_split_calendar(api_key, engine, known)
    logger.info(f"Split events upserted: {split_count}")

    total = earnings_count + dividend_count + split_count
    logger.info(
        f"=== Weekly event calendar complete | "
        f"earnings: {earnings_count} | "
        f"dividends: {dividend_count} | "
        f"splits: {split_count} | "
        f"total: {total} ==="
    )
    print(f"\nSummary:")
    print(f"  Earnings events:  {earnings_count}")
    print(f"  Dividend events:  {dividend_count}")
    print(f"  Split events:     {split_count}")
    print(f"  Total:            {total}")


if __name__ == "__main__":
    main()
