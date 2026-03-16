"""
Weekly FMP bulk data refresh — fetches stock list, ETF list, and shares-float
from Financial Modeling Prep free bulk endpoints.

Uses 3 of the 250 free daily API calls.

Cron: 0 7 * * 1  (every Monday at 7 AM)

Usage:
    python scripts/weekly_fmp_bulk.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
import os
from datetime import datetime, timezone

import requests
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "weekly_fmp_bulk.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

from db.schema import get_engine, tickers_table, security_metadata_table

FMP_BASE = "https://financialmodelingprep.com/api"


def _fetch_json(url: str) -> list[dict]:
    """Fetch JSON array from FMP endpoint."""
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "Error Message" in data:
        raise ValueError(f"FMP API error: {data['Error Message']}")
    if not isinstance(data, list):
        raise ValueError(f"Expected list, got {type(data).__name__}")
    return data


def _get_existing_tickers(engine: sa.Engine) -> set[str]:
    """Return set of ticker symbols already in the DB."""
    with engine.connect() as conn:
        rows = conn.execute(sa.select(tickers_table.c.ticker)).fetchall()
    return {row[0] for row in rows}


def update_stock_list(api_key: str, engine: sa.Engine, existing: set[str]) -> int:
    """Fetch /v3/stock/list and update tickers table."""
    url = f"{FMP_BASE}/v3/stock/list?apikey={api_key}"
    logger.info("Fetching stock list...")
    data = _fetch_json(url)
    logger.info(f"Stock list: {len(data)} records from FMP")

    updated = 0
    with engine.begin() as conn:
        for item in data:
            symbol = (item.get("symbol") or "").strip().upper()
            if not symbol or symbol not in existing:
                continue
            values: dict = {}
            if item.get("name"):
                values["company_name"] = item["name"]
            if item.get("exchangeShortName"):
                values["exchange"] = item["exchangeShortName"]
            if not values:
                continue
            conn.execute(
                sa.update(tickers_table)
                .where(tickers_table.c.ticker == symbol)
                .values(**values)
            )
            updated += 1

    return updated


def update_etf_list(api_key: str, engine: sa.Engine, existing: set[str]) -> int:
    """Fetch /v3/etf/list and update tickers table for ETFs."""
    url = f"{FMP_BASE}/v3/etf/list?apikey={api_key}"
    logger.info("Fetching ETF list...")
    data = _fetch_json(url)
    logger.info(f"ETF list: {len(data)} records from FMP")

    updated = 0
    with engine.begin() as conn:
        for item in data:
            symbol = (item.get("symbol") or "").strip().upper()
            if not symbol or symbol not in existing:
                continue
            values: dict = {}
            if item.get("name"):
                values["company_name"] = item["name"]
            if item.get("exchangeShortName"):
                values["exchange"] = item["exchangeShortName"]
            values["asset_type"] = "etf"
            conn.execute(
                sa.update(tickers_table)
                .where(tickers_table.c.ticker == symbol)
                .values(**values)
            )
            updated += 1

    return updated


def update_shares_float(api_key: str, engine: sa.Engine, existing: set[str]) -> int:
    """Fetch /v4/shares_float/all and update security_metadata."""
    url = f"{FMP_BASE}/v4/shares_float/all?apikey={api_key}"
    logger.info("Fetching shares float (bulk)...")
    data = _fetch_json(url)
    logger.info(f"Shares float: {len(data)} records from FMP")

    now = datetime.now(timezone.utc)
    updated = 0
    with engine.begin() as conn:
        for item in data:
            symbol = (item.get("symbol") or "").strip().upper()
            if not symbol or symbol not in existing:
                continue

            float_shares = item.get("floatShares")
            outstanding = item.get("outstandingShares")
            if float_shares is None and outstanding is None:
                continue

            values: dict = {"updated_at": now}
            if float_shares is not None:
                values["float_shares"] = float(float_shares)
            if outstanding is not None:
                values["shares_outstanding"] = float(outstanding)

            # Upsert: try update first, insert if no row exists
            result = conn.execute(
                sa.update(security_metadata_table)
                .where(security_metadata_table.c.ticker == symbol)
                .values(**values)
            )
            if result.rowcount == 0:
                conn.execute(
                    sa.insert(security_metadata_table).values(
                        ticker=symbol, **values
                    )
                )
            updated += 1

    return updated


def main() -> None:
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        logger.error("FMP_API_KEY not set in environment. Add it to .env and retry.")
        sys.exit(1)

    logger.info("=== Weekly FMP bulk refresh started ===")
    engine = get_engine()
    existing = _get_existing_tickers(engine)
    logger.info(f"Found {len(existing)} existing tickers in DB")

    stock_updated = update_stock_list(api_key, engine, existing)
    logger.info(f"Stock list: {stock_updated} tickers updated")

    etf_updated = update_etf_list(api_key, engine, existing)
    logger.info(f"ETF list: {etf_updated} tickers updated")

    float_updated = update_shares_float(api_key, engine, existing)
    logger.info(f"Shares float: {float_updated} tickers updated")

    logger.info(
        f"=== Weekly FMP bulk refresh complete | "
        f"stocks: {stock_updated} | ETFs: {etf_updated} | "
        f"float: {float_updated} ==="
    )


if __name__ == "__main__":
    main()
