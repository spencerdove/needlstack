"""
Backfill sector and industry for tickers that have NULL values.

Usage:
    python3 scripts/backfill_sectors.py
"""
import concurrent.futures
import logging
import sys
import threading
import time
from pathlib import Path

import sqlalchemy as sa
import yfinance as yf

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.schema import get_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_SEMAPHORE = threading.BoundedSemaphore(8)
_LOCK = threading.Lock()
_UPDATED = 0
_FAILED = 0


def _fetch_sector(ticker: str, engine: sa.Engine) -> None:
    global _UPDATED, _FAILED
    with _SEMAPHORE:
        time.sleep(0.1)
        try:
            info = yf.Ticker(ticker).info or {}
            sector = info.get("sector")
            industry = info.get("industry")
            if not sector and not industry:
                return
            # Clean values
            sector = str(sector).strip() if sector else None
            industry = str(industry).strip() if industry else None
            with engine.begin() as conn:
                conn.execute(
                    sa.text(
                        "UPDATE tickers SET sector = COALESCE(:sector, sector), "
                        "industry = COALESCE(:industry, industry) WHERE ticker = :ticker"
                    ),
                    {"ticker": ticker, "sector": sector, "industry": industry},
                )
            with _LOCK:
                _UPDATED += 1
                if _UPDATED % 100 == 0:
                    logger.info(f"Updated {_UPDATED} tickers so far...")
        except Exception as exc:
            with _LOCK:
                _FAILED += 1
            logger.debug(f"Failed {ticker}: {exc}")


def main() -> None:
    engine = get_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT ticker FROM tickers "
                "WHERE sector IS NULL AND asset_type = 'equity' "
                "ORDER BY ticker"
            )
        ).fetchall()

    tickers = [r[0] for r in rows]
    logger.info(f"Found {len(tickers)} equity tickers with NULL sector")

    if not tickers:
        logger.info("Nothing to backfill")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_fetch_sector, t, engine) for t in tickers]
        concurrent.futures.wait(futures)

    logger.info(f"Done. Updated: {_UPDATED}, Failed: {_FAILED}")


if __name__ == "__main__":
    main()
