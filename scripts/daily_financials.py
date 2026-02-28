"""
Daily incremental financials refresh — run this via cron after market hours.

Skips tickers whose income_statements.filed_date is within the last 3 days
(SEC data only changes when companies file new 10-Q or 10-K).

Cron example (runs at 7 AM UTC Mon-Fri):
    0 7 * * 1-5 cd /path/to/needlstack && python scripts/daily_financials.py

Usage:
    python scripts/daily_financials.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
import os
from datetime import date, timedelta

from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "financials.log"

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
from ingestion.cik_lookup import update_tickers_cik
from ingestion.financials import download_financials
from ingestion.earnings import download_earnings_surprises

SKIP_WITHIN_DAYS = 3


def _get_pending_tickers(engine: sa.Engine) -> list[tuple[str, int]]:
    """
    Return (ticker, cik) pairs that need a financial refresh.

    A ticker is skipped if its most recent filed_date in income_statements
    is within the last SKIP_WITHIN_DAYS days.
    """
    cutoff = (date.today() - timedelta(days=SKIP_WITHIN_DAYS)).isoformat()

    with engine.connect() as conn:
        # All tickers with a CIK
        all_rows = conn.execute(
            sa.text("SELECT ticker, cik FROM tickers WHERE cik IS NOT NULL")
        ).fetchall()

        # Tickers with a recent filed_date — skip these
        recent = conn.execute(
            sa.text(
                """
                SELECT ticker, MAX(filed_date) AS max_filed
                FROM income_statements
                GROUP BY ticker
                HAVING max_filed >= :cutoff
                """
            ),
            {"cutoff": cutoff},
        ).fetchall()

    recent_tickers = {row[0] for row in recent}
    pending = [
        (row[0], int(row[1]))
        for row in all_rows
        if row[0] not in recent_tickers
    ]
    return pending


def main() -> None:
    logger.info("=== Daily financials refresh started ===")

    engine = init_db()

    # Keep CIK mapping current
    mapped = update_tickers_cik(engine)
    logger.info(f"CIK mapping refreshed: {mapped} tickers have CIKs.")

    pending = _get_pending_tickers(engine)
    tickers = [t for t, _ in pending]
    logger.info(
        f"{len(pending)} tickers need a financial refresh "
        f"(skipping tickers filed within last {SKIP_WITHIN_DAYS} days)."
    )

    if not pending:
        logger.info("All tickers are up to date. Nothing to do.")
    else:
        fin_rows, fin_failures = download_financials(pending, engine=engine)
        logger.info(f"Financials: {fin_rows} rows inserted, {len(fin_failures)} failures.")
        if fin_failures:
            logger.warning(f"Failed financials: {fin_failures}")

        earn_rows, earn_failures = download_earnings_surprises(tickers, engine=engine)
        logger.info(f"Earnings: {earn_rows} rows inserted, {len(earn_failures)} failures.")
        if earn_failures:
            logger.warning(f"Failed earnings: {earn_failures}")

        logger.info(
            f"=== Daily financials complete | "
            f"Financial rows: {fin_rows} | "
            f"Earnings rows: {earn_rows} | "
            f"Failures: {len(fin_failures) + len(earn_failures)} ==="
        )


if __name__ == "__main__":
    main()
