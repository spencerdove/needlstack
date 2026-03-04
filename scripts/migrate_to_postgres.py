"""
One-time migration: read all tables from SQLite in chunks and write to
Postgres in batches.

IMPORTANT: Set DATABASE_URL env var to target Postgres before running.
NOT reversible — does not delete the source SQLite database.

Usage:
    DATABASE_URL=postgresql://user:pass@host:5432/dbname python scripts/migrate_to_postgres.py

The script reads from the SQLite database at DB_PATH and writes to the Postgres
database specified by DATABASE_URL. Tables are written with if_exists='append'
so you can re-run partially if interrupted (duplicate PK rows will be rejected
by Postgres unique constraints — check logs for errors).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
import os

import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "migrate_to_postgres.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

from db.schema import DB_PATH, init_db

TABLES_TO_MIGRATE = [
    "tickers",
    "stock_prices",
    "income_statements",
    "balance_sheets",
    "cash_flows",
    "earnings_surprises",
    "security_metadata",
    "corporate_actions",
    "index_constituents",
    "company_profiles",
    "valuation_snapshots",
    "institutional_holdings",
    "institutional_summary",
    "sec_filings",
]

CHUNK_SIZE = 1000


def _get_sqlite_engine() -> sa.Engine:
    return sa.create_engine(f"sqlite:///{DB_PATH}", echo=False)


def _get_postgres_engine() -> sa.Engine:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise EnvironmentError(
            "DATABASE_URL env var not set. "
            "Set it to the target Postgres connection string before running."
        )
    return sa.create_engine(database_url, echo=False)


def _table_exists_in_sqlite(sqlite_engine: sa.Engine, table_name: str) -> bool:
    with sqlite_engine.connect() as conn:
        row = conn.execute(
            sa.text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
            ),
            {"t": table_name},
        ).fetchone()
    return row is not None


def migrate_table(
    table_name: str,
    sqlite_engine: sa.Engine,
    pg_engine: sa.Engine,
    chunksize: int = CHUNK_SIZE,
) -> int:
    """
    Read table from SQLite in chunks and append to Postgres.
    Returns total rows written.
    """
    if not _table_exists_in_sqlite(sqlite_engine, table_name):
        logger.warning(f"Table '{table_name}' does not exist in SQLite — skipping.")
        return 0

    total_rows = 0
    offset = 0

    while True:
        query = f"SELECT * FROM {table_name} LIMIT {chunksize} OFFSET {offset}"
        df = pd.read_sql(query, sqlite_engine)
        if df.empty:
            break

        try:
            df.to_sql(
                table_name,
                pg_engine,
                if_exists="append",
                index=False,
                chunksize=chunksize,
            )
            total_rows += len(df)
            logger.info(f"  {table_name}: wrote rows {offset}–{offset + len(df) - 1}")
        except Exception as exc:
            logger.error(f"  {table_name}: error writing chunk at offset {offset}: {exc}")
            # Continue to next chunk rather than aborting entire migration
            pass

        offset += chunksize
        if len(df) < chunksize:
            break

    return total_rows


def main() -> None:
    logger.info("=== PostgreSQL migration started ===")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error(
            "DATABASE_URL is not set. "
            "Export it before running:\n"
            "  export DATABASE_URL=postgresql://user:pass@host:5432/dbname"
        )
        sys.exit(1)

    logger.info(f"Source SQLite: {DB_PATH}")
    logger.info(f"Target Postgres: {database_url[:database_url.rfind('@') + 1]}***")

    sqlite_engine = _get_sqlite_engine()
    pg_engine = _get_postgres_engine()

    # Ensure all tables exist in Postgres
    logger.info("Initializing schema in Postgres...")
    init_db.__wrapped__ = None  # bypass any caching if present
    from db.schema import metadata
    metadata.create_all(pg_engine)

    grand_total = 0
    for table_name in TABLES_TO_MIGRATE:
        logger.info(f"Migrating table: {table_name}")
        rows = migrate_table(table_name, sqlite_engine, pg_engine)
        logger.info(f"  {table_name}: {rows} rows migrated")
        grand_total += rows

    logger.info(f"=== Migration complete | Total rows migrated: {grand_total} ===")


if __name__ == "__main__":
    main()
