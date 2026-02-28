"""
SQLAlchemy Core schema definitions and database initialization.
"""
from pathlib import Path

from dotenv import load_dotenv
import os
import sqlalchemy as sa

load_dotenv()

DB_PATH = Path(os.getenv("DB_PATH", "db/needlstack.db"))


def get_engine(db_path: Path = DB_PATH) -> sa.Engine:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return sa.create_engine(f"sqlite:///{db_path}", echo=False)


metadata = sa.MetaData()

tickers_table = sa.Table(
    "tickers",
    metadata,
    sa.Column("ticker", sa.Text, primary_key=True),
    sa.Column("company_name", sa.Text),
    sa.Column("sector", sa.Text),
    sa.Column("industry", sa.Text),
    sa.Column("added_date", sa.Date),
)

stock_prices_table = sa.Table(
    "stock_prices",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("date", sa.Date, nullable=False),
    sa.Column("open", sa.Float),
    sa.Column("high", sa.Float),
    sa.Column("low", sa.Float),
    sa.Column("close", sa.Float),
    sa.Column("adj_close", sa.Float),
    sa.Column("volume", sa.Integer),
    sa.PrimaryKeyConstraint("ticker", "date"),
)


def init_db(db_path: Path = DB_PATH) -> sa.Engine:
    """Create all tables if they don't exist. Returns the engine."""
    engine = get_engine(db_path)
    metadata.create_all(engine)
    return engine
