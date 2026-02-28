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
    sa.Column("cik", sa.Text),
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

income_statements_table = sa.Table(
    "income_statements",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("period_end", sa.Date, nullable=False),
    sa.Column("period_type", sa.Text, nullable=False),
    sa.Column("fiscal_year", sa.Integer),
    sa.Column("fiscal_quarter", sa.Integer),
    sa.Column("form_type", sa.Text),
    sa.Column("filed_date", sa.Date),
    sa.Column("revenue", sa.Float),
    sa.Column("cost_of_revenue", sa.Float),
    sa.Column("gross_profit", sa.Float),
    sa.Column("operating_income", sa.Float),
    sa.Column("pretax_income", sa.Float),
    sa.Column("income_tax", sa.Float),
    sa.Column("net_income", sa.Float),
    sa.Column("eps_basic", sa.Float),
    sa.Column("eps_diluted", sa.Float),
    sa.Column("shares_basic", sa.Float),
    sa.Column("shares_diluted", sa.Float),
    sa.PrimaryKeyConstraint("ticker", "period_end", "period_type"),
)

balance_sheets_table = sa.Table(
    "balance_sheets",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("period_end", sa.Date, nullable=False),
    sa.Column("period_type", sa.Text, nullable=False),
    sa.Column("filed_date", sa.Date),
    sa.Column("cash", sa.Float),
    sa.Column("current_assets", sa.Float),
    sa.Column("total_assets", sa.Float),
    sa.Column("accounts_payable", sa.Float),
    sa.Column("current_liabilities", sa.Float),
    sa.Column("long_term_debt", sa.Float),
    sa.Column("total_liabilities", sa.Float),
    sa.Column("stockholders_equity", sa.Float),
    sa.Column("retained_earnings", sa.Float),
    sa.PrimaryKeyConstraint("ticker", "period_end", "period_type"),
)

cash_flows_table = sa.Table(
    "cash_flows",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("period_end", sa.Date, nullable=False),
    sa.Column("period_type", sa.Text, nullable=False),
    sa.Column("filed_date", sa.Date),
    sa.Column("operating_cf", sa.Float),
    sa.Column("capex", sa.Float),
    sa.Column("investing_cf", sa.Float),
    sa.Column("financing_cf", sa.Float),
    sa.Column("dividends_paid", sa.Float),
    sa.Column("stock_repurchases", sa.Float),
    sa.Column("free_cash_flow", sa.Float),
    sa.PrimaryKeyConstraint("ticker", "period_end", "period_type"),
)

earnings_surprises_table = sa.Table(
    "earnings_surprises",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("earnings_date", sa.Date, nullable=False),
    sa.Column("eps_estimate", sa.Float),
    sa.Column("eps_actual", sa.Float),
    sa.Column("eps_surprise_pct", sa.Float),
    sa.PrimaryKeyConstraint("ticker", "earnings_date"),
)


def run_migrations(engine: sa.Engine) -> None:
    """Apply any schema migrations needed for existing databases."""
    with engine.connect() as conn:
        cols = conn.execute(sa.text("PRAGMA table_info(tickers)")).fetchall()
        col_names = {row[1] for row in cols}
        if "cik" not in col_names:
            conn.execute(sa.text("ALTER TABLE tickers ADD COLUMN cik TEXT"))
            conn.commit()


def init_db(db_path: Path = DB_PATH) -> sa.Engine:
    """Create all tables if they don't exist and run migrations. Returns the engine."""
    engine = get_engine(db_path)
    metadata.create_all(engine)
    run_migrations(engine)
    return engine
