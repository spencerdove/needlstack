"""
SQLAlchemy Core schema definitions and database initialization.
"""
from pathlib import Path

from dotenv import load_dotenv
import os
import sqlalchemy as sa
from sqlalchemy import event

load_dotenv()

DB_PATH = Path(os.getenv("DB_PATH", "db/needlstack.db"))


def _set_sqlite_pragmas(dbapi_conn, _):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("PRAGMA synchronous = NORMAL")
    cursor.execute("PRAGMA cache_size = 20000")
    cursor.execute("PRAGMA temp_store = MEMORY")
    cursor.close()


def get_engine(db_path: Path = DB_PATH) -> sa.Engine:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        engine = sa.create_engine(database_url, echo=False)
        if database_url.startswith("sqlite:///"):
            event.listen(engine, "connect", _set_sqlite_pragmas)
        return engine
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = sa.create_engine(f"sqlite:///{db_path}", echo=False)
    event.listen(engine, "connect", _set_sqlite_pragmas)
    return engine


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
    # Phase 3 additions
    sa.Column("asset_type", sa.Text, server_default="equity"),
    sa.Column("exchange", sa.Text),
    sa.Column("is_active", sa.Integer, server_default="1"),
    sa.Column("first_seen_date", sa.Date),
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
    # Phase 2 addition
    sa.Column("dollar_volume", sa.Float),
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

# ── Phase 2 tables ────────────────────────────────────────────────────────────

security_metadata_table = sa.Table(
    "security_metadata",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), primary_key=True),
    sa.Column("shares_outstanding", sa.Float),
    sa.Column("float_shares", sa.Float),
    sa.Column("market_cap", sa.Float),
    sa.Column("enterprise_value", sa.Float),
    sa.Column("avg_volume_30d", sa.Float),
    sa.Column("avg_dollar_vol_30d", sa.Float),
    sa.Column("updated_at", sa.DateTime),
)

corporate_actions_table = sa.Table(
    "corporate_actions",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("action_date", sa.Date, nullable=False),
    sa.Column("action_type", sa.Text, nullable=False),
    sa.Column("ratio", sa.Float),
    sa.Column("amount", sa.Float),
    sa.Column("notes", sa.Text),
    sa.PrimaryKeyConstraint("ticker", "action_date", "action_type"),
)

index_constituents_table = sa.Table(
    "index_constituents",
    metadata,
    sa.Column("index_id", sa.Text, nullable=False),
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("added_date", sa.Date, nullable=False),
    sa.Column("removed_date", sa.Date),
    sa.Column("weight", sa.Float),
    sa.PrimaryKeyConstraint("index_id", "ticker", "added_date"),
)

# ── Phase 3 tables ────────────────────────────────────────────────────────────

company_profiles_table = sa.Table(
    "company_profiles",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), primary_key=True),
    sa.Column("description", sa.Text),
    sa.Column("employees", sa.Integer),
    sa.Column("website", sa.Text),
    sa.Column("country", sa.Text),
    sa.Column("city", sa.Text),
    sa.Column("state", sa.Text),
    sa.Column("updated_at", sa.DateTime),
)

# ── Phase 4 tables ────────────────────────────────────────────────────────────

valuation_snapshots_table = sa.Table(
    "valuation_snapshots",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("snapshot_date", sa.Date, nullable=False),
    sa.Column("pe_ttm", sa.Float),
    sa.Column("pb", sa.Float),
    sa.Column("ps_ttm", sa.Float),
    sa.Column("ev_ebitda", sa.Float),
    sa.Column("peg_ratio", sa.Float),
    sa.PrimaryKeyConstraint("ticker", "snapshot_date"),
)

institutional_holdings_table = sa.Table(
    "institutional_holdings",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("institution_cik", sa.Text, nullable=False),
    sa.Column("report_date", sa.Date, nullable=False),
    sa.Column("institution_name", sa.Text),
    sa.Column("filed_date", sa.Date),
    sa.Column("shares_held", sa.Float),
    sa.Column("market_value", sa.Float),
    sa.Column("pct_of_portfolio", sa.Float),
    sa.Column("change_shares", sa.Float),
    sa.PrimaryKeyConstraint("ticker", "institution_cik", "report_date"),
)

institutional_summary_table = sa.Table(
    "institutional_summary",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), primary_key=True),
    sa.Column("report_date", sa.Date),
    sa.Column("total_institutions", sa.Integer),
    sa.Column("total_shares_held", sa.Float),
    sa.Column("pct_outstanding_held", sa.Float),
    sa.Column("net_change_shares", sa.Float),
    sa.Column("top_holder_name", sa.Text),
    sa.Column("top_holder_pct", sa.Float),
    sa.Column("updated_at", sa.DateTime),
)

sec_filings_table = sa.Table(
    "sec_filings",
    metadata,
    sa.Column("accession_number", sa.Text, primary_key=True),
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker")),
    sa.Column("cik", sa.Text),
    sa.Column("form_type", sa.Text),
    sa.Column("filed_date", sa.Date),
    sa.Column("period_of_report", sa.Date),
    sa.Column("primary_doc_url", sa.Text),
    sa.Column("items_reported", sa.Text),
)

# ── Phase 5 tables ────────────────────────────────────────────────────────────

news_sources_table = sa.Table(
    "news_sources",
    metadata,
    sa.Column("source_id", sa.Text, primary_key=True),
    sa.Column("name", sa.Text),
    sa.Column("rss_url", sa.Text),
    sa.Column("is_active", sa.Integer, server_default="1"),
    sa.Column("fetch_interval_min", sa.Integer, server_default="30"),
    sa.Column("last_fetched_at", sa.DateTime),
)

news_articles_table = sa.Table(
    "news_articles",
    metadata,
    sa.Column("article_id", sa.Text, primary_key=True),
    sa.Column("source_id", sa.Text, sa.ForeignKey("news_sources.source_id")),
    sa.Column("url", sa.Text, unique=True),
    sa.Column("title", sa.Text),
    sa.Column("author", sa.Text),
    sa.Column("published_at", sa.DateTime),
    sa.Column("fetched_at", sa.DateTime),
    sa.Column("full_text", sa.Text),
    sa.Column("raw_rss_summary", sa.Text),
    sa.Column("word_count", sa.Integer),
    sa.Column("is_paywalled", sa.Integer),
    sa.Column("categories", sa.Text),
)

article_tickers_table = sa.Table(
    "article_tickers",
    metadata,
    sa.Column("article_id", sa.Text, sa.ForeignKey("news_articles.article_id"), nullable=False),
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("mention_count", sa.Integer),
    sa.Column("mention_in_title", sa.Integer),
    sa.PrimaryKeyConstraint("article_id", "ticker"),
)

article_sentiment_table = sa.Table(
    "article_sentiment",
    metadata,
    sa.Column("article_id", sa.Text, sa.ForeignKey("news_articles.article_id"), primary_key=True),
    sa.Column("compound_score", sa.Float),
    sa.Column("positive", sa.Float),
    sa.Column("negative", sa.Float),
    sa.Column("neutral", sa.Float),
    sa.Column("sentiment_label", sa.Text),
    sa.Column("scored_at", sa.DateTime),
    sa.Column("model_version", sa.Text),
)

ticker_sentiment_daily_table = sa.Table(
    "ticker_sentiment_daily",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("date", sa.Date, nullable=False),
    sa.Column("mention_count", sa.Integer),
    sa.Column("article_count", sa.Integer),
    sa.Column("source_count", sa.Integer),
    sa.Column("avg_sentiment", sa.Float),
    sa.Column("bullish_count", sa.Integer),
    sa.Column("bearish_count", sa.Integer),
    sa.Column("neutral_count", sa.Integer),
    sa.Column("title_mention_count", sa.Integer),
    sa.PrimaryKeyConstraint("ticker", "date"),
)

# ── Phase 6 tables ────────────────────────────────────────────────────────────

content_items_table = sa.Table(
    "content_items",
    metadata,
    sa.Column("content_id", sa.Text, primary_key=True),
    sa.Column("source_type", sa.Text),
    sa.Column("source_id", sa.Text),
    sa.Column("external_id", sa.Text),
    sa.Column("url", sa.Text),
    sa.Column("title", sa.Text),
    sa.Column("author", sa.Text),
    sa.Column("published_at", sa.DateTime),
    sa.Column("fetched_at", sa.DateTime),
    sa.Column("body_text", sa.Text),
    sa.Column("word_count", sa.Integer),
    sa.Column("engagement_score", sa.Float),
    sa.Column("raw_json", sa.Text),
)

content_tickers_table = sa.Table(
    "content_tickers",
    metadata,
    sa.Column("content_id", sa.Text, sa.ForeignKey("content_items.content_id"), nullable=False),
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("mention_count", sa.Integer),
    sa.Column("mention_in_title", sa.Integer),
    sa.Column("confidence", sa.Float),
    sa.PrimaryKeyConstraint("content_id", "ticker"),
)

content_sentiment_table = sa.Table(
    "content_sentiment",
    metadata,
    sa.Column("content_id", sa.Text, sa.ForeignKey("content_items.content_id"), primary_key=True),
    sa.Column("compound_score", sa.Float),
    sa.Column("positive", sa.Float),
    sa.Column("negative", sa.Float),
    sa.Column("neutral", sa.Float),
    sa.Column("sentiment_label", sa.Text),
    sa.Column("scored_at", sa.DateTime),
    sa.Column("model_version", sa.Text),
)

narratives_table = sa.Table(
    "narratives",
    metadata,
    sa.Column("narrative_id", sa.Text, primary_key=True),
    sa.Column("name", sa.Text),
    sa.Column("description", sa.Text),
    sa.Column("keywords", sa.Text),
    sa.Column("related_tickers", sa.Text),
    sa.Column("created_at", sa.DateTime),
    sa.Column("last_seen_at", sa.DateTime),
    sa.Column("is_active", sa.Integer, server_default="1"),
)

narrative_signals_table = sa.Table(
    "narrative_signals",
    metadata,
    sa.Column("narrative_id", sa.Text, sa.ForeignKey("narratives.narrative_id"), nullable=False),
    sa.Column("date", sa.Date, nullable=False),
    sa.Column("mention_count", sa.Integer),
    sa.Column("momentum_score", sa.Float),
    sa.PrimaryKeyConstraint("narrative_id", "date"),
)

# ── Phase 7 tables ────────────────────────────────────────────────────────────

derived_metrics_table = sa.Table(
    "derived_metrics",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("date", sa.Date, nullable=False),
    sa.Column("revenue_yoy_growth", sa.Float),
    sa.Column("net_income_yoy_growth", sa.Float),
    sa.Column("eps_yoy_growth", sa.Float),
    sa.Column("gross_margin", sa.Float),
    sa.Column("operating_margin", sa.Float),
    sa.Column("net_margin", sa.Float),
    sa.Column("fcf_margin", sa.Float),
    sa.Column("roe", sa.Float),
    sa.Column("roa", sa.Float),
    sa.Column("debt_to_equity", sa.Float),
    sa.Column("current_ratio", sa.Float),
    sa.Column("pe_ttm", sa.Float),
    sa.Column("ev_ebitda", sa.Float),
    sa.PrimaryKeyConstraint("ticker", "date"),
)

agent_conversations_table = sa.Table(
    "agent_conversations",
    metadata,
    sa.Column("conversation_id", sa.Text, primary_key=True),
    sa.Column("created_at", sa.DateTime),
    sa.Column("context_tickers", sa.Text),
    sa.Column("model_used", sa.Text),
)

agent_messages_table = sa.Table(
    "agent_messages",
    metadata,
    sa.Column("message_id", sa.Text, primary_key=True),
    sa.Column("conversation_id", sa.Text, sa.ForeignKey("agent_conversations.conversation_id")),
    sa.Column("role", sa.Text),
    sa.Column("content", sa.Text),
    sa.Column("created_at", sa.DateTime),
    sa.Column("tokens_used", sa.Integer),
)


def _sqlite_column_exists(conn: sa.Connection, table: str, column: str) -> bool:
    rows = conn.execute(sa.text(f"PRAGMA table_info({table})")).fetchall()
    return column in {row[1] for row in rows}


def _sqlite_table_exists(conn: sa.Connection, table: str) -> bool:
    row = conn.execute(
        sa.text("SELECT name FROM sqlite_master WHERE type='table' AND name=:t"),
        {"t": table},
    ).fetchone()
    return row is not None


def run_migrations(engine: sa.Engine) -> None:
    """Apply schema migrations for existing databases (SQLite only)."""
    dialect = engine.dialect.name
    if dialect != "sqlite":
        return

    with engine.connect() as conn:
        # tickers: Phase 1 migration
        if _sqlite_column_exists(conn, "tickers", "added_date") and not _sqlite_column_exists(conn, "tickers", "cik"):
            conn.execute(sa.text("ALTER TABLE tickers ADD COLUMN cik TEXT"))
            conn.commit()

        # tickers: Phase 3 migrations
        for col, defn in [
            ("asset_type", "TEXT DEFAULT 'equity'"),
            ("exchange", "TEXT"),
            ("is_active", "INTEGER DEFAULT 1"),
            ("first_seen_date", "DATE"),
        ]:
            if _sqlite_table_exists(conn, "tickers") and not _sqlite_column_exists(conn, "tickers", col):
                conn.execute(sa.text(f"ALTER TABLE tickers ADD COLUMN {col} {defn}"))
                conn.commit()

        # stock_prices: Phase 2 migration
        if _sqlite_table_exists(conn, "stock_prices") and not _sqlite_column_exists(conn, "stock_prices", "dollar_volume"):
            conn.execute(sa.text("ALTER TABLE stock_prices ADD COLUMN dollar_volume REAL"))
            conn.commit()

        # Performance indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_sp_ticker ON stock_prices(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_is_ticker ON income_statements(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_bs_ticker ON balance_sheets(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_cf_ticker ON cash_flows(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_es_ticker ON earnings_surprises(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_na_pub_at ON news_articles(published_at)",
            "CREATE INDEX IF NOT EXISTS idx_at_ticker ON article_tickers(ticker)",
        ]:
            conn.execute(sa.text(idx_sql))
        conn.commit()


def init_db(db_path: Path = DB_PATH) -> sa.Engine:
    """Create all tables if they don't exist and run migrations. Returns the engine."""
    engine = get_engine(db_path)
    metadata.create_all(engine)
    run_migrations(engine)
    return engine
