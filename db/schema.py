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
    sa.Column("interest_expense", sa.Float),
    # Phase XBRL normalization additions
    sa.Column("sga", sa.Float),
    sa.Column("rd_expense", sa.Float),
    sa.Column("operating_expenses", sa.Float),
    sa.Column("interest_income", sa.Float),
    sa.Column("other_income_expense", sa.Float),
    sa.Column("ebit", sa.Float),
    sa.Column("net_income_attributable", sa.Float),
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
    sa.Column("inventory", sa.Float),
    sa.Column("accounts_receivable", sa.Float),
    sa.Column("short_term_debt", sa.Float),
    sa.Column("goodwill", sa.Float),
    sa.Column("intangible_assets", sa.Float),
    # Phase XBRL normalization additions
    sa.Column("ppe_net", sa.Float),
    sa.Column("short_term_investments", sa.Float),
    sa.Column("long_term_investments", sa.Float),
    sa.Column("operating_lease_rou", sa.Float),
    sa.Column("finance_lease_rou", sa.Float),
    sa.Column("deferred_revenue", sa.Float),
    sa.Column("accrued_liabilities", sa.Float),
    sa.Column("deferred_tax_assets", sa.Float),
    sa.Column("deferred_tax_liabilities", sa.Float),
    sa.Column("operating_lease_liability", sa.Float),
    sa.Column("finance_lease_liability", sa.Float),
    sa.Column("noncontrolling_interest", sa.Float),
    sa.Column("treasury_stock", sa.Float),
    sa.Column("additional_paid_in_capital", sa.Float),
    sa.Column("other_current_assets", sa.Float),
    sa.Column("other_noncurrent_assets", sa.Float),
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
    sa.Column("depreciation_amortization", sa.Float),
    # Phase XBRL normalization additions
    sa.Column("acquisitions", sa.Float),
    sa.Column("debt_repayment", sa.Float),
    sa.Column("debt_issuance", sa.Float),
    sa.Column("stock_issuance", sa.Float),
    sa.Column("asset_sale_proceeds", sa.Float),
    sa.Column("interest_paid", sa.Float),
    sa.Column("taxes_paid", sa.Float),
    sa.PrimaryKeyConstraint("ticker", "period_end", "period_type"),
)

financial_quality_scores_table = sa.Table(
    "financial_quality_scores",
    metadata,
    sa.Column("ticker", sa.Text, sa.ForeignKey("tickers.ticker"), nullable=False),
    sa.Column("period_end", sa.Date, nullable=False),
    sa.Column("period_type", sa.Text, nullable=False),
    sa.Column("statement_type", sa.Text, nullable=False),  # 'income', 'balance', 'cashflow'
    sa.Column("tag_coverage_score", sa.Float),
    sa.Column("derivation_score", sa.Float),
    sa.Column("context_confidence", sa.Float),
    sa.Column("calc_consistency", sa.Float),
    sa.Column("overall_score", sa.Float),
    sa.PrimaryKeyConstraint("ticker", "period_end", "period_type", "statement_type"),
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
    sa.Column("ev_ebit", sa.Float),
    sa.Column("ev_revenue", sa.Float),
    sa.Column("p_fcf", sa.Float),
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
    # Growth
    sa.Column("revenue_qoq_growth", sa.Float),
    sa.Column("operating_income_yoy_growth", sa.Float),
    sa.Column("ocf_yoy_growth", sa.Float),
    sa.Column("fcf_yoy_growth", sa.Float),
    sa.Column("ebitda_yoy_growth", sa.Float),
    sa.Column("revenue_3yr_cagr", sa.Float),
    sa.Column("revenue_5yr_cagr", sa.Float),
    sa.Column("eps_3yr_cagr", sa.Float),
    sa.Column("eps_5yr_cagr", sa.Float),
    # Margins
    sa.Column("pretax_margin", sa.Float),
    sa.Column("ocf_margin", sa.Float),
    sa.Column("ebitda_margin", sa.Float),
    sa.Column("capex_to_revenue", sa.Float),
    # Returns
    sa.Column("roic", sa.Float),
    sa.Column("roce", sa.Float),
    # Cash flow
    sa.Column("ocf_per_share", sa.Float),
    sa.Column("fcf_per_share", sa.Float),
    sa.Column("cash_conversion_ratio", sa.Float),
    sa.Column("accrual_ratio", sa.Float),
    # Liquidity
    sa.Column("quick_ratio", sa.Float),
    sa.Column("cash_ratio", sa.Float),
    sa.Column("working_capital", sa.Float),
    sa.Column("net_debt", sa.Float),
    # Leverage
    sa.Column("debt_to_assets", sa.Float),
    sa.Column("debt_to_capital", sa.Float),
    sa.Column("equity_ratio", sa.Float),
    sa.Column("net_debt_to_ebitda", sa.Float),
    sa.Column("debt_to_ebitda", sa.Float),
    sa.Column("interest_coverage", sa.Float),
    # Efficiency
    sa.Column("asset_turnover", sa.Float),
    sa.Column("inventory_turnover", sa.Float),
    sa.Column("receivables_turnover", sa.Float),
    sa.Column("payables_turnover", sa.Float),
    sa.Column("dso", sa.Float),
    sa.Column("dio", sa.Float),
    sa.Column("dpo", sa.Float),
    sa.Column("ccc", sa.Float),
    # Per-share
    sa.Column("book_value_per_share", sa.Float),
    sa.Column("tangible_book_value_per_share", sa.Float),
    # Absolute values
    sa.Column("ebitda", sa.Float),
    sa.Column("ocf_ttm", sa.Float),
    sa.Column("fcf_ttm", sa.Float),
    # Shareholder returns
    sa.Column("dividend_yield", sa.Float),
    sa.Column("dividend_payout_ratio", sa.Float),
    sa.Column("buyback_yield", sa.Float),
    sa.Column("shareholder_yield", sa.Float),
    # XBRL normalization additions
    sa.Column("sga_margin", sa.Float),
    sa.Column("rd_margin", sa.Float),
    sa.Column("ppe_turnover", sa.Float),
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

# ── Phase 9 — Validation tables ───────────────────────────────────────────────

validation_runs_table = sa.Table(
    "validation_runs", metadata,
    sa.Column("run_id", sa.Text, primary_key=True),
    sa.Column("triggered_at", sa.DateTime, nullable=False),
    sa.Column("n_tickers", sa.Integer),
    sa.Column("n_periods", sa.Integer),
    sa.Column("overall_pass_rate", sa.Float),
    sa.Column("avg_score", sa.Float),
    sa.Column("triggered_by", sa.Text),
    sa.Column("notes", sa.Text),
)

validation_results_table = sa.Table(
    "validation_results", metadata,
    sa.Column("run_id", sa.Text, sa.ForeignKey("validation_runs.run_id"), nullable=False),
    sa.Column("ticker", sa.Text, nullable=False),
    sa.Column("period_end", sa.Date, nullable=False),
    sa.Column("period_type", sa.Text, nullable=False),
    sa.Column("metric_name", sa.Text, nullable=False),
    sa.Column("pipeline_value", sa.Float),
    sa.Column("fmp_value", sa.Float),
    sa.Column("edgar_value", sa.Float),
    sa.Column("pct_diff_fmp", sa.Float),
    sa.Column("pct_diff_edgar", sa.Float),
    sa.Column("tolerance", sa.Float),
    sa.Column("passed", sa.Integer),
    sa.Column("mismatch_type", sa.Text),
    sa.PrimaryKeyConstraint("run_id", "ticker", "period_end", "period_type", "metric_name"),
)

validation_identity_checks_table = sa.Table(
    "validation_identity_checks", metadata,
    sa.Column("run_id", sa.Text, sa.ForeignKey("validation_runs.run_id"), nullable=False),
    sa.Column("ticker", sa.Text, nullable=False),
    sa.Column("period_end", sa.Date, nullable=False),
    sa.Column("period_type", sa.Text, nullable=False),
    sa.Column("identity_name", sa.Text, nullable=False),
    sa.Column("lhs_value", sa.Float),
    sa.Column("rhs_value", sa.Float),
    sa.Column("diff_pct", sa.Float),
    sa.Column("passed", sa.Integer),
    sa.PrimaryKeyConstraint("run_id", "ticker", "period_end", "period_type", "identity_name"),
)

validation_scores_table = sa.Table(
    "validation_scores", metadata,
    sa.Column("run_id", sa.Text, sa.ForeignKey("validation_runs.run_id"), nullable=False),
    sa.Column("ticker", sa.Text, nullable=False),
    sa.Column("period_end", sa.Date, nullable=False),
    sa.Column("period_type", sa.Text, nullable=False),
    sa.Column("metric_accuracy_score", sa.Float),
    sa.Column("identity_score", sa.Float),
    sa.Column("vendor_agreement_score", sa.Float),
    sa.Column("overall_score", sa.Float),
    sa.Column("n_metrics_evaluated", sa.Integer),
    sa.Column("n_metrics_passed", sa.Integer),
    sa.Column("n_identities_evaluated", sa.Integer),
    sa.Column("n_identities_passed", sa.Integer),
    sa.PrimaryKeyConstraint("run_id", "ticker", "period_end", "period_type"),
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

        # income_statements: metrics expansion
        for col in ["interest_expense"]:
            if _sqlite_table_exists(conn, "income_statements") and not _sqlite_column_exists(conn, "income_statements", col):
                conn.execute(sa.text(f"ALTER TABLE income_statements ADD COLUMN {col} REAL"))
                conn.commit()

        # income_statements: XBRL normalization expansion
        for col in ["sga", "rd_expense", "operating_expenses", "interest_income",
                    "other_income_expense", "ebit", "net_income_attributable"]:
            if _sqlite_table_exists(conn, "income_statements") and not _sqlite_column_exists(conn, "income_statements", col):
                conn.execute(sa.text(f"ALTER TABLE income_statements ADD COLUMN {col} REAL"))
                conn.commit()

        # balance_sheets: metrics expansion
        for col in ["inventory", "accounts_receivable", "short_term_debt", "goodwill", "intangible_assets"]:
            if _sqlite_table_exists(conn, "balance_sheets") and not _sqlite_column_exists(conn, "balance_sheets", col):
                conn.execute(sa.text(f"ALTER TABLE balance_sheets ADD COLUMN {col} REAL"))
                conn.commit()

        # balance_sheets: XBRL normalization expansion
        for col in ["ppe_net", "short_term_investments", "long_term_investments",
                    "operating_lease_rou", "finance_lease_rou", "deferred_revenue",
                    "accrued_liabilities", "deferred_tax_assets", "deferred_tax_liabilities",
                    "operating_lease_liability", "finance_lease_liability",
                    "noncontrolling_interest", "treasury_stock", "additional_paid_in_capital",
                    "other_current_assets", "other_noncurrent_assets"]:
            if _sqlite_table_exists(conn, "balance_sheets") and not _sqlite_column_exists(conn, "balance_sheets", col):
                conn.execute(sa.text(f"ALTER TABLE balance_sheets ADD COLUMN {col} REAL"))
                conn.commit()

        # cash_flows: metrics expansion
        for col in ["depreciation_amortization"]:
            if _sqlite_table_exists(conn, "cash_flows") and not _sqlite_column_exists(conn, "cash_flows", col):
                conn.execute(sa.text(f"ALTER TABLE cash_flows ADD COLUMN {col} REAL"))
                conn.commit()

        # cash_flows: XBRL normalization expansion
        for col in ["acquisitions", "debt_repayment", "debt_issuance", "stock_issuance",
                    "asset_sale_proceeds", "interest_paid", "taxes_paid"]:
            if _sqlite_table_exists(conn, "cash_flows") and not _sqlite_column_exists(conn, "cash_flows", col):
                conn.execute(sa.text(f"ALTER TABLE cash_flows ADD COLUMN {col} REAL"))
                conn.commit()

        # financial_quality_scores table: create if missing
        if not _sqlite_table_exists(conn, "financial_quality_scores"):
            conn.execute(sa.text(
                """
                CREATE TABLE IF NOT EXISTS financial_quality_scores (
                    ticker TEXT NOT NULL,
                    period_end DATE NOT NULL,
                    period_type TEXT NOT NULL,
                    statement_type TEXT NOT NULL,
                    tag_coverage_score REAL,
                    derivation_score REAL,
                    context_confidence REAL,
                    calc_consistency REAL,
                    overall_score REAL,
                    PRIMARY KEY (ticker, period_end, period_type, statement_type)
                )
                """
            ))
            conn.commit()

        # valuation_snapshots: metrics expansion
        for col in ["ev_ebit", "ev_revenue", "p_fcf"]:
            if _sqlite_table_exists(conn, "valuation_snapshots") and not _sqlite_column_exists(conn, "valuation_snapshots", col):
                conn.execute(sa.text(f"ALTER TABLE valuation_snapshots ADD COLUMN {col} REAL"))
                conn.commit()

        # derived_metrics: metrics expansion
        _new_derived_cols = [
            "revenue_qoq_growth", "operating_income_yoy_growth", "ocf_yoy_growth",
            "fcf_yoy_growth", "ebitda_yoy_growth", "revenue_3yr_cagr", "revenue_5yr_cagr",
            "eps_3yr_cagr", "eps_5yr_cagr", "pretax_margin", "ocf_margin", "ebitda_margin",
            "capex_to_revenue", "roic", "roce", "ocf_per_share", "fcf_per_share",
            "cash_conversion_ratio", "accrual_ratio", "quick_ratio", "cash_ratio",
            "working_capital", "net_debt", "debt_to_assets", "debt_to_capital",
            "equity_ratio", "net_debt_to_ebitda", "debt_to_ebitda", "interest_coverage",
            "asset_turnover", "inventory_turnover", "receivables_turnover", "payables_turnover",
            "dso", "dio", "dpo", "ccc", "book_value_per_share", "tangible_book_value_per_share",
            "ebitda", "ocf_ttm", "fcf_ttm", "dividend_yield", "dividend_payout_ratio",
            "buyback_yield", "shareholder_yield",
            "sga_margin", "rd_margin", "ppe_turnover",
        ]
        for col in _new_derived_cols:
            if _sqlite_table_exists(conn, "derived_metrics") and not _sqlite_column_exists(conn, "derived_metrics", col):
                conn.execute(sa.text(f"ALTER TABLE derived_metrics ADD COLUMN {col} REAL"))
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

        # validation tables: create if missing
        for tbl_name, create_sql in [
            ("validation_runs", """
                CREATE TABLE IF NOT EXISTS validation_runs (
                    run_id TEXT PRIMARY KEY,
                    triggered_at DATETIME NOT NULL,
                    n_tickers INTEGER,
                    n_periods INTEGER,
                    overall_pass_rate REAL,
                    avg_score REAL,
                    triggered_by TEXT,
                    notes TEXT
                )
            """),
            ("validation_results", """
                CREATE TABLE IF NOT EXISTS validation_results (
                    run_id TEXT NOT NULL REFERENCES validation_runs(run_id),
                    ticker TEXT NOT NULL,
                    period_end DATE NOT NULL,
                    period_type TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    pipeline_value REAL,
                    fmp_value REAL,
                    edgar_value REAL,
                    pct_diff_fmp REAL,
                    pct_diff_edgar REAL,
                    tolerance REAL,
                    passed INTEGER,
                    mismatch_type TEXT,
                    PRIMARY KEY (run_id, ticker, period_end, period_type, metric_name)
                )
            """),
            ("validation_identity_checks", """
                CREATE TABLE IF NOT EXISTS validation_identity_checks (
                    run_id TEXT NOT NULL REFERENCES validation_runs(run_id),
                    ticker TEXT NOT NULL,
                    period_end DATE NOT NULL,
                    period_type TEXT NOT NULL,
                    identity_name TEXT NOT NULL,
                    lhs_value REAL,
                    rhs_value REAL,
                    diff_pct REAL,
                    passed INTEGER,
                    PRIMARY KEY (run_id, ticker, period_end, period_type, identity_name)
                )
            """),
            ("validation_scores", """
                CREATE TABLE IF NOT EXISTS validation_scores (
                    run_id TEXT NOT NULL REFERENCES validation_runs(run_id),
                    ticker TEXT NOT NULL,
                    period_end DATE NOT NULL,
                    period_type TEXT NOT NULL,
                    metric_accuracy_score REAL,
                    identity_score REAL,
                    vendor_agreement_score REAL,
                    overall_score REAL,
                    n_metrics_evaluated INTEGER,
                    n_metrics_passed INTEGER,
                    n_identities_evaluated INTEGER,
                    n_identities_passed INTEGER,
                    PRIMARY KEY (run_id, ticker, period_end, period_type)
                )
            """),
        ]:
            if not _sqlite_table_exists(conn, tbl_name):
                conn.execute(sa.text(create_sql))
                conn.commit()

        # validation indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_vr_ticker ON validation_results(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_vs_ticker ON validation_scores(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_vr_run ON validation_results(run_id)",
        ]:
            conn.execute(sa.text(idx_sql))
        conn.commit()


def init_db(db_path: Path = DB_PATH) -> sa.Engine:
    """Create all tables if they don't exist and run migrations. Returns the engine."""
    engine = get_engine(db_path)
    metadata.create_all(engine)
    run_migrations(engine)
    return engine
