# Needlstack — Claude Project Instructions

## Project Overview

Needlstack is a financial analysis platform at needlstack.com. It ingests market data, earnings filings, news, and social sentiment and surfaces insights via an interactive SPA on GitHub Pages.

## Current State (Phases 1–9 complete as of 2026-03-21)

### Data Pipeline
- **Prices**: yfinance daily OHLCV → `stock_prices` table + `docs/data/prices/{T}.json`
- **Financials**: SEC EDGAR XBRL API (10-K/10-Q) → `income_statements`, `balance_sheets`, `cash_flows`
  - Fields: revenue, gross_profit, operating_income, pretax_income, income_tax, net_income, eps_diluted, interest_expense, cost_of_revenue (income); cash, current_assets/liabilities, total_assets, long_term_debt, total_liabilities, equity, inventory, accounts_receivable, short_term_debt, goodwill, intangible_assets (balance); operating_cf, capex, investing_cf, financing_cf, dividends_paid, stock_repurchases, free_cash_flow, depreciation_amortization (cashflow)
- **Metadata**: yfinance market_cap, float, shares, enterprise_value, avg volumes → `security_metadata`
- **Valuations**: computed from DB — pe_ttm, pb, ps_ttm, ev_ebitda (proper = op_income + D&A), ev_ebit, ev_revenue, p_fcf → `valuation_snapshots`
- **Derived Metrics**: `analysis/compute_metrics.py` → 60+ metrics per ticker → `derived_metrics` table
- **News/RSS**: feedparser + trafilatura → `news_articles`, `article_tickers`, `article_sentiment`
- **Social**: praw (Reddit) + StockTwits → `content_items`, `content_tickers`, `content_sentiment`
- **Sentiment**: VADER → `ticker_sentiment_daily`
- **Narratives**: keyword-phrase detection → `narratives`, `narrative_signals`
- **AI Agent**: Claude claude-sonnet-4-6 via Anthropic SDK → `agent/runner.py`, `agent/api.py` (FastAPI)
- **Bulk Reference Data**: SEC EDGAR submissions (SIC, fiscal year, entity type), OpenFIGI (FIGI, security type), Alpha Vantage (IPO dates), FMP (float, calendars) → `sic_lookup`, `identifier_crosswalk`, `ticker_history`, `event_calendar`, `source_lineage`
- **Email Notifications**: Resend API for changelog emails → `ingestion/email.py`, `scripts/send_changelog.py`
- **Feedback**: email reply ingestion + Claude classification → `feedback` table, `ingestion/feedback.py`, `ingestion/feedback_classifier.py`

### Data Serving
- All exported JSON committed to `docs/data/` and served by GitHub Pages (no R2/CDN)
- Frontend `DATA_BASE_URL` uses relative `/data/` path
- R2 migration deferred until Cloudflare spending controls are in place

### DB Schema (38 tables)
Key tables: `tickers`, `stock_prices`, `income_statements`, `balance_sheets`, `cash_flows`, `security_metadata`, `valuation_snapshots`, `derived_metrics`, `institutional_holdings`, `institutional_summary`, `sec_filings`, `news_sources`, `news_articles`, `article_tickers`, `article_sentiment`, `ticker_sentiment_daily`, `content_items`, `content_tickers`, `content_sentiment`, `narratives`, `narrative_signals`, `sic_lookup`, `identifier_crosswalk`, `ticker_history`, `event_calendar`, `source_lineage`, `feedback`
- `tickers` table new columns: `sic_code`, `sic_description`, `security_type`, `currency`, `country`, `ipo_date`, `delist_date`, `fiscal_year_end`, `state_of_incorporation`, `entity_type`, `sub_industry`
- `security_metadata` new columns: `short_interest`, `short_ratio`, `insider_pct`, `institutional_pct`
- `company_profiles` new columns: `ceo`, `phone`, `reporting_currency`, `accounting_standard`

### Derived Metrics Computed (analysis/compute_metrics.py)
- **Growth**: revenue_yoy, net_income_yoy, eps_yoy, operating_income_yoy, ocf_yoy, fcf_yoy, ebitda_yoy, revenue_qoq, revenue_3yr_cagr, revenue_5yr_cagr, eps_3yr_cagr, eps_5yr_cagr
- **Margins**: gross, operating, net, pretax, ocf, ebitda, fcf, capex_to_revenue
- **Returns**: roe, roa, roic, roce
- **Liquidity**: current_ratio, quick_ratio, cash_ratio, working_capital, net_debt
- **Leverage**: debt_to_equity, debt_to_assets, debt_to_capital, equity_ratio, net_debt_to_ebitda, debt_to_ebitda, interest_coverage
- **Efficiency**: asset_turnover, inventory_turnover, receivables_turnover, payables_turnover, dso, dio, dpo, ccc
- **Cash Flow**: ocf_ttm, fcf_ttm, ebitda, ocf_per_share, fcf_per_share, cash_conversion_ratio, accrual_ratio
- **Per-share**: book_value_per_share, tangible_book_value_per_share
- **Shareholder returns**: dividend_yield, dividend_payout_ratio, buyback_yield, shareholder_yield

### Frontend (docs/)
- `docs/index.html` — SPA with Plotly.js (CDN)
- `docs/assets/main.js` — all client-side logic
- `docs/assets/style.css` — dark-mode UI
- **7 tabs**: Ownership, Filings, News, Social, Narratives, Metrics, AI Chat
- **Data files per ticker** (10 total): prices, financials, metadata, corporate_actions, profiles, ownership, sentiment, news, social, **metrics**
- Metrics tab: 10 categories, 60+ metric cards with trend indicators
- Valuation data: fetched from `financials/{T}.json` → `valuation_snapshots` key

### Cron Schedule (GitHub Actions)
- Every 2h: *(none currently, CI deploys on push)*
- Daily `0 23 * * 1-5`: `daily_financials.py`, `daily_macro.py`, `daily_valuations.py`, `daily_metrics.py`, `daily_filings.py`, `daily_sentiment_agg.py`
- Every 30min: `rss_poll.py`, `social_poll.py`
- Every 15min: `article_fetch.py`
- Daily `0 1 * * *`: `daily_sentiment_agg.py`
- Daily `0 2 * * *`: `daily_narrative.py`
- Weekly: `weekly_metadata.py`, `weekly_profiles.py`, `weekly_fmp_bulk.py` (Mon 7am), `weekly_event_calendar.py` (Mon 8am)
- Quarterly: `quarterly_13f.py`

### Export
- `scripts/export_data.py` — exports all 10 file types per ticker + 4 global files
- `LOCAL_EXPORT=1 python scripts/export_data.py` to write to docs/data/
- Data committed to git and served by GitHub Pages — no R2 upload currently

### Email Notifications
- `scripts/send_changelog.py v4.0 [--dry-run]` — send changelog to subscribers
- `data/subscribers.json` — manual subscriber list (gitignored)
- Resend API for sending, replies go to Gmail (RESEND_REPLY_TO)
- `scripts/classify_feedback.py` — classify feedback with Claude
- `scripts/feedback_report.py` — print feedback summary

## Tech Stack

- **Python 3.11+**, pandas, SQLAlchemy, SQLite (Postgres optional via DATABASE_URL)
- **Vanilla JS** (no framework), Plotly.js 2.35.0
- **GitHub Pages** (main branch → /docs), custom domain needlstack.com via GoDaddy
- **FastAPI + uvicorn** for agent API at api.needlstack.com (not yet deployed)
- **Claude claude-sonnet-4-6** via Anthropic SDK for AI agent
- **Resend** for email notifications (free tier, 100/day)

## Code Conventions

- Python 3.11+ syntax
- Use `pathlib` over `os.path`
- Use `_safe_divide()` in `analysis/compute_metrics.py` for all ratio computations
- Use `_yoy_growth()` for YoY growth computations
- Use `_compute_ttm()` / `_compute_ttm_prior_year()` for TTM aggregations
- Use `_sqlite_column_exists()` in `db/schema.py` for migration guards
- Use `_write_or_upload()` in `scripts/export_data.py` for all JSON writes
- Keep secrets in `.env` — never commit. DB at `db/needlstack.db` (gitignored)

## What NOT to do

- Do not hardcode ticker lists — always make configurable
- **Do not scope scripts to S&P 500** — always use `get_active_tickers(asset_types=["equity"])` for the full universe
- Do not mix web frontend code with data pipeline code
- Do not commit `.env`, `data/`, or `db/` to git
- Do not use `X | None` union syntax (use `Optional[X]`)
- yfinance ingestion: max 2 threads, 0.5s delay, exponential backoff on rate limits
