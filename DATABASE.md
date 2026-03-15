# Needlstack — Database Schema

SQLite at `db/needlstack.db`. 25 tables across 6 logical groups. All foreign keys reference `tickers.ticker`. Primary keys are marked **PK**. All financial values are stored in their native units (dollars, shares, ratios) unless noted.

---

## Entity Relationship Overview

```
                              ┌──────────────────────────────────────────┐
                              │                tickers                   │
                              │  ticker PK · company_name · sector       │
                              │  industry · cik · exchange · is_active   │
                              └─────────────────┬────────────────────────┘
                                                │ FK: ticker
          ┌─────────────────┬──────────────────┼──────────────────┬─────────────────────┐
          │                 │                  │                  │                     │
          ▼                 ▼                  ▼                  ▼                     ▼
  ┌───────────────┐ ┌──────────────┐ ┌────────────────┐ ┌──────────────────┐ ┌──────────────────┐
  │  stock_prices │ │  FINANCIALS  │ │    COMPUTED    │ │  NEWS/SENTIMENT  │ │   INSTITUTIONAL  │
  │  (market data)│ │  (3 tables + │ │  (2 tables)    │ │  (7 tables)      │ │   (3 tables)     │
  └───────────────┘ │  quality)    │ └────────────────┘ └──────────────────┘ └──────────────────┘
                    └──────────────┘
```

---

## Group 1 — Universe

### `tickers`
The master list of all securities. Every other table's FK points here.

| Column | Type | Notes |
|--------|------|-------|
| **ticker** | TEXT PK | e.g. `AAPL` |
| company_name | TEXT | |
| sector | TEXT | GICS sector |
| industry | TEXT | GICS industry |
| cik | TEXT | SEC EDGAR CIK — required for XBRL ingestion |
| exchange | TEXT | NYSE, NASDAQ, etc. |
| asset_type | TEXT | `equity` (default), `etf`, `index`, `macro` |
| is_active | INTEGER | 1 = active, 0 = delisted |
| added_date | DATE | Date added to universe |
| first_seen_date | DATE | Date first encountered in NASDAQ Trader feed |

### `index_constituents`
Tracks membership in SP500, NDX100, DOW30, SP400, SP600.

| Column | Type | Notes |
|--------|------|-------|
| **index_id** | TEXT PK | e.g. `SP500` |
| **ticker** | TEXT PK | FK → tickers |
| **added_date** | DATE PK | |
| removed_date | DATE | NULL = still a member |
| weight | FLOAT | Index weight (if available) |

---

## Group 2 — Market Data

### `stock_prices`
Daily OHLCV. ~1,500 rows per ticker from 2020 to present.

| Column | Type | Notes |
|--------|------|-------|
| **ticker** | TEXT PK | FK → tickers |
| **date** | DATE PK | |
| open | FLOAT | |
| high | FLOAT | |
| low | FLOAT | |
| close | FLOAT | |
| adj_close | FLOAT | Split/dividend adjusted |
| volume | INTEGER | |
| dollar_volume | FLOAT | `close × volume` — for liquidity screening |

### `security_metadata`
One row per ticker — overwritten on each weekly run.

| Column | Type | Notes |
|--------|------|-------|
| **ticker** | TEXT PK | FK → tickers |
| shares_outstanding | FLOAT | |
| float_shares | FLOAT | |
| market_cap | FLOAT | USD |
| enterprise_value | FLOAT | USD |
| avg_volume_30d | FLOAT | 30-day average shares volume |
| avg_dollar_vol_30d | FLOAT | 30-day average dollar volume |
| updated_at | DATETIME | |

### `corporate_actions`
Stock splits and dividend payments.

| Column | Type | Notes |
|--------|------|-------|
| **ticker** | TEXT PK | FK → tickers |
| **action_date** | DATE PK | |
| **action_type** | TEXT PK | `split` or `dividend` |
| ratio | FLOAT | Split ratio (e.g. 4.0 for 4:1) |
| amount | FLOAT | Dividend amount per share (USD) |
| notes | TEXT | |

---

## Group 3 — Financials (SEC EDGAR XBRL)

All three statement tables share the same compound primary key: `(ticker, period_end, period_type)`. `period_type` is `A` (annual) or `Q` (quarterly). All values in USD (or USD/share for EPS).

### `income_statements`

| Column | Type | Notes |
|--------|------|-------|
| **ticker** | TEXT PK | FK → tickers |
| **period_end** | DATE PK | |
| **period_type** | TEXT PK | `A` or `Q` |
| fiscal_year | INTEGER | |
| fiscal_quarter | INTEGER | 1–4, NULL for annual |
| form_type | TEXT | `10-K` or `10-Q` |
| filed_date | DATE | |
| revenue | FLOAT | |
| cost_of_revenue | FLOAT | |
| gross_profit | FLOAT | May be derived: `revenue − cost_of_revenue` |
| sga | FLOAT | Selling, general & administrative expense |
| rd_expense | FLOAT | Research & development expense |
| operating_expenses | FLOAT | Total operating expenses (if reported as single line) |
| operating_income | FLOAT | May be derived |
| ebit | FLOAT | Alias for operating_income |
| interest_income | FLOAT | |
| interest_expense | FLOAT | |
| other_income_expense | FLOAT | Non-operating items |
| pretax_income | FLOAT | |
| income_tax | FLOAT | |
| net_income | FLOAT | May be derived: `pretax_income − income_tax` |
| net_income_attributable | FLOAT | Net income to parent (excl. NCI) |
| eps_basic | FLOAT | USD/share |
| eps_diluted | FLOAT | USD/share |
| shares_basic | FLOAT | Weighted average basic shares |
| shares_diluted | FLOAT | Weighted average diluted shares |

### `balance_sheets`

| Column | Type | Notes |
|--------|------|-------|
| **ticker** | TEXT PK | FK → tickers |
| **period_end** | DATE PK | |
| **period_type** | TEXT PK | `A` or `Q` |
| filed_date | DATE | |
| **Assets** | | |
| cash | FLOAT | Cash & equivalents |
| short_term_investments | FLOAT | Marketable securities (current) |
| long_term_investments | FLOAT | Marketable securities (non-current) |
| accounts_receivable | FLOAT | Net current |
| inventory | FLOAT | |
| other_current_assets | FLOAT | |
| current_assets | FLOAT | Total current assets |
| ppe_net | FLOAT | Property, plant & equipment (net) |
| operating_lease_rou | FLOAT | Right-of-use asset (operating leases) |
| finance_lease_rou | FLOAT | Right-of-use asset (finance leases) |
| goodwill | FLOAT | |
| intangible_assets | FLOAT | Finite-lived intangibles (net) |
| deferred_tax_assets | FLOAT | |
| other_noncurrent_assets | FLOAT | |
| total_assets | FLOAT | |
| **Liabilities** | | |
| accounts_payable | FLOAT | |
| accrued_liabilities | FLOAT | |
| deferred_revenue | FLOAT | Contract liabilities |
| short_term_debt | FLOAT | Current portion of debt |
| operating_lease_liability | FLOAT | Total (current + non-current) |
| finance_lease_liability | FLOAT | |
| current_liabilities | FLOAT | Total current liabilities |
| long_term_debt | FLOAT | Non-current portion |
| deferred_tax_liabilities | FLOAT | |
| total_liabilities | FLOAT | |
| **Equity** | | |
| additional_paid_in_capital | FLOAT | |
| retained_earnings | FLOAT | Accumulated deficit if negative |
| treasury_stock | FLOAT | |
| noncontrolling_interest | FLOAT | Minority interest |
| stockholders_equity | FLOAT | May be derived: `total_assets − total_liabilities` |

### `cash_flows`

| Column | Type | Notes |
|--------|------|-------|
| **ticker** | TEXT PK | FK → tickers |
| **period_end** | DATE PK | |
| **period_type** | TEXT PK | `A` or `Q` |
| filed_date | DATE | |
| **Operating** | | |
| operating_cf | FLOAT | Net cash from operations |
| depreciation_amortization | FLOAT | Non-cash add-back |
| **Investing** | | |
| capex | FLOAT | Capital expenditures (reported as positive outflow) |
| acquisitions | FLOAT | Business acquisitions (net of cash acquired) |
| asset_sale_proceeds | FLOAT | Proceeds from asset/business sales |
| investing_cf | FLOAT | Net cash from investing |
| **Financing** | | |
| debt_repayment | FLOAT | Repayments of long-term debt |
| debt_issuance | FLOAT | Proceeds from new debt |
| stock_issuance | FLOAT | Proceeds from equity issuance |
| dividends_paid | FLOAT | |
| stock_repurchases | FLOAT | Buybacks |
| financing_cf | FLOAT | Net cash from financing |
| **Supplemental** | | |
| interest_paid | FLOAT | Cash interest paid |
| taxes_paid | FLOAT | Cash taxes paid |
| free_cash_flow | FLOAT | Derived: `operating_cf − abs(capex)` |

### `financial_quality_scores`
Per-period quality scores for each statement type. Written alongside each upsert.

| Column | Type | Notes |
|--------|------|-------|
| **ticker** | TEXT PK | FK → tickers |
| **period_end** | DATE PK | |
| **period_type** | TEXT PK | |
| **statement_type** | TEXT PK | `income`, `balance`, or `cashflow` |
| tag_coverage_score | FLOAT | Fraction of expected fields with non-null values |
| derivation_score | FLOAT | Fraction of populated fields that were directly tagged (higher = better) |
| context_confidence | FLOAT | Reserved — avg confidence of selected candidate facts |
| calc_consistency | FLOAT | Arithmetic identity check score |
| overall_score | FLOAT | Weighted composite (0–100) |

---

## Group 4 — Computed

### `valuation_snapshots`
One row per (ticker, trading day). Populated by `ingestion/valuations.py` from existing DB data — no external API calls.

| Column | Type | Notes |
|--------|------|-------|
| **ticker** | TEXT PK | FK → tickers |
| **snapshot_date** | DATE PK | |
| pe_ttm | FLOAT | Price / TTM EPS diluted |
| pb | FLOAT | Market cap / (equity / shares) |
| ps_ttm | FLOAT | Market cap / TTM revenue |
| ev_ebitda | FLOAT | EV / (TTM op income + TTM D&A) |
| ev_ebit | FLOAT | EV / TTM operating income |
| ev_revenue | FLOAT | EV / TTM revenue |
| p_fcf | FLOAT | Market cap / TTM free cash flow |
| peg_ratio | FLOAT | P/E TTM / EPS growth YoY |

### `derived_metrics`
One row per (ticker, date). 60+ metrics computed by `analysis/compute_metrics.py`.

| Column | Type | Category |
|--------|------|----------|
| **ticker** | TEXT PK | FK → tickers |
| **date** | DATE PK | |
| **Growth** | | |
| revenue_yoy_growth | FLOAT | YoY % |
| revenue_qoq_growth | FLOAT | QoQ % |
| revenue_3yr_cagr | FLOAT | 3-year CAGR |
| revenue_5yr_cagr | FLOAT | 5-year CAGR |
| net_income_yoy_growth | FLOAT | |
| eps_yoy_growth | FLOAT | |
| eps_3yr_cagr | FLOAT | |
| eps_5yr_cagr | FLOAT | |
| operating_income_yoy_growth | FLOAT | |
| ebitda_yoy_growth | FLOAT | |
| ocf_yoy_growth | FLOAT | |
| fcf_yoy_growth | FLOAT | |
| **Margins (TTM-based)** | | |
| gross_margin | FLOAT | Gross profit / revenue |
| operating_margin | FLOAT | Operating income / revenue |
| net_margin | FLOAT | Net income / revenue |
| pretax_margin | FLOAT | |
| ebitda_margin | FLOAT | |
| ocf_margin | FLOAT | |
| fcf_margin | FLOAT | |
| capex_to_revenue | FLOAT | |
| sga_margin | FLOAT | SG&A / revenue |
| rd_margin | FLOAT | R&D / revenue |
| **Returns** | | |
| roe | FLOAT | TTM net income / avg equity |
| roa | FLOAT | TTM net income / avg assets |
| roic | FLOAT | NOPAT / invested capital |
| roce | FLOAT | Op income / (assets − current liabilities) |
| **Liquidity** | | |
| current_ratio | FLOAT | Current assets / current liabilities |
| quick_ratio | FLOAT | (Current assets − inventory) / current liabilities |
| cash_ratio | FLOAT | Cash / current liabilities |
| working_capital | FLOAT | Current assets − current liabilities (USD) |
| net_debt | FLOAT | LT debt + ST debt − cash (USD) |
| **Leverage** | | |
| debt_to_equity | FLOAT | |
| debt_to_assets | FLOAT | |
| debt_to_capital | FLOAT | |
| equity_ratio | FLOAT | |
| net_debt_to_ebitda | FLOAT | |
| debt_to_ebitda | FLOAT | |
| interest_coverage | FLOAT | Operating income / interest expense |
| **Efficiency** | | |
| asset_turnover | FLOAT | Revenue / avg total assets |
| inventory_turnover | FLOAT | COGS / avg inventory |
| receivables_turnover | FLOAT | Revenue / avg accounts receivable |
| payables_turnover | FLOAT | COGS / avg accounts payable |
| dso | FLOAT | Days sales outstanding |
| dio | FLOAT | Days inventory outstanding |
| dpo | FLOAT | Days payables outstanding |
| ccc | FLOAT | Cash conversion cycle (DSO + DIO − DPO) |
| ppe_turnover | FLOAT | Revenue / avg net PPE |
| **Cash Flow** | | |
| ebitda | FLOAT | TTM (USD) |
| ocf_ttm | FLOAT | TTM operating cash flow (USD) |
| fcf_ttm | FLOAT | TTM free cash flow (USD) |
| ocf_per_share | FLOAT | |
| fcf_per_share | FLOAT | |
| cash_conversion_ratio | FLOAT | OCF / net income |
| accrual_ratio | FLOAT | (Net income − OCF) / avg assets |
| **Per-Share** | | |
| book_value_per_share | FLOAT | |
| tangible_book_value_per_share | FLOAT | (Equity − goodwill − intangibles) / shares |
| **Valuation (from snapshots)** | | |
| pe_ttm | FLOAT | Pulled from valuation_snapshots |
| ev_ebitda | FLOAT | Pulled from valuation_snapshots |
| **Shareholder Returns** | | |
| dividend_yield | FLOAT | TTM dividends / market cap |
| dividend_payout_ratio | FLOAT | TTM dividends / TTM net income |
| buyback_yield | FLOAT | TTM buybacks / market cap |
| shareholder_yield | FLOAT | Dividend yield + buyback yield |

---

## Group 5 — News & Sentiment

```
news_sources ──► news_articles ──► article_tickers  (many-to-many via article_id × ticker)
                      │
                      └──► article_sentiment (one-to-one)

ticker_sentiment_daily (daily rollup — no FK to articles)
```

### `news_sources`

| Column | Type | Notes |
|--------|------|-------|
| **source_id** | TEXT PK | e.g. `reuters`, `seekingalpha` |
| name | TEXT | Display name |
| rss_url | TEXT | Feed URL |
| is_active | INTEGER | 1 = poll this source |
| fetch_interval_min | INTEGER | Default 30 |
| last_fetched_at | DATETIME | |

### `news_articles`

| Column | Type | Notes |
|--------|------|-------|
| **article_id** | TEXT PK | SHA-256 of URL |
| source_id | TEXT | FK → news_sources |
| url | TEXT | Unique |
| title | TEXT | |
| author | TEXT | |
| published_at | DATETIME | |
| fetched_at | DATETIME | |
| full_text | TEXT | Extracted by trafilatura; NULL until article_fetch runs |
| raw_rss_summary | TEXT | Original RSS `<description>` |
| word_count | INTEGER | |
| is_paywalled | INTEGER | 1 if full_text extraction returned <100 words |
| categories | TEXT | Comma-separated RSS categories |

### `article_tickers`

| Column | Type | Notes |
|--------|------|-------|
| **article_id** | TEXT PK | FK → news_articles |
| **ticker** | TEXT PK | FK → tickers |
| mention_count | INTEGER | Total mentions in body |
| mention_in_title | INTEGER | 1 if ticker mentioned in title |

### `article_sentiment`

| Column | Type | Notes |
|--------|------|-------|
| **article_id** | TEXT PK | FK → news_articles |
| compound_score | FLOAT | VADER compound −1.0 to +1.0 |
| positive | FLOAT | VADER pos component |
| negative | FLOAT | VADER neg component |
| neutral | FLOAT | VADER neu component |
| sentiment_label | TEXT | `bullish`, `bearish`, or `neutral` |
| scored_at | DATETIME | |
| model_version | TEXT | e.g. `vader-3.3.2` |

### `ticker_sentiment_daily`
Daily rollup across all articles mentioning a ticker. Written by `daily_sentiment_agg.py`.

| Column | Type | Notes |
|--------|------|-------|
| **ticker** | TEXT PK | FK → tickers |
| **date** | DATE PK | |
| mention_count | INTEGER | Total ticker mentions across all articles |
| article_count | INTEGER | Number of distinct articles |
| source_count | INTEGER | Number of distinct news sources |
| avg_sentiment | FLOAT | Average VADER compound score |
| bullish_count | INTEGER | Articles labeled bullish |
| bearish_count | INTEGER | Articles labeled bearish |
| neutral_count | INTEGER | Articles labeled neutral |
| title_mention_count | INTEGER | Articles where ticker was in the headline |

---

## Group 6 — Social Sentiment

Same three-table structure as news, but for Reddit posts/comments and StockTwits messages.

### `content_items`

| Column | Type | Notes |
|--------|------|-------|
| **content_id** | TEXT PK | Hash of source + external_id |
| source_type | TEXT | `reddit` or `stocktwits` |
| source_id | TEXT | Subreddit name or `stocktwits` |
| external_id | TEXT | Reddit post/comment ID or StockTwits message ID |
| url | TEXT | |
| title | TEXT | Reddit post title (NULL for comments) |
| author | TEXT | Username |
| published_at | DATETIME | |
| fetched_at | DATETIME | |
| body_text | TEXT | Post/comment body |
| word_count | INTEGER | |
| engagement_score | FLOAT | Reddit: upvotes; StockTwits: likes |
| raw_json | TEXT | Full API response stored for reprocessing |

### `content_tickers`

| Column | Type | Notes |
|--------|------|-------|
| **content_id** | TEXT PK | FK → content_items |
| **ticker** | TEXT PK | FK → tickers |
| mention_count | INTEGER | |
| mention_in_title | INTEGER | 1 if in post title |
| confidence | FLOAT | Extraction confidence score |

### `content_sentiment`

| Column | Type | Notes |
|--------|------|-------|
| **content_id** | TEXT PK | FK → content_items |
| compound_score | FLOAT | VADER compound |
| positive | FLOAT | |
| negative | FLOAT | |
| neutral | FLOAT | |
| sentiment_label | TEXT | `bullish`, `bearish`, or `neutral` |
| scored_at | DATETIME | |
| model_version | TEXT | |

---

## Group 7 — Market Narratives

### `narratives`
User-defined investment themes. Seeded from `data/seed_narratives.json`.

| Column | Type | Notes |
|--------|------|-------|
| **narrative_id** | TEXT PK | e.g. `ai_capex` |
| name | TEXT | Display name |
| description | TEXT | |
| keywords | TEXT | JSON array of keyword phrases |
| related_tickers | TEXT | JSON array of tickers |
| created_at | DATETIME | |
| last_seen_at | DATETIME | Last date with any signal |
| is_active | INTEGER | |

### `narrative_signals`
Daily mention counts per narrative, computed by `daily_narrative.py`.

| Column | Type | Notes |
|--------|------|-------|
| **narrative_id** | TEXT PK | FK → narratives |
| **date** | DATE PK | |
| mention_count | INTEGER | Articles mentioning any keyword that day |
| momentum_score | FLOAT | Rolling change in mention frequency |

---

## Group 8 — Institutional Holdings

### `institutional_holdings`
One row per fund–ticker–quarter. Parsed from SEC EDGAR 13F-HR filings.

| Column | Type | Notes |
|--------|------|-------|
| **ticker** | TEXT PK | FK → tickers |
| **institution_cik** | TEXT PK | SEC CIK of the fund |
| **report_date** | DATE PK | Quarter-end date |
| institution_name | TEXT | |
| filed_date | DATE | Date 13F was filed |
| shares_held | FLOAT | Shares at quarter-end |
| market_value | FLOAT | USD at quarter-end |
| pct_of_portfolio | FLOAT | % of fund's reported AUM |
| change_shares | FLOAT | Change from prior quarter |

### `institutional_summary`
Aggregate view per ticker — one row, overwritten each quarter.

| Column | Type | Notes |
|--------|------|-------|
| **ticker** | TEXT PK | FK → tickers |
| report_date | DATE | Most recent quarter |
| total_institutions | INTEGER | Count of distinct filers holding this stock |
| total_shares_held | FLOAT | Sum across all institutions |
| pct_outstanding_held | FLOAT | % of shares_outstanding held institutionally |
| net_change_shares | FLOAT | Sum of change_shares (net institutional flow) |
| top_holder_name | TEXT | Largest holder by market value |
| top_holder_pct | FLOAT | Top holder's % of portfolio |
| updated_at | DATETIME | |

---

## Group 9 — SEC Filings

### `sec_filings`
8-K event filings from EDGAR. Not full text — metadata only.

| Column | Type | Notes |
|--------|------|-------|
| **accession_number** | TEXT PK | EDGAR accession number with dashes |
| ticker | TEXT | FK → tickers |
| cik | TEXT | |
| form_type | TEXT | `8-K`, `8-K/A`, etc. |
| filed_date | DATE | |
| period_of_report | DATE | |
| primary_doc_url | TEXT | Direct URL to the filing document |
| items_reported | TEXT | Comma-separated item numbers (e.g. `1.01,9.01`) |

---

## Group 10 — Agent Conversations

### `agent_conversations`

| Column | Type | Notes |
|--------|------|-------|
| **conversation_id** | TEXT PK | UUID |
| created_at | DATETIME | |
| context_tickers | TEXT | JSON array of tickers passed as context |
| model_used | TEXT | e.g. `claude-sonnet-4-6` |

### `agent_messages`

| Column | Type | Notes |
|--------|------|-------|
| **message_id** | TEXT PK | UUID |
| conversation_id | TEXT | FK → agent_conversations |
| role | TEXT | `user`, `assistant`, or `tool` |
| content | TEXT | Message text or tool result JSON |
| created_at | DATETIME | |
| tokens_used | INTEGER | Token count for assistant turns |

---

## Group 11 — Validation

Stores results from `scripts/run_validation.py` runs.

```
validation_runs
    ├── validation_results         (one row per ticker × period × metric)
    ├── validation_identity_checks (one row per ticker × period × identity check)
    └── validation_scores          (one row per ticker × period — aggregate)
```

### `validation_runs`

| Column | Type | Notes |
|--------|------|-------|
| **run_id** | TEXT PK | UUID |
| triggered_at | DATETIME | |
| n_tickers | INTEGER | |
| n_periods | INTEGER | |
| overall_pass_rate | FLOAT | Fraction of fields that passed |
| avg_score | FLOAT | Average score across all periods (0–100) |
| triggered_by | TEXT | `manual`, `cron`, etc. |
| notes | TEXT | Free-text annotation |

### `validation_results`
Field-level comparison result.

| Column | Type | Notes |
|--------|------|-------|
| **run_id** | TEXT PK | FK → validation_runs |
| **ticker** | TEXT PK | |
| **period_end** | DATE PK | |
| **period_type** | TEXT PK | |
| **metric_name** | TEXT PK | e.g. `revenue`, `capex` |
| pipeline_value | FLOAT | Value from our DB |
| fmp_value | FLOAT | Value from FMP reference vendor |
| edgar_value | FLOAT | Value directly from EDGAR API (future use) |
| pct_diff_fmp | FLOAT | `(pipeline − fmp) / abs(fmp)` |
| pct_diff_edgar | FLOAT | |
| tolerance | FLOAT | Threshold used for pass/fail |
| passed | INTEGER | 1 = pass, 0 = fail |
| mismatch_type | TEXT | `missing_pipeline`, `missing_vendor`, `pipeline_error`, `vendor_disagreement` |

### `validation_identity_checks`
Arithmetic consistency checks per period.

| Column | Type | Notes |
|--------|------|-------|
| **run_id** | TEXT PK | FK → validation_runs |
| **ticker** | TEXT PK | |
| **period_end** | DATE PK | |
| **period_type** | TEXT PK | |
| **identity_name** | TEXT PK | e.g. `gross_profit_eq_rev_minus_cogs` |
| lhs_value | FLOAT | Left-hand side of the identity |
| rhs_value | FLOAT | Right-hand side |
| diff_pct | FLOAT | `(lhs − rhs) / abs(rhs)` |
| passed | INTEGER | 1 if diff_pct < tolerance |

### `validation_scores`
Aggregate score per ticker-period.

| Column | Type | Notes |
|--------|------|-------|
| **run_id** | TEXT PK | FK → validation_runs |
| **ticker** | TEXT PK | |
| **period_end** | DATE PK | |
| **period_type** | TEXT PK | |
| metric_accuracy_score | FLOAT | |
| identity_score | FLOAT | |
| vendor_agreement_score | FLOAT | |
| overall_score | FLOAT | Composite 0–100 |
| n_metrics_evaluated | INTEGER | |
| n_metrics_passed | INTEGER | |
| n_identities_evaluated | INTEGER | |
| n_identities_passed | INTEGER | |

---

## Indexes

| Index | Table | Column(s) | Purpose |
|-------|-------|-----------|---------|
| idx_sp_ticker | stock_prices | ticker | Ticker lookups |
| idx_is_ticker | income_statements | ticker | |
| idx_bs_ticker | balance_sheets | ticker | |
| idx_cf_ticker | cash_flows | ticker | |
| idx_es_ticker | earnings_surprises | ticker | |
| idx_na_pub_at | news_articles | published_at | Recency queries |
| idx_at_ticker | article_tickers | ticker | Sentiment joins |
| idx_vr_ticker | validation_results | ticker | Validation queries |
| idx_vs_ticker | validation_scores | ticker | |
| idx_vr_run | validation_results | run_id | Run-level aggregation |

---

## Key Design Notes

- **Single source of truth**: everything except the SPA and agent reads from and writes to this DB
- **No ORM**: tables are defined as `sa.Table` objects; all queries use `sa.text()` with named parameters
- **Migrations**: `run_migrations()` applies `ALTER TABLE ... ADD COLUMN` guarded by `_sqlite_column_exists()` — no migration framework, no downtime
- **Postgres-ready**: `DATABASE_URL` env var switches `get_engine()` from SQLite to Postgres; migrations skip on non-SQLite dialects
- **Idempotent upserts**: all ingestion uses `INSERT OR REPLACE` keyed on natural business keys — safe to re-run
- **WAL mode**: SQLite is configured with WAL journal mode + synchronous=NORMAL for concurrent read performance
