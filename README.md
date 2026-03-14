# Needlstack

A personal financial analysis platform that ingests market data, SEC filings, news, and social sentiment into a local data lake and surfaces insights through an interactive web UI and an AI research agent.

Live site: **[needlstack.com](https://needlstack.com)**

---

## Architecture

There are two lenses to understand Needlstack: **the system** (what talks to what, where data lives, how it moves) and **the repository** (how code is organized into directories). This section covers both and explains how they map to each other.

---

### System Architecture — Data Funnel & Web Stack

This diagram shows the runtime picture: external data sources on the left, the database in the middle, and the two consumer surfaces (website and AI agent) on the right.

```
  EXTERNAL SOURCES                 LOCAL MACHINE                          PUBLIC
  ─────────────────     ─────────────────────────────────────     ──────────────────

  SEC EDGAR (XBRL) ──►
  yfinance         ──►  ┌──────────────────────────────────┐
  RSS Feeds        ──►  │      INGESTION (scheduled)       │
  Reddit (praw)    ──►  │  pull → parse → upsert to DB     │
  StockTwits       ──►  └──────────────┬───────────────────┘
                                       │
                                       ▼
                         ┌─────────────────────────┐
                         │   SQLite DB             │
                         │   db/needlstack.db      │
                         │   21+ tables            │
                         └────────┬────────────────┘
                                  │
                    ┌─────────────┼──────────────────┐
                    │             │                  │
                    ▼             ▼                  ▼
            ┌──────────────┐  ┌──────────┐   ┌────────────────┐
            │  ANALYSIS    │  │  AGENT   │   │    EXPORT      │
            │  compute 60+ │  │  Claude  │   │  → JSON files  │
            │  metrics     │  │  tool    │   │                │
            │  valuations  │  │  loop    │   └───────┬────────┘
            └──────┬───────┘  └────┬─────┘          │
                   │               │                 ▼
                   │ (writes back  │          ┌─────────────────┐
                   │  to DB)       │          │  Cloudflare R2  │
                   │               │          │  data.needl-    │
                   │               │          │  stack.com      │
                   │               ▼          └────────┬────────┘
                   │        ┌────────────┐            │
                   │        │  FastAPI   │            │  (CDN, served
                   │        │  api.needl-│            │   as static
                   │        │  stack.com │            │   JSON over
                   │        └─────▲──────┘            │   HTTPS)
                   │              │                   │
                   │              │           ┌───────▼────────────┐
                   └──────────────┘           │  GitHub Pages SPA  │
                   (analysis writes           │  needlstack.com    │
                    to DB; agent              │  Plotly.js + JS    │
                    reads from DB             │  7 tabs            │
                    via tools)                └────────────────────┘
```

**Key design decisions visible in this diagram:**

- **The DB is the single source of truth.** All ingestion writes to it. Analysis reads from it and writes back. The export layer reads from it to produce JSON.
- **The website has no backend.** The SPA fetches pre-built JSON files from Cloudflare R2 directly in the browser. There are no database queries at request time on the web path — everything is pre-computed.
- **The AI agent is the only live query path.** When a user asks a question in the AI Chat tab, the browser calls `api.needlstack.com`, which runs a FastAPI server that queries SQLite with Claude as the reasoning layer. This is the only path where the DB is hit at request time.
- **Analysis feeds both consumers.** Derived metrics and valuation snapshots are written back to the DB by the analysis layer, and then picked up by both the export pipeline (which puts them in `metrics/{T}.json`) and the agent tools (which query them live from the DB).

---

### Repository Architecture

This diagram shows how the Python code is organized into directories, independent of runtime topology.

```
needlstack/
│
├── db/                          ← Schema definitions and DB access
│   └── schema.py                  All 21 tables defined here as SQLAlchemy
│                                  Table objects. init_db() creates them.
│                                  run_migrations() applies ALTER TABLEs.
│
├── ingestion/                   ← One module per data source or pipeline stage
│   ├── financials.py              SEC EDGAR XBRL → income/balance/cashflow
│   ├── prices.py                  yfinance → stock_prices
│   ├── metadata.py                yfinance → security_metadata
│   ├── corporate_actions.py       yfinance → splits + dividends
│   ├── valuations.py              DB-only → valuation_snapshots (no API calls)
│   ├── sec_13f.py                 EDGAR → institutional_holdings
│   ├── sec_filings.py             EDGAR → sec_filings (8-K metadata)
│   ├── rss_feeds.py               feedparser → news_articles
│   ├── article_extractor.py       trafilatura → full article text
│   ├── ticker_mentions.py         two-pass regex → article_tickers
│   ├── sentiment.py               VADER → article_sentiment
│   ├── sentiment_aggregator.py    daily rollup → ticker_sentiment_daily
│   ├── reddit.py                  praw → content_items
│   ├── stocktwits.py              public API → content_items
│   ├── narratives.py              keyword scan → narrative_signals
│   ├── indexes.py                 Wikipedia → index_constituents
│   └── universe.py                NASDAQ Trader → tickers
│
├── analysis/                    ← Derived computations (reads DB, writes back)
│   └── compute_metrics.py         60+ metrics per ticker per day
│
├── scripts/                     ← Runnable entry points (the cron targets)
│   ├── daily_financials.py        calls ingestion/financials.py
│   ├── daily_metrics.py           calls analysis/compute_metrics.py
│   ├── daily_valuations.py        calls ingestion/valuations.py
│   ├── daily_macro.py             calls ingestion/prices.py for macro symbols
│   ├── daily_filings.py           calls ingestion/sec_filings.py
│   ├── daily_sentiment_agg.py     calls ingestion/sentiment_aggregator.py
│   ├── daily_narrative.py         calls ingestion/narratives.py
│   ├── rss_poll.py                calls rss_feeds + ticker_mentions + sentiment
│   ├── article_fetch.py           calls article_extractor.py
│   ├── social_poll.py             calls reddit.py + stocktwits.py
│   ├── weekly_metadata.py         calls ingestion/metadata.py
│   ├── weekly_profiles.py         calls ingestion/profiles.py
│   ├── quarterly_13f.py           calls ingestion/sec_13f.py
│   ├── export_data.py             reads DB → writes JSON to R2 or docs/data/
│   └── init_db.py                 calls db/schema.init_db()
│
├── agent/                       ← AI agent (reads DB live at request time)
│   ├── tools.py                   Tool definitions + SQL implementations
│   ├── runner.py                  Agentic loop (Claude API + tool execution)
│   ├── cli.py                     CLI entry point
│   └── api.py                     FastAPI /chat endpoint
│
├── storage/                     ← Infrastructure adapters
│   └── r2.py                      Cloudflare R2 upload via boto3 S3 API
│
└── docs/                        ← Static website (served by GitHub Pages)
    ├── index.html                 Single HTML file — loads JS/CSS from CDN
    ├── assets/
    │   ├── main.js                All client logic: state, fetching, charts, tabs
    │   └── style.css              Dark-mode stylesheet
    └── data/                      Local export target (gitignored)
```

---

### How the Two Architectures Relate

The repository directories map cleanly onto the system layers:

| System Layer | Repo Directory | Role |
|---|---|---|
| Database (center of the funnel) | `db/` | Defines the schema; everything else imports from here |
| Ingestion (pull from sources) | `ingestion/` | Library modules — no entry points, imported by scripts |
| Cron jobs (orchestration) | `scripts/` | Entry points that compose ingestion + analysis modules |
| Analysis (compute from DB) | `analysis/` | Pure DB reads/writes, no external calls |
| Export (DB → JSON) | `scripts/export_data.py` | Reads all tables, serializes to JSON, uploads to R2 |
| Storage adapter | `storage/` | Thin wrapper — only used by export_data.py |
| AI agent | `agent/` | Separate consumer of the DB; also exposes the FastAPI server |
| Website | `docs/` | Completely decoupled from DB — consumes only the exported JSON |

**The critical decoupling point** is between `scripts/export_data.py` and `docs/`. Once JSON lands in R2, the website knows nothing about SQLite, Python, or the ingestion pipeline. The SPA is a standalone static application. This means:
- The website works even if the DB machine is offline
- Adding a new metric requires only (1) computing it in Python and (2) including it in the export — no web server changes
- The website can be developed locally with `LOCAL_EXPORT=1` to write to `docs/data/` and opened as a plain HTML file

---

## Repository Structure

```
needlstack/
├── db/
│   └── schema.py              # SQLAlchemy table definitions + migrations
├── ingestion/                 # One module per data source
│   ├── financials.py          # SEC EDGAR XBRL → income/balance/cashflow
│   ├── prices.py              # yfinance → stock_prices
│   ├── metadata.py            # yfinance → security_metadata
│   ├── corporate_actions.py   # yfinance → splits + dividends
│   ├── valuations.py          # DB-only → valuation_snapshots
│   ├── sec_13f.py             # EDGAR → institutional holdings
│   ├── sec_filings.py         # EDGAR → 8-K metadata
│   ├── rss_feeds.py           # feedparser → news_articles
│   ├── article_extractor.py   # trafilatura → full article text
│   ├── ticker_mentions.py     # two-pass extraction → article_tickers
│   ├── sentiment.py           # VADER → article_sentiment
│   ├── sentiment_aggregator.py# daily rollup → ticker_sentiment_daily
│   ├── reddit.py              # praw → content_items (reddit)
│   ├── stocktwits.py          # public API → content_items (stocktwits)
│   ├── narratives.py          # keyword-phrase → narrative_signals
│   ├── indexes.py             # Wikipedia scrape → index_constituents
│   └── universe.py            # NASDAQ Trader → tickers
├── analysis/
│   └── compute_metrics.py     # 60+ derived metrics → derived_metrics
├── scripts/                   # Runnable entry points (cron targets)
│   ├── daily_financials.py
│   ├── daily_metrics.py
│   ├── daily_valuations.py
│   ├── daily_macro.py
│   ├── daily_filings.py
│   ├── daily_sentiment_agg.py
│   ├── daily_narrative.py
│   ├── rss_poll.py
│   ├── article_fetch.py
│   ├── social_poll.py
│   ├── weekly_metadata.py
│   ├── weekly_profiles.py
│   ├── quarterly_13f.py
│   ├── export_data.py
│   └── init_db.py
├── agent/
│   ├── tools.py               # Claude tool_use definitions + DB queries
│   ├── runner.py              # Agentic loop (max 10 iterations)
│   ├── cli.py                 # CLI: python agent/cli.py "question"
│   └── api.py                 # FastAPI /chat endpoint
├── storage/
│   └── r2.py                  # Cloudflare R2 upload wrapper
└── docs/                      # GitHub Pages SPA
    ├── index.html
    ├── assets/
    │   ├── main.js            # All client-side logic
    │   └── style.css          # Dark-mode UI
    └── data/                  # Local export target (not committed)
```

---

## Detailed Component Breakdown

### 1. Database Schema (`db/schema.py`)

The entire data model lives in a single SQLite file at `db/needlstack.db`. SQLAlchemy Core is used (not ORM) — tables are defined as `sa.Table` objects and queries are written in raw SQL with `sa.text()`. This keeps things simple and fast.

**21 tables across logical groups:**

| Group | Tables |
|---|---|
| Universe | `tickers`, `index_constituents` |
| Market data | `stock_prices`, `security_metadata`, `corporate_actions` |
| Fundamentals | `income_statements`, `balance_sheets`, `cash_flows`, `earnings_surprises` |
| Computed | `valuation_snapshots`, `derived_metrics` |
| Filings | `sec_filings`, `institutional_holdings`, `institutional_summary` |
| Company | `company_profiles` |
| News | `news_sources`, `news_articles`, `article_tickers`, `article_sentiment`, `ticker_sentiment_daily` |
| Social | `content_items`, `content_tickers`, `content_sentiment` |
| Narratives | `narratives`, `narrative_signals` |
| Agent | `agent_conversations`, `agent_messages` |

**Migrations** are handled inline in `run_migrations()` using `ALTER TABLE ... ADD COLUMN` guarded by `_sqlite_column_exists()` checks — no migration framework needed. `init_db.py` calls `metadata.create_all()` then `run_migrations()` so the same script works on a fresh or existing database.

**Postgres support** is available via the `DATABASE_URL` environment variable — if set, `get_engine()` uses that instead of the local SQLite path. The only limitation is that `run_migrations()` skips itself on non-SQLite dialects (Postgres handles `CREATE TABLE IF NOT EXISTS` at the DDL level).

---

### 2. Financial Statement Ingestion (`ingestion/financials.py`)

This module pulls SEC EDGAR XBRL data — the same structured financial data that powers Bloomberg terminals, except it's completely free.

**How it works:**

1. **Fetch** — calls `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json` for each ticker's CIK number. This returns every GAAP fact the company has ever reported, tagged with XBRL concept names.

2. **Parse** — the response contains facts under `us-gaap` namespace. Each fact has a form type (10-K, 10-Q), period end date, filing date, fiscal period (Q1–Q4, FY), and value. The `TAG_MAP` dict maps our column names to ordered lists of XBRL aliases to try in priority order, since companies use different tags for the same concept (e.g. revenue might be `Revenues`, `RevenueFromContractWithCustomerExcludingAssessedTax`, or `SalesRevenueNet`).

3. **Deduplicate** — for each `(end_date, form_type)` key, keep the most recently filed version across all tag aliases. This handles amended filings correctly.

4. **Route** — each unique period key is checked against `INCOME_COLS`, `BALANCE_COLS`, and `CASHFLOW_COLS` to determine which table(s) it belongs to. A period can appear in multiple tables (e.g. a 10-Q updates income, balance, and cashflow simultaneously).

5. **Upsert** — `INSERT OR REPLACE` into each table. The primary key is `(ticker, period_end, period_type)` so re-running is idempotent.

**Rate limiting** — EDGAR allows ~10 requests/second. The `rate_limit` parameter in `download_financials()` sleeps `1/rate_limit` seconds between tickers.

**Fields ingested:**
- Income: revenue, cost_of_revenue, gross_profit, operating_income, pretax_income, income_tax, net_income, eps_basic/diluted, shares_basic/diluted, interest_expense
- Balance: cash, current_assets, total_assets, accounts_payable, current_liabilities, long_term_debt, total_liabilities, stockholders_equity, retained_earnings, inventory, accounts_receivable, short_term_debt, goodwill, intangible_assets
- Cash flow: operating_cf, capex, investing_cf, financing_cf, dividends_paid, stock_repurchases, free_cash_flow (derived), depreciation_amortization

---

### 3. Price & Metadata Ingestion (`ingestion/prices.py`, `ingestion/metadata.py`)

**Prices** use `yfinance` to pull daily OHLCV history. `adj_close` accounts for splits and dividends. `dollar_volume = close × volume` is computed on insert for liquidity screening. The primary key is `(ticker, date)` so incremental runs only add new rows.

**Metadata** pulls the current security snapshot from yfinance: market cap, enterprise value, float shares, shares outstanding, and 30-day average volume. This is a single-row-per-ticker table (`security_metadata`) that gets overwritten on each weekly run. Market cap and enterprise value are used by `valuations.py` to compute all EV-based multiples.

**Corporate actions** (`ingestion/corporate_actions.py`) stores splits and dividends from yfinance. The frontend uses these to draw split lines and dividend annotations directly on the price chart.

---

### 4. Valuation Multiples (`ingestion/valuations.py`)

Runs daily against existing DB data — no external API calls. Computes a snapshot of valuation multiples for each ticker and writes one row per day to `valuation_snapshots`.

**Multiples computed:**

| Multiple | Formula |
|---|---|
| P/E TTM | Latest close price ÷ TTM EPS diluted |
| P/B | Market cap ÷ (stockholders_equity ÷ shares_outstanding) |
| P/S TTM | Market cap ÷ TTM revenue |
| EV/EBITDA | Enterprise value ÷ (TTM operating_income + TTM D&A) |
| EV/EBIT | Enterprise value ÷ TTM operating_income |
| EV/Revenue | Enterprise value ÷ TTM revenue |
| P/FCF | Market cap ÷ TTM free_cash_flow |

**TTM** (trailing twelve months) is the sum of the last 4 `period_type='Q'` rows. A `None` is stored whenever the denominator is zero, negative, or missing — never a divide-by-zero error.

The 252-row history stored in `valuation_snapshots` (one per trading day) is what drives the P/E and EV/EBITDA chart overlays in the frontend.

---

### 5. Derived Metrics (`analysis/compute_metrics.py`)

The most computationally dense module — computes 60+ metrics per ticker per day entirely from existing DB rows. Runs after valuations so it can pull the latest P/E and EV/EBITDA from `valuation_snapshots`.

**Helper functions:**
- `_safe_divide(n, d)` — returns `None` on zero/None denominator, never raises
- `_yoy_growth(current, prior)` — `(current - prior) / abs(prior)`, returns `None` if prior is zero
- `_compute_ttm(df, col)` — sums the last 4 quarterly rows; returns `None` if fewer than 4 rows with data
- `_compute_ttm_prior_year(df, col)` — rows 4–8 back (prior year TTM)
- `_compute_ttm_n_years_ago(df, col, n)` — rows `(n*4)` to `(n*4)+4` back for CAGR base
- `_cagr(current, base, years)` — `(current/base)^(1/years) - 1`

**Metric categories:**

**Growth** — Revenue YoY/QoQ/3Y CAGR/5Y CAGR, EPS YoY/3Y CAGR/5Y CAGR, Operating Income YoY, EBITDA YoY, OCF YoY, FCF YoY. CAGR uses 24 quarters of income history (LIMIT 24 in the query).

**Margins** — Gross, Operating, Net, Pretax, OCF, EBITDA, FCF, CapEx-to-Revenue. All are TTM-based: `_compute_ttm(income_df, col) / _compute_ttm(income_df, "revenue")`.

**Returns** — ROE, ROA computed as `TTM_net_income / latest_balance_value`. ROIC = `NOPAT / invested_capital` where `NOPAT = operating_income × (1 - effective_tax_rate)` and `invested_capital = equity + long_term_debt - cash`. ROCE = `operating_income / (total_assets - current_liabilities)`.

**Liquidity** — Current ratio, quick ratio (`(current_assets - inventory) / current_liabilities`), cash ratio, working capital (absolute), net debt (`long_term_debt + short_term_debt - cash`).

**Leverage** — Debt/equity, debt/assets, debt/capital, equity ratio, net debt/EBITDA, debt/EBITDA, interest coverage (`operating_income / interest_expense`).

**Efficiency** — Asset turnover uses 2-period average assets. Inventory turnover, receivables turnover, and payables turnover all use 2-period averages where available. DSO/DIO/DPO are `365 / turnover`. CCC = `DSO + DIO - DPO`.

**Per-share** — Book value per share, tangible book value per share (`(equity - goodwill - intangibles) / shares`), OCF/share, FCF/share.

**Shareholder returns** — Dividend yield (`abs(TTM dividends paid) / shares / price`), payout ratio, buyback yield (`abs(TTM buybacks) / market_cap`), shareholder yield (dividends + buybacks combined).

---

### 6. News & Sentiment Pipeline

The news pipeline runs in three stages, each as a separate cron job:

**Stage 1 — RSS polling (`scripts/rss_poll.py`, every 30 min)**

`ingestion/rss_feeds.py` calls `poll_all_feeds()` which iterates every active source in `news_sources`. Each source is fetched with `feedparser` and new articles (those not already in `news_articles` by URL) are inserted. The `article_id` is a SHA-256 hash of the URL, making inserts idempotent. After new articles land, `ticker_mentions.py` does a two-pass extraction on the title and raw RSS summary: first an exact-match set lookup against all known tickers, then a regex scan for `$TICKER` patterns. Results go to `article_tickers`. VADER sentiment is scored on the same article text and stored in `article_sentiment`.

**Stage 2 — Full text extraction (`scripts/article_fetch.py`, every 15 min)**

`ingestion/article_extractor.py` uses `trafilatura` to fetch and extract the full article body for any article in `news_articles` that has `full_text IS NULL`. Trafilatura is robust at stripping navigation, ads, and boilerplate from financial news sites. Paywall detection is handled by checking if the extracted word count is below a threshold.

**Stage 3 — Daily aggregation (`scripts/daily_sentiment_agg.py`, daily at 01:00 UTC)**

`ingestion/sentiment_aggregator.py` rolls up the per-article sentiment scores into `ticker_sentiment_daily`: mention count, article count, source count, average compound score, and bullish/neutral/bearish breakdowns. The 30-day window exported to the frontend is sourced from this table.

---

### 7. Social Sentiment (`ingestion/reddit.py`, `ingestion/stocktwits.py`)

**Reddit** (`scripts/social_poll.py`, every 30 min) — uses `praw` to pull from r/wallstreetbets, r/investing, r/stocks, r/options, r/SecurityAnalysis, r/pennystocks. Posts and comments are stored in `content_items` with `source_type='reddit'`. Ticker extraction and VADER scoring run on the body text.

**StockTwits** (`scripts/social_poll.py`, every 30 min) — hits the public StockTwits stream API (no auth required for basic access). The `bullish`/`bearish` sentiment labels users self-apply are stored alongside VADER's computed score in `content_sentiment`.

Both sources write to the same `content_items` / `content_tickers` / `content_sentiment` table structure, which keeps the sentiment aggregation logic source-agnostic.

---

### 8. Institutional Holdings (`ingestion/sec_13f.py`)

Parses SEC EDGAR 13F filings — the quarterly institutional ownership disclosures required for funds managing >$100M. The module:

1. Queries EDGAR for each institution's most recent 13F-HR filing
2. Parses the XML holdings table
3. Upserts into `institutional_holdings` (one row per fund–ticker–quarter)
4. Rolls up into `institutional_summary` (total institutions, % outstanding held, net change)

The `scripts/quarterly_13f.py` cron (`0 9 1 1,4,7,10 *`) runs on the first of each quarter-end month to catch new filings.

---

### 9. Market Narratives (`ingestion/narratives.py`)

Narratives are user-defined investment themes (e.g. "AI datacenter capex", "interest rate sensitivity", "EV adoption"). Each narrative has a list of keyword phrases and related tickers stored in `narratives`. The `scripts/daily_narrative.py` cron scans the past day's articles for keyword hits, counts mentions per narrative, and writes a `momentum_score` to `narrative_signals`. The momentum score is a rolling change in mention frequency. The Narratives tab in the frontend surfaces these with clickable related tickers.

---

### 10. Export Pipeline (`scripts/export_data.py`)

The bridge between the private DB and the public-facing website. Runs after the daily ingestion jobs complete.

**10 JSON files per ticker:**

| File | Contents |
|---|---|
| `prices/{T}.json` | Full OHLCV history |
| `financials/{T}.json` | Income, balance, cashflow statements + valuation snapshots |
| `metadata/{T}.json` | Market cap, shares, enterprise value |
| `corporate_actions/{T}.json` | Splits and dividends |
| `profiles/{T}.json` | Company description, employees, website |
| `ownership/{T}.json` | Institutional summary + top 10 holders |
| `sentiment/{T}.json` | 30-day daily sentiment scores |
| `news/{T}.json` | 20 most recent articles with sentiment labels |
| `social/{T}.json` | 30-day Reddit + StockTwits mention counts |
| `metrics/{T}.json` | Latest derived metrics snapshot + 8-entry history |

**4 global files:** `tickers.json`, `indexes.json`, `macro.json`, `narratives.json`

**Incremental mode** — an export log at `data/export_log.json` records the last exported price date per ticker. On subsequent runs, tickers whose price data hasn't changed are skipped. Pass `--no-incremental` to force a full export.

**Storage** — controlled by `LOCAL_EXPORT` environment variable:
- `LOCAL_EXPORT=1` → writes to `docs/data/` (for local development or GitHub Pages)
- Default → uploads to Cloudflare R2 via `storage/r2.py` using the boto3 S3-compatible API. Files are served from `data.needlstack.com` with 1-hour cache headers.

**Parallelism** — DB queries run sequentially (SQLite doesn't benefit from concurrent reads). Uploads to R2 are parallelized with `ThreadPoolExecutor(max_workers=16)`.

---

### 11. AI Agent (`agent/`)

An agentic loop powered by Claude that can answer investment research questions by querying the local data lake.

**Tools available to the agent:**

| Tool | What it does |
|---|---|
| `get_price_history` | OHLCV data for date range |
| `get_financial_summary` | Income/balance/cashflow statements |
| `get_valuation_multiples` | P/E, EV/EBITDA history |
| `get_sentiment_trend` | Daily news sentiment + mention counts |
| `compare_tickers` | Side-by-side derived metrics comparison |
| `screen_stocks` | Filter stocks by metric thresholds |
| `get_institutional_flows` | 13F ownership changes by quarter |
| `get_narrative_context` | Market narrative signal strength |

**Loop mechanics** (`agent/runner.py`):
1. Creates a conversation record in `agent_conversations`
2. Sends the user message to `claude-sonnet-4-6` with all tool definitions
3. If Claude returns `stop_reason="tool_use"`, executes each tool against the DB and feeds results back as `tool_result` messages
4. Repeats up to 10 iterations until `stop_reason="end_turn"`
5. All turns (user, assistant, tool results) are stored in `agent_messages`

**Interfaces:**
- **CLI**: `python agent/cli.py "Compare AAPL and MSFT margins" --tickers AAPL MSFT`
- **API**: FastAPI server at `api.needlstack.com` — single `POST /chat` endpoint that the website's AI Chat tab calls. Requires `ANTHROPIC_API_KEY` in the environment.

---

### 12. Frontend SPA (`docs/`)

A single-page application served by GitHub Pages. No build step, no framework — vanilla JS with Plotly.js loaded from CDN.

**Data loading** — on ticker add, 10 JSON files are fetched in parallel with `Promise.all()`. Results are cached in `state.cache[ticker]` for the session lifetime.

**Chart** — Plotly candlestick with up to 3 y-axes. Financial statement series (revenue, net income, FCF) are annual-period-only to avoid quarterly clutter. Valuation multiples (P/E, EV/EBITDA) plot daily snapshots. Corporate action markers (split lines, dividend labels) are drawn as Plotly shapes/annotations.

**7 tabs:**

| Tab | Content |
|---|---|
| Ownership | Institutional summary stats + top 10 holders table |
| Filings | Recent 8-K filings with item badges and SEC links |
| News | 10 most recent articles with source, date, and sentiment badge |
| Social | Reddit + StockTwits daily mention bar chart |
| Narratives | Active market narratives with momentum scores and related tickers |
| Metrics | 60+ metric cards organized into 10 categories with trend arrows |
| AI Chat | Chat UI proxied to `api.needlstack.com/chat` |

**Metrics tab detail** — each metric card shows the current value and a trend arrow (↑/↓) colored green/red based on whether the metric improved or declined since the prior day's snapshot. Formatting helpers (`fmtPct`, `fmtMult`, `fmtBn`, `fmtNum`, `fmtDays`) handle consistent display across all metric types.

---

## Cron Schedule

| Schedule | Script | What it does |
|---|---|---|
| `*/30 * * * *` | `rss_poll.py` | Poll RSS feeds, extract tickers, score sentiment |
| `*/15 * * * *` | `article_fetch.py` | Fetch full text for new articles |
| `*/30 * * * *` | `social_poll.py` | Poll Reddit + StockTwits |
| `0 23 * * 1-5` | `daily_financials.py` | EDGAR XBRL financials (rate-limited) |
| `0 23 * * 1-5` | `daily_macro.py` | Macro/index prices |
| `0 23 * * 1-5` | `daily_valuations.py` | Recompute valuation multiples |
| `0 23 * * 1-5` | `daily_metrics.py` | Recompute 60+ derived metrics |
| `0 23 * * 1-5` | `daily_filings.py` | Fetch new 8-K filings |
| `0 1 * * *` | `daily_sentiment_agg.py` | Roll up daily sentiment scores |
| `0 2 * * *` | `daily_narrative.py` | Score narrative signals |
| `0 7 * * 1` | `weekly_metadata.py` | Refresh market cap, shares, EV |
| `0 8 * * 0` | `weekly_profiles.py` | Refresh company profiles |
| `0 9 1 1,4,7,10 *` | `quarterly_13f.py` | Parse new 13F institutional filings |

---

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Create .env from example
cp .env.example .env
# Set ANTHROPIC_API_KEY, R2_* credentials if needed

# Initialize DB and run one-time backfills
python scripts/init_db.py
python scripts/backfill_indexes.py
python scripts/backfill_universe.py
python scripts/init_news_sources.py

# Backfill financial history (slow — rate-limited to EDGAR)
python scripts/backfill_financials.py

# Export data locally for frontend development
LOCAL_EXPORT=1 python scripts/export_data.py --tickers AAPL MSFT GOOGL
```

**Run the AI agent locally:**
```bash
python agent/cli.py "What is AAPL's FCF margin trend?" --tickers AAPL
```

**Run the API server locally:**
```bash
uvicorn agent.api:app --host 0.0.0.0 --port 8000
```

**Environment variables:**
| Variable | Required | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | For agent | Claude API key |
| `R2_ACCOUNT_ID` | For R2 export | Cloudflare account ID |
| `R2_ACCESS_KEY_ID` | For R2 export | R2 API token key |
| `R2_SECRET_ACCESS_KEY` | For R2 export | R2 API token secret |
| `R2_BUCKET_NAME` | For R2 export | Bucket name (default: needlstack) |
| `DATABASE_URL` | Optional | Postgres URL (overrides SQLite) |
| `DB_PATH` | Optional | SQLite path (default: db/needlstack.db) |
| `LOCAL_EXPORT` | Optional | Set to `1` to write JSON locally |
