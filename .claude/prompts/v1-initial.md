# Needlstack — Claude Project Instructions

## Project Overview

Needlstack is an evolving financial and investing analysis platform. The goal is to build an intelligent data pipeline and model that ingests market data, earnings filings, news, and social sentiment — and ultimately surfaces insights on a personal webpage.

## Core Goals (in order of priority)

1. **Data ingestion** — historical and live stock price data, refreshed on a cadence
2. **Earnings & filings** — SEC EDGAR filings (10-K, 10-Q, 8-K, earnings transcripts)
3. **Intelligent data model** — structured, queryable storage enabling deep analysis and visualization
4. **News & sentiment** — RSS feeds, Reddit, StockTwits, and other free sources for market/industry/stock signal
5. **Web surface** — eventually expose insights via a webpage (domain already purchased)

## Tech Stack

- **Primary language**: Python (preferred for all data, pipeline, and analysis work)
- **Web frontend**: TBD (separate from Python backend work)
- **Packages**: Use whatever Python packages are best suited — install without asking
- **Data sources**: Free sources first; paid APIs are on the table later if needed

## Free Data Sources to Leverage

### Stock Prices
- `yfinance` — Yahoo Finance wrapper, excellent free historical + live data
- `alpha_vantage` — free tier (25 req/day), good for supplemental data
- `polygon.io` — free tier available

### Earnings & SEC Filings
- **SEC EDGAR** — fully free public API (`https://data.sec.gov`)
- `sec-edgar-downloader` — Python package for bulk EDGAR downloads
- `edgartools` — modern Python library for parsing SEC filings

### News & RSS
- `feedparser` — parse any RSS/Atom feed
- Free financial RSS feeds: Reuters, MarketWatch, Seeking Alpha, Yahoo Finance, Benzinga
- `newspaper3k` or `trafilatura` — article text extraction from URLs

### Social & Sentiment
- **Reddit** — `praw` (Reddit API, free) — r/wallstreetbets, r/investing, r/stocks
- **StockTwits** — free public API, no auth required for basic stream
- `vaderSentiment` or `transformers` (FinBERT) for NLP sentiment scoring

## Architecture Principles

- Keep data ingestion, storage, and analysis as separate, modular Python modules
- Use a local database (SQLite to start, Postgres if scale demands) for the data model
- Design schema with future visualization in mind — time series friendly
- All scheduled/recurring jobs should be written as standalone scripts runnable via cron or a scheduler (e.g., APScheduler)
- Store raw source data before transformation — never lose the original

## Code Conventions

- Python 3.11+
- Use `pathlib` over `os.path`
- Prefer `pandas` for tabular data manipulation
- Use `dataclasses` or `pydantic` for data models
- Write modular, single-responsibility functions
- Keep secrets/API keys in `.env` files (never hardcode, never commit)
- Use `python-dotenv` to load env vars

## Project Structure (evolving)

```
needlstack/
├── CLAUDE.md
├── .claude/
│   └── prompts/         # archived CLAUDE.md iterations
├── data/                # raw and processed data files (gitignored)
├── db/                  # database files (gitignored)
├── ingestion/           # data ingestion scripts per source
├── models/              # data models / schema definitions
├── analysis/            # analysis and signal scripts
├── sentiment/           # news/social sentiment pipeline
├── scheduler/           # cron/scheduled job definitions
├── web/                 # future web surface
├── requirements.txt
└── .env.example
```

## What NOT to do

- Do not ask before installing Python packages — just use them
- Do not hardcode ticker lists — make them configurable
- Do not mix web frontend code with data pipeline code
- Do not commit `.env`, `data/`, or `db/` to git
