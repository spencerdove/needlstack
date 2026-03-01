"""
Export SQLite data to per-ticker JSON files for the Needlstack website.

Usage:
    python scripts/export_data.py                    # export all tickers
    python scripts/export_data.py --tickers AAPL MSFT  # export specific tickers
"""

import argparse
import json
import sys
from pathlib import Path

import sqlalchemy as sa

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))
from db.schema import get_engine

DOCS_DIR = Path(__file__).parent.parent / "docs"
DATA_DIR = DOCS_DIR / "data"
PRICES_DIR = DATA_DIR / "prices"
FINANCIALS_DIR = DATA_DIR / "financials"


def setup_dirs() -> None:
    PRICES_DIR.mkdir(parents=True, exist_ok=True)
    FINANCIALS_DIR.mkdir(parents=True, exist_ok=True)


def export_tickers(conn: sa.Connection) -> list[str]:
    rows = conn.execute(
        sa.text("SELECT ticker, company_name, sector, industry FROM tickers ORDER BY ticker")
    ).fetchall()
    tickers_data = [
        {"ticker": r[0], "company_name": r[1], "sector": r[2], "industry": r[3]}
        for r in rows
    ]
    (DATA_DIR / "tickers.json").write_text(json.dumps(tickers_data, separators=(",", ":")))
    print(f"Exported {len(tickers_data)} tickers to tickers.json")
    return [r[0] for r in rows]


def export_prices(conn: sa.Connection, ticker: str) -> None:
    rows = conn.execute(
        sa.text(
            "SELECT date, open, high, low, close, adj_close, volume "
            "FROM stock_prices WHERE ticker = :ticker ORDER BY date"
        ),
        {"ticker": ticker},
    ).fetchall()
    data = [
        {
            "date": str(r[0]),
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
            "adj_close": r[5],
            "volume": r[6],
        }
        for r in rows
    ]
    (PRICES_DIR / f"{ticker}.json").write_text(json.dumps(data, separators=(",", ":")))


def export_financials(conn: sa.Connection, ticker: str) -> None:
    # Income statements
    inc_rows = conn.execute(
        sa.text(
            "SELECT period_end, period_type, fiscal_year, fiscal_quarter, "
            "revenue, gross_profit, operating_income, net_income, eps_diluted "
            "FROM income_statements WHERE ticker = :ticker ORDER BY period_end"
        ),
        {"ticker": ticker},
    ).fetchall()
    income_statements = [
        {
            "period_end": str(r[0]),
            "period_type": r[1],
            "fiscal_year": r[2],
            "fiscal_quarter": r[3],
            "revenue": r[4],
            "gross_profit": r[5],
            "operating_income": r[6],
            "net_income": r[7],
            "eps_diluted": r[8],
        }
        for r in inc_rows
    ]

    # Balance sheets
    bs_rows = conn.execute(
        sa.text(
            "SELECT period_end, period_type, cash, total_assets, "
            "long_term_debt, total_liabilities, stockholders_equity "
            "FROM balance_sheets WHERE ticker = :ticker ORDER BY period_end"
        ),
        {"ticker": ticker},
    ).fetchall()
    balance_sheets = [
        {
            "period_end": str(r[0]),
            "period_type": r[1],
            "cash": r[2],
            "total_assets": r[3],
            "long_term_debt": r[4],
            "total_liabilities": r[5],
            "stockholders_equity": r[6],
        }
        for r in bs_rows
    ]

    # Cash flows
    cf_rows = conn.execute(
        sa.text(
            "SELECT period_end, period_type, operating_cf, capex, "
            "dividends_paid, free_cash_flow "
            "FROM cash_flows WHERE ticker = :ticker ORDER BY period_end"
        ),
        {"ticker": ticker},
    ).fetchall()
    cash_flows = [
        {
            "period_end": str(r[0]),
            "period_type": r[1],
            "operating_cf": r[2],
            "capex": r[3],
            "dividends_paid": r[4],
            "free_cash_flow": r[5],
        }
        for r in cf_rows
    ]

    # Earnings surprises
    es_rows = conn.execute(
        sa.text(
            "SELECT earnings_date, eps_estimate, eps_actual, eps_surprise_pct "
            "FROM earnings_surprises WHERE ticker = :ticker ORDER BY earnings_date"
        ),
        {"ticker": ticker},
    ).fetchall()
    earnings_surprises = [
        {
            "earnings_date": str(r[0]),
            "eps_estimate": r[1],
            "eps_actual": r[2],
            "eps_surprise_pct": r[3],
        }
        for r in es_rows
    ]

    data = {
        "income_statements": income_statements,
        "balance_sheets": balance_sheets,
        "cash_flows": cash_flows,
        "earnings_surprises": earnings_surprises,
    }
    (FINANCIALS_DIR / f"{ticker}.json").write_text(json.dumps(data, separators=(",", ":")))


def main() -> None:
    parser = argparse.ArgumentParser(description="Export needlstack DB to JSON for website")
    parser.add_argument(
        "--tickers",
        nargs="+",
        metavar="TICKER",
        help="Export only these tickers (default: all)",
    )
    args = parser.parse_args()

    setup_dirs()
    engine = get_engine()

    with engine.connect() as conn:
        all_tickers = export_tickers(conn)

        tickers_to_export = args.tickers if args.tickers else all_tickers
        # Validate requested tickers
        invalid = set(tickers_to_export) - set(all_tickers)
        if invalid:
            print(f"Warning: unknown tickers skipped: {sorted(invalid)}")
            tickers_to_export = [t for t in tickers_to_export if t not in invalid]

        total = len(tickers_to_export)
        for i, ticker in enumerate(tickers_to_export, 1):
            export_prices(conn, ticker)
            export_financials(conn, ticker)
            if i % 50 == 0 or i == total:
                print(f"  {i}/{total} tickers exported")

    print(f"Done. Data written to {DATA_DIR}")


if __name__ == "__main__":
    main()
