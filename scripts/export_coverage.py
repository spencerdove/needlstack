"""
Export data coverage summary to docs/data/coverage.json.

Usage:
    python scripts/export_coverage.py
    LOCAL_EXPORT=1 python scripts/export_coverage.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.schema import get_engine

DOCS_DIR = Path(__file__).parent.parent / "docs"
DATA_DIR = DOCS_DIR / "data"
LOCAL_EXPORT = os.environ.get("LOCAL_EXPORT", "0") == "1"


def _write_or_upload(key: str, data: str) -> None:
    if LOCAL_EXPORT:
        out_path = DATA_DIR / key
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(data)
    else:
        from storage.r2 import upload_json
        upload_json(key, data)


def main() -> None:
    engine = get_engine()

    with engine.connect() as conn:
        # Total tickers
        total = conn.execute(sa.text("SELECT COUNT(*) FROM tickers")).fetchone()[0]

        # Asset type breakdown
        asset_rows = conn.execute(
            sa.text("SELECT asset_type, COUNT(*) FROM tickers GROUP BY asset_type")
        ).fetchall()
        asset_counts = {r[0]: r[1] for r in asset_rows}
        equity_count = asset_counts.get("equity", 0)
        etf_count = asset_counts.get("etf", 0)
        equity_etf_count = equity_count + etf_count

        # Tickers with prices
        tickers_with_prices = conn.execute(
            sa.text("SELECT COUNT(DISTINCT ticker) FROM stock_prices")
        ).fetchone()[0]

        categories = []

        # Categories that should use equity-only denominator
        financial_categories = {
            "Income Statements", "Balance Sheets", "Cash Flows",
            "Security Metadata", "Valuation Snapshots", "Derived Metrics",
            "Ownership",
        }

        # Helper for table-based categories
        def add_category(name: str, table: str, date_col: str = "date",
                         ticker_col: str = "ticker") -> None:
            # Choose denominator based on category type
            if name == "Prices":
                denom = equity_etf_count
                denom_label = "equity+etf"
            elif name in financial_categories:
                denom = equity_count
                denom_label = "equity"
            else:
                denom = total
                denom_label = "all"

            try:
                row = conn.execute(sa.text(
                    f"SELECT COUNT(DISTINCT {ticker_col}), MIN({date_col}), MAX({date_col}) FROM {table}"
                )).fetchone()
                count = row[0] or 0
                earliest = str(row[1]) if row[1] else None
                latest = str(row[2]) if row[2] else None

                # Sample missing tickers (limit 50)
                missing_rows = conn.execute(sa.text(
                    f"SELECT ticker FROM tickers WHERE ticker NOT IN "
                    f"(SELECT DISTINCT {ticker_col} FROM {table}) "
                    f"ORDER BY ticker LIMIT 50"
                )).fetchall()
                sample_missing = [r[0] for r in missing_rows]
            except Exception:
                count = 0
                earliest = None
                latest = None
                sample_missing = []

            categories.append({
                "name": name,
                "tickers_with_data": count,
                "denominator": denom,
                "denominator_label": denom_label,
                "pct_coverage": round(count / denom * 100, 1) if denom > 0 else 0,
                "earliest_date": earliest,
                "latest_date": latest,
                "sample_missing": sample_missing,
            })

        add_category("Prices", "stock_prices", "date")
        add_category("Income Statements", "income_statements", "period_end")
        add_category("Balance Sheets", "balance_sheets", "period_end")
        add_category("Cash Flows", "cash_flows", "period_end")
        add_category("Security Metadata", "security_metadata", "updated_at")
        add_category("Valuation Snapshots", "valuation_snapshots", "snapshot_date")
        add_category("Derived Metrics", "derived_metrics", "date")

        # News (article_tickers)
        try:
            row = conn.execute(sa.text(
                "SELECT COUNT(DISTINCT ticker) FROM article_tickers"
            )).fetchone()
            news_count = row[0] or 0
            news_dates = conn.execute(sa.text(
                "SELECT MIN(na.published_at), MAX(na.published_at) FROM news_articles na "
                "JOIN article_tickers at ON at.article_id = na.article_id"
            )).fetchone()
            earliest_news_date = str(news_dates[0])[:10] if news_dates and news_dates[0] else None
            latest_news_date = str(news_dates[1])[:10] if news_dates and news_dates[1] else None
        except Exception:
            news_count = 0
            earliest_news_date = None
            latest_news_date = None
        categories.append({
            "name": "News",
            "tickers_with_data": news_count,
            "denominator": total,
            "denominator_label": "all",
            "pct_coverage": round(news_count / total * 100, 1) if total > 0 else 0,
            "earliest_date": earliest_news_date,
            "latest_date": latest_news_date,
            "sample_missing": [],
        })

        # Social (content_tickers)
        try:
            row = conn.execute(sa.text(
                "SELECT COUNT(DISTINCT ticker) FROM content_tickers"
            )).fetchone()
            social_count = row[0] or 0
        except Exception:
            social_count = 0
        categories.append({
            "name": "Social",
            "tickers_with_data": social_count,
            "denominator": total,
            "denominator_label": "all",
            "pct_coverage": round(social_count / total * 100, 1) if total > 0 else 0,
            "earliest_date": None,
            "latest_date": None,
            "sample_missing": [],
        })

        # Sentiment
        try:
            row = conn.execute(sa.text(
                "SELECT COUNT(DISTINCT ticker), MIN(date), MAX(date) FROM ticker_sentiment_daily"
            )).fetchone()
            sent_count = row[0] or 0
            sent_earliest = str(row[1]) if row[1] else None
            sent_latest = str(row[2]) if row[2] else None
        except Exception:
            sent_count = 0
            sent_earliest = None
            sent_latest = None
        categories.append({
            "name": "Sentiment",
            "tickers_with_data": sent_count,
            "denominator": total,
            "denominator_label": "all",
            "pct_coverage": round(sent_count / total * 100, 1) if total > 0 else 0,
            "earliest_date": sent_earliest,
            "latest_date": sent_latest,
            "sample_missing": [],
        })

        # Ownership
        add_category("Ownership", "institutional_summary", "report_date")

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_tickers": total,
        "equity_tickers": equity_count,
        "etf_tickers": etf_count,
        "asset_type_counts": asset_counts,
        "tickers_with_prices": tickers_with_prices,
        "categories": categories,
    }

    data_str = json.dumps(result, separators=(",", ":"))
    _write_or_upload("coverage.json", data_str)
    print(f"Exported coverage.json: {total} total tickers ({equity_count} equity, {etf_count} ETF), {len(categories)} categories")


if __name__ == "__main__":
    main()
