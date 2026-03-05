"""
Export SQLite data to per-ticker JSON files for the Needlstack website.

Usage:
    python scripts/export_data.py                       # export all tickers → R2
    python scripts/export_data.py --tickers AAPL MSFT   # export specific tickers
    python scripts/export_data.py --skip-global         # skip global exports
    LOCAL_EXPORT=1 python scripts/export_data.py        # write to docs/data/ locally
"""

import argparse
import concurrent.futures
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import sqlalchemy as sa

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))
from db.schema import get_engine

DOCS_DIR = Path(__file__).parent.parent / "docs"
DATA_DIR = DOCS_DIR / "data"
EXPORT_LOG_PATH = Path(__file__).parent.parent / "data" / "export_log.json"

LOCAL_EXPORT = os.environ.get("LOCAL_EXPORT", "0") == "1"


# ── Storage helpers ────────────────────────────────────────────────────────────

def _write_or_upload(key: str, data: str) -> None:
    if LOCAL_EXPORT:
        out_path = DATA_DIR / key
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(data)
    else:
        from storage.r2 import upload_json
        upload_json(key, data)


def _load_export_log() -> dict:
    if EXPORT_LOG_PATH.exists():
        try:
            return json.loads(EXPORT_LOG_PATH.read_text())
        except Exception:
            return {}
    return {}


def _save_export_log(log: dict) -> None:
    EXPORT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    EXPORT_LOG_PATH.write_text(json.dumps(log, separators=(",", ":")))


def _get_latest_price_date(conn: sa.Connection, ticker: str) -> str | None:
    row = conn.execute(
        sa.text("SELECT MAX(date) FROM stock_prices WHERE ticker = :ticker"),
        {"ticker": ticker},
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def _filter_unchanged(conn: sa.Connection, tickers: list[str], log: dict) -> list[str]:
    """Skip tickers whose latest price date hasn't changed since last export."""
    out = []
    for ticker in tickers:
        last_exported = log.get(ticker)
        if last_exported is None:
            out.append(ticker)
            continue
        latest_date = _get_latest_price_date(conn, ticker)
        if latest_date is None or latest_date > last_exported[:10]:
            out.append(ticker)
    return out


# ── Per-ticker export functions (return JSON string) ──────────────────────────

def export_prices(conn: sa.Connection, ticker: str) -> str:
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
    return json.dumps(data, separators=(",", ":"))


def export_financials_v2(conn: sa.Connection, ticker: str) -> str:
    # Income statements
    try:
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
    except Exception:
        income_statements = []

    # Balance sheets
    try:
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
    except Exception:
        balance_sheets = []

    # Cash flows
    try:
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
    except Exception:
        cash_flows = []

    # Earnings surprises
    try:
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
    except Exception:
        earnings_surprises = []

    # Valuation snapshots (last 252 rows)
    try:
        vs_rows = conn.execute(
            sa.text(
                "SELECT snapshot_date, pe_ttm, pb, ps_ttm, ev_ebitda "
                "FROM valuation_snapshots WHERE ticker = :ticker "
                "ORDER BY snapshot_date DESC LIMIT 252"
            ),
            {"ticker": ticker},
        ).fetchall()
        valuation_snapshots = [
            {
                "snapshot_date": str(r[0]),
                "pe_ttm": r[1],
                "pb": r[2],
                "ps_ttm": r[3],
                "ev_ebitda": r[4],
            }
            for r in reversed(vs_rows)
        ]
    except Exception:
        valuation_snapshots = []

    data = {
        "income_statements": income_statements,
        "balance_sheets": balance_sheets,
        "cash_flows": cash_flows,
        "earnings_surprises": earnings_surprises,
        "valuation_snapshots": valuation_snapshots,
    }
    return json.dumps(data, separators=(",", ":"))


def export_metadata(conn: sa.Connection, ticker: str) -> str:
    try:
        row = conn.execute(
            sa.text(
                "SELECT market_cap, float_shares, shares_outstanding, enterprise_value, "
                "avg_volume_30d, avg_dollar_vol_30d, updated_at "
                "FROM security_metadata WHERE ticker = :ticker"
            ),
            {"ticker": ticker},
        ).fetchone()
        if row:
            data = {
                "market_cap": row[0],
                "float_shares": row[1],
                "shares_outstanding": row[2],
                "enterprise_value": row[3],
                "avg_volume_30d": row[4],
                "avg_dollar_vol_30d": row[5],
                "updated_at": str(row[6]) if row[6] is not None else None,
            }
        else:
            data = {}
    except Exception:
        data = {}
    return json.dumps(data, separators=(",", ":"))


def export_corporate_actions(conn: sa.Connection, ticker: str) -> str:
    try:
        rows = conn.execute(
            sa.text(
                "SELECT action_date, action_type, ratio, amount "
                "FROM corporate_actions WHERE ticker = :ticker ORDER BY action_date"
            ),
            {"ticker": ticker},
        ).fetchall()
        data = [
            {
                "action_date": str(r[0]),
                "action_type": r[1],
                "ratio": r[2],
                "amount": r[3],
            }
            for r in rows
        ]
    except Exception:
        data = []
    return json.dumps(data, separators=(",", ":"))


def export_profiles(conn: sa.Connection, ticker: str) -> str:
    try:
        row = conn.execute(
            sa.text(
                "SELECT description, employees, website, country, city, state "
                "FROM company_profiles WHERE ticker = :ticker"
            ),
            {"ticker": ticker},
        ).fetchone()
        if row:
            data = {
                "description": row[0],
                "employees": row[1],
                "website": row[2],
                "country": row[3],
                "city": row[4],
                "state": row[5],
            }
        else:
            data = {}
    except Exception:
        data = {}
    return json.dumps(data, separators=(",", ":"))


def export_ownership(conn: sa.Connection, ticker: str) -> str:
    try:
        summary_row = conn.execute(
            sa.text(
                "SELECT report_date, total_institutions, pct_outstanding_held, net_change_shares "
                "FROM institutional_summary WHERE ticker = :ticker "
                "ORDER BY report_date DESC LIMIT 1"
            ),
            {"ticker": ticker},
        ).fetchone()
        if summary_row:
            summary = {
                "report_date": str(summary_row[0]),
                "total_institutions": summary_row[1],
                "pct_outstanding_held": summary_row[2],
                "net_change_shares": summary_row[3],
            }
            latest_date = summary_row[0]
            holder_rows = conn.execute(
                sa.text(
                    "SELECT institution_name, shares_held, market_value, "
                    "pct_of_portfolio, change_shares "
                    "FROM institutional_holdings "
                    "WHERE ticker = :ticker AND report_date = :report_date "
                    "ORDER BY shares_held DESC LIMIT 10"
                ),
                {"ticker": ticker, "report_date": latest_date},
            ).fetchall()
            top_holders = [
                {
                    "institution_name": r[0],
                    "shares_held": r[1],
                    "market_value": r[2],
                    "pct_of_portfolio": r[3],
                    "change_shares": r[4],
                }
                for r in holder_rows
            ]
        else:
            summary = None
            top_holders = []
        data = {"summary": summary, "top_holders": top_holders}
    except Exception:
        data = {"summary": None, "top_holders": []}
    return json.dumps(data, separators=(",", ":"))


def export_sentiment(conn: sa.Connection, ticker: str) -> str:
    try:
        rows = conn.execute(
            sa.text(
                "SELECT date, bullish_score, bearish_score, compound_score, mention_count "
                "FROM ticker_sentiment_daily "
                "WHERE ticker = :ticker "
                "AND date >= date('now', '-30 days') "
                "ORDER BY date"
            ),
            {"ticker": ticker},
        ).fetchall()
        data = [
            {
                "date": str(r[0]),
                "bullish_score": r[1],
                "bearish_score": r[2],
                "compound_score": r[3],
                "mention_count": r[4],
            }
            for r in rows
        ]
    except Exception:
        data = []
    return json.dumps(data, separators=(",", ":"))


def export_news(conn: sa.Connection, ticker: str) -> str:
    try:
        rows = conn.execute(
            sa.text(
                "SELECT na.article_id, na.title, na.url, na.source_id, na.published_at, "
                "asen.sentiment_label, asen.compound_score, at.mention_in_title "
                "FROM article_tickers at "
                "JOIN news_articles na ON na.article_id = at.article_id "
                "LEFT JOIN article_sentiment asen ON asen.article_id = at.article_id "
                "WHERE at.ticker = :ticker "
                "ORDER BY na.published_at DESC LIMIT 20"
            ),
            {"ticker": ticker},
        ).fetchall()
        data = [
            {
                "article_id": r[0],
                "title": r[1],
                "url": r[2],
                "source_id": r[3],
                "published_at": str(r[4]) if r[4] is not None else None,
                "sentiment_label": r[5],
                "compound_score": r[6],
                "mention_in_title": r[7],
            }
            for r in rows
        ]
    except Exception:
        data = []
    return json.dumps(data, separators=(",", ":"))


def export_social(conn: sa.Connection, ticker: str) -> str:
    reddit: list[dict] = []
    stocktwits: list[dict] = []

    try:
        reddit_rows = conn.execute(
            sa.text(
                "SELECT DATE(ci.created_at) as day, "
                "COUNT(*) as mention_count, AVG(ci.sentiment_score) as avg_sentiment "
                "FROM content_tickers ct "
                "JOIN content_items ci ON ci.item_id = ct.item_id "
                "WHERE ct.ticker = :ticker "
                "AND ci.source_type = 'reddit' "
                "AND ci.created_at >= date('now', '-30 days') "
                "GROUP BY day ORDER BY day"
            ),
            {"ticker": ticker},
        ).fetchall()
        reddit = [
            {
                "date": str(r[0]),
                "mention_count": r[1],
                "avg_sentiment": r[2],
            }
            for r in reddit_rows
        ]
    except Exception:
        reddit = []

    try:
        st_rows = conn.execute(
            sa.text(
                "SELECT DATE(ci.created_at) as day, "
                "COUNT(*) as mention_count, AVG(ci.sentiment_score) as avg_sentiment, "
                "SUM(CASE WHEN ci.sentiment_label = 'bullish' THEN 1 ELSE 0 END) as bullish_count, "
                "SUM(CASE WHEN ci.sentiment_label = 'bearish' THEN 1 ELSE 0 END) as bearish_count "
                "FROM content_tickers ct "
                "JOIN content_items ci ON ci.item_id = ct.item_id "
                "WHERE ct.ticker = :ticker "
                "AND ci.source_type = 'stocktwits' "
                "AND ci.created_at >= date('now', '-30 days') "
                "GROUP BY day ORDER BY day"
            ),
            {"ticker": ticker},
        ).fetchall()
        stocktwits = [
            {
                "date": str(r[0]),
                "mention_count": r[1],
                "avg_sentiment": r[2],
                "bullish_count": r[3],
                "bearish_count": r[4],
            }
            for r in st_rows
        ]
    except Exception:
        stocktwits = []

    data = {"reddit": reddit, "stocktwits": stocktwits}
    return json.dumps(data, separators=(",", ":"))


# ── Global export functions (return JSON string) ───────────────────────────────

def export_tickers_global(conn: sa.Connection) -> tuple[str, list[str]]:
    rows = conn.execute(
        sa.text(
            "SELECT ticker, company_name, sector, industry, asset_type, exchange "
            "FROM tickers ORDER BY ticker"
        )
    ).fetchall()
    tickers_data = [
        {
            "ticker": r[0],
            "company_name": r[1],
            "sector": r[2],
            "industry": r[3],
            "asset_type": r[4],
            "exchange": r[5],
        }
        for r in rows
    ]
    return json.dumps(tickers_data, separators=(",", ":")), [r[0] for r in rows]


def export_indexes_global(conn: sa.Connection) -> str:
    try:
        rows = conn.execute(
            sa.text(
                "SELECT index_id, ticker FROM index_constituents "
                "WHERE removed_date IS NULL ORDER BY index_id, ticker"
            )
        ).fetchall()
        indexes: dict[str, list[str]] = {}
        for r in rows:
            indexes.setdefault(r[0], []).append(r[1])
    except Exception:
        indexes = {}
    return json.dumps(indexes, separators=(",", ":"))


def export_macro_global(conn: sa.Connection) -> str:
    try:
        rows = conn.execute(
            sa.text(
                "SELECT sp.ticker, t.company_name, sp.close, sp.date, t.asset_type "
                "FROM stock_prices sp "
                "JOIN tickers t ON t.ticker = sp.ticker "
                "WHERE ("
                "  t.asset_type IN ('index', 'fx', 'commodity') "
                "  OR sp.ticker LIKE '^%' "
                "  OR sp.ticker LIKE '%-X' "
                "  OR sp.ticker LIKE '%=F' "
                "  OR sp.ticker LIKE 'BTC-%'"
                ") "
                "AND sp.date = ("
                "  SELECT MAX(sp2.date) FROM stock_prices sp2 WHERE sp2.ticker = sp.ticker"
                ") "
                "ORDER BY sp.ticker"
            )
        ).fetchall()
        data = [
            {
                "ticker": r[0],
                "name": r[1],
                "close": r[2],
                "date": str(r[3]),
                "asset_type": r[4],
            }
            for r in rows
        ]
    except Exception:
        data = []
    return json.dumps(data, separators=(",", ":"))


def export_narratives_global(conn: sa.Connection) -> str:
    try:
        narrative_rows = conn.execute(
            sa.text(
                "SELECT narrative_id, name, description FROM narratives WHERE is_active = 1"
            )
        ).fetchall()

        data = []
        for nr in narrative_rows:
            narrative_id = nr[0]

            try:
                rt_row = conn.execute(
                    sa.text(
                        "SELECT related_tickers FROM narratives WHERE narrative_id = :nid"
                    ),
                    {"nid": narrative_id},
                ).fetchone()
                if rt_row and rt_row[0]:
                    related_tickers = json.loads(rt_row[0])
                else:
                    related_tickers = []
            except Exception:
                related_tickers = []

            try:
                signal_rows = conn.execute(
                    sa.text(
                        "SELECT date, mention_count, momentum_score "
                        "FROM narrative_signals "
                        "WHERE narrative_id = :nid "
                        "AND date >= date('now', '-30 days') "
                        "ORDER BY date"
                    ),
                    {"nid": narrative_id},
                ).fetchall()
                signals = [
                    {
                        "date": str(r[0]),
                        "mention_count": r[1],
                        "momentum_score": r[2],
                    }
                    for r in signal_rows
                ]
            except Exception:
                signals = []

            data.append(
                {
                    "narrative_id": narrative_id,
                    "name": nr[1],
                    "description": nr[2],
                    "related_tickers": related_tickers,
                    "signals": signals,
                }
            )
    except Exception:
        data = []
    return json.dumps(data, separators=(",", ":"))


# ── Main ───────────────────────────────────────────────────────────────────────

def _collect_ticker_pairs(conn: sa.Connection, ticker: str) -> list[tuple[str, str]]:
    return [
        (f"prices/{ticker}.json", export_prices(conn, ticker)),
        (f"financials/{ticker}.json", export_financials_v2(conn, ticker)),
        (f"metadata/{ticker}.json", export_metadata(conn, ticker)),
        (f"corporate_actions/{ticker}.json", export_corporate_actions(conn, ticker)),
        (f"profiles/{ticker}.json", export_profiles(conn, ticker)),
        (f"ownership/{ticker}.json", export_ownership(conn, ticker)),
        (f"sentiment/{ticker}.json", export_sentiment(conn, ticker)),
        (f"news/{ticker}.json", export_news(conn, ticker)),
        (f"social/{ticker}.json", export_social(conn, ticker)),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Export needlstack DB to JSON for website")
    parser.add_argument(
        "--tickers",
        nargs="+",
        metavar="TICKER",
        help="Export only these tickers (default: all)",
    )
    parser.add_argument(
        "--skip-global",
        action="store_true",
        help="Skip global exports (tickers.json, indexes.json, macro.json, narratives.json)",
    )
    parser.add_argument(
        "--no-incremental",
        action="store_true",
        help="Ignore export log and export all tickers regardless of change",
    )
    args = parser.parse_args()

    engine = get_engine()
    log = _load_export_log()

    destination = "docs/data/ (local)" if LOCAL_EXPORT else "R2"
    print(f"Export destination: {destination}")

    with engine.connect() as conn:
        tickers_json, all_tickers = export_tickers_global(conn)

        # Global files
        if not args.skip_global:
            global_pairs = [
                ("tickers.json", tickers_json),
                ("indexes.json", export_indexes_global(conn)),
                ("macro.json", export_macro_global(conn)),
                ("narratives.json", export_narratives_global(conn)),
            ]
            for key, data in global_pairs:
                _write_or_upload(key, data)
            print(f"Exported {len(global_pairs)} global files")

        tickers_to_export = args.tickers if args.tickers else all_tickers

        # Validate requested tickers
        invalid = set(tickers_to_export) - set(all_tickers)
        if invalid:
            print(f"Warning: unknown tickers skipped: {sorted(invalid)}")
            tickers_to_export = [t for t in tickers_to_export if t not in invalid]

        # Incremental: skip unchanged tickers
        if not args.no_incremental and not args.tickers:
            before = len(tickers_to_export)
            tickers_to_export = _filter_unchanged(conn, tickers_to_export, log)
            skipped = before - len(tickers_to_export)
            if skipped:
                print(f"Skipping {skipped} unchanged tickers (use --no-incremental to force)")

        total = len(tickers_to_export)
        print(f"Exporting {total} tickers...")

        # Collect all (key, data) pairs — sequential DB queries
        all_uploads: list[tuple[str, str]] = []
        for i, ticker in enumerate(tickers_to_export, 1):
            all_uploads.extend(_collect_ticker_pairs(conn, ticker))
            if i % 100 == 0 or i == total:
                print(f"  Queried {i}/{total} tickers")

    # Parallel uploads
    n_uploads = len(all_uploads)
    print(f"Uploading {n_uploads} files...")
    upload_errors = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        futures = {pool.submit(_write_or_upload, key, data): key for key, data in all_uploads}
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                fut.result()
            except Exception as exc:
                upload_errors += 1
                print(f"  Upload error for {futures[fut]}: {exc}")
            if i % 1000 == 0 or i == n_uploads:
                print(f"  Uploaded {i}/{n_uploads} files")

    if upload_errors == 0:
        # Update export log for successfully exported tickers
        now = datetime.now(timezone.utc).isoformat()
        for ticker in tickers_to_export:
            log[ticker] = now
        _save_export_log(log)
        print(f"Export log updated for {total} tickers")
    else:
        print(f"Warning: {upload_errors} upload errors — export log not updated")

    print(f"Done. {n_uploads - upload_errors}/{n_uploads} files exported successfully.")


if __name__ == "__main__":
    main()
