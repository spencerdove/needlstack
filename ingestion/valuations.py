"""
Pure DB computation — no external API calls.

Computes valuation multiples from existing tables and upserts into
valuation_snapshots with snapshot_date = today.

Formulas
--------
TTM values: sum of last 4 period_type='Q' rows (sorted by period_end DESC)
from income_statements.

P/E TTM      = latest close price / TTM EPS diluted
P/B          = market_cap / book_value_per_share
               book_value_per_share = stockholders_equity / shares_outstanding
               (from security_metadata + balance_sheets)
P/S TTM      = market_cap / TTM revenue
EV/EBITDA    = enterprise_value / TTM operating_income (proxy for EBITDA)

NULL is stored on division by zero or negative denominator.
"""
import logging
from datetime import date
from typing import Optional

import sqlalchemy as sa

from db.schema import get_engine

logger = logging.getLogger(__name__)


def _safe_div(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    """Divide numerator by denominator; return None on zero, None, or negative denom."""
    if numerator is None or denominator is None:
        return None
    if denominator <= 0:
        return None
    return numerator / denominator


def _get_ttm_income(conn: sa.Connection, ticker: str) -> dict:
    """Sum the last 4 quarterly income statement rows for TTM computation."""
    rows = conn.execute(
        sa.text(
            """
            SELECT revenue, operating_income, eps_diluted
            FROM income_statements
            WHERE ticker = :ticker
              AND period_type = 'Q'
            ORDER BY period_end DESC
            LIMIT 4
            """
        ),
        {"ticker": ticker},
    ).fetchall()

    if not rows:
        return {"revenue": None, "operating_income": None, "eps_diluted": None}

    def _sum_col(idx: int) -> Optional[float]:
        vals = [r[idx] for r in rows if r[idx] is not None]
        return sum(vals) if vals else None

    return {
        "revenue": _sum_col(0),
        "operating_income": _sum_col(1),
        "eps_diluted": _sum_col(2),
    }


def _get_latest_price(conn: sa.Connection, ticker: str) -> Optional[float]:
    """Return the most recent closing price for ticker."""
    row = conn.execute(
        sa.text(
            """
            SELECT close FROM stock_prices
            WHERE ticker = :ticker
              AND close IS NOT NULL
            ORDER BY date DESC
            LIMIT 1
            """
        ),
        {"ticker": ticker},
    ).fetchone()
    return float(row[0]) if row else None


def _get_security_metadata(conn: sa.Connection, ticker: str) -> dict:
    """Return market_cap, enterprise_value, shares_outstanding from security_metadata."""
    row = conn.execute(
        sa.text(
            """
            SELECT market_cap, enterprise_value, shares_outstanding
            FROM security_metadata
            WHERE ticker = :ticker
            """
        ),
        {"ticker": ticker},
    ).fetchone()
    if not row:
        return {"market_cap": None, "enterprise_value": None, "shares_outstanding": None}
    return {
        "market_cap": float(row[0]) if row[0] is not None else None,
        "enterprise_value": float(row[1]) if row[1] is not None else None,
        "shares_outstanding": float(row[2]) if row[2] is not None else None,
    }


def _get_latest_equity(conn: sa.Connection, ticker: str) -> Optional[float]:
    """Return the most recent stockholders_equity from balance_sheets."""
    row = conn.execute(
        sa.text(
            """
            SELECT stockholders_equity FROM balance_sheets
            WHERE ticker = :ticker
              AND stockholders_equity IS NOT NULL
            ORDER BY period_end DESC
            LIMIT 1
            """
        ),
        {"ticker": ticker},
    ).fetchone()
    return float(row[0]) if row else None


def _upsert_valuations(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO valuation_snapshots
                    (ticker, snapshot_date, pe_ttm, pb, ps_ttm, ev_ebitda, peg_ratio)
                VALUES
                    (:ticker, :snapshot_date, :pe_ttm, :pb, :ps_ttm, :ev_ebitda, :peg_ratio)
                """
            ),
            rows,
        )
    return len(rows)


def compute_valuations(
    tickers: list[str],
    engine: Optional[sa.Engine] = None,
) -> tuple[int, list[str]]:
    """
    Compute valuation multiples from existing DB data and upsert into
    valuation_snapshots.

    Returns (rows_upserted, failed_tickers).
    """
    if engine is None:
        engine = get_engine()

    snapshot_date = date.today().isoformat()
    total_rows = 0
    failed: list[str] = []

    with engine.connect() as conn:
        for ticker in tickers:
            try:
                ttm = _get_ttm_income(conn, ticker)
                latest_price = _get_latest_price(conn, ticker)
                meta = _get_security_metadata(conn, ticker)
                equity = _get_latest_equity(conn, ticker)

                # P/E TTM = price / TTM EPS diluted
                pe_ttm = _safe_div(latest_price, ttm["eps_diluted"])

                # P/B = market_cap / book_value_per_share
                #       book_value_per_share = stockholders_equity / shares_outstanding
                bvps = _safe_div(equity, meta["shares_outstanding"])
                pb = _safe_div(meta["market_cap"], bvps) if bvps and bvps > 0 else None

                # P/S TTM = market_cap / TTM revenue
                ps_ttm = _safe_div(meta["market_cap"], ttm["revenue"])

                # EV/EBITDA = enterprise_value / TTM operating_income (proxy)
                ev_ebitda = _safe_div(meta["enterprise_value"], ttm["operating_income"])

                rows = [{
                    "ticker": ticker,
                    "snapshot_date": snapshot_date,
                    "pe_ttm": pe_ttm,
                    "pb": pb,
                    "ps_ttm": ps_ttm,
                    "ev_ebitda": ev_ebitda,
                    "peg_ratio": None,  # not computed here
                }]

                inserted = _upsert_valuations(engine, rows)
                total_rows += inserted
                logger.debug(
                    f"{ticker}: valuation_snapshot upserted "
                    f"(P/E={pe_ttm}, P/B={pb}, P/S={ps_ttm}, EV/EBITDA={ev_ebitda})"
                )

            except Exception as exc:
                logger.error(f"Failed to compute valuations for {ticker}: {exc}")
                failed.append(ticker)

    logger.info(f"compute_valuations: {total_rows} rows upserted, {len(failed)} failures")
    return total_rows, failed
