"""
Compute derived financial metrics from existing DB tables.
Pure pandas/SQL computation — no external API calls.

TTM computation: sum of last 4 period_type='Q' rows sorted by period_end DESC.
YoY growth = (TTM - prior_year_TTM) / abs(prior_year_TTM).
Store NULL if denominator is 0 or prior year is missing.

Metrics computed:
- revenue_yoy_growth: from income_statements.revenue
- net_income_yoy_growth: from income_statements.net_income
- eps_yoy_growth: from income_statements.eps_diluted
- gross_margin: gross_profit / revenue (TTM)
- operating_margin: operating_income / revenue (TTM)
- net_margin: net_income / revenue (TTM)
- fcf_margin: free_cash_flow / revenue (from cash_flows, TTM)
- roe: net_income / stockholders_equity (latest annual or TTM)
- roa: net_income / total_assets (latest annual or TTM)
- debt_to_equity: long_term_debt / stockholders_equity
- current_ratio: current_assets / current_liabilities
- pe_ttm: from valuation_snapshots latest
- ev_ebitda: from valuation_snapshots latest
"""
import logging
from datetime import date
from typing import Optional

import pandas as pd
import sqlalchemy as sa

from db.schema import get_engine

logger = logging.getLogger(__name__)


def _safe_divide(numerator, denominator):
    """Return numerator / denominator, or None if denominator is 0 or None."""
    if denominator is None or numerator is None:
        return None
    try:
        denom = float(denominator)
        if denom == 0.0:
            return None
        return float(numerator) / denom
    except (TypeError, ValueError):
        return None


def _yoy_growth(ttm_current, ttm_prior):
    """Compute YoY growth rate. Returns None if denominator is 0 or prior is None."""
    if ttm_current is None or ttm_prior is None:
        return None
    try:
        prior = float(ttm_prior)
        if prior == 0.0:
            return None
        return (float(ttm_current) - prior) / abs(prior)
    except (TypeError, ValueError):
        return None


def _compute_ttm(df: pd.DataFrame, col: str) -> Optional[float]:
    """Sum the last 4 quarterly rows for a given column. Returns None if fewer than 4 rows."""
    quarterly = df[df["period_type"] == "Q"].sort_values("period_end", ascending=False)
    last4 = quarterly.head(4)
    if len(last4) < 4:
        return None
    values = last4[col].dropna()
    if len(values) < 4:
        return None
    return float(values.sum())


def _compute_ttm_prior_year(df: pd.DataFrame, col: str) -> Optional[float]:
    """Sum the 4 quarterly rows from 4-8 periods ago (prior year TTM)."""
    quarterly = df[df["period_type"] == "Q"].sort_values("period_end", ascending=False)
    prior4 = quarterly.iloc[4:8]
    if len(prior4) < 4:
        return None
    values = prior4[col].dropna()
    if len(values) < 4:
        return None
    return float(values.sum())


def _compute_ticker_metrics(ticker: str, engine: sa.Engine) -> dict:
    """Compute all derived metrics for a single ticker. Returns a dict ready for upsert."""
    metrics: dict = {"ticker": ticker, "date": date.today().isoformat()}

    with engine.connect() as conn:
        # ── Income statements ──────────────────────────────────────────────────
        is_rows = conn.execute(
            sa.text(
                """
                SELECT period_end, period_type, revenue, gross_profit,
                       operating_income, net_income, eps_diluted
                FROM income_statements
                WHERE ticker = :ticker
                ORDER BY period_end DESC
                LIMIT 16
                """
            ),
            {"ticker": ticker},
        ).fetchall()

        # ── Balance sheets ─────────────────────────────────────────────────────
        bs_rows = conn.execute(
            sa.text(
                """
                SELECT period_end, period_type, current_assets, current_liabilities,
                       long_term_debt, stockholders_equity, total_assets
                FROM balance_sheets
                WHERE ticker = :ticker
                ORDER BY period_end DESC
                LIMIT 8
                """
            ),
            {"ticker": ticker},
        ).fetchall()

        # ── Cash flows ─────────────────────────────────────────────────────────
        cf_rows = conn.execute(
            sa.text(
                """
                SELECT period_end, period_type, free_cash_flow
                FROM cash_flows
                WHERE ticker = :ticker
                ORDER BY period_end DESC
                LIMIT 16
                """
            ),
            {"ticker": ticker},
        ).fetchall()

        # ── Valuation snapshots ────────────────────────────────────────────────
        vs_row = conn.execute(
            sa.text(
                """
                SELECT pe_ttm, ev_ebitda
                FROM valuation_snapshots
                WHERE ticker = :ticker
                ORDER BY snapshot_date DESC
                LIMIT 1
                """
            ),
            {"ticker": ticker},
        ).fetchone()

    if is_rows:
        is_df = pd.DataFrame(
            is_rows,
            columns=["period_end", "period_type", "revenue", "gross_profit",
                     "operating_income", "net_income", "eps_diluted"],
        )

        # TTM values
        ttm_revenue = _compute_ttm(is_df, "revenue")
        ttm_gross_profit = _compute_ttm(is_df, "gross_profit")
        ttm_operating_income = _compute_ttm(is_df, "operating_income")
        ttm_net_income = _compute_ttm(is_df, "net_income")
        ttm_eps = _compute_ttm(is_df, "eps_diluted")

        # Prior-year TTM values for YoY growth
        prior_revenue = _compute_ttm_prior_year(is_df, "revenue")
        prior_net_income = _compute_ttm_prior_year(is_df, "net_income")
        prior_eps = _compute_ttm_prior_year(is_df, "eps_diluted")

        metrics["revenue_yoy_growth"] = _yoy_growth(ttm_revenue, prior_revenue)
        metrics["net_income_yoy_growth"] = _yoy_growth(ttm_net_income, prior_net_income)
        metrics["eps_yoy_growth"] = _yoy_growth(ttm_eps, prior_eps)

        metrics["gross_margin"] = _safe_divide(ttm_gross_profit, ttm_revenue)
        metrics["operating_margin"] = _safe_divide(ttm_operating_income, ttm_revenue)
        metrics["net_margin"] = _safe_divide(ttm_net_income, ttm_revenue)
    else:
        for key in ["revenue_yoy_growth", "net_income_yoy_growth", "eps_yoy_growth",
                    "gross_margin", "operating_margin", "net_margin"]:
            metrics[key] = None
        ttm_revenue = None
        ttm_net_income = None

    # ── FCF margin ─────────────────────────────────────────────────────────────
    if cf_rows:
        cf_df = pd.DataFrame(cf_rows, columns=["period_end", "period_type", "free_cash_flow"])
        ttm_fcf = _compute_ttm(cf_df, "free_cash_flow")
        metrics["fcf_margin"] = _safe_divide(ttm_fcf, ttm_revenue)
    else:
        metrics["fcf_margin"] = None

    # ── Balance sheet ratios ───────────────────────────────────────────────────
    if bs_rows:
        bs_df = pd.DataFrame(
            bs_rows,
            columns=["period_end", "period_type", "current_assets", "current_liabilities",
                     "long_term_debt", "stockholders_equity", "total_assets"],
        )

        # Use the most recent row (any period type)
        latest_bs = bs_df.iloc[0]

        equity = latest_bs["stockholders_equity"]
        total_assets = latest_bs["total_assets"]
        long_term_debt = latest_bs["long_term_debt"]
        current_assets = latest_bs["current_assets"]
        current_liabilities = latest_bs["current_liabilities"]

        metrics["roe"] = _safe_divide(ttm_net_income, equity)
        metrics["roa"] = _safe_divide(ttm_net_income, total_assets)
        metrics["debt_to_equity"] = _safe_divide(long_term_debt, equity)
        metrics["current_ratio"] = _safe_divide(current_assets, current_liabilities)
    else:
        for key in ["roe", "roa", "debt_to_equity", "current_ratio"]:
            metrics[key] = None

    # ── Valuation multiples ────────────────────────────────────────────────────
    if vs_row:
        metrics["pe_ttm"] = vs_row[0]
        metrics["ev_ebitda"] = vs_row[1]
    else:
        metrics["pe_ttm"] = None
        metrics["ev_ebitda"] = None

    return metrics


def _upsert_derived_metrics(engine: sa.Engine, rows: list[dict]) -> int:
    """Upsert rows into derived_metrics table."""
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO derived_metrics (
                    ticker, date,
                    revenue_yoy_growth, net_income_yoy_growth, eps_yoy_growth,
                    gross_margin, operating_margin, net_margin, fcf_margin,
                    roe, roa, debt_to_equity, current_ratio,
                    pe_ttm, ev_ebitda
                ) VALUES (
                    :ticker, :date,
                    :revenue_yoy_growth, :net_income_yoy_growth, :eps_yoy_growth,
                    :gross_margin, :operating_margin, :net_margin, :fcf_margin,
                    :roe, :roa, :debt_to_equity, :current_ratio,
                    :pe_ttm, :ev_ebitda
                )
                """
            ),
            rows,
        )
    return len(rows)


def compute_derived_metrics(
    tickers: list[str],
    engine: Optional[sa.Engine] = None,
) -> tuple[int, list[str]]:
    """
    Compute derived financial metrics for a list of tickers and upsert into derived_metrics.

    Returns (rows_upserted, failed_tickers).
    """
    if engine is None:
        engine = get_engine()

    rows: list[dict] = []
    failed: list[str] = []

    for ticker in tickers:
        try:
            metrics = _compute_ticker_metrics(ticker, engine)
            rows.append(metrics)
            logger.debug(f"{ticker}: metrics computed")
        except Exception as exc:
            logger.error(f"Failed to compute metrics for {ticker}: {exc}")
            failed.append(ticker)

    upserted = _upsert_derived_metrics(engine, rows)
    logger.info(f"Derived metrics: {upserted} rows upserted, {len(failed)} failures.")
    return upserted, failed
