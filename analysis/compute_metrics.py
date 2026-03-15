"""
Compute derived financial metrics from existing DB tables.
Pure pandas/SQL computation — no external API calls.

TTM computation: sum of last 4 period_type='Q' rows sorted by period_end DESC.
YoY growth = (TTM - prior_year_TTM) / abs(prior_year_TTM).
Store NULL if denominator is 0 or prior year is missing.

Metrics computed:
- revenue_yoy_growth, net_income_yoy_growth, eps_yoy_growth
- gross_margin, operating_margin, net_margin, fcf_margin
- pretax_margin, ocf_margin, ebitda_margin, capex_to_revenue
- roe, roa, roic, roce
- debt_to_equity, current_ratio, quick_ratio, cash_ratio
- working_capital, net_debt
- debt_to_assets, debt_to_capital, equity_ratio
- net_debt_to_ebitda, debt_to_ebitda, interest_coverage
- asset_turnover, inventory_turnover, receivables_turnover, payables_turnover
- dso, dio, dpo, ccc
- ocf_per_share, fcf_per_share, cash_conversion_ratio, accrual_ratio
- book_value_per_share, tangible_book_value_per_share
- ebitda, ocf_ttm, fcf_ttm
- revenue_qoq_growth, operating_income_yoy_growth, ocf_yoy_growth, fcf_yoy_growth
- ebitda_yoy_growth, revenue_3yr_cagr, revenue_5yr_cagr, eps_3yr_cagr, eps_5yr_cagr
- dividend_yield, dividend_payout_ratio, buyback_yield, shareholder_yield
- pe_ttm, ev_ebitda (from valuation_snapshots)
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


def _compute_ttm_n_years_ago(df: pd.DataFrame, col: str, years: int) -> Optional[float]:
    """Sum 4 quarterly rows starting at (years*4) periods back."""
    quarterly = df[df["period_type"] == "Q"].sort_values("period_end", ascending=False)
    start = years * 4
    end = start + 4
    slice4 = quarterly.iloc[start:end]
    if len(slice4) < 4:
        return None
    values = slice4[col].dropna()
    if len(values) < 4:
        return None
    return float(values.sum())


def _cagr(current, base, years: int) -> Optional[float]:
    """Compute CAGR = (current/base)^(1/years) - 1. Returns None if invalid."""
    if current is None or base is None or base <= 0 or years <= 0:
        return None
    try:
        return (float(current) / float(base)) ** (1.0 / years) - 1.0
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _avg(*vals) -> Optional[float]:
    """Average of non-None values. Returns None if no values."""
    valid = [float(v) for v in vals if v is not None]
    return sum(valid) / len(valid) if valid else None


def _compute_ticker_metrics(ticker: str, engine: sa.Engine) -> dict:
    """Compute all derived metrics for a single ticker. Returns a dict ready for upsert."""
    metrics: dict = {"ticker": ticker, "date": date.today().isoformat()}

    with engine.connect() as conn:
        # ── Income statements ──────────────────────────────────────────────────
        is_rows = conn.execute(
            sa.text(
                """
                SELECT period_end, period_type, revenue, gross_profit,
                       operating_income, net_income, eps_diluted,
                       pretax_income, income_tax, cost_of_revenue, interest_expense,
                       sga, rd_expense
                FROM income_statements
                WHERE ticker = :ticker
                ORDER BY period_end DESC
                LIMIT 24
                """
            ),
            {"ticker": ticker},
        ).fetchall()

        # ── Balance sheets ─────────────────────────────────────────────────────
        bs_rows = conn.execute(
            sa.text(
                """
                SELECT period_end, period_type, current_assets, current_liabilities,
                       long_term_debt, stockholders_equity, total_assets,
                       cash, accounts_payable, inventory, accounts_receivable,
                       goodwill, intangible_assets, short_term_debt, total_liabilities,
                       ppe_net
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
                SELECT period_end, period_type, free_cash_flow, operating_cf,
                       capex, dividends_paid, stock_repurchases, depreciation_amortization
                FROM cash_flows
                WHERE ticker = :ticker
                ORDER BY period_end DESC
                LIMIT 24
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

        # ── Security metadata ──────────────────────────────────────────────────
        meta_row = conn.execute(
            sa.text(
                """
                SELECT market_cap, shares_outstanding
                FROM security_metadata
                WHERE ticker = :ticker
                """
            ),
            {"ticker": ticker},
        ).fetchone()

        # ── Latest price ───────────────────────────────────────────────────────
        price_row = conn.execute(
            sa.text(
                """
                SELECT close FROM stock_prices
                WHERE ticker = :ticker AND close IS NOT NULL
                ORDER BY date DESC LIMIT 1
                """
            ),
            {"ticker": ticker},
        ).fetchone()

    market_cap = float(meta_row[0]) if meta_row and meta_row[0] is not None else None
    shares_outstanding = float(meta_row[1]) if meta_row and meta_row[1] is not None else None
    latest_price = float(price_row[0]) if price_row else None

    # ── Income statement metrics ───────────────────────────────────────────────
    ttm_revenue = None
    ttm_gross_profit = None
    ttm_operating_income = None
    ttm_net_income = None
    ttm_eps = None
    ttm_pretax_income = None
    ttm_income_tax = None
    ttm_cost_of_revenue = None
    ttm_interest_expense = None

    ttm_sga = None
    ttm_rd_expense = None

    if is_rows:
        is_df = pd.DataFrame(
            is_rows,
            columns=["period_end", "period_type", "revenue", "gross_profit",
                     "operating_income", "net_income", "eps_diluted",
                     "pretax_income", "income_tax", "cost_of_revenue", "interest_expense",
                     "sga", "rd_expense"],
        )

        ttm_revenue = _compute_ttm(is_df, "revenue")
        ttm_gross_profit = _compute_ttm(is_df, "gross_profit")
        ttm_operating_income = _compute_ttm(is_df, "operating_income")
        ttm_net_income = _compute_ttm(is_df, "net_income")
        ttm_eps = _compute_ttm(is_df, "eps_diluted")
        ttm_pretax_income = _compute_ttm(is_df, "pretax_income")
        ttm_income_tax = _compute_ttm(is_df, "income_tax")
        ttm_cost_of_revenue = _compute_ttm(is_df, "cost_of_revenue")
        ttm_interest_expense = _compute_ttm(is_df, "interest_expense")
        ttm_sga = _compute_ttm(is_df, "sga")
        ttm_rd_expense = _compute_ttm(is_df, "rd_expense")

        prior_revenue = _compute_ttm_prior_year(is_df, "revenue")
        prior_net_income = _compute_ttm_prior_year(is_df, "net_income")
        prior_eps = _compute_ttm_prior_year(is_df, "eps_diluted")
        prior_operating_income = _compute_ttm_prior_year(is_df, "operating_income")

        metrics["revenue_yoy_growth"] = _yoy_growth(ttm_revenue, prior_revenue)
        metrics["net_income_yoy_growth"] = _yoy_growth(ttm_net_income, prior_net_income)
        metrics["eps_yoy_growth"] = _yoy_growth(ttm_eps, prior_eps)
        metrics["operating_income_yoy_growth"] = _yoy_growth(ttm_operating_income, prior_operating_income)

        metrics["gross_margin"] = _safe_divide(ttm_gross_profit, ttm_revenue)
        metrics["operating_margin"] = _safe_divide(ttm_operating_income, ttm_revenue)
        metrics["net_margin"] = _safe_divide(ttm_net_income, ttm_revenue)
        metrics["pretax_margin"] = _safe_divide(ttm_pretax_income, ttm_revenue)
        metrics["sga_margin"] = _safe_divide(ttm_sga, ttm_revenue)
        metrics["rd_margin"] = _safe_divide(ttm_rd_expense, ttm_revenue)

        # QoQ revenue growth (latest quarter vs prior quarter)
        quarterly_sorted = is_df[is_df["period_type"] == "Q"].sort_values("period_end", ascending=False)
        if len(quarterly_sorted) >= 2:
            q0 = quarterly_sorted.iloc[0]["revenue"]
            q1 = quarterly_sorted.iloc[1]["revenue"]
            if q0 is not None and q1 is not None and q1 != 0:
                metrics["revenue_qoq_growth"] = (float(q0) - float(q1)) / abs(float(q1))
            else:
                metrics["revenue_qoq_growth"] = None
        else:
            metrics["revenue_qoq_growth"] = None

        # CAGR calculations (3yr and 5yr)
        rev_3yr_ago = _compute_ttm_n_years_ago(is_df, "revenue", 3)
        rev_5yr_ago = _compute_ttm_n_years_ago(is_df, "revenue", 5)
        eps_3yr_ago = _compute_ttm_n_years_ago(is_df, "eps_diluted", 3)
        eps_5yr_ago = _compute_ttm_n_years_ago(is_df, "eps_diluted", 5)

        metrics["revenue_3yr_cagr"] = _cagr(ttm_revenue, rev_3yr_ago, 3)
        metrics["revenue_5yr_cagr"] = _cagr(ttm_revenue, rev_5yr_ago, 5)
        metrics["eps_3yr_cagr"] = _cagr(ttm_eps, eps_3yr_ago, 3)
        metrics["eps_5yr_cagr"] = _cagr(ttm_eps, eps_5yr_ago, 5)
    else:
        for key in ["revenue_yoy_growth", "net_income_yoy_growth", "eps_yoy_growth",
                    "operating_income_yoy_growth", "gross_margin", "operating_margin",
                    "net_margin", "pretax_margin", "sga_margin", "rd_margin",
                    "revenue_qoq_growth", "revenue_3yr_cagr", "revenue_5yr_cagr",
                    "eps_3yr_cagr", "eps_5yr_cagr"]:
            metrics[key] = None

    # ── Cash flow metrics ──────────────────────────────────────────────────────
    ttm_fcf = None
    ttm_ocf = None
    ttm_capex = None
    ttm_dividends = None
    ttm_buybacks = None
    ttm_da = None

    if cf_rows:
        cf_df = pd.DataFrame(
            cf_rows,
            columns=["period_end", "period_type", "free_cash_flow", "operating_cf",
                     "capex", "dividends_paid", "stock_repurchases", "depreciation_amortization"],
        )
        ttm_fcf = _compute_ttm(cf_df, "free_cash_flow")
        ttm_ocf = _compute_ttm(cf_df, "operating_cf")
        ttm_capex = _compute_ttm(cf_df, "capex")
        ttm_dividends = _compute_ttm(cf_df, "dividends_paid")
        ttm_buybacks = _compute_ttm(cf_df, "stock_repurchases")
        ttm_da = _compute_ttm(cf_df, "depreciation_amortization")

        prior_ocf = _compute_ttm_prior_year(cf_df, "operating_cf")
        prior_fcf = _compute_ttm_prior_year(cf_df, "free_cash_flow")

        metrics["fcf_margin"] = _safe_divide(ttm_fcf, ttm_revenue)
        metrics["ocf_margin"] = _safe_divide(ttm_ocf, ttm_revenue)
        metrics["capex_to_revenue"] = _safe_divide(ttm_capex, ttm_revenue)
        metrics["ocf_yoy_growth"] = _yoy_growth(ttm_ocf, prior_ocf)
        metrics["fcf_yoy_growth"] = _yoy_growth(ttm_fcf, prior_fcf)
        metrics["ocf_ttm"] = ttm_ocf
        metrics["fcf_ttm"] = ttm_fcf
        metrics["cash_conversion_ratio"] = _safe_divide(ttm_ocf, ttm_net_income)
        metrics["ocf_per_share"] = _safe_divide(ttm_ocf, shares_outstanding)
        metrics["fcf_per_share"] = _safe_divide(ttm_fcf, shares_outstanding)
    else:
        for key in ["fcf_margin", "ocf_margin", "capex_to_revenue", "ocf_yoy_growth",
                    "fcf_yoy_growth", "ocf_ttm", "fcf_ttm", "cash_conversion_ratio",
                    "ocf_per_share", "fcf_per_share"]:
            metrics[key] = None

    # ── EBITDA ─────────────────────────────────────────────────────────────────
    ebitda = None
    if ttm_operating_income is not None and ttm_da is not None:
        ebitda = ttm_operating_income + ttm_da
    elif ttm_operating_income is not None:
        ebitda = ttm_operating_income  # proxy if D&A unavailable

    metrics["ebitda"] = ebitda
    metrics["ebitda_margin"] = _safe_divide(ebitda, ttm_revenue)

    if cf_rows and ttm_da is not None:
        prior_da = _compute_ttm_prior_year(cf_df, "depreciation_amortization")
        prior_op = prior_operating_income if is_rows else None
        prior_ebitda = None
        if prior_op is not None and prior_da is not None:
            prior_ebitda = prior_op + prior_da
        elif prior_op is not None:
            prior_ebitda = prior_op
        metrics["ebitda_yoy_growth"] = _yoy_growth(ebitda, prior_ebitda)
    else:
        metrics["ebitda_yoy_growth"] = None

    # ── Balance sheet metrics ──────────────────────────────────────────────────
    if bs_rows:
        bs_df = pd.DataFrame(
            bs_rows,
            columns=["period_end", "period_type", "current_assets", "current_liabilities",
                     "long_term_debt", "stockholders_equity", "total_assets",
                     "cash", "accounts_payable", "inventory", "accounts_receivable",
                     "goodwill", "intangible_assets", "short_term_debt", "total_liabilities",
                     "ppe_net"],
        )

        latest_bs = bs_df.iloc[0]
        prior_bs = bs_df.iloc[1] if len(bs_df) > 1 else None

        equity = latest_bs["stockholders_equity"]
        total_assets = latest_bs["total_assets"]
        long_term_debt = latest_bs["long_term_debt"]
        current_assets = latest_bs["current_assets"]
        current_liabilities = latest_bs["current_liabilities"]
        cash = latest_bs["cash"]
        accounts_payable = latest_bs["accounts_payable"]
        inventory = latest_bs["inventory"]
        accounts_receivable = latest_bs["accounts_receivable"]
        goodwill = latest_bs["goodwill"]
        intangible_assets = latest_bs["intangible_assets"]
        short_term_debt = latest_bs["short_term_debt"]
        total_liabilities = latest_bs["total_liabilities"]

        ppe_net = latest_bs["ppe_net"]

        prior_total_assets = prior_bs["total_assets"] if prior_bs is not None else None
        prior_accounts_payable = prior_bs["accounts_payable"] if prior_bs is not None else None
        prior_inventory = prior_bs["inventory"] if prior_bs is not None else None
        prior_accounts_receivable = prior_bs["accounts_receivable"] if prior_bs is not None else None
        prior_ppe_net = prior_bs["ppe_net"] if prior_bs is not None else None

        # PPE turnover = revenue / avg(ppe_net)
        avg_ppe = _avg(ppe_net, prior_ppe_net)
        metrics["ppe_turnover"] = _safe_divide(ttm_revenue, avg_ppe)

        # Profitability / returns
        metrics["roe"] = _safe_divide(ttm_net_income, equity)
        metrics["roa"] = _safe_divide(ttm_net_income, total_assets)

        # ROIC = NOPAT / invested_capital
        # NOPAT = operating_income * (1 - eff_tax_rate)
        # eff_tax_rate = income_tax / pretax_income
        eff_tax_rate = _safe_divide(ttm_income_tax, ttm_pretax_income)
        if eff_tax_rate is not None:
            eff_tax_rate = max(0.0, min(eff_tax_rate, 1.0))  # clamp to [0, 1]
        if ttm_operating_income is not None and eff_tax_rate is not None:
            nopat = ttm_operating_income * (1.0 - eff_tax_rate)
        elif ttm_operating_income is not None:
            nopat = ttm_operating_income * 0.79  # assume ~21% tax rate fallback
        else:
            nopat = None

        invested_capital = None
        if equity is not None and long_term_debt is not None and cash is not None:
            invested_capital = float(equity) + float(long_term_debt) - float(cash)
        elif equity is not None and long_term_debt is not None:
            invested_capital = float(equity) + float(long_term_debt)

        metrics["roic"] = _safe_divide(nopat, invested_capital)

        # ROCE = operating_income / capital_employed
        # capital_employed = total_assets - current_liabilities
        capital_employed = None
        if total_assets is not None and current_liabilities is not None:
            capital_employed = float(total_assets) - float(current_liabilities)
        metrics["roce"] = _safe_divide(ttm_operating_income, capital_employed)

        # Liquidity
        metrics["debt_to_equity"] = _safe_divide(long_term_debt, equity)
        metrics["current_ratio"] = _safe_divide(current_assets, current_liabilities)

        quick_assets = None
        if current_assets is not None and inventory is not None:
            quick_assets = float(current_assets) - float(inventory)
        elif current_assets is not None:
            quick_assets = float(current_assets)
        metrics["quick_ratio"] = _safe_divide(quick_assets, current_liabilities)
        metrics["cash_ratio"] = _safe_divide(cash, current_liabilities)

        working_capital = None
        if current_assets is not None and current_liabilities is not None:
            working_capital = float(current_assets) - float(current_liabilities)
        metrics["working_capital"] = working_capital

        net_debt = None
        if long_term_debt is not None and cash is not None:
            st_debt = float(short_term_debt) if short_term_debt is not None else 0.0
            net_debt = float(long_term_debt) + st_debt - float(cash)
        metrics["net_debt"] = net_debt

        # Leverage
        metrics["debt_to_assets"] = _safe_divide(long_term_debt, total_assets)
        denom_cap = None
        if long_term_debt is not None and equity is not None:
            denom_cap = float(long_term_debt) + float(equity)
        metrics["debt_to_capital"] = _safe_divide(long_term_debt, denom_cap)
        metrics["equity_ratio"] = _safe_divide(equity, total_assets)
        metrics["net_debt_to_ebitda"] = _safe_divide(net_debt, ebitda)
        metrics["debt_to_ebitda"] = _safe_divide(long_term_debt, ebitda)
        metrics["interest_coverage"] = _safe_divide(ttm_operating_income, ttm_interest_expense)

        # Efficiency
        avg_assets = _avg(total_assets, prior_total_assets)
        metrics["asset_turnover"] = _safe_divide(ttm_revenue, avg_assets)

        avg_payables = _avg(accounts_payable, prior_accounts_payable)
        payables_turnover = _safe_divide(ttm_cost_of_revenue, avg_payables)
        metrics["payables_turnover"] = payables_turnover
        metrics["dpo"] = _safe_divide(365.0, payables_turnover)

        avg_inventory = _avg(inventory, prior_inventory)
        if avg_inventory is not None:
            inv_turn = _safe_divide(ttm_cost_of_revenue, avg_inventory)
            metrics["inventory_turnover"] = inv_turn
            metrics["dio"] = _safe_divide(365.0, inv_turn)
        else:
            metrics["inventory_turnover"] = None
            metrics["dio"] = None

        avg_receivables = _avg(accounts_receivable, prior_accounts_receivable)
        if avg_receivables is not None:
            rec_turn = _safe_divide(ttm_revenue, avg_receivables)
            metrics["receivables_turnover"] = rec_turn
            metrics["dso"] = _safe_divide(365.0, rec_turn)
        else:
            metrics["receivables_turnover"] = None
            metrics["dso"] = None

        dso = metrics.get("dso")
        dio = metrics.get("dio")
        dpo = metrics.get("dpo")
        if dso is not None and dio is not None and dpo is not None:
            metrics["ccc"] = dso + dio - dpo
        else:
            metrics["ccc"] = None

        # Per-share
        metrics["book_value_per_share"] = _safe_divide(equity, shares_outstanding)
        tangible_equity = None
        if equity is not None:
            g = float(goodwill) if goodwill is not None else 0.0
            ia = float(intangible_assets) if intangible_assets is not None else 0.0
            tangible_equity = float(equity) - g - ia
        metrics["tangible_book_value_per_share"] = _safe_divide(tangible_equity, shares_outstanding)

        # Accrual ratio
        metrics["accrual_ratio"] = _safe_divide(
            (ttm_net_income - ttm_ocf) if ttm_net_income is not None and ttm_ocf is not None else None,
            avg_assets,
        )
    else:
        for key in ["roe", "roa", "roic", "roce", "debt_to_equity", "current_ratio",
                    "quick_ratio", "cash_ratio", "working_capital", "net_debt",
                    "debt_to_assets", "debt_to_capital", "equity_ratio",
                    "net_debt_to_ebitda", "debt_to_ebitda", "interest_coverage",
                    "asset_turnover", "inventory_turnover", "receivables_turnover",
                    "payables_turnover", "dso", "dio", "dpo", "ccc",
                    "book_value_per_share", "tangible_book_value_per_share", "accrual_ratio",
                    "ppe_turnover"]:
            metrics[key] = None

    # ── Shareholder returns ────────────────────────────────────────────────────
    if ttm_dividends is not None and shares_outstanding is not None and latest_price is not None and latest_price > 0:
        div_per_share = abs(ttm_dividends) / shares_outstanding
        metrics["dividend_yield"] = div_per_share / latest_price
    else:
        metrics["dividend_yield"] = None

    metrics["dividend_payout_ratio"] = _safe_divide(
        abs(ttm_dividends) if ttm_dividends is not None else None,
        ttm_net_income,
    )

    if ttm_buybacks is not None and market_cap is not None and market_cap > 0:
        metrics["buyback_yield"] = abs(ttm_buybacks) / market_cap
    else:
        metrics["buyback_yield"] = None

    total_returned = None
    if ttm_dividends is not None and ttm_buybacks is not None:
        total_returned = abs(ttm_dividends) + abs(ttm_buybacks)
    elif ttm_dividends is not None:
        total_returned = abs(ttm_dividends)
    elif ttm_buybacks is not None:
        total_returned = abs(ttm_buybacks)

    if total_returned is not None and market_cap is not None and market_cap > 0:
        metrics["shareholder_yield"] = total_returned / market_cap
    else:
        metrics["shareholder_yield"] = None

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
                    pe_ttm, ev_ebitda,
                    revenue_qoq_growth, operating_income_yoy_growth, ocf_yoy_growth,
                    fcf_yoy_growth, ebitda_yoy_growth, revenue_3yr_cagr, revenue_5yr_cagr,
                    eps_3yr_cagr, eps_5yr_cagr,
                    pretax_margin, ocf_margin, ebitda_margin, capex_to_revenue,
                    roic, roce,
                    ocf_per_share, fcf_per_share, cash_conversion_ratio, accrual_ratio,
                    quick_ratio, cash_ratio, working_capital, net_debt,
                    debt_to_assets, debt_to_capital, equity_ratio,
                    net_debt_to_ebitda, debt_to_ebitda, interest_coverage,
                    asset_turnover, inventory_turnover, receivables_turnover, payables_turnover,
                    dso, dio, dpo, ccc,
                    book_value_per_share, tangible_book_value_per_share,
                    ebitda, ocf_ttm, fcf_ttm,
                    dividend_yield, dividend_payout_ratio, buyback_yield, shareholder_yield,
                    sga_margin, rd_margin, ppe_turnover
                ) VALUES (
                    :ticker, :date,
                    :revenue_yoy_growth, :net_income_yoy_growth, :eps_yoy_growth,
                    :gross_margin, :operating_margin, :net_margin, :fcf_margin,
                    :roe, :roa, :debt_to_equity, :current_ratio,
                    :pe_ttm, :ev_ebitda,
                    :revenue_qoq_growth, :operating_income_yoy_growth, :ocf_yoy_growth,
                    :fcf_yoy_growth, :ebitda_yoy_growth, :revenue_3yr_cagr, :revenue_5yr_cagr,
                    :eps_3yr_cagr, :eps_5yr_cagr,
                    :pretax_margin, :ocf_margin, :ebitda_margin, :capex_to_revenue,
                    :roic, :roce,
                    :ocf_per_share, :fcf_per_share, :cash_conversion_ratio, :accrual_ratio,
                    :quick_ratio, :cash_ratio, :working_capital, :net_debt,
                    :debt_to_assets, :debt_to_capital, :equity_ratio,
                    :net_debt_to_ebitda, :debt_to_ebitda, :interest_coverage,
                    :asset_turnover, :inventory_turnover, :receivables_turnover, :payables_turnover,
                    :dso, :dio, :dpo, :ccc,
                    :book_value_per_share, :tangible_book_value_per_share,
                    :ebitda, :ocf_ttm, :fcf_ttm,
                    :dividend_yield, :dividend_payout_ratio, :buyback_yield, :shareholder_yield,
                    :sga_margin, :rd_margin, :ppe_turnover
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
