"""
Comparison engine: pipeline DB values vs Finnhub (primary) and EDGAR (cross-validation).
"""
import logging
from datetime import date
from typing import Optional

import sqlalchemy as sa

from ingestion.validation.metric_map import (
    ACCOUNTING_IDENTITIES,
    CONCEPT_TO_METRIC,
    METRIC_MAP,
    VALIDATION_METRICS,
    get_tolerance,
)
from ingestion.validation.finnhub_client import (
    FinnhubClient,
    match_vendor_period,
    parse_finnhub_date,
)

logger = logging.getLogger(__name__)


def normalize_finnhub(filings: list[dict]) -> dict[tuple, dict]:
    """
    Convert Finnhub financials-reported filing list →
        {(period_end_str, period_type): {internal_metric: value}}.

    Each filing has:
        year, quarter (0=annual), endDate, report: {ic, bs, cf}
    Each ic/bs/cf item:
        concept ("us-gaap_TagName"), value, unit, label

    Derivations computed here:
        free_cash_flow  = operating_cf - capex
        gross_margin    = gross_profit / revenue
    """
    result: dict[tuple, dict] = {}

    for filing in filings:
        end_raw = filing.get("endDate", "")
        period_end = parse_finnhub_date(end_raw)
        if not period_end:
            continue

        quarter = filing.get("quarter", 0)
        period_type = "A" if quarter == 0 else "Q"
        key = (period_end, period_type)

        if key not in result:
            result[key] = {}

        report = filing.get("report", {})
        for section in ("ic", "bs", "cf"):
            for item in report.get(section, []):
                concept = item.get("concept", "")
                raw_val = item.get("value")
                if raw_val is None:
                    continue

                mapping = CONCEPT_TO_METRIC.get(concept)
                if mapping is None:
                    continue

                internal_name, negate = mapping
                # Only populate if not already set (priority: first concept wins)
                if internal_name not in result[key]:
                    val = float(raw_val)
                    if negate:
                        val = -val
                    result[key][internal_name] = val

        # Derive free_cash_flow = operating_cf - capex
        row = result[key]
        if "free_cash_flow" not in row:
            ocf = row.get("operating_cf")
            capex = row.get("capex")
            if ocf is not None and capex is not None:
                row["free_cash_flow"] = ocf - capex

        # Derive gross_margin = gross_profit / revenue
        if "gross_margin" not in row:
            gp = row.get("gross_profit")
            rev = row.get("revenue")
            if gp is not None and rev and rev != 0:
                row["gross_margin"] = gp / rev

    return result


def fetch_pipeline_periods(ticker: str, conn: sa.Connection) -> dict[tuple, dict]:
    """
    Query income_statements + balance_sheets + cash_flows for a ticker.
    Returns {(period_end_str, period_type): {metric: value}}.
    """
    result: dict[tuple, dict] = {}

    # Income statement
    rows = conn.execute(sa.text(
        """SELECT period_end, period_type, revenue, cost_of_revenue, gross_profit,
                  operating_expenses, operating_income, pretax_income, income_tax,
                  net_income, eps_basic, eps_diluted, shares_diluted
           FROM income_statements WHERE ticker = :ticker"""
    ), {"ticker": ticker}).fetchall()
    for row in rows:
        pe = str(row[0])
        pt = row[1]
        key = (pe, pt)
        if key not in result:
            result[key] = {}
        cols = [
            "revenue", "cost_of_revenue", "gross_profit", "operating_expenses",
            "operating_income", "pretax_income", "income_tax", "net_income",
            "eps_basic", "eps_diluted", "shares_diluted",
        ]
        for i, col in enumerate(cols):
            if row[i + 2] is not None:
                result[key][col] = float(row[i + 2])
        # Derived gross_margin
        gp = result[key].get("gross_profit")
        rev = result[key].get("revenue")
        if gp is not None and rev and rev != 0:
            result[key]["gross_margin"] = gp / rev

    # Balance sheet
    rows = conn.execute(sa.text(
        """SELECT period_end, period_type, cash, accounts_receivable, inventory,
                  current_assets, total_assets, current_liabilities,
                  total_liabilities, stockholders_equity
           FROM balance_sheets WHERE ticker = :ticker"""
    ), {"ticker": ticker}).fetchall()
    for row in rows:
        pe = str(row[0])
        pt = row[1]
        key = (pe, pt)
        if key not in result:
            result[key] = {}
        cols = [
            "cash", "accounts_receivable", "inventory", "current_assets",
            "total_assets", "current_liabilities", "total_liabilities", "stockholders_equity",
        ]
        for i, col in enumerate(cols):
            if row[i + 2] is not None:
                result[key][col] = float(row[i + 2])

    # Cash flows
    rows = conn.execute(sa.text(
        """SELECT period_end, period_type, operating_cf, capex, investing_cf,
                  financing_cf, free_cash_flow
           FROM cash_flows WHERE ticker = :ticker"""
    ), {"ticker": ticker}).fetchall()
    for row in rows:
        pe = str(row[0])
        pt = row[1]
        key = (pe, pt)
        if key not in result:
            result[key] = {}
        cols = ["operating_cf", "capex", "investing_cf", "financing_cf", "free_cash_flow"]
        for i, col in enumerate(cols):
            if row[i + 2] is not None:
                result[key][col] = float(row[i + 2])

    return result


def _find_vendor_match(
    pipeline_end: str,
    pipeline_type: str,
    vendor_normalized: dict[tuple, dict],
) -> Optional[dict]:
    """
    Find the best Finnhub period matching a pipeline period (±15 days).
    Returns the metric dict for that period, or None if no match.
    """
    try:
        pip_dt = date.fromisoformat(pipeline_end)
    except (ValueError, TypeError):
        return None

    best_key = None
    best_diff = 999
    for (v_date, v_type) in vendor_normalized:
        if v_type != pipeline_type:
            continue
        try:
            v_dt = date.fromisoformat(v_date)
        except (ValueError, TypeError):
            continue
        diff = abs((v_dt - pip_dt).days)
        if diff <= 15 and diff < best_diff:
            best_diff = diff
            best_key = (v_date, v_type)

    return vendor_normalized.get(best_key) if best_key else None


def compare_metric(
    metric_name: str,
    pipeline_val: Optional[float],
    vendor_val: Optional[float],
    edgar_val: Optional[float],
) -> dict:
    """
    Compare one metric: pipeline vs Finnhub (primary vendor) vs EDGAR (cross-check).

    Result keys mirror the validation_results DB schema:
        pct_diff_fmp    → populated with Finnhub % diff (column name unchanged for schema compat)
        pct_diff_edgar  → EDGAR % diff

    Mismatch classification:
        missing_pipeline  — pipeline has no value
        missing_vendor    — Finnhub has no value (can't validate; counts as pass/skip)
        pipeline_error    — pipeline disagrees with Finnhub AND EDGAR confirms Finnhub
        vendor_disagreement — pipeline disagrees with Finnhub but EDGAR agrees with pipeline
    """
    tolerance = get_tolerance(metric_name)
    result = {
        "metric_name": metric_name,
        "pipeline_value": pipeline_val,
        "fmp_value": vendor_val,       # column reused for Finnhub value
        "edgar_value": edgar_val,
        "pct_diff_fmp": None,          # column reused for Finnhub % diff
        "pct_diff_edgar": None,
        "tolerance": tolerance,
        "passed": 0,
        "mismatch_type": None,
    }

    if pipeline_val is None:
        result["mismatch_type"] = "missing_pipeline"
        result["passed"] = 0
        return result

    if vendor_val is None:
        result["mismatch_type"] = "missing_vendor"
        result["passed"] = 1  # can't validate — skip
        return result

    # Percentage difference vs Finnhub
    if abs(vendor_val) > 0:
        pct_diff = abs(pipeline_val - vendor_val) / abs(vendor_val)
    elif abs(pipeline_val) < 1e-6:
        pct_diff = 0.0
    else:
        pct_diff = 1.0  # vendor=0, pipeline!=0

    result["pct_diff_fmp"] = pct_diff  # stored in fmp column for schema compat

    # Percentage difference vs EDGAR
    if edgar_val is not None and abs(edgar_val) > 0:
        result["pct_diff_edgar"] = abs(pipeline_val - edgar_val) / abs(edgar_val)

    if pct_diff <= tolerance:
        result["passed"] = 1
        return result

    # Cross-API voting: if EDGAR agrees with pipeline but disagrees with Finnhub
    # → likely a Finnhub restatement or normalization difference, not our error
    if edgar_val is not None and abs(edgar_val) > 0:
        edgar_vs_pipeline = abs(pipeline_val - edgar_val) / abs(edgar_val)
        edgar_vs_vendor = abs(edgar_val - vendor_val) / abs(vendor_val) if abs(vendor_val) > 0 else 1.0
        if edgar_vs_pipeline <= tolerance and edgar_vs_vendor > tolerance:
            result["mismatch_type"] = "vendor_disagreement"
            result["passed"] = 1
            return result

    result["mismatch_type"] = "pipeline_error"
    result["passed"] = 0
    return result


def _eval_identity_expr(expr: str, row: dict) -> Optional[float]:
    """Safely evaluate a simple arithmetic identity expression against a row dict."""
    try:
        local_vars = {k: v for k, v in row.items() if v is not None}
        result = eval(expr, {"__builtins__": {}}, local_vars)  # noqa: S307
        return float(result)
    except Exception:
        return None


def check_identity(identity: dict, row: dict) -> dict:
    """Evaluate one accounting identity against a pipeline row dict."""
    name = identity["name"]
    tolerance = identity["tolerance"]

    lhs_fields = identity.get("lhs", [])
    lhs_val = None
    if lhs_fields:
        vals = [row.get(f) for f in lhs_fields]
        if all(v is not None for v in vals):
            lhs_val = sum(float(v) for v in vals)

    rhs_expr = identity.get("rhs_expr")
    rhs_fields = identity.get("rhs", [])
    rhs_val = None
    if rhs_expr:
        rhs_val = _eval_identity_expr(rhs_expr, row)
    elif rhs_fields:
        vals = [row.get(f) for f in rhs_fields]
        if all(v is not None for v in vals):
            rhs_val = sum(float(v) for v in vals)

    passed = 0
    diff_pct = None
    if lhs_val is not None and rhs_val is not None:
        denom = abs(lhs_val) if abs(lhs_val) > 0 else abs(rhs_val)
        if denom > 0:
            diff_pct = abs(lhs_val - rhs_val) / denom
        else:
            diff_pct = 0.0 if abs(lhs_val - rhs_val) < 1e-6 else 1.0
        passed = 1 if diff_pct <= tolerance else 0

    return {
        "identity_name": name,
        "lhs_value": lhs_val,
        "rhs_value": rhs_val,
        "diff_pct": diff_pct,
        "passed": passed,
    }


def run_comparison(
    ticker: str,
    cik: int,
    engine: sa.Engine,
    finnhub_client: FinnhubClient,
) -> dict:
    """
    Full comparison for one ticker.

    1. Fetch Finnhub financials-reported (annual + quarterly) — primary vendor
    2. Fetch EDGAR companyfacts — cross-validation fallback
    3. Query pipeline DB
    4. Compare most recent annual + most recent quarterly period
    5. Run 25 metric comparisons + 5 identity checks per period

    Returns {'metric_results': [...], 'identity_results': [...]}.
    """
    # ── Finnhub (primary vendor) ──────────────────────────────────────────────
    try:
        filings = finnhub_client.fetch_all(ticker)
        vendor_normalized = normalize_finnhub(filings)
        logger.debug("%s: Finnhub returned %d filings, %d periods normalized",
                     ticker, len(filings), len(vendor_normalized))
    except Exception as exc:
        logger.error("Finnhub fetch failed for %s: %s", ticker, exc)
        vendor_normalized = {}

    # ── EDGAR direct (cross-validation) ──────────────────────────────────────
    edgar_periods: dict[tuple, dict] = {}
    try:
        from ingestion.financials import fetch_company_facts, parse_facts
        facts_json = fetch_company_facts(cik)
        inc_rows, bal_rows, cf_rows = parse_facts(facts_json, ticker)
        for row in inc_rows + bal_rows + cf_rows:
            pe = str(row["period_end"])
            pt = row["period_type"]
            key = (pe, pt)
            if key not in edgar_periods:
                edgar_periods[key] = {}
            edgar_periods[key].update(
                {k: v for k, v in row.items() if not k.startswith("_") and v is not None}
            )
    except Exception as exc:
        logger.warning("EDGAR direct fetch failed for %s (CIK %s): %s", ticker, cik, exc)

    # ── Pipeline DB ───────────────────────────────────────────────────────────
    with engine.connect() as conn:
        pipeline_periods = fetch_pipeline_periods(ticker, conn)

    if not pipeline_periods:
        logger.warning("No pipeline data for %s — skipping", ticker)
        return {"metric_results": [], "identity_results": []}

    # Select most recent annual + most recent quarterly
    annual_keys = sorted([k for k in pipeline_periods if k[1] == "A"], reverse=True)
    quarterly_keys = sorted([k for k in pipeline_periods if k[1] == "Q"], reverse=True)
    selected_keys = []
    if annual_keys:
        selected_keys.append(annual_keys[0])
    if quarterly_keys:
        selected_keys.append(quarterly_keys[0])

    metric_results = []
    identity_results = []

    for (period_end, period_type) in selected_keys:
        pipeline_row = pipeline_periods[(period_end, period_type)]
        vendor_row = _find_vendor_match(period_end, period_type, vendor_normalized) or {}
        edgar_row = edgar_periods.get((period_end, period_type), {})

        # 25 metric comparisons
        for metric_name in VALIDATION_METRICS:
            pipeline_val = pipeline_row.get(metric_name)
            vendor_val = vendor_row.get(metric_name)
            edgar_val = edgar_row.get(metric_name)

            cmp = compare_metric(metric_name, pipeline_val, vendor_val, edgar_val)
            cmp["ticker"] = ticker
            cmp["period_end"] = period_end
            cmp["period_type"] = period_type
            metric_results.append(cmp)

        # 5 accounting identity checks
        for identity in ACCOUNTING_IDENTITIES:
            id_result = check_identity(identity, pipeline_row)
            id_result["ticker"] = ticker
            id_result["period_end"] = period_end
            id_result["period_type"] = period_type
            identity_results.append(id_result)

    return {"metric_results": metric_results, "identity_results": identity_results}
