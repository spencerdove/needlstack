"""
Data quality scoring for XBRL-extracted financial rows.

Produces a score dict per row that gets stored in financial_quality_scores.
"""
from typing import Optional

CANONICAL_WEIGHTS = {
    "income": {
        "revenue": 0.20,
        "gross_profit": 0.10,
        "operating_income": 0.15,
        "net_income": 0.15,
        "eps_diluted": 0.10,
        "interest_expense": 0.05,
        "sga": 0.08,
        "rd_expense": 0.07,
        "pretax_income": 0.05,
        "income_tax": 0.05,
    },
    "balance": {
        "total_assets": 0.15,
        "total_liabilities": 0.10,
        "stockholders_equity": 0.10,
        "cash": 0.10,
        "current_assets": 0.08,
        "current_liabilities": 0.08,
        "long_term_debt": 0.08,
        "accounts_receivable": 0.06,
        "goodwill": 0.05,
        "inventory": 0.05,
        "ppe_net": 0.05,
        "short_term_debt": 0.05,
        "accounts_payable": 0.05,
    },
    "cashflow": {
        "operating_cf": 0.30,
        "capex": 0.20,
        "free_cash_flow": 0.20,
        "investing_cf": 0.10,
        "financing_cf": 0.10,
        "depreciation_amortization": 0.10,
    },
}


def score_row(
    row: dict,
    col_type: str,
    derived_fields: Optional[list] = None,
) -> dict:
    """
    Score a single financial row.

    Args:
        row: dict of field → value
        col_type: 'income', 'balance', or 'cashflow'
        derived_fields: list of field names that were derived (not directly tagged)

    Returns:
        dict with tag_coverage_score, derivation_score, calc_consistency, overall_score
    """
    if derived_fields is None:
        derived_fields = []

    derived_set = set(derived_fields)
    weights = CANONICAL_WEIGHTS.get(col_type, {})

    tag_coverage = 0.0
    derivation_coverage = 0.0
    for field, weight in weights.items():
        if row.get(field) is not None:
            if field in derived_set:
                derivation_coverage += weight
            else:
                tag_coverage += weight

    # Balance sheet integrity: assets ≈ liabilities + equity (within 1%)
    calc_consistency = 1.0
    if col_type == "balance":
        assets = row.get("total_assets")
        liab = row.get("total_liabilities")
        equity = row.get("stockholders_equity")
        if all(v is not None for v in [assets, liab, equity]):
            try:
                diff = abs(float(assets) - float(liab) - float(equity)) / (abs(float(assets)) + 1)
                calc_consistency = max(0.0, 1.0 - diff * 100)
            except (TypeError, ValueError, ZeroDivisionError):
                calc_consistency = 0.0

    overall = tag_coverage * 0.6 + derivation_coverage * 0.2 + calc_consistency * 0.2

    return {
        "tag_coverage_score": round(tag_coverage, 3),
        "derivation_score": round(derivation_coverage, 3),
        "calc_consistency": round(calc_consistency, 3),
        "overall_score": round(overall, 3),
    }
