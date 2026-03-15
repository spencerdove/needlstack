"""
Derivation rules for XBRL financial data.

Fills in None fields using arithmetic relationships between other fields.
Applied after direct tag extraction but before quality scoring.
"""
from typing import Optional


def _get(row: dict, key: str) -> Optional[float]:
    val = row.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def apply_income_derivations(row: dict, derived_fields: list) -> None:
    """
    Apply income statement derivations in-place.
    Appends canonical field names to derived_fields when a value is derived.
    """
    # gross_profit = revenue - cost_of_revenue
    if row.get("gross_profit") is None:
        rev = _get(row, "revenue")
        cor = _get(row, "cost_of_revenue")
        if rev is not None and cor is not None:
            row["gross_profit"] = rev - cor
            derived_fields.append("gross_profit")

    # cost_of_revenue = revenue - gross_profit (reverse derivation)
    if row.get("cost_of_revenue") is None:
        rev = _get(row, "revenue")
        gp  = _get(row, "gross_profit")
        if rev is not None and gp is not None:
            row["cost_of_revenue"] = rev - gp
            derived_fields.append("cost_of_revenue")

    # operating_income from gross_profit minus opex components
    if row.get("operating_income") is None:
        gp = _get(row, "gross_profit")
        if gp is not None:
            sga = _get(row, "sga") or 0.0
            rd = _get(row, "rd_expense") or 0.0
            opex = _get(row, "operating_expenses") or 0.0
            deductions = sga + rd + opex
            if deductions > 0:
                row["operating_income"] = gp - deductions
                derived_fields.append("operating_income")

    # operating_income alternate path: revenue - cost_of_revenue - operating_expenses
    if row.get("operating_income") is None:
        rev  = _get(row, "revenue")
        cor  = _get(row, "cost_of_revenue")
        opex = _get(row, "operating_expenses")
        if rev is not None and cor is not None and opex is not None:
            row["operating_income"] = rev - cor - opex
            derived_fields.append("operating_income")

    # ebit = operating_income (alias)
    if row.get("ebit") is None and row.get("operating_income") is not None:
        row["ebit"] = _get(row, "operating_income")
        derived_fields.append("ebit")

    # net_income = pretax_income - income_tax
    if row.get("net_income") is None:
        pretax = _get(row, "pretax_income")
        if pretax is not None:
            tax = _get(row, "income_tax") or 0.0
            row["net_income"] = pretax - tax
            derived_fields.append("net_income")

    # net_income_attributable falls back to net_income
    if row.get("net_income_attributable") is None and row.get("net_income") is not None:
        row["net_income_attributable"] = _get(row, "net_income")
        derived_fields.append("net_income_attributable")


def apply_cashflow_derivations(row: dict, derived_fields: list) -> None:
    """Apply cash flow derivations in-place."""
    # free_cash_flow = operating_cf - abs(capex)
    if row.get("free_cash_flow") is None:
        op_cf = _get(row, "operating_cf")
        capex = _get(row, "capex")
        if op_cf is not None and capex is not None:
            row["free_cash_flow"] = op_cf - abs(capex)
            derived_fields.append("free_cash_flow")


def apply_balance_derivations(row: dict, derived_fields: list) -> None:
    """Apply balance sheet derivations in-place."""
    # stockholders_equity = total_assets - total_liabilities (last resort)
    if row.get("stockholders_equity") is None:
        assets = _get(row, "total_assets")
        liab = _get(row, "total_liabilities")
        if assets is not None and liab is not None:
            row["stockholders_equity"] = assets - liab
            derived_fields.append("stockholders_equity")


def apply_derivations(row: dict, col_type: str) -> list:
    """
    Apply all derivations for the given statement type.

    col_type: 'income', 'balance', or 'cashflow'
    Returns list of field names that were derived (not directly tagged).
    """
    derived: list = []
    if col_type == "income":
        apply_income_derivations(row, derived)
    elif col_type == "balance":
        apply_balance_derivations(row, derived)
    elif col_type == "cashflow":
        apply_cashflow_derivations(row, derived)
    return derived
