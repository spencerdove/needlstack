"""
Field mappings, tolerances, and accounting identity definitions for validation.

Finnhub /stock/financials-reported concepts use the format "us-gaap_TagName".
Each metric carries a priority-ordered list of concepts to try — first match wins.
"""
from typing import Optional

# 25 metrics to validate.
# Each entry: internal_field -> {finnhub_concepts, finnhub_section, finnhub_negate, description}
#
# finnhub_concepts: priority-ordered list of us-gaap concept strings (first match wins).
#                  None means the value is derived (computed from other metrics).
# finnhub_section:  "ic" | "bs" | "cf"
# finnhub_negate:   True → multiply Finnhub value by -1 before comparing.
#                   Finnhub reports capex as a positive outflow (same as our pipeline),
#                   so no negation is needed (unlike FMP which used negative values).
METRIC_MAP: dict[str, dict] = {
    # ── Income Statement ──────────────────────────────────────────────────────
    "revenue": {
        "finnhub_concepts": [
            "us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax",
            "us-gaap_RevenueFromContractWithCustomerIncludingAssessedTax",
            "us-gaap_Revenues",
            "us-gaap_SalesRevenueNet",
            "us-gaap_SalesRevenueGoodsNet",
        ],
        "finnhub_section": "ic",
        "finnhub_negate": False,
        "description": "Total revenue",
    },
    "cost_of_revenue": {
        "finnhub_concepts": [
            "us-gaap_CostOfGoodsAndServicesSold",
            "us-gaap_CostOfRevenue",
            "us-gaap_CostOfGoodsSold",
            "us-gaap_CostOfServices",
        ],
        "finnhub_section": "ic",
        "finnhub_negate": False,
        "description": "Cost of goods/services sold",
    },
    "gross_profit": {
        "finnhub_concepts": [
            "us-gaap_GrossProfit",
        ],
        "finnhub_section": "ic",
        "finnhub_negate": False,
        "description": "Gross profit",
    },
    "operating_expenses": {
        "finnhub_concepts": [
            "us-gaap_OperatingExpenses",
            "us-gaap_CostsAndExpenses",
            "us-gaap_OperatingCostsAndExpenses",
        ],
        "finnhub_section": "ic",
        "finnhub_negate": False,
        "description": "Operating expenses (excl COGS)",
    },
    "operating_income": {
        "finnhub_concepts": [
            "us-gaap_OperatingIncomeLoss",
        ],
        "finnhub_section": "ic",
        "finnhub_negate": False,
        "description": "Operating income/loss",
    },
    "pretax_income": {
        "finnhub_concepts": [
            "us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
            "us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
            "us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesDomestic",
        ],
        "finnhub_section": "ic",
        "finnhub_negate": False,
        "description": "Pre-tax income",
    },
    "income_tax": {
        "finnhub_concepts": [
            "us-gaap_IncomeTaxExpenseBenefit",
            "us-gaap_CurrentIncomeTaxExpenseBenefit",
        ],
        "finnhub_section": "ic",
        "finnhub_negate": False,
        "description": "Income tax expense",
    },
    "net_income": {
        "finnhub_concepts": [
            "us-gaap_NetIncomeLoss",
            "us-gaap_ProfitLoss",
            "us-gaap_NetIncomeLossAvailableToCommonStockholdersBasic",
        ],
        "finnhub_section": "ic",
        "finnhub_negate": False,
        "description": "Net income",
    },
    "eps_basic": {
        "finnhub_concepts": [
            "us-gaap_EarningsPerShareBasic",
        ],
        "finnhub_section": "ic",
        "finnhub_negate": False,
        "description": "EPS basic",
    },
    "eps_diluted": {
        "finnhub_concepts": [
            "us-gaap_EarningsPerShareDiluted",
        ],
        "finnhub_section": "ic",
        "finnhub_negate": False,
        "description": "EPS diluted",
    },
    "shares_diluted": {
        "finnhub_concepts": [
            "us-gaap_WeightedAverageNumberOfDilutedSharesOutstanding",
            "us-gaap_WeightedAverageNumberOfSharesOutstandingDiluted",
        ],
        "finnhub_section": "ic",
        "finnhub_negate": False,
        "description": "Weighted average diluted shares",
    },

    # ── Balance Sheet ─────────────────────────────────────────────────────────
    "cash": {
        "finnhub_concepts": [
            "us-gaap_CashAndCashEquivalentsAtCarryingValue",
            "us-gaap_CashAndCashEquivalents",
            "us-gaap_Cash",
        ],
        "finnhub_section": "bs",
        "finnhub_negate": False,
        "description": "Cash and equivalents",
    },
    "accounts_receivable": {
        "finnhub_concepts": [
            "us-gaap_AccountsReceivableNetCurrent",
            "us-gaap_ReceivablesNetCurrent",
            "us-gaap_TradeAndOtherReceivablesNetCurrent",
        ],
        "finnhub_section": "bs",
        "finnhub_negate": False,
        "description": "Net receivables",
    },
    "inventory": {
        "finnhub_concepts": [
            "us-gaap_InventoryNet",
            "us-gaap_InventoryGross",
            "us-gaap_InventoryFinishedGoods",
        ],
        "finnhub_section": "bs",
        "finnhub_negate": False,
        "description": "Inventory",
    },
    "current_assets": {
        "finnhub_concepts": [
            "us-gaap_AssetsCurrent",
        ],
        "finnhub_section": "bs",
        "finnhub_negate": False,
        "description": "Total current assets",
    },
    "total_assets": {
        "finnhub_concepts": [
            "us-gaap_Assets",
        ],
        "finnhub_section": "bs",
        "finnhub_negate": False,
        "description": "Total assets",
    },
    "current_liabilities": {
        "finnhub_concepts": [
            "us-gaap_LiabilitiesCurrent",
        ],
        "finnhub_section": "bs",
        "finnhub_negate": False,
        "description": "Total current liabilities",
    },
    "total_liabilities": {
        "finnhub_concepts": [
            "us-gaap_Liabilities",
        ],
        "finnhub_section": "bs",
        "finnhub_negate": False,
        "description": "Total liabilities",
    },
    "stockholders_equity": {
        "finnhub_concepts": [
            "us-gaap_StockholdersEquity",
            "us-gaap_StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        ],
        "finnhub_section": "bs",
        "finnhub_negate": False,
        "description": "Total stockholders equity",
    },

    # ── Cash Flow Statement ───────────────────────────────────────────────────
    "operating_cf": {
        "finnhub_concepts": [
            "us-gaap_NetCashProvidedByUsedInOperatingActivities",
        ],
        "finnhub_section": "cf",
        "finnhub_negate": False,
        "description": "Operating cash flow",
    },
    "capex": {
        # Finnhub reports PaymentsToAcquirePropertyPlantAndEquipment as a
        # positive value (cash outflow). Our pipeline also stores capex positive.
        # No negation required (unlike FMP which stored it negative).
        "finnhub_concepts": [
            "us-gaap_PaymentsToAcquirePropertyPlantAndEquipment",
            "us-gaap_PaymentsForCapitalImprovements",
            "us-gaap_PaymentsToAcquireProductiveAssets",
        ],
        "finnhub_section": "cf",
        "finnhub_negate": False,
        "description": "Capital expenditures (pipeline: positive, Finnhub: positive)",
    },
    "investing_cf": {
        "finnhub_concepts": [
            "us-gaap_NetCashProvidedByUsedInInvestingActivities",
        ],
        "finnhub_section": "cf",
        "finnhub_negate": False,
        "description": "Investing cash flow",
    },
    "financing_cf": {
        "finnhub_concepts": [
            "us-gaap_NetCashProvidedByUsedInFinancingActivities",
        ],
        "finnhub_section": "cf",
        "finnhub_negate": False,
        "description": "Financing cash flow",
    },
    "free_cash_flow": {
        # No direct Finnhub concept — derived as operating_cf - capex after normalization.
        "finnhub_concepts": None,
        "finnhub_section": "cf",
        "finnhub_negate": False,
        "description": "Free cash flow (derived: operating_cf - capex)",
    },

    # ── Derived ───────────────────────────────────────────────────────────────
    "gross_margin": {
        # No direct Finnhub concept — derived as gross_profit / revenue.
        "finnhub_concepts": None,
        "finnhub_section": "ic",
        "finnhub_negate": False,
        "description": "Gross margin % (derived: gross_profit / revenue)",
    },
}

# Default tolerances per metric (fraction, e.g. 0.01 = 1%)
VALIDATION_TOLERANCES: dict[str, float] = {
    # 1% for most income statement and balance sheet items
    "revenue": 0.01,
    "cost_of_revenue": 0.01,
    "gross_profit": 0.01,
    "operating_expenses": 0.01,
    "operating_income": 0.01,
    "pretax_income": 0.01,
    "income_tax": 0.01,
    "net_income": 0.01,
    "cash": 0.01,
    "accounts_receivable": 0.01,
    "inventory": 0.01,
    "current_assets": 0.01,
    "total_assets": 0.01,
    "current_liabilities": 0.01,
    "total_liabilities": 0.01,
    "stockholders_equity": 0.01,
    "investing_cf": 0.01,
    "financing_cf": 0.01,
    # 2% for EPS, shares, and cash flow items
    "eps_basic": 0.02,
    "eps_diluted": 0.02,
    "shares_diluted": 0.02,
    "operating_cf": 0.02,
    "capex": 0.02,
    # 3% for derived metrics
    "free_cash_flow": 0.03,
    "gross_margin": 0.03,
}

# 5 accounting identities to check against the pipeline's own DB values.
# These run independently of the external vendor — pure self-consistency checks.
ACCOUNTING_IDENTITIES: list[dict] = [
    {
        "name": "assets_eq_liab_plus_equity",
        "description": "total_assets ≈ total_liabilities + stockholders_equity",
        "lhs": ["total_assets"],
        "rhs": ["total_liabilities", "stockholders_equity"],
        "tolerance": 0.01,
    },
    {
        "name": "gross_profit_eq_rev_minus_cogs",
        "description": "gross_profit ≈ revenue - cost_of_revenue",
        "lhs": ["gross_profit"],
        "rhs_expr": "revenue - cost_of_revenue",
        "tolerance": 0.01,
    },
    {
        "name": "fcf_eq_ocf_minus_capex",
        "description": "free_cash_flow ≈ operating_cf - capex",
        "lhs": ["free_cash_flow"],
        "rhs_expr": "operating_cf - capex",
        "tolerance": 0.02,
    },
    {
        "name": "net_income_eq_pretax_minus_tax",
        "description": "net_income ≈ pretax_income - income_tax",
        "lhs": ["net_income"],
        "rhs_expr": "pretax_income - income_tax",
        "tolerance": 0.03,
    },
    {
        "name": "gross_margin_consistency",
        "description": "gross_margin ≈ gross_profit / revenue",
        "lhs": ["gross_margin"],
        "rhs_expr": "gross_profit / revenue",
        "tolerance": 0.02,
    },
]

VALIDATION_METRICS: list[str] = list(METRIC_MAP.keys())

# Pre-built lookup: finnhub_concept → (internal_metric_name, finnhub_negate)
# Used by normalize_finnhub() for O(1) concept matching.
# Priority is preserved because dicts are insertion-ordered and we only set a
# concept the first time it appears (earlier entries in METRIC_MAP win).
CONCEPT_TO_METRIC: dict[str, tuple] = {}
for _metric, _cfg in METRIC_MAP.items():
    _concepts = _cfg.get("finnhub_concepts") or []
    for _concept in _concepts:
        if _concept not in CONCEPT_TO_METRIC:
            CONCEPT_TO_METRIC[_concept] = (_metric, _cfg["finnhub_negate"])


def get_tolerance(metric_name: str) -> float:
    """Return validation tolerance for a metric (default 0.01)."""
    return VALIDATION_TOLERANCES.get(metric_name, 0.01)
