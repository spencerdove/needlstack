"""
Download quarterly/annual financial statements from SEC EDGAR XBRL API
and upsert into income_statements, balance_sheets, and cash_flows tables.
"""
import logging
import time
from typing import Optional

import requests
import sqlalchemy as sa

from db.schema import get_engine
from ingestion.xbrl_context import ContextSelector
from ingestion.xbrl_derivations import apply_derivations
from ingestion.xbrl_presentation import PresentationLinkbase
from ingestion.xbrl_quality import score_row

logger = logging.getLogger(__name__)

SEC_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
USER_AGENT = "needlstack/1.0 (financial data pipeline; contact@example.com)"

# Maps our column names to ordered lists of XBRL US-GAAP tag aliases to try.
# Priority-ordered within each list — first tag that exists in the filing wins.
TAG_MAP: dict[str, list[str]] = {
    # ── INCOME STATEMENT ──────────────────────────────────────────
    "revenue": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
        "SalesRevenueServicesNet",
        "RevenueNotFromContractWithCustomer",
        "OtherSalesRevenueNet",
        "RevenueFromContractWithCustomer",
        "InterestAndDividendIncomeOperating",  # for banks
    ],
    "cost_of_revenue": [
        "CostOfRevenue",
        "CostOfGoodsAndServicesSold",
        "CostOfGoodsSold",
        "CostOfServices",
        "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization",
        "DirectCostsAndExpenses",
    ],
    "gross_profit": [
        "GrossProfit",
    ],
    "sga": [
        "SellingGeneralAndAdministrativeExpense",
        "GeneralAndAdministrativeExpense",
        "SellingAndMarketingExpense",
        "SellingExpense",
        "MarketingAndAdvertisingExpense",
    ],
    "rd_expense": [
        "ResearchAndDevelopmentExpense",
        "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost",
        "ResearchAndDevelopmentInProcess",
    ],
    "operating_expenses": [
        "OperatingExpenses",
        "CostsAndExpenses",
        "OperatingCostsAndExpenses",
        "NoninterestExpense",  # banks
    ],
    "operating_income": [
        "OperatingIncomeLoss",
        "IncomeLossFromContinuingOperationsBeforeInterestExpenseInterestIncomeIncomeTaxesExtraordinaryItemsNoncontrollingInterestsNet",
    ],
    "interest_income": [
        "InvestmentIncomeInterest",
        "InterestAndDividendIncomeOperating",
        "InterestIncomeOperating",
        "InterestAndInvestmentIncome",
    ],
    "interest_expense": [
        "InterestExpense",
        "InterestAndDebtExpense",
        "InterestExpenseDebt",
        "InterestExpenseLongTermDebt",
    ],
    "other_income_expense": [
        "OtherNonoperatingIncomeExpense",
        "OtherIncome",
        "NonoperatingIncomeExpense",
        "OtherNonoperatingIncome",
    ],
    "pretax_income": [
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
    ],
    "income_tax": [
        "IncomeTaxExpenseBenefit",
        "CurrentIncomeTaxExpenseBenefit",
    ],
    "net_income": [
        "NetIncomeLoss",
        "ProfitLoss",
        "NetIncomeLossAvailableToCommonStockholdersBasic",
        "IncomeLossFromContinuingOperations",
    ],
    "net_income_attributable": [
        "NetIncomeLossAttributableToParent",
        "IncomeLossAttributableToParent",
        "NetIncomeLoss",  # fallback
    ],
    "ebit": [
        "OperatingIncomeLoss",  # alias — will derive if not found
    ],
    "eps_basic": ["EarningsPerShareBasic"],
    "eps_diluted": ["EarningsPerShareDiluted"],
    "shares_basic": ["WeightedAverageNumberOfSharesOutstandingBasic"],
    "shares_diluted": [
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingDiluted",
    ],

    # ── BALANCE SHEET — ASSETS ────────────────────────────────────
    "cash": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashAndCashEquivalents",
        "Cash",
    ],
    "short_term_investments": [
        "MarketableSecuritiesCurrent",
        "ShortTermInvestments",
        "AvailableForSaleSecuritiesDebtSecuritiesCurrent",
        "HeldToMaturitySecuritiesCurrent",
        "TradingSecuritiesCurrent",
    ],
    "long_term_investments": [
        "MarketableSecuritiesNoncurrent",
        "LongTermInvestments",
        "AvailableForSaleSecuritiesDebtSecuritiesNoncurrent",
    ],
    "accounts_receivable": [
        "AccountsReceivableNetCurrent",
        "ReceivablesNetCurrent",
        "TradeAndOtherReceivablesNetCurrent",
        "BilledContractReceivables",
    ],
    "inventory": [
        "InventoryNet",
        "InventoryFinishedGoods",
        "InventoryFinishedGoodsNetOfReserves",
        "InventoryGross",
    ],
    "other_current_assets": [
        "OtherAssetsCurrent",
        "PrepaidExpenseAndOtherAssetsCurrent",
        "PrepaidExpenseCurrent",
    ],
    "current_assets": ["AssetsCurrent"],
    "ppe_net": [
        "PropertyPlantAndEquipmentNet",
        "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization",
    ],
    "operating_lease_rou": ["OperatingLeaseRightOfUseAsset"],
    "finance_lease_rou": [
        "FinanceLeaseRightOfUseAsset",
        "CapitalLeaseObligationsAsset",
    ],
    "goodwill": ["Goodwill"],
    "intangible_assets": [
        "FiniteLivedIntangibleAssetsNet",
        "IntangibleAssetsNetExcludingGoodwill",
        "IntangibleAssetsNet",
        "IndefiniteLivedIntangibleAssetsExcludingGoodwill",
    ],
    "deferred_tax_assets": [
        "DeferredIncomeTaxAssetsNet",
        "DeferredTaxAssetsNet",
    ],
    "other_noncurrent_assets": [
        "OtherAssetsNoncurrent",
        "OtherAssets",
    ],
    "total_assets": ["Assets"],

    # ── BALANCE SHEET — LIABILITIES ───────────────────────────────
    "accounts_payable": [
        "AccountsPayableCurrent",
        "AccountsPayableAndAccruedLiabilitiesCurrent",
    ],
    "accrued_liabilities": [
        "AccruedLiabilitiesCurrent",
        "OtherAccruedLiabilitiesCurrent",
        "AccruedAndOtherCurrentLiabilities",
        "EmployeeRelatedLiabilitiesCurrent",
    ],
    "deferred_revenue": [
        "DeferredRevenueCurrent",
        "ContractWithCustomerLiabilityCurrent",
        "DeferredRevenueAndCreditsCurrent",
        "ContractWithCustomerLiability",
    ],
    "short_term_debt": [
        "ShortTermBorrowings",
        "LongTermDebtCurrent",
        "DebtCurrent",
        "CommercialPaper",
    ],
    "operating_lease_liability": [
        "OperatingLeaseLiability",
        # Sum of current + noncurrent handled separately below
    ],
    "finance_lease_liability": [
        "FinanceLeaseLiability",
        "CapitalLeaseObligations",
    ],
    "current_liabilities": ["LiabilitiesCurrent"],
    "long_term_debt": [
        "LongTermDebt",
        "LongTermDebtNoncurrent",
        "LongTermNotesPayable",
        "SeniorNotes",
        "UnsecuredDebt",
    ],
    "deferred_tax_liabilities": [
        "DeferredIncomeTaxLiabilitiesNet",
        "DeferredTaxLiabilitiesNet",
    ],
    "total_liabilities": ["Liabilities"],

    # ── BALANCE SHEET — EQUITY ────────────────────────────────────
    "additional_paid_in_capital": [
        "AdditionalPaidInCapital",
        "AdditionalPaidInCapitalCommonStock",
    ],
    "retained_earnings": ["RetainedEarningsAccumulatedDeficit"],
    "treasury_stock": [
        "TreasuryStockValue",
        "TreasuryStockCommonValue",
    ],
    "noncontrolling_interest": [
        "MinorityInterest",
        "MinorityInterestInSubsidiaries",
    ],
    "stockholders_equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],

    # ── CASH FLOW STATEMENT ───────────────────────────────────────
    "operating_cf": ["NetCashProvidedByUsedInOperatingActivities"],
    "depreciation_amortization": [
        "DepreciationDepletionAndAmortization",
        "DepreciationAndAmortization",
        "Depreciation",
        "AmortizationOfIntangibleAssets",
    ],
    "capex": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsForCapitalImprovements",
        "PaymentsToAcquireProductiveAssets",
    ],
    "acquisitions": [
        "PaymentsToAcquireBusinessesNetOfCashAcquired",
        "PaymentsToAcquireBusinessesGross",
        "PaymentsToAcquireBusinessAndIntangibleAssets",
    ],
    "asset_sale_proceeds": [
        "ProceedsFromSaleOfPropertyPlantAndEquipment",
        "ProceedsFromDivestitureOfBusinesses",
        "ProceedsFromSalesOfBusinessAffiliateAndProductiveAssets",
    ],
    "investing_cf": ["NetCashProvidedByUsedInInvestingActivities"],
    "debt_repayment": [
        "RepaymentsOfLongTermDebt",
        "RepaymentsOfDebt",
        "RepaymentsOfLinesOfCredit",
        "RepaymentsOfNotesPayable",
        "RepaymentsOfLongTermDebtAndCapitalSecurities",
    ],
    "debt_issuance": [
        "ProceedsFromIssuanceOfLongTermDebt",
        "ProceedsFromDebtNetOfIssuanceCosts",
        "ProceedsFromIssuanceOfSeniorLongTermDebt",
        "ProceedsFromLinesOfCredit",
    ],
    "stock_issuance": [
        "ProceedsFromIssuanceOfCommonStock",
        "ProceedsFromIssuanceOfSharesUnderIncentiveAndShareBasedCompensationPlansIncludingStockOptions",
        "ProceedsFromStockOptionsExercised",
    ],
    "dividends_paid": [
        "PaymentsOfDividends",
        "PaymentsOfDividendsCommonStock",
        "PaymentsOfDividendsMinorityInterest",
    ],
    "stock_repurchases": [
        "PaymentsForRepurchaseOfCommonStock",
    ],
    "financing_cf": ["NetCashProvidedByUsedInFinancingActivities"],
    "interest_paid": ["InterestPaid", "InterestPaidNet"],
    "taxes_paid": ["IncomeTaxesPaid", "IncomeTaxesPaidNet"],
}

# Fields reported as positive outflows by EDGAR — stored as-reported.
# Derivation layer uses abs() when computing net values.
OUTFLOW_FIELDS = {
    "capex", "acquisitions", "debt_repayment", "dividends_paid",
    "stock_repurchases", "cost_of_revenue", "sga", "rd_expense",
    "operating_expenses", "interest_expense", "income_tax",
}

# Which columns belong to which table
INCOME_COLS = {
    "revenue", "cost_of_revenue", "gross_profit", "sga", "rd_expense",
    "operating_expenses", "operating_income", "interest_income", "interest_expense",
    "other_income_expense", "pretax_income", "income_tax", "net_income",
    "net_income_attributable", "ebit", "eps_basic", "eps_diluted",
    "shares_basic", "shares_diluted",
}
BALANCE_COLS = {
    "cash", "short_term_investments", "long_term_investments", "accounts_receivable",
    "inventory", "other_current_assets", "current_assets", "ppe_net",
    "operating_lease_rou", "finance_lease_rou", "goodwill", "intangible_assets",
    "deferred_tax_assets", "other_noncurrent_assets", "total_assets",
    "accounts_payable", "accrued_liabilities", "deferred_revenue", "short_term_debt",
    "operating_lease_liability", "finance_lease_liability", "current_liabilities",
    "long_term_debt", "deferred_tax_liabilities", "total_liabilities",
    "additional_paid_in_capital", "retained_earnings", "treasury_stock",
    "noncontrolling_interest", "stockholders_equity",
}
CASHFLOW_COLS = {
    "operating_cf", "depreciation_amortization", "capex", "acquisitions",
    "asset_sale_proceeds", "investing_cf", "debt_repayment", "debt_issuance",
    "stock_issuance", "dividends_paid", "stock_repurchases", "financing_cf",
    "interest_paid", "taxes_paid",
}

# EPS and share counts use non-USD units
SHARES_UNIT_COLS = {"eps_basic", "eps_diluted", "shares_basic", "shares_diluted"}

# Additional tags for lease liability splitting (current + noncurrent → total)
_LEASE_CURRENT_TAG = "OperatingLeaseLiabilityCurrent"
_LEASE_NONCURRENT_TAG = "OperatingLeaseLiabilityNoncurrent"

_context_selector = ContextSelector()
_presentation = PresentationLinkbase()


def fetch_company_facts(cik: int) -> dict:
    """Fetch raw XBRL companyfacts JSON from SEC EDGAR for a given CIK."""
    url = SEC_FACTS_URL.format(cik=cik)
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _accepted_units(col_name: str) -> set:
    if col_name in ("eps_basic", "eps_diluted"):
        return {"USD/shares"}
    if col_name in ("shares_basic", "shares_diluted"):
        return {"shares"}
    return {"USD"}


def _extract_tag_facts_with_context(
    us_gaap: dict,
    col_name: str,
    tag_aliases: list,
    statement_concepts: Optional[dict] = None,
) -> dict:
    """
    Collect all facts for this column across all tag aliases, then use
    ContextSelector to pick the best fact for each (end_date, form_type) key.

    Returns dict keyed by (end_date, form_type) → {value, filed, fp, fy, start, tag}.
    """
    accepted = _accepted_units(col_name)
    # Collect all candidates per (end, form) key
    candidates: dict = {}

    for tag in tag_aliases:
        tag_data = us_gaap.get(tag)
        if tag_data is None:
            continue
        units_data = tag_data.get("units", {})
        for unit_label, facts in units_data.items():
            if unit_label not in accepted:
                continue
            for fact in facts:
                form = fact.get("form", "")
                if form not in ("10-Q", "10-K"):
                    continue
                end = fact.get("end")
                if not end:
                    continue
                key = (end, form)
                if key not in candidates:
                    candidates[key] = []
                candidates[key].append({
                    "value": fact.get("val"),
                    "filed": fact.get("filed", ""),
                    "fp": fact.get("fp", ""),
                    "fy": fact.get("fy"),
                    "start": fact.get("start"),
                    "end": end,
                    "accn": fact.get("accn", ""),
                    "tag": tag,
                })

    # Select best candidate per key
    result: dict = {}
    for key, fact_list in candidates.items():
        _, form_type = key
        best = _context_selector.select_best(
            fact_list, col_name, form_type, statement_concepts
        )
        if best is not None:
            result[key] = best

    return result


def _extract_lease_liability_sum(
    us_gaap: dict, form_type: str
) -> dict[tuple, float]:
    """
    Sum OperatingLeaseLiabilityCurrent + OperatingLeaseLiabilityNoncurrent
    for each (end_date, form_type) key where both exist.
    """
    result: dict[tuple, float] = {}
    accepted = {"USD"}

    def _collect(tag: str) -> dict[tuple, float]:
        out = {}
        tag_data = us_gaap.get(tag)
        if tag_data is None:
            return out
        for unit_label, facts in tag_data.get("units", {}).items():
            if unit_label not in accepted:
                continue
            for fact in facts:
                form = fact.get("form", "")
                if form not in ("10-Q", "10-K"):
                    continue
                end = fact.get("end")
                val = fact.get("val")
                if end and val is not None:
                    key = (end, form)
                    # keep latest filed
                    if key not in out or fact.get("filed", "") > out.get(f"{key}_filed", ""):
                        out[key] = float(val)
                        out[f"{key}_filed"] = fact.get("filed", "")
        # remove _filed helper keys
        return {k: v for k, v in out.items() if not isinstance(k, str)}

    current = _collect(_LEASE_CURRENT_TAG)
    noncurrent = _collect(_LEASE_NONCURRENT_TAG)
    for key in set(current) & set(noncurrent):
        result[key] = current[key] + noncurrent[key]
    return result


def parse_facts(
    facts_json: dict,
    ticker: str,
    statement_concepts: Optional[dict] = None,
) -> tuple:
    """
    Parse SEC companyfacts JSON into three lists of row dicts:
    (income_rows, balance_rows, cashflow_rows).

    Quality scores are computed but returned in-band as a '_quality' key
    for the upsert functions to handle separately.
    """
    us_gaap = facts_json.get("facts", {}).get("us-gaap", {})

    # Collect per-column data keyed by (end_date, form_type)
    col_data: dict = {}
    for col_name, aliases in TAG_MAP.items():
        col_data[col_name] = _extract_tag_facts_with_context(
            us_gaap, col_name, aliases, statement_concepts
        )

    # Handle operating_lease_liability: prefer direct tag, fall back to sum
    lease_sum = _extract_lease_liability_sum(us_gaap, "any")
    for key, total in lease_sum.items():
        if key not in col_data.get("operating_lease_liability", {}):
            if "operating_lease_liability" not in col_data:
                col_data["operating_lease_liability"] = {}
            # Build a synthetic fact dict
            col_data["operating_lease_liability"][key] = {
                "value": total,
                "filed": "",
                "fp": key[1],  # form_type as proxy
                "fy": None,
                "start": None,
                "end": key[0],
                "accn": "",
            }

    # Gather all unique (end_date, form_type) keys
    all_keys: set[tuple] = set()
    for col_facts in col_data.values():
        all_keys.update(col_facts.keys())

    def _make_base(end_date: str, form_type: str, meta: dict) -> dict:
        fp = meta.get("fp", "")
        period_type = "Q" if fp in ("Q1", "Q2", "Q3", "Q4") else "A"
        fiscal_quarter = int(fp[1]) if period_type == "Q" else None
        return {
            "ticker": ticker,
            "period_end": end_date,
            "period_type": period_type,
            "fiscal_year": meta.get("fy"),
            "fiscal_quarter": fiscal_quarter,
            "form_type": form_type,
            "filed_date": meta.get("filed"),
        }

    income_rows: list[dict] = []
    balance_rows: list[dict] = []
    cashflow_rows: list[dict] = []

    for (end_date, form_type) in all_keys:
        # Find any metadata from whichever col has data for this key
        meta: dict = {}
        for col_facts in col_data.values():
            if (end_date, form_type) in col_facts:
                meta = col_facts[(end_date, form_type)]
                break

        base = _make_base(end_date, form_type, meta)

        # Income statement row
        if any((end_date, form_type) in col_data.get(c, {}) for c in INCOME_COLS):
            row = dict(base)
            for col in INCOME_COLS:
                fact = col_data.get(col, {}).get((end_date, form_type))
                row[col] = fact["value"] if fact else None
            derived = apply_derivations(row, "income")
            quality = score_row(row, "income", derived)
            row["_quality"] = quality
            row["_derived_fields"] = derived
            income_rows.append(row)

        # Balance sheet row
        if any((end_date, form_type) in col_data.get(c, {}) for c in BALANCE_COLS):
            row = {
                "ticker": ticker,
                "period_end": end_date,
                "period_type": base["period_type"],
                "filed_date": base["filed_date"],
            }
            for col in BALANCE_COLS:
                fact = col_data.get(col, {}).get((end_date, form_type))
                row[col] = fact["value"] if fact else None
            derived = apply_derivations(row, "balance")
            quality = score_row(row, "balance", derived)
            row["_quality"] = quality
            row["_derived_fields"] = derived
            balance_rows.append(row)

        # Cash flow row
        if any((end_date, form_type) in col_data.get(c, {}) for c in CASHFLOW_COLS):
            row = {
                "ticker": ticker,
                "period_end": end_date,
                "period_type": base["period_type"],
                "filed_date": base["filed_date"],
                "free_cash_flow": None,  # set by derivation if computable
            }
            for col in CASHFLOW_COLS:
                fact = col_data.get(col, {}).get((end_date, form_type))
                row[col] = fact["value"] if fact else None
            derived = apply_derivations(row, "cashflow")
            quality = score_row(row, "cashflow", derived)
            row["_quality"] = quality
            row["_derived_fields"] = derived
            cashflow_rows.append(row)

    return income_rows, balance_rows, cashflow_rows


def _strip_internal(rows: list[dict]) -> list[dict]:
    """Remove internal _quality/_derived_fields keys from rows before upsert."""
    out = []
    for row in rows:
        r = {k: v for k, v in row.items() if not k.startswith("_")}
        out.append(r)
    return out


def _build_quality_rows(
    ticker: str,
    rows: list[dict],
    statement_type: str,
) -> list[dict]:
    """Build financial_quality_scores rows from parsed rows."""
    quality_rows = []
    for row in rows:
        q = row.get("_quality")
        if q is None:
            continue
        quality_rows.append({
            "ticker": ticker,
            "period_end": row["period_end"],
            "period_type": row["period_type"],
            "statement_type": statement_type,
            "tag_coverage_score": q.get("tag_coverage_score"),
            "derivation_score": q.get("derivation_score"),
            "context_confidence": None,  # reserved for future use
            "calc_consistency": q.get("calc_consistency"),
            "overall_score": q.get("overall_score"),
        })
    return quality_rows


def _upsert_income(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    clean = _strip_internal(rows)
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO income_statements
                    (ticker, period_end, period_type, fiscal_year, fiscal_quarter,
                     form_type, filed_date, revenue, cost_of_revenue, gross_profit,
                     sga, rd_expense, operating_expenses, operating_income,
                     interest_income, interest_expense, other_income_expense,
                     pretax_income, income_tax, net_income, net_income_attributable,
                     ebit, eps_basic, eps_diluted, shares_basic, shares_diluted)
                VALUES
                    (:ticker, :period_end, :period_type, :fiscal_year, :fiscal_quarter,
                     :form_type, :filed_date, :revenue, :cost_of_revenue, :gross_profit,
                     :sga, :rd_expense, :operating_expenses, :operating_income,
                     :interest_income, :interest_expense, :other_income_expense,
                     :pretax_income, :income_tax, :net_income, :net_income_attributable,
                     :ebit, :eps_basic, :eps_diluted, :shares_basic, :shares_diluted)
                """
            ),
            clean,
        )
    return len(clean)


def _upsert_balance(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    clean = _strip_internal(rows)
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO balance_sheets
                    (ticker, period_end, period_type, filed_date,
                     cash, short_term_investments, long_term_investments,
                     accounts_receivable, inventory, other_current_assets, current_assets,
                     ppe_net, operating_lease_rou, finance_lease_rou,
                     goodwill, intangible_assets, deferred_tax_assets,
                     other_noncurrent_assets, total_assets,
                     accounts_payable, accrued_liabilities, deferred_revenue,
                     short_term_debt, operating_lease_liability, finance_lease_liability,
                     current_liabilities, long_term_debt, deferred_tax_liabilities,
                     total_liabilities, additional_paid_in_capital, retained_earnings,
                     treasury_stock, noncontrolling_interest, stockholders_equity)
                VALUES
                    (:ticker, :period_end, :period_type, :filed_date,
                     :cash, :short_term_investments, :long_term_investments,
                     :accounts_receivable, :inventory, :other_current_assets, :current_assets,
                     :ppe_net, :operating_lease_rou, :finance_lease_rou,
                     :goodwill, :intangible_assets, :deferred_tax_assets,
                     :other_noncurrent_assets, :total_assets,
                     :accounts_payable, :accrued_liabilities, :deferred_revenue,
                     :short_term_debt, :operating_lease_liability, :finance_lease_liability,
                     :current_liabilities, :long_term_debt, :deferred_tax_liabilities,
                     :total_liabilities, :additional_paid_in_capital, :retained_earnings,
                     :treasury_stock, :noncontrolling_interest, :stockholders_equity)
                """
            ),
            clean,
        )
    return len(clean)


def _upsert_cashflow(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    clean = _strip_internal(rows)
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO cash_flows
                    (ticker, period_end, period_type, filed_date,
                     operating_cf, depreciation_amortization, capex, acquisitions,
                     asset_sale_proceeds, investing_cf, debt_repayment, debt_issuance,
                     stock_issuance, dividends_paid, stock_repurchases, financing_cf,
                     free_cash_flow, interest_paid, taxes_paid)
                VALUES
                    (:ticker, :period_end, :period_type, :filed_date,
                     :operating_cf, :depreciation_amortization, :capex, :acquisitions,
                     :asset_sale_proceeds, :investing_cf, :debt_repayment, :debt_issuance,
                     :stock_issuance, :dividends_paid, :stock_repurchases, :financing_cf,
                     :free_cash_flow, :interest_paid, :taxes_paid)
                """
            ),
            clean,
        )
    return len(clean)


def _upsert_quality(engine: sa.Engine, quality_rows: list[dict]) -> None:
    if not quality_rows:
        return
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO financial_quality_scores
                    (ticker, period_end, period_type, statement_type,
                     tag_coverage_score, derivation_score, context_confidence,
                     calc_consistency, overall_score)
                VALUES
                    (:ticker, :period_end, :period_type, :statement_type,
                     :tag_coverage_score, :derivation_score, :context_confidence,
                     :calc_consistency, :overall_score)
                """
            ),
            quality_rows,
        )


def download_financials(
    tickers_with_cik: list[tuple[str, int]],
    engine: Optional[sa.Engine] = None,
    rate_limit: int = 9,
) -> tuple[int, list[str]]:
    """
    Fetch SEC EDGAR companyfacts for each (ticker, cik) pair and upsert into
    income_statements, balance_sheets, and cash_flows tables.

    Returns (total_rows_inserted, failed_tickers).
    """
    if engine is None:
        engine = get_engine()

    total_rows = 0
    failed: list[str] = []
    sleep_interval = 1.0 / rate_limit

    for ticker, cik in tickers_with_cik:
        try:
            facts_json = fetch_company_facts(cik)
            stmt_concepts = _presentation.get_statement_concepts(cik)
            income_rows, balance_rows, cashflow_rows = parse_facts(
                facts_json, ticker, stmt_concepts
            )

            # Collect quality rows before stripping internal keys
            quality_rows = (
                _build_quality_rows(ticker, income_rows, "income")
                + _build_quality_rows(ticker, balance_rows, "balance")
                + _build_quality_rows(ticker, cashflow_rows, "cashflow")
            )

            n = (
                _upsert_income(engine, income_rows)
                + _upsert_balance(engine, balance_rows)
                + _upsert_cashflow(engine, cashflow_rows)
            )
            _upsert_quality(engine, quality_rows)
            total_rows += n
            logger.debug(
                f"{ticker}: {n} rows inserted "
                f"({len(income_rows)}I/{len(balance_rows)}B/{len(cashflow_rows)}CF)"
            )
        except Exception as exc:
            logger.error(f"Failed to process {ticker} (CIK {cik}): {exc}")
            failed.append(ticker)
        finally:
            time.sleep(sleep_interval)

    return total_rows, failed
