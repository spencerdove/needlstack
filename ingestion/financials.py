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

logger = logging.getLogger(__name__)

SEC_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
USER_AGENT = "needlstack/1.0 (financial data pipeline; contact@example.com)"

# Maps our column names to ordered lists of XBRL US-GAAP tag aliases to try.
TAG_MAP: dict[str, list[str]] = {
    # Income statement
    "revenue": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
    ],
    "cost_of_revenue": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
    "gross_profit": ["GrossProfit"],
    "operating_income": ["OperatingIncomeLoss"],
    "pretax_income": [
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"
    ],
    "income_tax": ["IncomeTaxExpenseBenefit"],
    "net_income": ["NetIncomeLoss"],
    "eps_basic": ["EarningsPerShareBasic"],
    "eps_diluted": ["EarningsPerShareDiluted"],
    "shares_basic": ["WeightedAverageNumberOfSharesOutstandingBasic"],
    "shares_diluted": ["WeightedAverageNumberOfDilutedSharesOutstanding"],
    # Balance sheet
    "cash": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsAndShortTermInvestments",
    ],
    "current_assets": ["AssetsCurrent"],
    "total_assets": ["Assets"],
    "accounts_payable": ["AccountsPayableCurrent"],
    "current_liabilities": ["LiabilitiesCurrent"],
    "long_term_debt": ["LongTermDebt", "LongTermDebtNoncurrent"],
    "total_liabilities": ["Liabilities"],
    "stockholders_equity": [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "retained_earnings": ["RetainedEarningsAccumulatedDeficit"],
    # Cash flow
    "operating_cf": ["NetCashProvidedByUsedInOperatingActivities"],
    "capex": ["PaymentsToAcquirePropertyPlantAndEquipment"],
    "investing_cf": ["NetCashProvidedByUsedInInvestingActivities"],
    "financing_cf": ["NetCashProvidedByUsedInFinancingActivities"],
    "dividends_paid": ["PaymentsOfDividends", "PaymentsOfDividendsCommonStock"],
    "stock_repurchases": ["PaymentsForRepurchaseOfCommonStock"],
}

# Which columns belong to which table
INCOME_COLS = {
    "revenue", "cost_of_revenue", "gross_profit", "operating_income",
    "pretax_income", "income_tax", "net_income", "eps_basic", "eps_diluted",
    "shares_basic", "shares_diluted",
}
BALANCE_COLS = {
    "cash", "current_assets", "total_assets", "accounts_payable",
    "current_liabilities", "long_term_debt", "total_liabilities",
    "stockholders_equity", "retained_earnings",
}
CASHFLOW_COLS = {
    "operating_cf", "capex", "investing_cf", "financing_cf",
    "dividends_paid", "stock_repurchases",
}

# EPS and share counts use "shares" unit rather than "USD"
SHARES_UNIT_COLS = {"eps_basic", "eps_diluted", "shares_basic", "shares_diluted"}


def fetch_company_facts(cik: int) -> dict:
    """Fetch raw XBRL companyfacts JSON from SEC EDGAR for a given CIK."""
    url = SEC_FACTS_URL.format(cik=cik)
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _extract_tag_facts(
    us_gaap: dict, col_name: str, tag_aliases: list[str]
) -> dict[tuple[str, str], dict]:
    """
    Collect all facts with form_type in ('10-Q', '10-K') across all tag aliases,
    merging results and keeping the most recently filed version for each
    (end_date, form_type) key.

    Returns a dict keyed by (end_date, form_type) → {value, filed, fp, fy}.
    """
    # EPS tags use "USD/shares"; share count tags use "shares"
    if col_name in ("eps_basic", "eps_diluted"):
        accepted_units = {"USD/shares"}
    elif col_name in ("shares_basic", "shares_diluted"):
        accepted_units = {"shares"}
    else:
        accepted_units = {"USD"}

    result: dict[tuple[str, str], dict] = {}

    for tag in tag_aliases:
        tag_data = us_gaap.get(tag)
        if tag_data is None:
            continue
        units_data = tag_data.get("units", {})
        facts_list = []
        for unit_label, facts in units_data.items():
            if unit_label in accepted_units:
                facts_list.extend(facts)

        for fact in facts_list:
            form = fact.get("form", "")
            if form not in ("10-Q", "10-K"):
                continue
            end = fact.get("end")
            if not end:
                continue
            key = (end, form)
            # Keep the most recently filed version across all aliases
            existing = result.get(key)
            if existing is None or fact.get("filed", "") > existing.get("filed", ""):
                result[key] = {
                    "value": fact.get("val"),
                    "filed": fact.get("filed"),
                    "fp": fact.get("fp", ""),
                    "fy": fact.get("fy"),
                }

    return result


def parse_facts(
    facts_json: dict, ticker: str
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Parse SEC companyfacts JSON into three lists of row dicts:
    (income_rows, balance_rows, cashflow_rows).
    """
    us_gaap = facts_json.get("facts", {}).get("us-gaap", {})

    # Collect per-column data keyed by (end_date, form_type)
    col_data: dict[str, dict[tuple[str, str], dict]] = {}
    for col_name, aliases in TAG_MAP.items():
        col_data[col_name] = _extract_tag_facts(us_gaap, col_name, aliases)

    # Gather all unique (end_date, form_type) keys across all columns
    all_keys: set[tuple[str, str]] = set()
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
        # Find any metadata (filed, fp, fy) from whichever col has data for this key
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
            balance_rows.append(row)

        # Cash flow row
        if any((end_date, form_type) in col_data.get(c, {}) for c in CASHFLOW_COLS):
            row = {
                "ticker": ticker,
                "period_end": end_date,
                "period_type": base["period_type"],
                "filed_date": base["filed_date"],
            }
            for col in CASHFLOW_COLS:
                fact = col_data.get(col, {}).get((end_date, form_type))
                row[col] = fact["value"] if fact else None
            # Derive free_cash_flow
            op_cf = row.get("operating_cf")
            capex = row.get("capex")
            if op_cf is not None and capex is not None:
                row["free_cash_flow"] = op_cf - abs(capex)
            else:
                row["free_cash_flow"] = None
            cashflow_rows.append(row)

    return income_rows, balance_rows, cashflow_rows


def _upsert_income(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO income_statements
                    (ticker, period_end, period_type, fiscal_year, fiscal_quarter,
                     form_type, filed_date, revenue, cost_of_revenue, gross_profit,
                     operating_income, pretax_income, income_tax, net_income,
                     eps_basic, eps_diluted, shares_basic, shares_diluted)
                VALUES
                    (:ticker, :period_end, :period_type, :fiscal_year, :fiscal_quarter,
                     :form_type, :filed_date, :revenue, :cost_of_revenue, :gross_profit,
                     :operating_income, :pretax_income, :income_tax, :net_income,
                     :eps_basic, :eps_diluted, :shares_basic, :shares_diluted)
                """
            ),
            rows,
        )
    return len(rows)


def _upsert_balance(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO balance_sheets
                    (ticker, period_end, period_type, filed_date, cash,
                     current_assets, total_assets, accounts_payable,
                     current_liabilities, long_term_debt, total_liabilities,
                     stockholders_equity, retained_earnings)
                VALUES
                    (:ticker, :period_end, :period_type, :filed_date, :cash,
                     :current_assets, :total_assets, :accounts_payable,
                     :current_liabilities, :long_term_debt, :total_liabilities,
                     :stockholders_equity, :retained_earnings)
                """
            ),
            rows,
        )
    return len(rows)


def _upsert_cashflow(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO cash_flows
                    (ticker, period_end, period_type, filed_date, operating_cf,
                     capex, investing_cf, financing_cf, dividends_paid,
                     stock_repurchases, free_cash_flow)
                VALUES
                    (:ticker, :period_end, :period_type, :filed_date, :operating_cf,
                     :capex, :investing_cf, :financing_cf, :dividends_paid,
                     :stock_repurchases, :free_cash_flow)
                """
            ),
            rows,
        )
    return len(rows)


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
            income_rows, balance_rows, cashflow_rows = parse_facts(facts_json, ticker)
            n = (
                _upsert_income(engine, income_rows)
                + _upsert_balance(engine, balance_rows)
                + _upsert_cashflow(engine, cashflow_rows)
            )
            total_rows += n
            logger.debug(f"{ticker}: {n} rows inserted ({len(income_rows)}I/{len(balance_rows)}B/{len(cashflow_rows)}CF)")
        except Exception as exc:
            logger.error(f"Failed to process {ticker} (CIK {cik}): {exc}")
            failed.append(ticker)
        finally:
            time.sleep(sleep_interval)

    return total_rows, failed
