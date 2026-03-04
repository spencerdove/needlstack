"""
Claude tool_use definitions and Python implementations.
All tools query the DB directly — no external API calls.
"""
import json
import logging
from datetime import date, timedelta
from typing import Any, Optional

import sqlalchemy as sa

from db.schema import get_engine

logger = logging.getLogger(__name__)


TOOL_DEFINITIONS = [
    {
        "name": "get_price_history",
        "description": "Get historical OHLCV price data for a ticker. Returns up to 252 rows.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "Stock ticker symbol"},
                "start_date": {"type": "string", "description": "Start date YYYY-MM-DD"},
                "end_date": {
                    "type": "string",
                    "description": "End date YYYY-MM-DD (optional, defaults to today)",
                },
            },
            "required": ["ticker", "start_date"],
        },
    },
    {
        "name": "get_financial_summary",
        "description": "Get income statement, balance sheet, and cash flow data for a ticker.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "periods": {
                    "type": "integer",
                    "description": "Number of periods to return (default 8)",
                },
                "period_type": {
                    "type": "string",
                    "enum": ["A", "Q"],
                    "description": "Annual (A) or Quarterly (Q)",
                },
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_valuation_multiples",
        "description": "Get current and historical valuation multiples (PE, PB, EV/EBITDA) for a ticker.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_sentiment_trend",
        "description": "Get daily news sentiment and mention count for a ticker over recent days.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "days": {
                    "type": "integer",
                    "description": "Number of days of history (default 30)",
                },
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "compare_tickers",
        "description": "Compare multiple tickers side-by-side on derived financial metrics.",
        "input_schema": {
            "type": "object",
            "properties": {
                "tickers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of ticker symbols",
                },
                "metrics": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Metric names to compare (e.g. ['pe_ttm', 'gross_margin', 'revenue_yoy_growth'])",
                },
            },
            "required": ["tickers"],
        },
    },
    {
        "name": "screen_stocks",
        "description": "Screen stocks based on financial metric filters.",
        "input_schema": {
            "type": "object",
            "properties": {
                "filters": {
                    "type": "object",
                    "description": (
                        "Dict of metric_name -> {lt, gt, lte, gte} constraints. "
                        'E.g. {"pe_ttm": {"lt": 20}, "fcf_margin": {"gt": 0.15}}'
                    ),
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default 20)",
                },
            },
            "required": ["filters"],
        },
    },
    {
        "name": "get_institutional_flows",
        "description": "Get institutional ownership changes from 13F filings for a ticker.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "quarters": {
                    "type": "integer",
                    "description": "Number of quarters (default 4)",
                },
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_narrative_context",
        "description": "Get context for a market narrative including recent signal strength.",
        "input_schema": {
            "type": "object",
            "properties": {
                "narrative_id": {
                    "type": "string",
                    "description": "Narrative slug e.g. 'ai-datacenter-capex'",
                },
            },
            "required": ["narrative_id"],
        },
    },
]


# ── Tool implementation functions ─────────────────────────────────────────────

def _get_price_history(tool_input: dict, engine: sa.Engine) -> list[dict]:
    ticker = tool_input["ticker"].upper()
    start_date = tool_input["start_date"]
    end_date = tool_input.get("end_date") or date.today().isoformat()

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT date, open, high, low, close, adj_close, volume, dollar_volume
                FROM stock_prices
                WHERE ticker = :ticker
                  AND date >= :start_date
                  AND date <= :end_date
                ORDER BY date ASC
                LIMIT 252
                """
            ),
            {"ticker": ticker, "start_date": start_date, "end_date": end_date},
        ).fetchall()

    if not rows:
        return []

    return [
        {
            "date": str(r[0]),
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
            "adj_close": r[5],
            "volume": r[6],
            "dollar_volume": r[7],
        }
        for r in rows
    ]


def _get_financial_summary(tool_input: dict, engine: sa.Engine) -> dict:
    ticker = tool_input["ticker"].upper()
    periods = tool_input.get("periods", 8)
    period_type = tool_input.get("period_type", "Q")

    with engine.connect() as conn:
        is_rows = conn.execute(
            sa.text(
                """
                SELECT period_end, period_type, fiscal_year, fiscal_quarter,
                       revenue, gross_profit, operating_income, net_income,
                       eps_basic, eps_diluted, shares_diluted
                FROM income_statements
                WHERE ticker = :ticker AND period_type = :period_type
                ORDER BY period_end DESC
                LIMIT :periods
                """
            ),
            {"ticker": ticker, "period_type": period_type, "periods": periods},
        ).fetchall()

        bs_rows = conn.execute(
            sa.text(
                """
                SELECT period_end, period_type,
                       cash, current_assets, total_assets,
                       current_liabilities, long_term_debt, total_liabilities,
                       stockholders_equity, retained_earnings
                FROM balance_sheets
                WHERE ticker = :ticker AND period_type = :period_type
                ORDER BY period_end DESC
                LIMIT :periods
                """
            ),
            {"ticker": ticker, "period_type": period_type, "periods": periods},
        ).fetchall()

        cf_rows = conn.execute(
            sa.text(
                """
                SELECT period_end, period_type,
                       operating_cf, capex, investing_cf, financing_cf,
                       dividends_paid, stock_repurchases, free_cash_flow
                FROM cash_flows
                WHERE ticker = :ticker AND period_type = :period_type
                ORDER BY period_end DESC
                LIMIT :periods
                """
            ),
            {"ticker": ticker, "period_type": period_type, "periods": periods},
        ).fetchall()

    def _rows_to_dicts(rows, cols):
        return [dict(zip(cols, r)) for r in rows]

    return {
        "ticker": ticker,
        "period_type": period_type,
        "income_statements": _rows_to_dicts(
            is_rows,
            ["period_end", "period_type", "fiscal_year", "fiscal_quarter",
             "revenue", "gross_profit", "operating_income", "net_income",
             "eps_basic", "eps_diluted", "shares_diluted"],
        ),
        "balance_sheets": _rows_to_dicts(
            bs_rows,
            ["period_end", "period_type", "cash", "current_assets", "total_assets",
             "current_liabilities", "long_term_debt", "total_liabilities",
             "stockholders_equity", "retained_earnings"],
        ),
        "cash_flows": _rows_to_dicts(
            cf_rows,
            ["period_end", "period_type", "operating_cf", "capex", "investing_cf",
             "financing_cf", "dividends_paid", "stock_repurchases", "free_cash_flow"],
        ),
    }


def _get_valuation_multiples(tool_input: dict, engine: sa.Engine) -> list[dict]:
    ticker = tool_input["ticker"].upper()

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT snapshot_date, pe_ttm, pb, ps_ttm, ev_ebitda, peg_ratio
                FROM valuation_snapshots
                WHERE ticker = :ticker
                ORDER BY snapshot_date DESC
                LIMIT 20
                """
            ),
            {"ticker": ticker},
        ).fetchall()

    if not rows:
        return []

    return [
        {
            "snapshot_date": str(r[0]),
            "pe_ttm": r[1],
            "pb": r[2],
            "ps_ttm": r[3],
            "ev_ebitda": r[4],
            "peg_ratio": r[5],
        }
        for r in rows
    ]


def _get_sentiment_trend(tool_input: dict, engine: sa.Engine) -> list[dict]:
    ticker = tool_input["ticker"].upper()
    days = tool_input.get("days", 30)
    start_date = (date.today() - timedelta(days=days)).isoformat()

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT date, mention_count, article_count, source_count,
                       avg_sentiment, bullish_count, bearish_count, neutral_count,
                       title_mention_count
                FROM ticker_sentiment_daily
                WHERE ticker = :ticker AND date >= :start_date
                ORDER BY date DESC
                """
            ),
            {"ticker": ticker, "start_date": start_date},
        ).fetchall()

    if not rows:
        return []

    return [
        {
            "date": str(r[0]),
            "mention_count": r[1],
            "article_count": r[2],
            "source_count": r[3],
            "avg_sentiment": r[4],
            "bullish_count": r[5],
            "bearish_count": r[6],
            "neutral_count": r[7],
            "title_mention_count": r[8],
        }
        for r in rows
    ]


def _compare_tickers(tool_input: dict, engine: sa.Engine) -> list[dict]:
    tickers = [t.upper() for t in tool_input["tickers"]]
    requested_metrics = tool_input.get("metrics")

    # All available columns in derived_metrics
    all_metric_cols = [
        "revenue_yoy_growth", "net_income_yoy_growth", "eps_yoy_growth",
        "gross_margin", "operating_margin", "net_margin", "fcf_margin",
        "roe", "roa", "debt_to_equity", "current_ratio", "pe_ttm", "ev_ebitda",
    ]

    if requested_metrics:
        # Whitelist to valid column names to prevent injection
        cols = [m for m in requested_metrics if m in all_metric_cols]
        if not cols:
            cols = all_metric_cols
    else:
        cols = all_metric_cols

    select_cols = ", ".join(cols)
    placeholders = ", ".join(f":t{i}" for i in range(len(tickers)))
    params = {f"t{i}": t for i, t in enumerate(tickers)}

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                f"""
                SELECT ticker, date, {select_cols}
                FROM derived_metrics
                WHERE ticker IN ({placeholders})
                  AND date = (
                      SELECT MAX(date) FROM derived_metrics dm2
                      WHERE dm2.ticker = derived_metrics.ticker
                  )
                ORDER BY ticker
                """
            ),
            params,
        ).fetchall()

    if not rows:
        return []

    result_cols = ["ticker", "date"] + cols
    return [dict(zip(result_cols, r)) for r in rows]


def _screen_stocks(tool_input: dict, engine: sa.Engine) -> list[dict]:
    filters: dict = tool_input.get("filters", {})
    limit = tool_input.get("limit", 20)

    all_metric_cols = {
        "revenue_yoy_growth", "net_income_yoy_growth", "eps_yoy_growth",
        "gross_margin", "operating_margin", "net_margin", "fcf_margin",
        "roe", "roa", "debt_to_equity", "current_ratio", "pe_ttm", "ev_ebitda",
    }

    operator_map = {"lt": "<", "gt": ">", "lte": "<=", "gte": ">="}

    where_clauses = []
    params: dict = {}

    for metric, constraints in filters.items():
        if metric not in all_metric_cols:
            continue
        if not isinstance(constraints, dict):
            continue
        for op_key, value in constraints.items():
            sql_op = operator_map.get(op_key)
            if sql_op is None:
                continue
            param_name = f"p_{metric}_{op_key}"
            where_clauses.append(f"{metric} {sql_op} :{param_name}")
            params[param_name] = value

    base_where = """
        date = (
            SELECT MAX(date) FROM derived_metrics dm2
            WHERE dm2.ticker = derived_metrics.ticker
        )
    """

    if where_clauses:
        full_where = base_where + " AND " + " AND ".join(where_clauses)
    else:
        full_where = base_where

    params["limit"] = limit

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                f"""
                SELECT dm.ticker, t.company_name, t.sector,
                       dm.date,
                       dm.revenue_yoy_growth, dm.net_income_yoy_growth, dm.eps_yoy_growth,
                       dm.gross_margin, dm.operating_margin, dm.net_margin, dm.fcf_margin,
                       dm.roe, dm.roa, dm.debt_to_equity, dm.current_ratio,
                       dm.pe_ttm, dm.ev_ebitda
                FROM derived_metrics dm
                LEFT JOIN tickers t ON t.ticker = dm.ticker
                WHERE {full_where}
                ORDER BY dm.ticker
                LIMIT :limit
                """
            ),
            params,
        ).fetchall()

    if not rows:
        return []

    cols = [
        "ticker", "company_name", "sector", "date",
        "revenue_yoy_growth", "net_income_yoy_growth", "eps_yoy_growth",
        "gross_margin", "operating_margin", "net_margin", "fcf_margin",
        "roe", "roa", "debt_to_equity", "current_ratio", "pe_ttm", "ev_ebitda",
    ]
    return [dict(zip(cols, r)) for r in rows]


def _get_institutional_flows(tool_input: dict, engine: sa.Engine) -> list[dict]:
    ticker = tool_input["ticker"].upper()
    quarters = tool_input.get("quarters", 4)

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                """
                SELECT institution_name, report_date, shares_held, market_value,
                       pct_of_portfolio, change_shares
                FROM institutional_holdings
                WHERE ticker = :ticker
                ORDER BY report_date DESC, shares_held DESC
                LIMIT :limit
                """
            ),
            {"ticker": ticker, "limit": quarters * 20},
        ).fetchall()

        summary_row = conn.execute(
            sa.text(
                """
                SELECT report_date, total_institutions, total_shares_held,
                       pct_outstanding_held, net_change_shares,
                       top_holder_name, top_holder_pct
                FROM institutional_summary
                WHERE ticker = :ticker
                """
            ),
            {"ticker": ticker},
        ).fetchone()

    result: dict = {
        "ticker": ticker,
        "summary": None,
        "top_holders": [],
    }

    if summary_row:
        result["summary"] = {
            "report_date": str(summary_row[0]) if summary_row[0] else None,
            "total_institutions": summary_row[1],
            "total_shares_held": summary_row[2],
            "pct_outstanding_held": summary_row[3],
            "net_change_shares": summary_row[4],
            "top_holder_name": summary_row[5],
            "top_holder_pct": summary_row[6],
        }

    if rows:
        result["top_holders"] = [
            {
                "institution_name": r[0],
                "report_date": str(r[1]) if r[1] else None,
                "shares_held": r[2],
                "market_value": r[3],
                "pct_of_portfolio": r[4],
                "change_shares": r[5],
            }
            for r in rows
        ]

    return result


def _get_narrative_context(tool_input: dict, engine: sa.Engine) -> dict:
    narrative_id = tool_input["narrative_id"]

    with engine.connect() as conn:
        narrative_row = conn.execute(
            sa.text(
                """
                SELECT narrative_id, name, description, keywords,
                       related_tickers, created_at, last_seen_at, is_active
                FROM narratives
                WHERE narrative_id = :narrative_id
                """
            ),
            {"narrative_id": narrative_id},
        ).fetchone()

        if not narrative_row:
            return {}

        signals = conn.execute(
            sa.text(
                """
                SELECT date, mention_count, momentum_score
                FROM narrative_signals
                WHERE narrative_id = :narrative_id
                ORDER BY date DESC
                LIMIT 30
                """
            ),
            {"narrative_id": narrative_id},
        ).fetchall()

    return {
        "narrative_id": narrative_row[0],
        "name": narrative_row[1],
        "description": narrative_row[2],
        "keywords": narrative_row[3],
        "related_tickers": narrative_row[4],
        "created_at": str(narrative_row[5]) if narrative_row[5] else None,
        "last_seen_at": str(narrative_row[6]) if narrative_row[6] else None,
        "is_active": bool(narrative_row[7]),
        "recent_signals": [
            {
                "date": str(r[0]),
                "mention_count": r[1],
                "momentum_score": r[2],
            }
            for r in signals
        ],
    }


# ── Dispatch ──────────────────────────────────────────────────────────────────

_TOOL_HANDLERS = {
    "get_price_history": _get_price_history,
    "get_financial_summary": _get_financial_summary,
    "get_valuation_multiples": _get_valuation_multiples,
    "get_sentiment_trend": _get_sentiment_trend,
    "compare_tickers": _compare_tickers,
    "screen_stocks": _screen_stocks,
    "get_institutional_flows": _get_institutional_flows,
    "get_narrative_context": _get_narrative_context,
}


def execute_tool(
    tool_name: str,
    tool_input: dict,
    engine: Optional[sa.Engine] = None,
) -> Any:
    """
    Dispatch a tool call to the appropriate implementation function.

    Returns JSON-serializable data (dict or list[dict]).
    Returns an empty list [] for unknown tools.
    """
    if engine is None:
        engine = get_engine()

    handler = _TOOL_HANDLERS.get(tool_name)
    if handler is None:
        logger.warning(f"Unknown tool requested: {tool_name}")
        return []

    try:
        return handler(tool_input, engine)
    except Exception as exc:
        logger.error(f"Tool {tool_name} failed with input {tool_input}: {exc}")
        return {"error": str(exc)}
