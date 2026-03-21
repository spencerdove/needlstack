"""
Usage tracking and cost monitoring for paid API services.

Logs every paid API call to the api_usage_log table, computes costs from
token counts, and exposes summary/health endpoints.
"""
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import sqlalchemy as sa
from fastapi import APIRouter

from db.schema import api_usage_log_table, get_engine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/usage", tags=["usage"])

# Anthropic pricing (USD per token) — update when pricing changes.
# See https://docs.anthropic.com/en/docs/about-claude/models
MODEL_PRICING = {
    "claude-sonnet-4-6": {"input": 3.00 / 1_000_000, "output": 15.00 / 1_000_000},
    "claude-sonnet-4-5-20241022": {"input": 3.00 / 1_000_000, "output": 15.00 / 1_000_000},
    "claude-haiku-4-5-20251001": {"input": 0.80 / 1_000_000, "output": 4.00 / 1_000_000},
    "claude-opus-4-6": {"input": 15.00 / 1_000_000, "output": 75.00 / 1_000_000},
}

DEFAULT_DAILY_BUDGET_USD = float(os.getenv("DAILY_BUDGET_USD", "5.00"))
DEFAULT_MONTHLY_BUDGET_USD = float(os.getenv("MONTHLY_BUDGET_USD", "50.00"))


def compute_cost(
    tokens_in: int,
    tokens_out: int,
    model: str = "claude-sonnet-4-6",
) -> float:
    """Compute USD cost from token counts and model pricing."""
    pricing = MODEL_PRICING.get(model)
    if not pricing:
        logger.warning(f"Unknown model '{model}' — using claude-sonnet-4-6 pricing")
        pricing = MODEL_PRICING["claude-sonnet-4-6"]
    return (tokens_in * pricing["input"]) + (tokens_out * pricing["output"])


def log_api_call(
    engine: sa.Engine,
    service: str,
    endpoint: Optional[str] = None,
    tokens_in: Optional[int] = None,
    tokens_out: Optional[int] = None,
    model: Optional[str] = None,
    user_id: Optional[str] = None,
    extra_metadata: Optional[str] = None,
) -> None:
    """Insert a row into api_usage_log."""
    cost = None
    if tokens_in is not None and tokens_out is not None and model:
        cost = compute_cost(tokens_in, tokens_out, model)

    row = {
        "id": str(uuid.uuid4()),
        "service": service,
        "endpoint": endpoint,
        "timestamp": datetime.now(timezone.utc),
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "cost_usd": cost,
        "user_id": user_id,
        "metadata_json": extra_metadata,
    }
    try:
        with engine.connect() as conn:
            conn.execute(api_usage_log_table.insert().values(**row))
            conn.commit()
    except Exception as exc:
        logger.error(f"Failed to log API usage: {exc}")


def get_usage_summary(
    engine: sa.Engine,
    period: str = "day",
) -> dict:
    """Aggregate usage by service for the given period (day/week/month)."""
    now = datetime.now(timezone.utc)
    if period == "day":
        since = now - timedelta(days=1)
    elif period == "week":
        since = now - timedelta(weeks=1)
    elif period == "month":
        since = now - timedelta(days=30)
    else:
        since = now - timedelta(days=1)

    query = sa.text("""
        SELECT
            service,
            COUNT(*) AS call_count,
            COALESCE(SUM(tokens_in), 0) AS total_tokens_in,
            COALESCE(SUM(tokens_out), 0) AS total_tokens_out,
            COALESCE(SUM(cost_usd), 0) AS total_cost_usd
        FROM api_usage_log
        WHERE timestamp >= :since
        GROUP BY service
        ORDER BY total_cost_usd DESC
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, {"since": since}).fetchall()

    services = []
    total_cost = 0.0
    total_calls = 0
    for row in rows:
        entry = {
            "service": row[0],
            "call_count": row[1],
            "total_tokens_in": row[2],
            "total_tokens_out": row[3],
            "total_cost_usd": round(row[4], 6),
        }
        services.append(entry)
        total_cost += row[4]
        total_calls += row[1]

    return {
        "period": period,
        "since": since.isoformat(),
        "total_calls": total_calls,
        "total_cost_usd": round(total_cost, 6),
        "services": services,
    }


def check_budget(engine: sa.Engine) -> dict:
    """Check if daily or monthly budget limits have been exceeded."""
    now = datetime.now(timezone.utc)
    day_start = now - timedelta(days=1)
    month_start = now - timedelta(days=30)

    query = sa.text("""
        SELECT
            COALESCE(SUM(CASE WHEN timestamp >= :day_start THEN cost_usd ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN timestamp >= :month_start THEN cost_usd ELSE 0 END), 0)
        FROM api_usage_log
        WHERE service = 'anthropic'
    """)

    with engine.connect() as conn:
        row = conn.execute(
            query, {"day_start": day_start, "month_start": month_start}
        ).fetchone()

    daily_spend = row[0] if row else 0.0
    monthly_spend = row[1] if row else 0.0

    return {
        "daily_spend_usd": round(daily_spend, 6),
        "daily_budget_usd": DEFAULT_DAILY_BUDGET_USD,
        "daily_budget_exceeded": daily_spend >= DEFAULT_DAILY_BUDGET_USD,
        "monthly_spend_usd": round(monthly_spend, 6),
        "monthly_budget_usd": DEFAULT_MONTHLY_BUDGET_USD,
        "monthly_budget_exceeded": monthly_spend >= DEFAULT_MONTHLY_BUDGET_USD,
    }


# --- FastAPI endpoints ---


@router.get("/summary")
async def usage_summary(period: str = "day"):
    """Return usage summary for the given period (day/week/month)."""
    engine = get_engine()
    return get_usage_summary(engine, period)


@router.get("/health")
async def usage_health():
    """Return budget status and quota health for all services."""
    engine = get_engine()
    budget = check_budget(engine)
    summary = get_usage_summary(engine, "day")
    return {
        "budget": budget,
        "today": summary,
    }
