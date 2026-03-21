"""
CLI tool to report API usage and costs.

Usage:
    python scripts/usage_report.py [--period day|week|month]
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import sqlalchemy as sa
from db.schema import get_engine
from agent.usage import get_usage_summary, check_budget


def print_usage_report(period: str) -> None:
    engine = get_engine()
    summary = get_usage_summary(engine, period)
    budget = check_budget(engine)

    print(f"\n{'=' * 60}")
    print(f"  Needlstack API Usage Report — {period.upper()}")
    print(f"  Since: {summary['since']}")
    print(f"{'=' * 60}")

    print(f"\n  Total API calls:  {summary['total_calls']:,}")
    print(f"  Total cost:       ${summary['total_cost_usd']:.4f}")

    if summary["services"]:
        print(f"\n  {'Service':<20} {'Calls':>8} {'Tokens In':>12} {'Tokens Out':>12} {'Cost':>10}")
        print(f"  {'-' * 62}")
        for svc in summary["services"]:
            print(
                f"  {svc['service']:<20} {svc['call_count']:>8,} "
                f"{svc['total_tokens_in']:>12,} {svc['total_tokens_out']:>12,} "
                f"${svc['total_cost_usd']:>9.4f}"
            )
    else:
        print("\n  No API calls recorded for this period.")

    print(f"\n  Budget Status (Anthropic)")
    print(f"  {'-' * 40}")
    daily_pct = (budget["daily_spend_usd"] / budget["daily_budget_usd"] * 100) if budget["daily_budget_usd"] > 0 else 0
    monthly_pct = (budget["monthly_spend_usd"] / budget["monthly_budget_usd"] * 100) if budget["monthly_budget_usd"] > 0 else 0
    daily_status = "EXCEEDED" if budget["daily_budget_exceeded"] else "OK"
    monthly_status = "EXCEEDED" if budget["monthly_budget_exceeded"] else "OK"
    print(f"  Daily:   ${budget['daily_spend_usd']:.4f} / ${budget['daily_budget_usd']:.2f} ({daily_pct:.1f}%) [{daily_status}]")
    print(f"  Monthly: ${budget['monthly_spend_usd']:.4f} / ${budget['monthly_budget_usd']:.2f} ({monthly_pct:.1f}%) [{monthly_status}]")

    # Also show historical data from agent_messages if available
    try:
        with engine.connect() as conn:
            row = conn.execute(
                sa.text("""
                    SELECT COUNT(*), COALESCE(SUM(tokens_used), 0)
                    FROM agent_messages
                    WHERE role = 'assistant' AND tokens_used IS NOT NULL
                """)
            ).fetchone()
            if row and row[0] > 0:
                print(f"\n  Historical (agent_messages table)")
                print(f"  {'-' * 40}")
                print(f"  Total assistant messages:  {row[0]:,}")
                print(f"  Total tokens tracked:     {row[1]:,}")
    except Exception:
        pass

    print(f"\n{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(description="Needlstack API usage report")
    parser.add_argument(
        "--period",
        choices=["day", "week", "month"],
        default="day",
        help="Time period for the report (default: day)",
    )
    args = parser.parse_args()
    print_usage_report(args.period)


if __name__ == "__main__":
    main()
