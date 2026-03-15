#!/usr/bin/env python3
"""
EDGAR XBRL Validation CLI.

Usage:
    python scripts/run_validation.py --tickers AAPL MSFT
    python scripts/run_validation.py --random 10
    python scripts/run_validation.py --all
    python scripts/run_validation.py --tickers AAPL --verbose
"""
import argparse
import logging
import random
import sys
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

from db.schema import get_engine, init_db
from ingestion.validation.comparator import run_comparison
from ingestion.validation.finnhub_client import FinnhubClient
from ingestion.validation.scorer import score_period
from ingestion.validation.storage import (
    upsert_identity_checks,
    upsert_results,
    upsert_run,
    upsert_scores,
    update_run_summary,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("run_validation")


def fetch_ticker_cik_pairs(engine: sa.Engine, tickers: Optional[list[str]] = None) -> list[tuple[str, int]]:
    with engine.connect() as conn:
        if tickers:
            placeholders = ",".join(f":t{i}" for i in range(len(tickers)))
            params = {f"t{i}": t for i, t in enumerate(tickers)}
            rows = conn.execute(
                sa.text(f"SELECT ticker, cik FROM tickers WHERE ticker IN ({placeholders}) AND cik IS NOT NULL"),
                params,
            ).fetchall()
        else:
            rows = conn.execute(
                sa.text("SELECT ticker, cik FROM tickers WHERE cik IS NOT NULL AND is_active = 1")
            ).fetchall()
    return [(row[0], int(row[1])) for row in rows]


def format_warnings(metric_results: list[dict], tolerance_multiple: float = 1.5) -> str:
    """Return a short warning string for metrics that are close to failing."""
    warns = []
    for r in metric_results:
        if r.get("mismatch_type") == "pipeline_error":
            pct = r.get("pct_diff_fmp")
            if pct is not None:
                warns.append(f"{r['metric_name']}: ERR ({pct*100:.1f}% diff)")
        elif r.get("mismatch_type") == "vendor_disagreement":
            warns.append(f"{r['metric_name']}: vendor_disagree")
    return ", ".join(warns[:3]) if warns else ""


def main():
    parser = argparse.ArgumentParser(description="Run EDGAR XBRL validation")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--tickers", nargs="+", metavar="TICKER")
    group.add_argument("--random", type=int, metavar="N")
    group.add_argument("--all", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--notes", type=str, default="")
    args = parser.parse_args()

    engine = init_db()

    # Resolve tickers
    if args.tickers:
        ticker_cik_pairs = fetch_ticker_cik_pairs(engine, args.tickers)
        triggered_by = "manual"
    elif args.all:
        ticker_cik_pairs = fetch_ticker_cik_pairs(engine)
        triggered_by = "manual"
    else:
        all_pairs = fetch_ticker_cik_pairs(engine)
        ticker_cik_pairs = random.sample(all_pairs, min(args.random, len(all_pairs)))
        triggered_by = "manual"

    if not ticker_cik_pairs:
        print("No tickers with CIK found. Run ingestion first.")
        sys.exit(1)

    # Init Finnhub client
    try:
        finnhub_client = FinnhubClient()
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    run_id = uuid4().hex
    triggered_at = datetime.now(timezone.utc).replace(tzinfo=None)

    # Create initial run record
    upsert_run(engine, {
        "run_id": run_id,
        "triggered_at": triggered_at,
        "n_tickers": len(ticker_cik_pairs),
        "n_periods": None,
        "overall_pass_rate": None,
        "avg_score": None,
        "triggered_by": triggered_by,
        "notes": args.notes,
    })

    all_metric_results = []
    all_identity_results = []
    all_scores = []

    print(f"\nRun ID: {run_id}")
    print(f"Validating {len(ticker_cik_pairs)} ticker(s)...\n")
    print(f"{'Ticker':<8} {'Period':<12} {'Score':>7}  {'Pass':>6}  Notes")
    print("-" * 65)

    for ticker, cik in ticker_cik_pairs:
        try:
            comparison = run_comparison(ticker, cik, engine, finnhub_client)
        except Exception as exc:
            logger.error("Comparison failed for %s: %s", ticker, exc)
            continue

        metric_results = comparison["metric_results"]
        identity_results = comparison["identity_results"]

        if not metric_results and not identity_results:
            logger.warning("No results for %s — skipping", ticker)
            continue

        # Group by period
        periods = set()
        for r in metric_results:
            periods.add((r["period_end"], r["period_type"]))

        for (period_end, period_type) in sorted(periods):
            period_metrics = [r for r in metric_results if r["period_end"] == period_end and r["period_type"] == period_type]
            period_identities = [r for r in identity_results if r["period_end"] == period_end and r["period_type"] == period_type]

            score = score_period(period_metrics, period_identities)
            score["ticker"] = ticker
            score["period_end"] = period_end
            score["period_type"] = period_type
            all_scores.append(score)

            # Format period label
            try:
                from datetime import date
                d = date.fromisoformat(period_end)
                period_label = f"{'FY' if period_type == 'A' else 'Q'}{d.year}"
            except (ValueError, TypeError):
                period_label = f"{period_type}{period_end}"

            n_eval = score["n_metrics_evaluated"]
            n_pass = score["n_metrics_passed"]
            overall = score["overall_score"]
            warns = format_warnings(period_metrics)
            print(f"{ticker:<8} {period_label:<12} {overall:>5.0f}/100  {n_pass:>2}/{n_eval:<2}  {warns}")

            if args.verbose:
                for r in sorted(period_metrics, key=lambda x: x.get("passed", 0)):
                    pct = f"{r['pct_diff_fmp']*100:.2f}%" if r.get("pct_diff_fmp") is not None else "N/A"
                    status = "PASS" if r.get("passed") == 1 else "FAIL"
                    mtype = r.get("mismatch_type") or ""
                    print(f"  {status} {r['metric_name']:<28} fmp_diff={pct:<8} {mtype}")
                for r in period_identities:
                    pct = f"{r['diff_pct']*100:.2f}%" if r.get("diff_pct") is not None else "N/A"
                    status = "PASS" if r.get("passed") == 1 else "FAIL"
                    print(f"  {status} [identity] {r['identity_name']:<25} diff={pct}")
                print()

        # Persist results
        upsert_results(engine, metric_results, run_id)
        upsert_identity_checks(engine, identity_results, run_id)
        upsert_scores(engine, [s for s in all_scores if s["ticker"] == ticker], run_id)

        all_metric_results.extend(metric_results)
        all_identity_results.extend(identity_results)

    # Update run summary
    update_run_summary(engine, run_id, all_scores, all_metric_results)

    # Summary
    evaluable = [r for r in all_metric_results if r.get("mismatch_type") != "missing_vendor"]
    n_evaluable = len(evaluable)
    n_passed = sum(1 for r in evaluable if r.get("passed") == 1)
    pass_rate = (n_passed / n_evaluable * 100) if n_evaluable > 0 else 0.0
    avg_score = sum(s["overall_score"] for s in all_scores) / len(all_scores) if all_scores else 0.0

    print("\n" + "─" * 65)
    print(f"Overall pass rate: {pass_rate:.1f}% | Avg score: {avg_score:.0f}/100 | Run ID: {run_id}")

    sys.exit(0 if avg_score >= 75 else 1)


if __name__ == "__main__":
    main()
