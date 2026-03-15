"""
DB upsert functions for validation tables.
Uses INSERT OR REPLACE INTO pattern consistent with rest of codebase.
"""
from datetime import datetime, timezone
from typing import Optional

import sqlalchemy as sa


def upsert_run(engine: sa.Engine, run_row: dict) -> None:
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO validation_runs
                    (run_id, triggered_at, n_tickers, n_periods,
                     overall_pass_rate, avg_score, triggered_by, notes)
                VALUES
                    (:run_id, :triggered_at, :n_tickers, :n_periods,
                     :overall_pass_rate, :avg_score, :triggered_by, :notes)
                """
            ),
            run_row,
        )


def upsert_results(engine: sa.Engine, rows: list[dict], run_id: str) -> None:
    if not rows:
        return
    data = [{**r, "run_id": run_id} for r in rows]
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO validation_results
                    (run_id, ticker, period_end, period_type, metric_name,
                     pipeline_value, fmp_value, edgar_value,
                     pct_diff_fmp, pct_diff_edgar, tolerance, passed, mismatch_type)
                VALUES
                    (:run_id, :ticker, :period_end, :period_type, :metric_name,
                     :pipeline_value, :fmp_value, :edgar_value,
                     :pct_diff_fmp, :pct_diff_edgar, :tolerance, :passed, :mismatch_type)
                """
            ),
            data,
        )


def upsert_identity_checks(engine: sa.Engine, rows: list[dict], run_id: str) -> None:
    if not rows:
        return
    data = [{**r, "run_id": run_id} for r in rows]
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO validation_identity_checks
                    (run_id, ticker, period_end, period_type, identity_name,
                     lhs_value, rhs_value, diff_pct, passed)
                VALUES
                    (:run_id, :ticker, :period_end, :period_type, :identity_name,
                     :lhs_value, :rhs_value, :diff_pct, :passed)
                """
            ),
            data,
        )


def upsert_scores(engine: sa.Engine, rows: list[dict], run_id: str) -> None:
    if not rows:
        return
    data = [{**r, "run_id": run_id} for r in rows]
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO validation_scores
                    (run_id, ticker, period_end, period_type,
                     metric_accuracy_score, identity_score, vendor_agreement_score,
                     overall_score, n_metrics_evaluated, n_metrics_passed,
                     n_identities_evaluated, n_identities_passed)
                VALUES
                    (:run_id, :ticker, :period_end, :period_type,
                     :metric_accuracy_score, :identity_score, :vendor_agreement_score,
                     :overall_score, :n_metrics_evaluated, :n_metrics_passed,
                     :n_identities_evaluated, :n_identities_passed)
                """
            ),
            data,
        )


def update_run_summary(
    engine: sa.Engine,
    run_id: str,
    scores: list[dict],
    results: list[dict],
) -> None:
    """Compute aggregate stats and write back to validation_runs."""
    if not scores:
        return

    n_periods = len(scores)
    avg_score = sum(s["overall_score"] for s in scores) / n_periods

    evaluable = [r for r in results if r.get("mismatch_type") != "missing_fmp"]
    n_evaluable = len(evaluable)
    n_passed = sum(1 for r in evaluable if r.get("passed") == 1)
    overall_pass_rate = (n_passed / n_evaluable) if n_evaluable > 0 else 0.0

    tickers = {s["ticker"] for s in scores}

    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                UPDATE validation_runs
                SET n_tickers = :n_tickers,
                    n_periods = :n_periods,
                    overall_pass_rate = :overall_pass_rate,
                    avg_score = :avg_score
                WHERE run_id = :run_id
                """
            ),
            {
                "run_id": run_id,
                "n_tickers": len(tickers),
                "n_periods": n_periods,
                "overall_pass_rate": round(overall_pass_rate, 4),
                "avg_score": round(avg_score, 2),
            },
        )
