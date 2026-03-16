"""
Export validation run data to docs/data/validation.json.

Usage:
    python scripts/export_validation.py
    LOCAL_EXPORT=1 python scripts/export_validation.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.schema import get_engine

DOCS_DIR = Path(__file__).parent.parent / "docs"
DATA_DIR = DOCS_DIR / "data"
LOCAL_EXPORT = os.environ.get("LOCAL_EXPORT", "0") == "1"


def _write_or_upload(key: str, data: str) -> None:
    if LOCAL_EXPORT:
        out_path = DATA_DIR / key
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(data)
    else:
        from storage.r2 import upload_json
        upload_json(key, data)


def main() -> None:
    engine = get_engine()

    with engine.connect() as conn:
        # Get all runs
        try:
            run_rows = conn.execute(sa.text(
                "SELECT run_id, triggered_at, n_tickers, overall_pass_rate, "
                "avg_score, notes "
                "FROM validation_runs ORDER BY triggered_at DESC LIMIT 10"
            )).fetchall()
        except Exception:
            run_rows = []

        runs = []
        for r in run_rows:
            runs.append({
                "run_id": r[0],
                "triggered_at": str(r[1]) if r[1] else None,
                "n_tickers": r[2],
                "overall_pass_rate": r[3],
                "avg_score": r[4],
                "notes": r[5],
            })

        latest_run = None
        if runs:
            latest_run_id = runs[0]["run_id"]

            # Summary from validation_results
            try:
                summary_row = conn.execute(sa.text(
                    "SELECT COUNT(*), SUM(CASE WHEN passed=1 THEN 1 ELSE 0 END) "
                    "FROM validation_results WHERE run_id = :run_id"
                ), {"run_id": latest_run_id}).fetchone()
                total_tests = summary_row[0] or 0
                passed = summary_row[1] or 0
                pass_rate = round(passed / total_tests, 3) if total_tests > 0 else 0
            except Exception:
                total_tests = 0
                passed = 0
                pass_rate = 0

            # Per-ticker scores from validation_scores (aggregate across periods)
            by_ticker = []
            try:
                ticker_rows = conn.execute(sa.text(
                    "SELECT ticker, AVG(overall_score) as avg_score, "
                    "SUM(n_metrics_passed) as n_passed, "
                    "SUM(n_metrics_evaluated) as n_total "
                    "FROM validation_scores WHERE run_id = :run_id "
                    "GROUP BY ticker ORDER BY avg_score ASC"
                ), {"run_id": latest_run_id}).fetchall()
                by_ticker = [
                    {
                        "ticker": r[0],
                        "overall_score": round(r[1], 3) if r[1] is not None else None,
                        "n_passed": r[2],
                        "n_total": r[3],
                    }
                    for r in ticker_rows
                ]
            except Exception:
                by_ticker = []

            # Worst metrics
            worst_metrics = []
            try:
                wm_rows = conn.execute(sa.text(
                    "SELECT metric_name, COUNT(*) as fail_count, "
                    "AVG(ABS(COALESCE(pct_diff_fmp, pct_diff_edgar))) as avg_pct_diff "
                    "FROM validation_results "
                    "WHERE run_id = :run_id AND passed = 0 "
                    "GROUP BY metric_name ORDER BY fail_count DESC LIMIT 10"
                ), {"run_id": latest_run_id}).fetchall()
                worst_metrics = [
                    {
                        "metric_name": r[0],
                        "fail_count": r[1],
                        "avg_pct_diff": round(r[2], 1) if r[2] else None,
                    }
                    for r in wm_rows
                ]
            except Exception:
                worst_metrics = []

            latest_run = {
                "run_id": latest_run_id,
                "summary": {
                    "total_tests": total_tests,
                    "passed": passed,
                    "pass_rate": pass_rate,
                },
                "by_ticker": by_ticker,
                "worst_metrics": worst_metrics,
            }

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runs": runs,
        "latest_run": latest_run,
    }

    data_str = json.dumps(result, separators=(",", ":"))
    _write_or_upload("validation.json", data_str)
    run_count = len(runs)
    ticker_count = len(latest_run["by_ticker"]) if latest_run else 0
    print(f"Exported validation.json: {run_count} runs, {ticker_count} tickers in latest")


if __name__ == "__main__":
    main()
