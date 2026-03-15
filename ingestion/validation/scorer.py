"""
Scoring engine: 0-100 composite score per (ticker, period).
"""


def score_period(
    metric_results: list[dict],
    identity_results: list[dict],
) -> dict:
    """
    Scores a single (ticker, period_end, period_type):

    metric_accuracy_score (0–40):
        n_passed / n_evaluable * 40
        Denominator excludes 'missing_vendor' rows

    identity_score (0–30):
        n_identities_passed / n_total * 30

    vendor_agreement_score (0–30):
        fraction where pipeline matches OR mismatch_type='vendor_disagreement' * 30

    overall_score = sum (0–100)
    """
    # --- metric_accuracy_score ---
    evaluable = [r for r in metric_results if r.get("mismatch_type") != "missing_vendor"]
    n_evaluable = len(evaluable)
    n_metric_passed = sum(1 for r in evaluable if r.get("passed") == 1)
    metric_accuracy_score = (n_metric_passed / n_evaluable * 40) if n_evaluable > 0 else 0.0

    # --- identity_score ---
    n_identities = len(identity_results)
    n_identity_passed = sum(1 for r in identity_results if r.get("passed") == 1)
    identity_score = (n_identity_passed / n_identities * 30) if n_identities > 0 else 0.0

    # --- vendor_agreement_score ---
    # Metrics that are "not our fault": passed=1 OR vendor_disagreement
    # Exclude missing_fmp from denominator
    n_total_metrics = len(metric_results)
    not_our_fault = sum(
        1 for r in metric_results
        if r.get("passed") == 1 or r.get("mismatch_type") == "vendor_disagreement"
    )
    vendor_agreement_score = (not_our_fault / n_total_metrics * 30) if n_total_metrics > 0 else 0.0

    overall_score = metric_accuracy_score + identity_score + vendor_agreement_score

    return {
        "metric_accuracy_score": round(metric_accuracy_score, 2),
        "identity_score": round(identity_score, 2),
        "vendor_agreement_score": round(vendor_agreement_score, 2),
        "overall_score": round(overall_score, 2),
        "n_metrics_evaluated": n_evaluable,
        "n_metrics_passed": n_metric_passed,
        "n_identities_evaluated": n_identities,
        "n_identities_passed": n_identity_passed,
    }
