"""
Context-aware fact selection for SEC EDGAR XBRL companyfacts data.

The companyfacts endpoint returns many facts per tag (e.g. YTD variants,
segment-level data, amended filings). This module scores each candidate fact
and selects the best one for a given (end_date, form_type) key.
"""
from datetime import datetime
from typing import Optional


class ContextSelector:
    """Score and select the best fact for a given period from multiple candidates."""

    # Expected fiscal period duration in days
    _EXPECTED_DAYS = {"Q": 91, "A": 365}
    _TOLERANCE = {"Q": 20, "A": 45}

    def _parse_date(self, s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    def _duration_days(self, fact: dict) -> Optional[int]:
        start = self._parse_date(fact.get("start"))
        end = self._parse_date(fact.get("end"))
        if start is None or end is None:
            return None
        return (end - start).days

    def score_fact(
        self,
        fact: dict,
        col_name: str,
        form_type: str,
        statement_concepts: Optional[dict] = None,
    ) -> float:
        """Return 0.0–1.0 confidence score. Higher = prefer this fact."""
        score = 0.0
        fp = fact.get("fp", "")
        duration = self._duration_days(fact)

        # +0.4 — prefer facts with duration close to expected period length
        # (segment-only facts often have mismatched durations vs consolidated period)
        period_key = "Q" if form_type == "10-Q" else "A"
        expected = self._EXPECTED_DAYS[period_key]
        tolerance = self._TOLERANCE[period_key]
        if duration is not None:
            diff = abs(duration - expected)
            if diff <= tolerance:
                score += 0.4
            elif diff <= tolerance * 2:
                score += 0.2

        # +0.3 — fp matches expected form type
        if form_type == "10-K" and fp in ("FY", "CY"):
            score += 0.3
        elif form_type == "10-Q" and fp in ("Q1", "Q2", "Q3"):
            score += 0.3
        elif form_type == "10-Q" and fp == "Q4":
            score += 0.1  # Q4 10-Q is unusual — partial credit

        # +0.2 — prefer consolidated (longer duration relative to period is a proxy)
        # Annual consolidated = ~365 days; segment facts often much shorter
        if period_key == "A" and duration is not None and duration >= 300:
            score += 0.2
        elif period_key == "Q" and duration is not None and 60 <= duration <= 120:
            score += 0.2

        # +0.35 — concept appears in primary statement presentation tree
        # Overrides any ambiguity between statement facts and note disclosures
        if statement_concepts is not None:
            tag = fact.get("tag")
            if tag and tag in statement_concepts:
                score += 0.35

        # +0.1 — more recently filed wins (amended filings preferred)
        # Applied at selection time by comparing filed dates, not as absolute score
        # (The select_best method handles this as tiebreaker)

        return min(score, 1.0)

    def select_best(
        self,
        facts: list,
        col_name: str,
        form_type: str,
        statement_concepts: Optional[dict] = None,
    ) -> Optional[dict]:
        """Return the single best fact from candidates for this (end_date, form_type)."""
        if not facts:
            return None
        if len(facts) == 1:
            return facts[0]

        # Score all candidates
        scored = [
            (self.score_fact(f, col_name, form_type, statement_concepts),
             f.get("filed", ""), f)
            for f in facts
        ]
        # Sort: highest score first, then most recently filed as tiebreaker
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        return scored[0][2]

    def ytd_to_quarterly(
        self,
        ytd_facts: dict,
        prior_ytd_facts: Optional[dict],
        col_name: str,
    ) -> Optional[float]:
        """
        Derive a point-in-time quarterly value from YTD facts.

        ytd_facts:       the current YTD fact dict  (e.g. 9-month period)
        prior_ytd_facts: the prior YTD fact dict    (e.g. 6-month period)

        Returns derived quarterly value or None.
        """
        if ytd_facts is None:
            return None
        curr_val = ytd_facts.get("value")
        if curr_val is None:
            return None
        if prior_ytd_facts is None:
            return None
        prior_val = prior_ytd_facts.get("value")
        if prior_val is None:
            return None
        return curr_val - prior_val
