"""
Financial Modeling Prep API client.
Free tier: 250 calls/day, ~4/sec.
Each ticker fetch = 6 calls (annual + quarterly per statement type).
"""
import logging
import os
import time
from datetime import date, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class FMPClient:
    BASE_URL = "https://financialmodelingprep.com/api/v3"

    def __init__(self, api_key: Optional[str] = None, rate_limit: int = 4):
        self.api_key = api_key or os.getenv("FMP_API_KEY", "")
        if not self.api_key:
            raise ValueError("FMP_API_KEY not set — provide api_key or set env var")
        self._sleep = 1.0 / rate_limit

    def _get(self, path: str, params: Optional[dict] = None) -> list:
        url = f"{self.BASE_URL}/{path}"
        p = {"apikey": self.api_key, **(params or {})}
        resp = requests.get(url, params=p, timeout=30)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "Error Message" in data:
            logger.warning("FMP error for %s: %s", path, data["Error Message"])
            return []
        return data if isinstance(data, list) else []

    def fetch_income(self, ticker: str, limit: int = 20) -> list[dict]:
        time.sleep(self._sleep)
        annual = self._get(f"income-statement/{ticker}", {"limit": limit, "period": "annual"})
        time.sleep(self._sleep)
        quarterly = self._get(f"income-statement/{ticker}", {"limit": 20, "period": "quarter"})
        return annual + quarterly

    def fetch_balance(self, ticker: str, limit: int = 20) -> list[dict]:
        time.sleep(self._sleep)
        annual = self._get(f"balance-sheet-statement/{ticker}", {"limit": limit, "period": "annual"})
        time.sleep(self._sleep)
        quarterly = self._get(f"balance-sheet-statement/{ticker}", {"limit": 20, "period": "quarter"})
        return annual + quarterly

    def fetch_cashflow(self, ticker: str, limit: int = 20) -> list[dict]:
        time.sleep(self._sleep)
        annual = self._get(f"cash-flow-statement/{ticker}", {"limit": limit, "period": "annual"})
        time.sleep(self._sleep)
        quarterly = self._get(f"cash-flow-statement/{ticker}", {"limit": 20, "period": "quarter"})
        return annual + quarterly

    def fetch_all(self, ticker: str) -> dict[str, list[dict]]:
        """Returns {'income': [...], 'balance': [...], 'cashflow': [...]}.
        6 API calls with rate limiting."""
        return {
            "income": self.fetch_income(ticker),
            "balance": self.fetch_balance(ticker),
            "cashflow": self.fetch_cashflow(ticker),
        }


def match_fmp_period(fmp_date_str: str, fmp_period: str, pipeline_end: str, pipeline_type: str) -> bool:
    """Check if an FMP record matches a pipeline period within ±15 days."""
    try:
        fmp_dt = date.fromisoformat(fmp_date_str)
        pip_dt = date.fromisoformat(pipeline_end)
    except (ValueError, TypeError):
        return False

    # Check period type match
    fmp_is_annual = fmp_period == "FY"
    pip_is_annual = pipeline_type == "A"
    if fmp_is_annual != pip_is_annual:
        return False

    # Check date proximity (±15 days)
    return abs((fmp_dt - pip_dt).days) <= 15
