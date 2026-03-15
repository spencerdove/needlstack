"""
Finnhub API client for /stock/financials-reported.

Free tier: 60 calls/minute global. Each ticker = 2 calls (annual + quarterly).
Response concepts use the format "us-gaap_TagName", matching our EDGAR TAG_MAP.
"""
import logging
import os
import time
from datetime import date
from typing import Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://finnhub.io/api/v1"


class FinnhubClient:
    def __init__(self, api_key: Optional[str] = None, rate_limit: int = 50):
        # rate_limit = max calls/min; sleep = 60/rate_limit seconds between calls
        self.api_key = api_key or os.getenv("FINNHUB_API_KEY", "")
        if not self.api_key:
            raise ValueError("FINNHUB_API_KEY not set — provide api_key or set env var")
        self._sleep = 60.0 / rate_limit  # seconds between calls

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        url = f"{BASE_URL}/{path}"
        headers = {"X-Finnhub-Token": self.api_key}
        resp = requests.get(url, headers=headers, params=params or {}, timeout=30)
        if resp.status_code == 404:
            return {}
        if resp.status_code == 429:
            logger.warning("Finnhub rate limit hit — sleeping 60s")
            time.sleep(60)
            resp = requests.get(url, headers=headers, params=params or {}, timeout=30)
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    def fetch_financials(self, ticker: str, freq: str = "annual") -> list[dict]:
        """
        Fetch financials-reported for one ticker and frequency.
        Returns the list of filing dicts from data[].
        Each filing: {year, quarter, form, endDate, startDate, report: {ic, bs, cf}}
        """
        time.sleep(self._sleep)
        raw = self._get(
            "stock/financials-reported",
            {"symbol": ticker, "freq": freq},
        )
        return raw.get("data", [])

    def fetch_all(self, ticker: str) -> list[dict]:
        """
        Fetch both annual and quarterly filings for a ticker.
        Returns a merged list of filing dicts (annual first, then quarterly).
        Annual:    quarter == 0, form == "10-K"
        Quarterly: quarter in (1,2,3,4), form == "10-Q"
        """
        annual = self.fetch_financials(ticker, freq="annual")
        quarterly = self.fetch_financials(ticker, freq="quarterly")
        return annual + quarterly


def parse_finnhub_date(date_str: str) -> Optional[str]:
    """
    Convert a Finnhub endDate string (e.g. "2024-12-28 00:00:00") to "YYYY-MM-DD".
    Returns None if parsing fails.
    """
    if not date_str:
        return None
    try:
        return str(date_str).split(" ")[0].split("T")[0]
    except Exception:
        return None


def match_vendor_period(
    vendor_date_str: str,
    vendor_period_type: str,
    pipeline_end: str,
    pipeline_type: str,
) -> bool:
    """Check if a Finnhub filing matches a pipeline period within ±15 days."""
    if vendor_period_type != pipeline_type:
        return False
    try:
        v_dt = date.fromisoformat(vendor_date_str)
        p_dt = date.fromisoformat(pipeline_end)
    except (ValueError, TypeError):
        return False
    return abs((v_dt - p_dt).days) <= 15
