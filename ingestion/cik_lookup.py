"""
Fetch and cache the SEC CIK ↔ ticker mapping, then populate the tickers table.

Cache lives at data/cik_map.json and is refreshed if >7 days old.
"""
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import requests
import sqlalchemy as sa
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
CACHE_FILE = DATA_DIR / "cik_map.json"
CACHE_TTL_DAYS = 7
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
USER_AGENT = "needlstack/1.0 (financial data pipeline; contact@example.com)"


def _cache_is_fresh() -> bool:
    if not CACHE_FILE.exists():
        return False
    mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=CACHE_TTL_DAYS)


def _fetch_from_sec() -> dict[str, int]:
    """Fetch company_tickers.json from SEC and return ticker → CIK mapping."""
    logger.info("Fetching CIK map from SEC EDGAR...")
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(SEC_TICKERS_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    raw = resp.json()

    cik_map: dict[str, int] = {}
    for entry in raw.values():
        ticker = entry["ticker"].upper().replace(".", "-")
        cik = int(entry["cik_str"])
        cik_map[ticker] = cik

    return cik_map


def get_cik_map(force_refresh: bool = False) -> dict[str, int]:
    """
    Return a dict mapping ticker → CIK (integer).

    Uses a local JSON cache; re-fetches from SEC if the cache is stale.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not force_refresh and _cache_is_fresh():
        logger.info("Loading CIK map from cache.")
        with open(CACHE_FILE) as f:
            return json.load(f)

    cik_map = _fetch_from_sec()
    with open(CACHE_FILE, "w") as f:
        json.dump(cik_map, f)
    logger.info(f"Cached {len(cik_map)} CIK mappings to {CACHE_FILE}")
    return cik_map


def update_tickers_cik(engine: sa.Engine, force_refresh: bool = False) -> int:
    """
    Populate the cik column in the tickers table for all matched tickers.

    Returns the count of tickers successfully mapped.
    """
    cik_map = get_cik_map(force_refresh=force_refresh)

    with engine.connect() as conn:
        tickers = [row[0] for row in conn.execute(sa.text("SELECT ticker FROM tickers")).fetchall()]

    updates = []
    for ticker in tickers:
        cik = cik_map.get(ticker)
        if cik is not None:
            updates.append({"cik": str(cik), "ticker": ticker})

    if updates:
        with engine.begin() as conn:
            conn.execute(
                sa.text("UPDATE tickers SET cik = :cik WHERE ticker = :ticker"),
                updates,
            )

    logger.info(f"Mapped CIK for {len(updates)}/{len(tickers)} tickers.")
    return len(updates)
