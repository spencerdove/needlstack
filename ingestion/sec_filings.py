"""
Parse 8-K filing metadata from SEC EDGAR submissions JSON and upsert into
the sec_filings table.

EDGAR rate limit: 10 req/sec — a 0.1 s default delay is used between requests.
"""
import json
import logging
import time
from typing import Optional

import sqlalchemy as sa

import requests

from db.schema import get_engine

logger = logging.getLogger(__name__)

EDGAR_HEADERS = {"User-Agent": "needlstack/1.0 (contact@needlstack.com)"}
EDGAR_BASE = "https://data.sec.gov/submissions"


def _fetch_edgar_submissions(cik: str) -> dict:
    """Fetch EDGAR submissions JSON for a CIK (zero-padded to 10 digits)."""
    cik_padded = f"{int(cik):010d}"
    url = f"{EDGAR_BASE}/CIK{cik_padded}.json"
    resp = requests.get(url, headers=EDGAR_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _extract_8k_rows(ticker: str, cik: str, submissions: dict) -> list[dict]:
    """
    Extract 8-K filings from submissions['filings']['recent'].

    Returns a list of row dicts ready for upsert.
    """
    recent = submissions.get("filings", {}).get("recent", {})
    form_types = recent.get("form", [])
    accession_numbers = recent.get("accessionNumber", [])
    filed_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    primary_docs = recent.get("primaryDocument", [])
    items = recent.get("items", [])

    rows: list[dict] = []
    for i, form in enumerate(form_types):
        if form.strip().upper() != "8-K":
            continue

        acc_no = accession_numbers[i] if i < len(accession_numbers) else None
        if acc_no is None:
            continue

        filed_date = filed_dates[i] if i < len(filed_dates) else None
        report_date = report_dates[i] if i < len(report_dates) else None
        primary_doc = primary_docs[i] if i < len(primary_docs) else None

        # Build primary document URL
        primary_doc_url = None
        if primary_doc and acc_no:
            cik_int = int(cik)
            acc_no_clean = acc_no.replace("-", "")
            primary_doc_url = (
                f"https://www.sec.gov/Archives/edgar/data/{cik_int}/"
                f"{acc_no_clean}/{primary_doc}"
            )

        # Parse items field: "1.01,2.02" → ["1.01","2.02"]
        raw_items = items[i] if i < len(items) else ""
        if raw_items and str(raw_items).strip():
            items_list = [s.strip() for s in str(raw_items).split(",") if s.strip()]
        else:
            items_list = []
        items_json = json.dumps(items_list)

        rows.append({
            "accession_number": acc_no,
            "ticker": ticker,
            "cik": cik,
            "form_type": "8-K",
            "filed_date": filed_date or None,
            "period_of_report": report_date or None,
            "primary_doc_url": primary_doc_url,
            "items_reported": items_json,
        })

    return rows


def _upsert_filings(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO sec_filings
                    (accession_number, ticker, cik, form_type,
                     filed_date, period_of_report, primary_doc_url, items_reported)
                VALUES
                    (:accession_number, :ticker, :cik, :form_type,
                     :filed_date, :period_of_report, :primary_doc_url, :items_reported)
                """
            ),
            rows,
        )
    return len(rows)


def download_sec_filings(
    tickers_with_ciks: list[tuple[str, str]],
    engine: Optional[sa.Engine] = None,
    delay: float = 0.1,
) -> tuple[int, list[str]]:
    """
    Fetch EDGAR submissions JSON for each (ticker, cik) pair, filter for
    8-K filings, and upsert into the sec_filings table.

    Parameters
    ----------
    tickers_with_ciks : list of (ticker, cik) tuples
    engine : SQLAlchemy engine (optional)
    delay : seconds between EDGAR requests (default 0.1 to respect 10 req/sec limit)

    Returns
    -------
    (total_rows_upserted, failed_tickers)
    """
    if engine is None:
        engine = get_engine()

    total_rows = 0
    failed: list[str] = []

    for ticker, cik in tickers_with_ciks:
        try:
            submissions = _fetch_edgar_submissions(cik)
            rows = _extract_8k_rows(ticker, cik, submissions)

            if not rows:
                logger.debug(f"{ticker}: no 8-K filings found in recent submissions")
                time.sleep(delay)
                continue

            inserted = _upsert_filings(engine, rows)
            total_rows += inserted
            logger.debug(f"{ticker}: {inserted} 8-K filings upserted")

        except Exception as exc:
            logger.error(f"Failed to process SEC filings for {ticker} (CIK {cik}): {exc}")
            failed.append(ticker)
        finally:
            time.sleep(delay)

    logger.info(f"download_sec_filings: {total_rows} rows upserted, {len(failed)} failures")
    return total_rows, failed
