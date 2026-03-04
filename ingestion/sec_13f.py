"""
Fetch 13F-HR institutional holdings from SEC EDGAR for a set of known
large institutions and upsert into institutional_holdings.

After fetching, compute_institutional_summary() aggregates holdings by
ticker for the latest report_date and upserts into institutional_summary.

EDGAR rate limit: 10 req/sec — a 0.1 s delay is used between requests.
"""
import json
import logging
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
import sqlalchemy as sa

from db.schema import get_engine

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
CUSIP_CACHE_FILE = DATA_DIR / "cusip_ticker_map.json"
CUSIP_CACHE_TTL_DAYS = 7

EDGAR_HEADERS = {"User-Agent": "needlstack/1.0 (contact@needlstack.com)"}
EDGAR_DELAY = 0.1  # seconds between EDGAR requests

TOP_INSTITUTION_CIKS = {
    "Blackrock": "1364742",
    "Vanguard": "102909",
    "Fidelity": "315066",
    "State Street": "93751",
    "JPMorgan": "1119169",
    "Goldman Sachs": "886982",
    "Morgan Stanley": "895421",
    "Invesco": "703956",
    "T Rowe Price": "1113169",
    "Capital Group": "40533",
}

# SEC EDGAR infotable XML namespace
_INFOTABLE_NS = {
    "ns": "http://www.sec.gov/Archives/edgar/data/",
    # The actual 13F infotable namespace varies; handle both
}


def _cache_is_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=CUSIP_CACHE_TTL_DAYS)


def _load_cusip_ticker_map() -> dict[str, str]:
    """
    Build CUSIP → ticker map from SEC company_tickers_exchange.json.
    Cache to data/cusip_ticker_map.json with a 7-day TTL.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if _cache_is_fresh(CUSIP_CACHE_FILE):
        logger.info("Loading CUSIP→ticker map from cache.")
        with open(CUSIP_CACHE_FILE) as f:
            return json.load(f)

    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    logger.info("Fetching company_tickers_exchange.json from SEC...")
    resp = requests.get(url, headers=EDGAR_HEADERS, timeout=30)
    resp.raise_for_status()
    time.sleep(EDGAR_DELAY)

    data = resp.json()
    # Format: {"fields": [...], "data": [[cik, name, ticker, exchange], ...]}
    fields = data.get("fields", [])
    rows = data.get("data", [])

    cusip_map: dict[str, str] = {}
    try:
        ticker_idx = fields.index("ticker")
        # Note: company_tickers_exchange.json does not contain CUSIPs directly.
        # We build a ticker set for reference; CUSIP mapping must come from
        # the actual 13F infotable or be supplemented by another source.
        # For now we build a name→ticker map and return raw data for downstream use.
        # The real CUSIP→ticker mapping is attempted via the filing itself.
        for row in rows:
            if len(row) > ticker_idx:
                ticker = str(row[ticker_idx]).strip().upper()
                cusip_map[ticker] = ticker  # identity placeholder
    except (ValueError, IndexError):
        pass

    with open(CUSIP_CACHE_FILE, "w") as f:
        json.dump(cusip_map, f)
    logger.info(f"Cached {len(cusip_map)} entries to {CUSIP_CACHE_FILE}")
    return cusip_map


def _fetch_edgar_submissions(cik: str) -> dict:
    """Fetch EDGAR submissions JSON for a given CIK (zero-padded to 10 digits)."""
    cik_padded = f"{int(cik):010d}"
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    resp = requests.get(url, headers=EDGAR_HEADERS, timeout=30)
    resp.raise_for_status()
    time.sleep(EDGAR_DELAY)
    return resp.json()


def _find_latest_13f(submissions: dict) -> Optional[dict]:
    """
    Return a dict with accessionNumber, filingDate, reportDate for the
    most recent 13F-HR filing, or None if not found.
    """
    recent = submissions.get("filings", {}).get("recent", {})
    form_types = recent.get("form", [])
    accession_numbers = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])

    for i, form in enumerate(form_types):
        if form.strip().upper() == "13F-HR":
            return {
                "accessionNumber": accession_numbers[i],
                "filingDate": filing_dates[i] if i < len(filing_dates) else None,
                "reportDate": report_dates[i] if i < len(report_dates) else None,
            }
    return None


def _fetch_infotable_xml(cik: str, accession_number: str) -> Optional[str]:
    """
    Download the infotable.xml document from the filing index page.
    Returns the XML text, or None on failure.
    """
    cik_padded = f"{int(cik):010d}"
    acc_no_dashes = accession_number.replace("-", "")
    index_url = (
        f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
        f"{acc_no_dashes}/{accession_number}-index.htm"
    )
    try:
        resp = requests.get(index_url, headers=EDGAR_HEADERS, timeout=30)
        resp.raise_for_status()
        time.sleep(EDGAR_DELAY)
        # Look for infotable.xml link in index HTML
        text = resp.text.lower()
        # Try direct URL construction
        infotable_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
            f"{acc_no_dashes}/infotable.xml"
        )
        resp2 = requests.get(infotable_url, headers=EDGAR_HEADERS, timeout=60)
        if resp2.status_code == 200:
            time.sleep(EDGAR_DELAY)
            return resp2.text
    except Exception as exc:
        logger.warning(f"Could not fetch infotable.xml for CIK {cik}: {exc}")
    return None


def _parse_infotable(xml_text: str, institution_name: str, institution_cik: str, report_date: str) -> list[dict]:
    """
    Parse infotable.xml and return a list of holding row dicts.
    Handles both namespaced and non-namespaced XML.
    """
    rows: list[dict] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.error(f"XML parse error for {institution_name}: {exc}")
        return rows

    # Strip namespace from tags for uniform handling
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]

    for entry in root.iter("infoTable"):
        def _text(tag: str) -> Optional[str]:
            el = entry.find(tag)
            return el.text.strip() if el is not None and el.text else None

        def _float(tag: str) -> Optional[float]:
            v = _text(tag)
            if v is None:
                return None
            try:
                return float(v.replace(",", ""))
            except ValueError:
                return None

        cusip = _text("cusip")
        name_of_issuer = _text("nameOfIssuer")
        shares_val = _float("sshPrnamt")
        market_val = _float("value")  # in thousands per SEC format
        if market_val is not None:
            market_val *= 1000  # convert to dollars

        if cusip is None:
            continue

        rows.append({
            "cusip": cusip,
            "name_of_issuer": name_of_issuer,
            "institution_cik": institution_cik,
            "institution_name": institution_name,
            "report_date": report_date,
            "shares_held": shares_val,
            "market_value": market_val,
            "pct_of_portfolio": None,  # computed after full table is known
            "change_shares": None,
            "filed_date": None,
        })

    return rows


def _compute_pct_of_portfolio(rows: list[dict]) -> list[dict]:
    """Fill pct_of_portfolio based on total market_value across all holdings."""
    total_mv = sum(r["market_value"] for r in rows if r["market_value"] is not None)
    if total_mv > 0:
        for r in rows:
            if r["market_value"] is not None:
                r["pct_of_portfolio"] = r["market_value"] / total_mv * 100
    return rows


def _cusip_to_ticker_lookup(cusip: str, ticker_map: dict[str, str]) -> Optional[str]:
    """
    Attempt to resolve a CUSIP to a ticker.
    The company_tickers_exchange.json doesn't contain CUSIPs, so this is
    best-effort using any supplementary mapping stored in ticker_map.
    """
    return ticker_map.get(cusip)


def _upsert_holdings(engine: sa.Engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                """
                INSERT OR REPLACE INTO institutional_holdings
                    (ticker, institution_cik, report_date, institution_name,
                     filed_date, shares_held, market_value, pct_of_portfolio, change_shares)
                VALUES
                    (:ticker, :institution_cik, :report_date, :institution_name,
                     :filed_date, :shares_held, :market_value, :pct_of_portfolio, :change_shares)
                """
            ),
            rows,
        )
    return len(rows)


def download_13f_holdings(engine: Optional[sa.Engine] = None) -> tuple[int, list[str]]:
    """
    Fetch the most recent 13F-HR filing for each institution in
    TOP_INSTITUTION_CIKS, parse infotable.xml, and upsert holdings into DB.

    Returns (total_rows_upserted, failed_institution_names).
    """
    if engine is None:
        engine = get_engine()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ticker_map = _load_cusip_ticker_map()

    # Also build a ticker lookup from the DB tickers table for CUSIP resolution
    # (future: enhance with a proper CUSIP→ticker table)
    with engine.connect() as conn:
        db_tickers = {
            r[0]: r[0]
            for r in conn.execute(sa.text("SELECT ticker FROM tickers")).fetchall()
        }
    ticker_map.update(db_tickers)

    total_rows = 0
    failed: list[str] = []

    for institution_name, cik in TOP_INSTITUTION_CIKS.items():
        try:
            logger.info(f"Fetching 13F for {institution_name} (CIK {cik})...")
            submissions = _fetch_edgar_submissions(cik)
            filing_info = _find_latest_13f(submissions)

            if filing_info is None:
                logger.warning(f"No 13F-HR filing found for {institution_name}")
                continue

            acc_no = filing_info["accessionNumber"]
            report_date = filing_info.get("reportDate") or filing_info.get("filingDate")
            filed_date = filing_info.get("filingDate")

            logger.info(f"  Latest 13F: {acc_no} (report_date={report_date})")

            xml_text = _fetch_infotable_xml(cik, acc_no)
            if xml_text is None:
                logger.warning(f"  Could not retrieve infotable.xml for {institution_name}")
                failed.append(institution_name)
                continue

            parsed_rows = _parse_infotable(xml_text, institution_name, cik, report_date)
            if not parsed_rows:
                logger.warning(f"  No holdings parsed for {institution_name}")
                continue

            parsed_rows = _compute_pct_of_portfolio(parsed_rows)

            # Map CUSIPs to tickers and build DB rows
            db_rows: list[dict] = []
            for r in parsed_rows:
                cusip = r.pop("cusip", None)
                r.pop("name_of_issuer", None)
                ticker = _cusip_to_ticker_lookup(cusip, ticker_map) if cusip else None
                if ticker is None:
                    # Skip holdings where ticker cannot be resolved
                    continue
                r["ticker"] = ticker
                r["filed_date"] = filed_date
                db_rows.append(r)

            inserted = _upsert_holdings(engine, db_rows)
            total_rows += inserted
            logger.info(f"  {institution_name}: {inserted} holdings upserted")

        except Exception as exc:
            logger.error(f"Failed to process 13F for {institution_name}: {exc}")
            failed.append(institution_name)

    logger.info(f"download_13f_holdings: {total_rows} rows upserted, {len(failed)} failures")
    return total_rows, failed


def compute_institutional_summary(engine: Optional[sa.Engine] = None) -> int:
    """
    Aggregate institutional_holdings by ticker for the latest report_date
    per ticker and upsert into institutional_summary.

    Returns the number of rows upserted.
    """
    if engine is None:
        engine = get_engine()

    now = datetime.utcnow().isoformat()

    with engine.connect() as conn:
        # Get all tickers that have any holdings
        tickers = [
            r[0]
            for r in conn.execute(
                sa.text("SELECT DISTINCT ticker FROM institutional_holdings")
            ).fetchall()
        ]

    rows_upserted = 0
    with engine.begin() as conn:
        for ticker in tickers:
            # Find the latest report_date for this ticker
            latest = conn.execute(
                sa.text(
                    """
                    SELECT MAX(report_date) FROM institutional_holdings
                    WHERE ticker = :ticker
                    """
                ),
                {"ticker": ticker},
            ).fetchone()

            if not latest or latest[0] is None:
                continue

            latest_date = latest[0]

            agg = conn.execute(
                sa.text(
                    """
                    SELECT
                        COUNT(DISTINCT institution_cik)   AS total_institutions,
                        SUM(shares_held)                  AS total_shares_held,
                        SUM(change_shares)                AS net_change_shares,
                        institution_name                  AS top_holder_name,
                        MAX(pct_of_portfolio)             AS top_holder_pct
                    FROM institutional_holdings
                    WHERE ticker = :ticker
                      AND report_date = :report_date
                    """
                ),
                {"ticker": ticker, "report_date": latest_date},
            ).fetchone()

            if not agg:
                continue

            # Compute pct_outstanding_held
            with engine.connect() as meta_conn:
                meta_row = meta_conn.execute(
                    sa.text(
                        "SELECT shares_outstanding FROM security_metadata WHERE ticker = :ticker"
                    ),
                    {"ticker": ticker},
                ).fetchone()
            shares_outstanding = float(meta_row[0]) if meta_row and meta_row[0] else None
            total_shares_held = float(agg[1]) if agg[1] is not None else None
            pct_outstanding = (
                total_shares_held / shares_outstanding * 100
                if total_shares_held and shares_outstanding and shares_outstanding > 0
                else None
            )

            conn.execute(
                sa.text(
                    """
                    INSERT OR REPLACE INTO institutional_summary
                        (ticker, report_date, total_institutions, total_shares_held,
                         pct_outstanding_held, net_change_shares,
                         top_holder_name, top_holder_pct, updated_at)
                    VALUES
                        (:ticker, :report_date, :total_institutions, :total_shares_held,
                         :pct_outstanding_held, :net_change_shares,
                         :top_holder_name, :top_holder_pct, :updated_at)
                    """
                ),
                {
                    "ticker": ticker,
                    "report_date": latest_date,
                    "total_institutions": int(agg[0]) if agg[0] is not None else None,
                    "total_shares_held": total_shares_held,
                    "pct_outstanding_held": pct_outstanding,
                    "net_change_shares": float(agg[2]) if agg[2] is not None else None,
                    "top_holder_name": agg[3],
                    "top_holder_pct": float(agg[4]) if agg[4] is not None else None,
                    "updated_at": now,
                },
            )
            rows_upserted += 1

    logger.info(f"compute_institutional_summary: {rows_upserted} rows upserted")
    return rows_upserted
