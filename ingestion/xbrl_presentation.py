"""
Fetch and parse the XBRL presentation linkbase from SEC EDGAR filing archives.
Identifies which concepts appear in primary financial statement roles vs disclosures.
"""
import logging
import re
import time
import xml.etree.ElementTree as ET
from typing import Optional
import requests

logger = logging.getLogger(__name__)

USER_AGENT = "needlstack/1.0 (financial data pipeline; contact@example.com)"
_XLINK = "http://www.w3.org/1999/xlink"

# Role URI keyword → statement type (matched against lowercased, stripped role URI)
_INCOME_KEYS  = ("incomestatement", "statementofoperations", "operationsandcomprehensive",
                 "statementsofoperations", "consolidatedstatementofoperations",
                 "earningsstatement", "profitloss")
_BALANCE_KEYS = ("balancesheet", "financialposition", "statementoffinancialposition",
                 "consolidatedbalancesheet")
_CF_KEYS      = ("cashflow", "cashflows", "statementofcashflows",
                 "consolidatedstatementofcashflows")
_SKIP_KEYS    = ("note", "disclosure", "policy", "parenthetical", "detail",
                 "supplemental", "schedule", "comprehensive")


class PresentationLinkbase:
    """
    Per-run singleton. Loads the presentation linkbase for a CIK from EDGAR,
    returns {concept_name: statement_type} for concepts on primary statements.

    Falls back to {} on any error — callers must handle empty dict gracefully.
    """

    def __init__(self, rate_limit: float = 0.15):
        self._sleep = rate_limit          # seconds between EDGAR requests
        self._cache: dict = {}            # cik → {concept: stmt_type}

    def get_statement_concepts(self, cik: int) -> dict:
        if cik in self._cache:
            return self._cache[cik]
        try:
            result = self._load(cik)
        except Exception as exc:
            logger.warning("PresentationLinkbase failed for CIK %s: %s", cik, exc)
            result = {}
        self._cache[cik] = result
        logger.debug("CIK %s: %d statement concepts loaded", cik, len(result))
        return result

    # ── private ──────────────────────────────────────────────────────────────

    def _get(self, url: str, **kw) -> requests.Response:
        time.sleep(self._sleep)
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30, **kw)
        r.raise_for_status()
        return r

    def _load(self, cik: int) -> dict:
        # 1. Submissions → most recent 10-K accession number
        sub = self._get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json").json()
        accn = self._find_accession(sub, "10-K") or self._find_accession(sub, "10-Q")
        if not accn:
            return {}

        # 2. Filing directory listing (HTML) → extract _pre.xml filename
        # Note: www.sec.gov serves the HTML directory listing; data.sec.gov does not
        accn_nd = accn.replace("-", "")
        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accn_nd}"
        html = self._get(f"{base_url}/").text
        m = re.search(r'href="[^"]*?/([^"/]+(?:_pre|-pre)\.xml)"', html, re.IGNORECASE)
        if not m:
            return {}
        pre_file = m.group(1)

        # 3. Fetch and parse _pre.xml
        xml_bytes = self._get(f"{base_url}/{pre_file}").content
        return self._parse(xml_bytes)

    @staticmethod
    def _find_accession(sub: dict, form: str) -> Optional[str]:
        recent = sub.get("filings", {}).get("recent", {})
        for f, a in zip(recent.get("form", []), recent.get("accessionNumber", [])):
            if f == form:
                return a
        return None

    @staticmethod
    def _classify_role(role_uri: str) -> Optional[str]:
        r = role_uri.lower().replace("-", "").replace("_", "").replace("/", "")
        if any(k in r for k in _SKIP_KEYS):
            return None
        if any(k in r for k in _INCOME_KEYS):
            return "income"
        if any(k in r for k in _BALANCE_KEYS):
            return "balance"
        if any(k in r for k in _CF_KEYS):
            return "cashflow"
        return None

    def _parse(self, xml_bytes: bytes) -> dict:
        root = ET.fromstring(xml_bytes)
        result: dict = {}

        for elem in root.iter():
            tag = elem.tag
            if not (tag.endswith("}presentationLink") or tag == "presentationLink"):
                continue

            role = elem.get(f"{{{_XLINK}}}role") or elem.get("role", "")
            stmt_type = self._classify_role(role)
            if stmt_type is None:
                continue

            # Collect loc label → concept_name from href fragment
            locs: dict = {}
            for child in elem:
                if child.tag.endswith("}loc") or child.tag == "loc":
                    label = child.get(f"{{{_XLINK}}}label", "")
                    href  = child.get(f"{{{_XLINK}}}href", "")
                    if "#" in href:
                        concept = href.split("#")[-1]
                        locs[label] = concept

            for concept in locs.values():
                if concept not in result:       # first statement type wins
                    result[concept] = stmt_type

        return result
