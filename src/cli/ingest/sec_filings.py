"""SEC filing ingestion — pulls 10-K and 10-Q filings from EDGAR and extracts narrative sections."""

import logging
import re
import time
import warnings

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from cli.edgar import _session as sec_session
from cli.ingest.models import FilingSection

logger = logging.getLogger(__name__)

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"

# Sections to extract from 10-K
TENK_SECTIONS = {
    "item1_business": r"item\s*1[\.\s:\-]*business",
    "item1a_risk_factors": r"item\s*1a[\.\s:\-]*risk\s*factors",
    "item7_mda": r"item\s*7[\.\s:\-]*management.{0,5}s?\s*discussion\s*and\s*analysis",
    "item7a_market_risk": r"item\s*7a[\.\s:\-]*quantitative\s*and\s*qualitative",
}

# Sections to extract from 10-Q
TENQ_SECTIONS = {
    "item2_mda": r"(?:part\s*i[,.\s]*)?item\s*2[\.\s:\-]*management.{0,5}s?\s*discussion\s*and\s*analysis",
    "item1a_risk_factors": r"(?:part\s*i[,.\s]*)?item\s*1a[\.\s:\-]*risk\s*factors",
}

# All possible section headers for finding boundaries
ALL_ITEM_PATTERNS = [
    r"item\s*1a",
    r"item\s*1b",
    r"item\s*1c",
    r"item\s*1[\.\s]",
    r"item\s*2",
    r"item\s*3",
    r"item\s*4",
    r"item\s*5",
    r"item\s*6",
    r"item\s*7a",
    r"item\s*7",
    r"item\s*8",
    r"item\s*9a",
    r"item\s*9b",
    r"item\s*9",
    r"item\s*10",
    r"item\s*11",
    r"item\s*12",
    r"item\s*13",
    r"item\s*14",
    r"item\s*15",
    r"part\s*i{1,3}",
    r"signatures",
]

# Rate limit: SEC allows 10 req/sec, we'll be conservative
_last_request_time = 0.0
_min_interval = 0.15


def _rate_limit():
    """Enforce rate limiting for SEC requests."""
    global _last_request_time
    now = time.monotonic()
    elapsed = now - _last_request_time
    if elapsed < _min_interval:
        time.sleep(_min_interval - elapsed)
    _last_request_time = time.monotonic()


def _sec_get(url: str) -> requests.Response:
    """Make a rate-limited GET request to SEC."""
    _rate_limit()
    resp = sec_session.get(url, timeout=30)
    resp.raise_for_status()
    return resp


def get_filing_urls(cik: str, filing_type: str, count: int) -> list[dict]:
    """Get recent filing URLs from EDGAR submissions API.

    Returns list of dicts with keys: accession, period, primary_doc_url
    """
    cik_padded = cik.lstrip("0").zfill(10)
    url = SUBMISSIONS_URL.format(cik=cik_padded)

    try:
        resp = _sec_get(url)
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        logger.warning(f"Failed to fetch submissions for CIK {cik}: {e}")
        return []

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])
    dates = recent.get("reportDate", [])

    results = []
    cik_num = cik.lstrip("0")

    for i, form in enumerate(forms):
        if form != filing_type:
            continue
        if len(results) >= count:
            break

        accession = accessions[i]
        accession_nodash = accession.replace("-", "")
        primary_doc = primary_docs[i]
        period = dates[i] if i < len(dates) else "unknown"

        doc_url = f"{ARCHIVES_BASE}/{cik_num}/{accession_nodash}/{primary_doc}"
        results.append({
            "accession": accession,
            "period": period,
            "primary_doc_url": doc_url,
        })

    return results


def _clean_text(html_text: str) -> str:
    """Strip HTML tags and clean whitespace from extracted text."""
    soup = BeautifulSoup(html_text, "lxml")

    # Remove script and style elements
    for element in soup(["script", "style"]):
        element.decompose()

    text = soup.get_text(separator="\n")

    # Clean up whitespace
    lines = []
    for line in text.splitlines():
        cleaned = line.strip()
        if cleaned:
            lines.append(cleaned)

    text = "\n".join(lines)

    # Collapse runs of 3+ newlines into 2
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()


def _extract_sections_from_html(html: str, section_patterns: dict[str, str]) -> dict[str, str]:
    """Extract named sections from an SEC filing HTML document.

    Uses regex to find section header boundaries and extract text between them.
    """
    # First pass: get full text with position tracking
    soup = BeautifulSoup(html, "lxml")

    # Remove tables to avoid financial data noise
    for table in soup.find_all("table"):
        table.decompose()

    # Get text content
    full_text = soup.get_text(separator="\n")

    results = {}

    for section_name, pattern in section_patterns.items():
        try:
            extracted = _extract_section(full_text, pattern)
            if extracted and len(extracted.strip()) > 200:
                results[section_name] = extracted.strip()
            else:
                logger.debug(f"Section {section_name} too short or not found")
        except Exception as e:
            logger.debug(f"Failed to extract {section_name}: {e}")

    return results


def _extract_section(text: str, start_pattern: str) -> str | None:
    """Extract a section from text by finding its header and the next section header."""
    # Find the start of this section
    match = re.search(start_pattern, text, re.IGNORECASE)
    if not match:
        return None

    start_pos = match.start()

    # Find the start of the next section after this one
    # Look for any item/part header that comes after our section
    best_end = len(text)
    search_from = match.end() + 100  # skip at least 100 chars past the header

    for next_pattern in ALL_ITEM_PATTERNS:
        # Skip the pattern that matches our own section
        if re.search(next_pattern, match.group(0), re.IGNORECASE):
            continue

        # Find occurrences of the next section pattern
        for next_match in re.finditer(next_pattern, text[search_from:], re.IGNORECASE):
            candidate_pos = search_from + next_match.start()

            # Verify this looks like a real section header (often on its own line or bold)
            # Check that the text around it looks like a header
            line_start = text.rfind("\n", 0, candidate_pos)
            if line_start == -1:
                line_start = 0
            prefix = text[line_start:candidate_pos].strip()

            # Real headers typically have little text before them on the line
            if len(prefix) < 30 and candidate_pos < best_end:
                best_end = candidate_pos
            break

    section_text = text[start_pos:best_end]

    # Clean up: remove the header line itself from the content
    lines = section_text.split("\n")
    # Skip first few lines that are the header
    content_lines = []
    past_header = False
    for line in lines:
        if not past_header:
            if re.search(start_pattern, line, re.IGNORECASE):
                past_header = True
                continue
        if past_header:
            content_lines.append(line)

    result = "\n".join(content_lines).strip()

    # Clean whitespace
    result = re.sub(r"\n{3,}", "\n\n", result)
    result = re.sub(r"[ \t]+", " ", result)

    return result


def ingest_sec_filings(cik: str, ticker: str) -> list[FilingSection]:
    """Pull SEC filings and extract narrative sections.

    Returns list of FilingSection objects.
    """
    all_sections: list[FilingSection] = []

    # Pull most recent 10-K
    logger.info(f"Fetching 10-K filings for {ticker} (CIK: {cik})")
    tenk_filings = get_filing_urls(cik, "10-K", count=1)

    for filing in tenk_filings:
        try:
            logger.info(f"  Fetching 10-K for period {filing['period']}")
            resp = _sec_get(filing["primary_doc_url"])
            html = resp.text

            sections = _extract_sections_from_html(html, TENK_SECTIONS)
            for section_name, text in sections.items():
                all_sections.append(FilingSection(
                    filing_type="10-K",
                    period=filing["period"],
                    section_name=section_name,
                    text=text,
                ))
            logger.info(f"  Extracted {len(sections)} sections from 10-K")
        except requests.RequestException as e:
            logger.warning(f"  Failed to fetch 10-K document: {e}")

    # Pull last 4 10-Qs
    logger.info(f"Fetching 10-Q filings for {ticker} (CIK: {cik})")
    tenq_filings = get_filing_urls(cik, "10-Q", count=4)

    for filing in tenq_filings:
        try:
            logger.info(f"  Fetching 10-Q for period {filing['period']}")
            resp = _sec_get(filing["primary_doc_url"])
            html = resp.text

            sections = _extract_sections_from_html(html, TENQ_SECTIONS)
            for section_name, text in sections.items():
                all_sections.append(FilingSection(
                    filing_type="10-Q",
                    period=filing["period"],
                    section_name=section_name,
                    text=text,
                ))
            logger.info(f"  Extracted {len(sections)} sections from 10-Q ({filing['period']})")
        except requests.RequestException as e:
            logger.warning(f"  Failed to fetch 10-Q document: {e}")

    return all_sections
