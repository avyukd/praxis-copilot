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
    "item1c_cybersecurity": r"item\s*1c[\.\s:\-]*cyber\s*security",
    "item3_legal_proceedings": r"item\s*3[\.\s:\-]*legal\s*proceedings",
    "item5_equity": r"item\s*5[\.\s:\-]*market\s*for\s*(?:the\s*)?registrant",
    "item7_mda": r"item\s*7[\.\s:\-]*management.{0,5}s?\s*discussion\s*and\s*analysis",
    "item7a_market_risk": r"item\s*7a[\.\s:\-]*quantitative\s*and\s*qualitative",
    "item9a_controls": r"item\s*9a[\.\s:\-]*controls\s*and\s*procedures",
    "item11_exec_comp": r"item\s*11[\.\s:\-]*executive\s*compensation",
}

# Sections to extract from 10-Q
TENQ_SECTIONS = {
    "item2_mda": r"(?:part\s*i[,.\s]*)?item\s*2[\.\s:\-]*management.{0,5}s?\s*discussion\s*and\s*analysis",
    "item1a_risk_factors": r"(?:part\s*i[i,.\s]*)?item\s*1a[\.\s:\-]*risk\s*factors",
    "part2_item1_legal": r"part\s*ii[,.\s]*item\s*1[\.\s:\-]*legal\s*proceedings",
}

# Notes to financial statements we want to extract (by topic keyword in note title)
# Note headers vary: "Note X - Title", "Note X: Title", "Note X\nTitle", etc.
# We use [\s\S]{0,30} to bridge the gap between "Note N" and the title keyword
_NP = r"note\s*\d+[\s\S]{0,30}"  # note prefix — matches "Note N - \n" style headers
NOTES_OF_INTEREST = {
    "note_revenue": _NP + r"revenue",
    "note_segments": _NP + r"(?:segment|reportable)",
    "note_debt": _NP + r"(?:(?:long[\-\s]term\s*)?debt|borrowing|credit\s*(?:facility|agreement))",
    "note_acquisitions": _NP + r"(?:acquisition|business\s*combination)",
    "note_goodwill": _NP + r"(?:goodwill|intangible)",
    "note_commitments": _NP + r"(?:commitment|contingenc)",
    "note_stock_comp": _NP + r"(?:stock[\-\s]*based\s*comp|share[\-\s]*based|equity\s*comp)",
    "note_leases": _NP + r"lease",
    "note_income_tax": _NP + r"income\s*tax",
    "note_subsequent": _NP + r"subsequent\s*event",
    "note_accounting_policies": _NP + r"(?:(?:summary\s*of\s*)?significant\s*)?accounting\s*polic",
    "note_derivatives": _NP + r"(?:derivative|financial\s*instrument)",
    "note_equity": _NP + r"(?:shareholders|stockholders).{0,5}\s*equity",
    "note_securities": _NP + r"(?:marketable\s*securities|cash\s*equivalents?\s*and\s*marketable)",
}

# Sections to extract from S-1 (IPO/spin-off prospectus)
S1_SECTIONS = {
    "prospectus_summary": r"prospectus\s*summary",
    "risk_factors": r"risk\s*factors",
    "use_of_proceeds": r"use\s*of\s*proceeds",
    "business": r"(?:our\s+)?business",
    "mda": r"management.{0,5}s?\s*discussion\s*and\s*analysis",
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


def _extract_notes_from_html(html: str) -> dict[str, str]:
    """Extract specific notes to financial statements from filing HTML.

    Unlike regular section extraction, this:
    - Searches within Item 8 (or Part I Item 1 for 10-Q)
    - Uses "Note N:" boundaries instead of "Item N:"
    - Keeps tables (segment data, debt schedules are in tables)
    """
    soup = BeautifulSoup(html, "lxml")

    # Remove scripts/styles but keep tables
    for element in soup(["script", "style"]):
        element.decompose()

    full_text = soup.get_text(separator="\n")

    # Build list of all note positions for boundary detection
    note_header_pattern = r"(?:^|\n)\s*(?:note|notes)\s+(\d+)[\.\s:\-]"
    note_positions = []
    for m in re.finditer(note_header_pattern, full_text, re.IGNORECASE):
        note_positions.append(m.start())
    # Add end-of-notes markers
    for end_pattern in [r"item\s*9", r"part\s*ii", r"signatures"]:
        m = re.search(end_pattern, full_text[len(full_text) // 3:], re.IGNORECASE)
        if m:
            note_positions.append(len(full_text) // 3 + m.start())

    note_positions.sort()

    results = {}
    for note_name, pattern in NOTES_OF_INTEREST.items():
        match = re.search(pattern, full_text, re.IGNORECASE)
        if not match:
            continue

        start_pos = match.start()
        # Find the next note boundary after this one
        end_pos = len(full_text)
        for pos in note_positions:
            if pos > start_pos + 200:
                end_pos = pos
                break

        text = full_text[start_pos:end_pos].strip()
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)

        if len(text) > 200:
            results[note_name] = text

    return results


def ingest_sec_filings(cik: str, ticker: str) -> list[FilingSection]:
    """Pull SEC filings and extract narrative sections.

    Returns list of FilingSection objects.
    """
    all_sections: list[FilingSection] = []

    # Pull last 2 years of 10-Ks (with notes to financial statements)
    logger.info(f"Fetching 10-K filings for {ticker} (CIK: {cik})")
    tenk_filings = get_filing_urls(cik, "10-K", count=2)
    all_sections.extend(_fetch_filing_sections(tenk_filings, "10-K", TENK_SECTIONS, extract_notes=True))

    # Pull last 8 10-Qs (with notes for quarterly updates)
    logger.info(f"Fetching 10-Q filings for {ticker} (CIK: {cik})")
    tenq_filings = get_filing_urls(cik, "10-Q", count=8)
    all_sections.extend(_fetch_filing_sections(tenq_filings, "10-Q", TENQ_SECTIONS, extract_notes=True))

    # Pull S-1 for recent IPOs/spinoffs (best-effort)
    logger.info(f"Checking for S-1 filings for {ticker} (CIK: {cik})")
    s1_filings = get_filing_urls(cik, "S-1", count=1)
    if s1_filings:
        all_sections.extend(_fetch_filing_sections(s1_filings, "S-1", S1_SECTIONS))
    else:
        # Also check S-1/A (amended S-1)
        s1a_filings = get_filing_urls(cik, "S-1/A", count=1)
        if s1a_filings:
            all_sections.extend(_fetch_filing_sections(s1a_filings, "S-1", S1_SECTIONS))

    return all_sections


def _fetch_filing_sections(
    filings: list[dict], filing_type: str, section_patterns: dict[str, str],
    extract_notes: bool = False,
) -> list[FilingSection]:
    """Fetch and extract sections from a list of filings."""
    sections: list[FilingSection] = []
    for filing in filings:
        try:
            logger.info(f"  Fetching {filing_type} for period {filing['period']}")
            resp = _sec_get(filing["primary_doc_url"])
            html = resp.text

            # Extract regular item-level sections
            extracted = _extract_sections_from_html(html, section_patterns)
            for section_name, text in extracted.items():
                sections.append(FilingSection(
                    filing_type=filing_type,
                    period=filing["period"],
                    section_name=section_name,
                    text=text,
                ))

            # Extract notes to financial statements
            if extract_notes:
                notes = _extract_notes_from_html(html)
                for note_name, text in notes.items():
                    sections.append(FilingSection(
                        filing_type=filing_type,
                        period=filing["period"],
                        section_name=note_name,
                        text=text,
                    ))
                if notes:
                    logger.info(f"  Extracted {len(notes)} notes from {filing_type} ({filing['period']})")

            logger.info(f"  Extracted {len(extracted)} sections from {filing_type} ({filing['period']})")
        except requests.RequestException as e:
            logger.warning(f"  Failed to fetch {filing_type} document: {e}")
    return sections
