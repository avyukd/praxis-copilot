"""Pull recent 10-K and 10-Q filings from EDGAR and store to S3."""
from __future__ import annotations

import logging
import time

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

SEC_USER_AGENT = "PraxisCopilot/0.1 (research-tool)"
EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"
S3_BUCKET = "praxis-copilot"

FILING_TYPES = ["10-K", "10-Q"]
MAX_FILINGS_PER_TYPE = 4

_last_request_time = 0.0
_MIN_INTERVAL = 0.11  # SEC rate limit: ~10 req/s, we stay conservative


def _rate_limit():
    global _last_request_time
    now = time.monotonic()
    elapsed = now - _last_request_time
    if elapsed < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - elapsed)
    _last_request_time = time.monotonic()


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": SEC_USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
    })
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


_session = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = _build_session()
    return _session


def _edgar_get(url: str, **kwargs) -> requests.Response:
    _rate_limit()
    resp = _get_session().get(url, timeout=30, **kwargs)
    resp.raise_for_status()
    return resp


def _search_filings(cik: str, filing_type: str, count: int) -> list[dict]:
    """Search EDGAR EFTS for recent filings of a given type."""
    # Strip leading zeros for the search query but keep padded CIK for archives
    cik_int = str(int(cik))
    url = EDGAR_SEARCH_URL
    params = {
        "q": f'"{filing_type}"',
        "dateRange": "custom",
        "startdt": "2020-01-01",
        "enddt": "2030-12-31",
        "forms": filing_type,
        "from": 0,
        "size": count,
    }

    # Try EFTS first
    try:
        resp = _edgar_get(url, params=params)
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        results = []
        for hit in hits:
            source = hit.get("_source", {})
            hit_cik = str(source.get("entity_id", ""))
            if hit_cik == cik_int or hit_cik == cik:
                results.append({
                    "accession_number": source.get("adsh", "").replace("-", ""),
                    "accession_display": source.get("adsh", ""),
                    "filing_type": source.get("forms", filing_type),
                    "filed_date": source.get("file_date", ""),
                })
        if results:
            return results[:count]
    except Exception:
        logger.info(f"EFTS search unavailable, falling back to submissions API")

    # Fallback: use submissions API
    return _search_via_submissions(cik, filing_type, count)


def _search_via_submissions(cik: str, filing_type: str, count: int) -> list[dict]:
    """Fallback: use the submissions API to find recent filings."""
    padded_cik = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{padded_cik}.json"
    try:
        resp = _edgar_get(url)
        data = resp.json()
    except Exception:
        logger.warning(f"Could not fetch submissions for CIK {cik}")
        return []

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    dates = recent.get("filingDate", [])
    primary_docs = recent.get("primaryDocument", [])

    results = []
    for i, form in enumerate(forms):
        if form == filing_type and len(results) < count:
            acc = accessions[i] if i < len(accessions) else ""
            results.append({
                "accession_number": acc.replace("-", ""),
                "accession_display": acc,
                "filing_type": form,
                "filed_date": dates[i] if i < len(dates) else "",
                "primary_document": primary_docs[i] if i < len(primary_docs) else "",
            })
    return results


def _fetch_filing_document(cik: str, accession_nodashes: str, accession_display: str) -> dict[str, bytes]:
    """Download the primary filing document(s) for a given accession."""
    padded_cik = str(int(cik))  # SEC archives use unpadded CIK
    base_url = f"{EDGAR_ARCHIVES_BASE}/{padded_cik}/{accession_nodashes}"

    # Try the JSON index first
    index_url = f"{base_url}/{accession_display}-index.json"
    documents: dict[str, bytes] = {}

    try:
        resp = _edgar_get(index_url)
        index_data = resp.json()
        items = index_data.get("directory", {}).get("item", [])

        for item in items:
            name = item.get("name", "")
            if not name:
                continue
            # Download HTML/HTM primary docs, skip images/xml/js/css
            lower = name.lower()
            if lower.endswith((".htm", ".html")):
                doc_url = f"{base_url}/{name}"
                try:
                    doc_resp = _edgar_get(doc_url)
                    documents[name] = doc_resp.content
                    logger.info(f"  Downloaded: {name}")
                except Exception:
                    logger.warning(f"  Failed to download: {name}")
        return documents
    except Exception:
        logger.info(f"JSON index unavailable for {accession_display}, trying primary doc")

    # Fallback: try to fetch the primary document directly if we know its name
    # Try common naming patterns
    for suffix in [f"{accession_display}.htm", f"{accession_display}-index.htm"]:
        try:
            url = f"{base_url}/{suffix}"
            resp = _edgar_get(url)
            documents[suffix] = resp.content
            return documents
        except Exception:
            continue

    logger.warning(f"Could not fetch any documents for {accession_display}")
    return documents


def ingest_filings(ticker: str, cik: str, s3_client=None) -> dict:
    """Pull recent 10-K and 10-Q filings from EDGAR and store to S3.

    Returns summary dict with counts of filings stored.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    s3_prefix = f"data/research/{ticker}/data/filings"
    total_stored = 0
    errors = 0

    for filing_type in FILING_TYPES:
        logger.info(f"Searching for {filing_type} filings for {ticker} (CIK: {cik})")

        try:
            filings = _search_filings(cik, filing_type, MAX_FILINGS_PER_TYPE)
        except Exception:
            logger.exception(f"Failed to search for {filing_type} filings")
            errors += 1
            continue

        logger.info(f"Found {len(filings)} {filing_type} filing(s)")

        for filing in filings:
            acc_nodashes = filing["accession_number"]
            acc_display = filing.get("accession_display", acc_nodashes)
            filed_date = filing.get("filed_date", "unknown")

            try:
                documents = _fetch_filing_document(cik, acc_nodashes, acc_display)
                if not documents:
                    logger.warning(f"No documents fetched for {acc_display}")
                    continue

                # Store each document to S3
                filing_prefix = f"{s3_prefix}/{filing_type}/{acc_display}"
                for filename, content in documents.items():
                    s3_key = f"{filing_prefix}/{filename}"
                    s3_client.put_object(
                        Bucket=S3_BUCKET,
                        Key=s3_key,
                        Body=content,
                        ContentType="text/html",
                    )
                    logger.info(f"Stored {s3_key}")

                # Store metadata
                import json
                meta = {
                    "ticker": ticker,
                    "cik": cik,
                    "filing_type": filing_type,
                    "accession_number": acc_display,
                    "filed_date": filed_date,
                    "documents": list(documents.keys()),
                }
                s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=f"{filing_prefix}/metadata.json",
                    Body=json.dumps(meta, indent=2),
                    ContentType="application/json",
                )
                total_stored += 1

            except Exception:
                logger.exception(f"Failed to fetch/store filing {acc_display}")
                errors += 1

    return {
        "filings_stored": total_stored,
        "errors": errors,
    }
