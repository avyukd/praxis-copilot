"""EDGAR filing document fetcher."""
from __future__ import annotations

import logging
import re

from src.modules.events.eight_k_scanner.edgar.client import edgar_get

logger = logging.getLogger(__name__)

ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"


def fetch_filing(cik: str, accession_number: str) -> dict:
    """Download a filing's index and all its documents.

    Returns dict with:
        metadata: filing metadata from the index
        documents: {filename: bytes_content}
    """
    acc_nodashes = accession_number.replace("-", "")
    base_url = f"{ARCHIVES_BASE}/{cik}/{acc_nodashes}"

    index_url = f"{base_url}/{accession_number}-index.json"
    logger.info(f"Fetching index: {index_url}")

    try:
        resp = edgar_get(index_url)
        index_data = resp.json()
        return _fetch_via_json_index(cik, accession_number, base_url, index_data)
    except Exception:
        logger.info(f"JSON index not available for {accession_number}, using directory listing")

    return _fetch_via_directory(cik, accession_number, base_url)


def _fetch_via_json_index(cik: str, accession_number: str, base_url: str, index_data: dict) -> dict:
    directory = index_data.get("directory", {})
    items = directory.get("item", [])

    documents: dict[str, bytes] = {}
    primary_doc = None

    for item in items:
        name = item.get("name", "")
        doc_type = item.get("type", "")

        if not name or not _is_downloadable(name):
            continue

        doc_url = f"{base_url}/{name}"
        try:
            doc_resp = edgar_get(doc_url)
            documents[name] = doc_resp.content
            logger.info(f"  Downloaded: {name} ({doc_type})")

            if doc_type in ("8-K", "8-K/A"):
                primary_doc = name
        except Exception:
            logger.warning(f"  Failed to download: {name}")

    return _build_result(cik, accession_number, documents, primary_doc)


def _fetch_via_directory(cik: str, accession_number: str, base_url: str) -> dict:
    acc_nodashes = accession_number.replace("-", "")
    dir_url = f"{base_url}/"
    path_prefix = f"/Archives/edgar/data/{cik}/{acc_nodashes}/"

    resp = edgar_get(dir_url)
    html = resp.text

    hrefs = re.findall(r'href="([^"]+)"', html)
    documents: dict[str, bytes] = {}
    primary_doc = None

    for href in hrefs:
        if href.startswith(("#", "http", "?")):
            continue

        if href.startswith(path_prefix):
            filename = href[len(path_prefix):]
        elif not href.startswith("/") and not href.startswith("."):
            filename = href
        else:
            continue

        if not filename or not _is_downloadable(filename):
            continue

        doc_url = f"{base_url}/{filename}"
        try:
            doc_resp = edgar_get(doc_url)
            documents[filename] = doc_resp.content
            logger.info(f"  Downloaded: {filename}")

            if not primary_doc and filename.endswith((".htm", ".html")):
                if "8k" in filename.lower() or "8-k" in filename.lower() or not any(
                    x in filename.lower() for x in ("ex-", "ex_", "ex1", "ex2", "ex9", "r1.", "r2.")
                ):
                    primary_doc = filename
        except Exception:
            logger.warning(f"  Failed to download: {filename}")

    return _build_result(cik, accession_number, documents, primary_doc)


def _is_downloadable(filename: str) -> bool:
    if not filename or "#" in filename:
        return False
    if "." not in filename:
        return False

    lower = filename.lower()

    if "index" in lower:
        return False

    skip_names = {"filingsummary.xml", "metalinks.json", "report.css", "show.js"}
    if lower in skip_names:
        return False

    skip_ext = (".xsd", ".js", ".css", ".zip", ".jpg", ".png", ".gif", ".txt")
    if lower.endswith(skip_ext):
        return False

    if lower.endswith(".xml"):
        return False

    if re.match(r"^r\d+\.htm$", lower):
        return False

    if not lower.endswith((".htm", ".html", ".pdf")):
        return False

    return True


def _fetch_acceptance_datetime(cik: str, accession_number: str) -> str:
    acc_nodashes = accession_number.replace("-", "")
    header_url = f"{ARCHIVES_BASE}/{cik}/{acc_nodashes}/{accession_number}-index.htm"
    try:
        resp = edgar_get(header_url)
        text = resp.text

        match = re.search(r"ACCEPTANCE-DATETIME[>\s]*(\d{14})", text)
        if match:
            return match.group(1)

        match = re.search(r"ACCEPTANCE-DATETIME[>\s]*([\d\-T:\.Z+]+)", text)
        if match:
            return match.group(1).strip()

        match = re.search(r"Accepted\s*</div>\s*<div[^>]*>\s*(20\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", text)
        if match:
            return match.group(1).strip()

        logger.warning(f"Could not parse acceptance datetime from {header_url}")
    except Exception as e:
        logger.warning(f"Could not fetch acceptance datetime for {accession_number}: {e}")
    return ""


def _build_result(cik: str, accession_number: str, documents: dict, primary_doc: str | None) -> dict:
    items_detected: list[str] = []
    if primary_doc and primary_doc in documents:
        items_detected = _detect_items(documents[primary_doc])

    acceptance_datetime = _fetch_acceptance_datetime(cik, accession_number)

    metadata = {
        "cik": cik,
        "accession_number": accession_number,
        "primary_doc": primary_doc,
        "items_detected": items_detected,
        "acceptance_datetime": acceptance_datetime,
        "exhibit_manifest": [
            {"filename": name, "type": _classify_exhibit(name)}
            for name in documents
            if name != primary_doc
        ],
    }

    return {"metadata": metadata, "documents": documents}


def _detect_items(content: bytes | str) -> list[str]:
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")
    pattern = r"Item\s+(\d+\.\d+)"
    matches = re.findall(pattern, content, re.IGNORECASE)
    return sorted(set(matches))


def _classify_exhibit(filename: str) -> str:
    name_lower = filename.lower()
    if "ex-99" in name_lower or "ex99" in name_lower:
        return "press_release"
    if "ex-10" in name_lower or "ex10" in name_lower:
        return "material_agreement"
    if "ex-2" in name_lower or "ex2" in name_lower:
        return "plan_of_acquisition"
    if name_lower.endswith((".jpg", ".png", ".gif")):
        return "image"
    if name_lower.endswith(".xml"):
        return "xbrl"
    return "other"
