"""Extractor Lambda: S3 PUT on index.json -> deterministic HTML parse + item split.

Data Plane only -- no LLM, no judgment. Produces extracted.json.
Triggered by S3 PutObject on:
  - data/raw/8k/{cik}/{accession}/index.json  (legacy 8-K path)
  - data/raw/filings/{cik}/{accession}/index.json  (unified filing path)
"""
from __future__ import annotations

import logging

from src.modules.events.eight_k_scanner.config import S3_BUCKET
from src.modules.events.eight_k_scanner.extract.parser import extract_filing, extract_generic_filing
from src.modules.events.eight_k_scanner.storage.s3 import read_json_from_s3

logger = logging.getLogger(__name__)


def lambda_handler(event, context=None):
    """Handle S3 PutObject events for new filing index.json files.

    Extracts text from raw HTML, splits by item, writes extracted.json.
    Supports both legacy data/raw/8k/ and new data/raw/filings/ paths.
    """
    logging.basicConfig(level=logging.INFO)

    records = event.get("Records", [])
    results = []

    for record in records:
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", S3_BUCKET)
        key = s3_info.get("object", {}).get("key", "")

        if not key.endswith("/index.json"):
            logger.debug(f"Ignoring non-index key: {key}")
            continue

        parts = key.split("/")

        # Legacy path: data/raw/8k/{cik}/{accession}/index.json
        if "8k" in parts:
            try:
                raw_idx = parts.index("8k")
                cik = parts[raw_idx + 1]
                accession = parts[raw_idx + 2]
            except (ValueError, IndexError):
                logger.warning(f"Cannot parse cik/accession from key: {key}")
                continue
            result = _extract_one(bucket, cik, accession, form_type="8-K")
            results.append(result)

        # Unified path: data/raw/filings/{cik}/{accession}/index.json
        elif "filings" in parts:
            try:
                raw_idx = parts.index("filings")
                cik = parts[raw_idx + 1]
                accession = parts[raw_idx + 2]
            except (ValueError, IndexError):
                logger.warning(f"Cannot parse cik/accession from key: {key}")
                continue
            # Read index.json to determine form type
            try:
                index_data = read_json_from_s3(bucket, key)
                form_type = index_data.get("form_type", "")
            except Exception:
                form_type = ""
            result = _extract_one(
                bucket, cik, accession, form_type=form_type, prefix="data/raw/filings",
            )
            results.append(result)

        else:
            logger.warning(f"Cannot determine filing type from key: {key}")

    return {"processed": len(results), "results": results}


def _extract_one(
    bucket: str,
    cik: str,
    accession: str,
    form_type: str = "",
    prefix: str | None = None,
) -> dict:
    """Run extraction for a single filing."""
    status = {"cik": cik, "accession": accession, "form_type": form_type, "action": "skipped"}

    try:
        # 8-K gets the detailed item/exhibit parser
        if form_type in ("8-K", "8-K/A") or prefix is None:
            extracted = extract_filing(cik, accession, bucket=bucket)
        else:
            # 10-K, 10-Q, etc. get generic text extraction
            extracted = extract_generic_filing(
                cik, accession, bucket=bucket, prefix=prefix, form_type=form_type,
            )

        if extracted is None:
            status["action"] = "already_extracted"
        else:
            status["action"] = "extracted"
            status["total_chars"] = extracted.total_chars
            if hasattr(extracted, "items"):
                status["items"] = list(extracted.items.keys())
    except Exception:
        logger.exception(f"Extraction failed for {cik}/{accession}")
        status["action"] = "error"
        status["reason"] = "extraction_failed"

    return status
