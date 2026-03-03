"""Extractor Lambda: S3 PUT on index.json -> deterministic HTML parse + item split.

Data Plane only -- no LLM, no judgment. Produces extracted.json.
Triggered by S3 PutObject on data/raw/8k/{cik}/{accession}/index.json.
"""
from __future__ import annotations

import logging

from src.modules.events.eight_k_scanner.config import S3_BUCKET, S3_RAW_PREFIX
from src.modules.events.eight_k_scanner.extract.parser import extract_filing
from src.modules.events.eight_k_scanner.storage.s3 import read_json_from_s3

logger = logging.getLogger(__name__)


def lambda_handler(event, context=None):
    """Handle S3 PutObject events for new filing index.json files.

    Extracts text from raw HTML, splits by item, writes extracted.json.
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

        # Parse cik and accession from key: data/raw/8k/{cik}/{accession}/index.json
        parts = key.split("/")
        try:
            raw_idx = parts.index("8k")
            cik = parts[raw_idx + 1]
            accession = parts[raw_idx + 2]
        except (ValueError, IndexError):
            logger.warning(f"Cannot parse cik/accession from key: {key}")
            continue

        result = _extract_one(bucket, cik, accession)
        results.append(result)

    return {"processed": len(results), "results": results}


def _extract_one(bucket: str, cik: str, accession: str) -> dict:
    """Run extraction for a single filing."""
    status = {"cik": cik, "accession": accession, "action": "skipped"}

    try:
        extracted = extract_filing(cik, accession, bucket=bucket)
        if extracted is None:
            status["action"] = "already_extracted"
        else:
            status["action"] = "extracted"
            status["total_chars"] = extracted.total_chars
            status["items"] = list(extracted.items.keys())
    except Exception:
        logger.exception(f"Extraction failed for {cik}/{accession}")
        status["action"] = "error"
        status["reason"] = "extraction_failed"

    return status
