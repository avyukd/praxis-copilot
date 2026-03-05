"""Extractor Lambda: S3 PUT index.json -> extracted.json (deterministic only).

Supports canonical paths:
  - data/raw/filings/{cik}/{accession}/index.json
  - data/raw/press_releases/{source}/{ticker}/{release_id}/index.json
"""
from __future__ import annotations

import logging

from src.modules.events.eight_k_scanner.config import S3_BUCKET
from src.modules.events.eight_k_scanner.extract.parser import extract_filing, extract_generic_filing
from src.modules.events.eight_k_scanner.storage.s3 import get_s3_client, read_json_from_s3, write_json_to_s3

logger = logging.getLogger(__name__)


def lambda_handler(event, context=None):
    """Handle S3 PutObject events for new index.json objects."""
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

        # Canonical SEC filings path: data/raw/filings/{cik}/{accession}/index.json
        if "filings" in parts:
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

        # Canonical press releases path: data/raw/press_releases/{source}/{ticker}/{release_id}/index.json
        elif "press_releases" in parts:
            try:
                raw_idx = parts.index("press_releases")
                source = parts[raw_idx + 1]
                ticker = parts[raw_idx + 2]
                release_id = parts[raw_idx + 3]
            except (ValueError, IndexError):
                logger.warning(f"Cannot parse source/ticker/release_id from key: {key}")
                continue
            result = _extract_press_release_one(bucket, source, ticker, release_id)
            results.append(result)

        # Legacy 8-K path support for residual objects (not canonical)
        elif "8k" in parts:
            try:
                raw_idx = parts.index("8k")
                cik = parts[raw_idx + 1]
                accession = parts[raw_idx + 2]
            except (ValueError, IndexError):
                logger.warning(f"Cannot parse cik/accession from key: {key}")
                continue
            result = _extract_one(bucket, cik, accession, form_type="8-K")
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
            extracted = extract_filing(cik, accession, bucket=bucket, prefix=prefix or "data/raw/8k")
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


def _extract_press_release_one(bucket: str, source: str, ticker: str, release_id: str) -> dict:
    """Extract a single press release into canonical extracted.json."""
    prefix = f"data/raw/press_releases/{source}/{ticker}/{release_id}"
    status = {
        "source": source,
        "ticker": ticker,
        "release_id": release_id,
        "action": "skipped",
    }
    try:
        body = get_s3_client().get_object(Bucket=bucket, Key=f"{prefix}/release.txt")["Body"].read()
        text = body.decode("utf-8", errors="ignore").strip()
        payload = {
            "source_type": "press_releases",
            "source": source,
            "ticker": ticker,
            "release_id": release_id,
            "text": text,
            "total_chars": len(text),
        }
        write_json_to_s3(bucket, f"{prefix}/extracted.json", payload)
        status["action"] = "extracted"
        status["total_chars"] = len(text)
    except Exception:
        logger.exception(f"Press release extraction failed for {source}/{ticker}/{release_id}")
        status["action"] = "error"
        status["reason"] = "extraction_failed"
    return status
