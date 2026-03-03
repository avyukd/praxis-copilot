"""S3 storage operations for praxis-copilot data plane."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError

from src.modules.events.eight_k_scanner.config import (
    S3_BUCKET,
    S3_RAW_PREFIX,
)

logger = logging.getLogger(__name__)

_s3_client = None


def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def read_json_from_s3(bucket: str, key: str) -> dict:
    resp = get_s3_client().get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read())


def write_json_to_s3(bucket: str, key: str, data: dict) -> None:
    get_s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json",
    )


def et_now_iso() -> str:
    """Return current Eastern Time timestamp as ISO-8601 with offset."""
    return datetime.now(ZoneInfo("America/New_York")).replace(microsecond=0).isoformat()


def filing_exists(cik: str, accession_number: str, bucket: str | None = None) -> bool:
    """Check if a filing has already been stored."""
    bucket = bucket or S3_BUCKET
    key = f"{S3_RAW_PREFIX}/{cik}/{accession_number}/index.json"
    try:
        get_s3_client().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def store_filing(
    cik: str,
    accession_number: str,
    metadata: dict,
    documents: dict[str, bytes],
    bucket: str | None = None,
    force: bool = False,
) -> bool:
    """Upload a complete filing to S3.

    Structure:
        data/raw/8k/{cik}/{accession}/
            index.json
            primary.htm
            ...
    """
    bucket = bucket or S3_BUCKET
    prefix = f"{S3_RAW_PREFIX}/{cik}/{accession_number}"

    if not force and filing_exists(cik, accession_number, bucket):
        logger.info(f"Filing {accession_number} already exists in S3, skipping")
        return False

    metadata = dict(metadata)
    metadata.setdefault("extracted_at", et_now_iso())
    metadata.setdefault("analyzed_at", None)

    write_json_to_s3(bucket, f"{prefix}/index.json", metadata)

    for filename, content in documents.items():
        key = f"{prefix}/{filename}"
        content_type = _guess_content_type(filename)
        get_s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=content,
            ContentType=content_type,
        )
        logger.debug(f"Uploaded {key}")

    logger.info(f"Stored filing {accession_number} ({len(documents)} docs)")
    return True


def _guess_content_type(filename: str) -> str:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return {
        "htm": "text/html",
        "html": "text/html",
        "xml": "application/xml",
        "json": "application/json",
        "txt": "text/plain",
        "jpg": "image/jpeg",
        "png": "image/png",
        "gif": "image/gif",
        "pdf": "application/pdf",
    }.get(ext, "application/octet-stream")
