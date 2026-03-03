"""Earnings call transcript ingestion (stub — API source TBD).

When an API source is chosen, this module should:
1. Fetch recent earnings call transcripts for the given ticker
2. Store them as structured JSON to s3://praxis-copilot/data/research/{ticker}/data/transcripts/

Expected transcript format per file:
    {
        "ticker": "NVDA",
        "quarter": "Q4",
        "fiscal_year": 2024,
        "date": "2025-02-26",
        "participants": [...],
        "prepared_remarks": [...],
        "qa_session": [...],
    }
"""
from __future__ import annotations

import json
import logging

import boto3

logger = logging.getLogger(__name__)

S3_BUCKET = "praxis-copilot"


def ingest_transcripts(ticker: str, cik: str, s3_client=None) -> dict:
    """Pull earnings call transcripts and store to S3.

    Currently a stub — logs that it is not yet implemented and writes
    a placeholder marker to S3 so downstream steps know the module ran.

    Returns summary dict.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    logger.info(
        f"Transcript ingestion not yet implemented for {ticker}. "
        f"API source TBD — skipping."
    )

    s3_prefix = f"data/research/{ticker}/data/transcripts"

    # Write a stub marker so downstream knows the module ran
    marker = {
        "ticker": ticker,
        "cik": cik,
        "status": "not_implemented",
        "message": "Transcript ingestion API source TBD. This is a placeholder.",
    }
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f"{s3_prefix}/_stub.json",
            Body=json.dumps(marker, indent=2),
            ContentType="application/json",
        )
    except Exception:
        logger.warning(f"Could not write transcript stub marker for {ticker}")

    return {
        "transcripts_stored": 0,
        "status": "not_implemented",
    }
