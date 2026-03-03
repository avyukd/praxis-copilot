"""Lambda handler for data ingestion — entry point for ticker data pull."""

import json
import logging

import boto3

from modules.data_ingestion.fundamentals import ingest_fundamentals
from modules.data_ingestion.models import IngestionResult
from modules.data_ingestion.sec_filings import ingest_sec_filings
from modules.data_ingestion.transcripts import ingest_transcripts

logger = logging.getLogger(__name__)

S3_BUCKET = "praxis-copilot"


def _upload_to_s3(s3_client, key: str, body: str | bytes) -> None:
    """Upload content to S3."""
    if isinstance(body, str):
        body = body.encode("utf-8")
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=body)


def handler(event: dict, context=None) -> dict:
    """Lambda entry point for data ingestion.

    Expects event: {"ticker": "NVDA", "cik": "0001045810"}
    Runs all three ingestion steps with error isolation.
    """
    ticker = event["ticker"].upper()
    cik = event["cik"]

    logging.basicConfig(level=logging.INFO)
    logger.info(f"Starting data ingestion for {ticker} (CIK: {cik})")

    result = IngestionResult(ticker=ticker)
    s3 = boto3.client("s3")
    base_prefix = f"data/research/{ticker}/data"

    # 1. SEC Filings
    try:
        sections = ingest_sec_filings(cik, ticker)
        for section in sections:
            s3_key = (
                f"{base_prefix}/filings/{section.filing_type}/"
                f"{section.period}/{section.section_name}.txt"
            )
            _upload_to_s3(s3, s3_key, section.text)
        result.filings_count = len(sections)
        logger.info(f"Ingested {len(sections)} filing sections for {ticker}")
    except Exception as e:
        msg = f"SEC filings ingestion failed for {ticker}: {e}"
        logger.warning(msg)
        result.warnings.append(msg)

    # 2. Fundamentals
    try:
        fundamentals = ingest_fundamentals(ticker)
        if fundamentals:
            s3_key = f"{base_prefix}/fundamentals/fundamentals.json"
            _upload_to_s3(s3, s3_key, json.dumps(fundamentals.model_dump(), default=str))
            result.fundamentals_source = fundamentals.source
            logger.info(f"Ingested fundamentals for {ticker} from {fundamentals.source}")
        else:
            msg = f"No fundamental data available for {ticker}"
            logger.warning(msg)
            result.warnings.append(msg)
    except Exception as e:
        msg = f"Fundamentals ingestion failed for {ticker}: {e}"
        logger.warning(msg)
        result.warnings.append(msg)

    # 3. Transcripts
    try:
        transcripts = ingest_transcripts(ticker)
        for transcript in transcripts:
            quarter = transcript["quarter"].replace("/", "-").replace(" ", "_")
            s3_key = f"{base_prefix}/transcripts/{quarter}.txt"
            _upload_to_s3(s3, s3_key, transcript["text"])
        result.transcripts_count = len(transcripts)
        logger.info(f"Ingested {len(transcripts)} transcript(s) for {ticker}")
    except Exception as e:
        msg = f"Transcript ingestion failed for {ticker}: {e}"
        logger.warning(msg)
        result.warnings.append(msg)

    logger.info(f"Ingestion complete for {ticker}: {result.model_dump()}")
    return result.model_dump()
