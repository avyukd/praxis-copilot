"""Data ingestion — pulls SEC filings, fundamentals, and transcripts to S3."""

import json
import logging

from cli.ingest.fundamentals import ingest_fundamentals
from cli.ingest.models import IngestionResult
from cli.ingest.sec_filings import ingest_sec_filings
from cli.ingest.transcripts import ingest_transcripts
from cli.s3 import BUCKET

logger = logging.getLogger(__name__)


def run_ingestion(ticker: str, cik: str, s3_client) -> IngestionResult:
    """Run all ingestion steps for a ticker, uploading results to S3.

    Each step is error-isolated — if one fails, the others still run.
    """
    logging.basicConfig(level=logging.INFO)
    logger.info(f"Starting data ingestion for {ticker} (CIK: {cik})")

    result = IngestionResult(ticker=ticker)
    base_prefix = f"data/research/{ticker}/data"

    # 1. SEC Filings
    try:
        sections = ingest_sec_filings(cik, ticker)
        for section in sections:
            s3_key = (
                f"{base_prefix}/filings/{section.filing_type}/"
                f"{section.period}/{section.section_name}.txt"
            )
            _upload(s3_client, s3_key, section.text)
        result.filings_count = len(sections)
        logger.info(f"Ingested {len(sections)} filing sections for {ticker}")
    except Exception as e:
        msg = f"SEC filings ingestion failed: {e}"
        logger.warning(msg)
        result.warnings.append(msg)

    # 2. Fundamentals
    try:
        fundamentals = ingest_fundamentals(ticker)
        if fundamentals:
            s3_key = f"{base_prefix}/fundamentals/fundamentals.json"
            _upload(s3_client, s3_key, json.dumps(fundamentals.model_dump(), default=str))
            result.fundamentals_source = fundamentals.source
            logger.info(f"Ingested fundamentals from {fundamentals.source}")
        else:
            result.warnings.append("No fundamental data available")
    except Exception as e:
        msg = f"Fundamentals ingestion failed: {e}"
        logger.warning(msg)
        result.warnings.append(msg)

    # 3. Transcripts
    try:
        transcripts = ingest_transcripts(ticker)
        for transcript in transcripts:
            quarter = transcript["quarter"].replace("/", "-").replace(" ", "_")
            s3_key = f"{base_prefix}/transcripts/{quarter}.txt"
            _upload(s3_client, s3_key, transcript["text"])
        result.transcripts_count = len(transcripts)
        logger.info(f"Ingested {len(transcripts)} transcript(s)")
    except Exception as e:
        msg = f"Transcript ingestion failed: {e}"
        logger.warning(msg)
        result.warnings.append(msg)

    logger.info(f"Ingestion complete for {ticker}: {result.model_dump()}")
    return result


def _upload(s3_client, key: str, body: str | bytes) -> None:
    if isinstance(body, str):
        body = body.encode("utf-8")
    s3_client.put_object(Bucket=BUCKET, Key=key, Body=body)
