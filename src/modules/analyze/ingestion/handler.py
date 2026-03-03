"""Data ingestion Lambda handler.

Triggered when a ticker is added to the universe. Orchestrates pulling
SEC filings, fundamentals, and earnings transcripts to S3.

Expected event payload:
    {"ticker": "NVDA", "cik": "0001045810"}
"""
from __future__ import annotations

import logging

import boto3

from src.modules.analyze.ingestion.sec_filings import ingest_filings
from src.modules.analyze.ingestion.fundamentals import ingest_fundamentals
from src.modules.analyze.ingestion.transcripts import ingest_transcripts

logger = logging.getLogger(__name__)


def lambda_handler(event=None, context=None):
    """Lambda entry point: orchestrate data ingestion for a ticker."""
    logging.getLogger().setLevel(logging.INFO)

    if event is None:
        event = {}

    ticker = event.get("ticker", "").upper()
    cik = event.get("cik", "")

    if not ticker or not cik:
        logger.error(f"Missing required fields. Got ticker={ticker!r}, cik={cik!r}")
        return {
            "status": "error",
            "message": "Both 'ticker' and 'cik' are required in the event payload.",
        }

    logger.info(f"Starting data ingestion for {ticker} (CIK: {cik})")

    s3_client = boto3.client("s3")
    results = {}

    # Step 1: SEC filings (10-K, 10-Q)
    logger.info(f"[1/3] Ingesting SEC filings for {ticker}")
    try:
        results["filings"] = ingest_filings(ticker, cik, s3_client=s3_client)
    except Exception:
        logger.exception(f"SEC filings ingestion failed for {ticker}")
        results["filings"] = {"error": "uncaught exception"}

    # Step 2: Fundamentals (XBRL company facts)
    logger.info(f"[2/3] Ingesting fundamentals for {ticker}")
    try:
        results["fundamentals"] = ingest_fundamentals(ticker, cik, s3_client=s3_client)
    except Exception:
        logger.exception(f"Fundamentals ingestion failed for {ticker}")
        results["fundamentals"] = {"error": "uncaught exception"}

    # Step 3: Transcripts (stub)
    logger.info(f"[3/3] Ingesting transcripts for {ticker}")
    try:
        results["transcripts"] = ingest_transcripts(ticker, cik, s3_client=s3_client)
    except Exception:
        logger.exception(f"Transcripts ingestion failed for {ticker}")
        results["transcripts"] = {"error": "uncaught exception"}

    logger.info(f"Data ingestion complete for {ticker}: {results}")

    return {
        "status": "ok",
        "ticker": ticker,
        "cik": cik,
        "results": results,
    }
