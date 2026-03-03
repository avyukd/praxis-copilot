"""Analyzer Lambda: S3 PUT on extracted.json -> LLM screening + enrichment.

Control Plane -- uses LLM for classification. Produces analysis.json.
Triggered by S3 PutObject on data/raw/8k/{cik}/{accession}/extracted.json.
"""
from __future__ import annotations

import logging

from src.modules.events.eight_k_scanner.alerts import send_alert
from src.modules.events.eight_k_scanner.analyze.llm import analyze_filing_with_usage
from src.modules.events.eight_k_scanner.models import ExtractedFiling
from src.modules.events.eight_k_scanner.config import (
    S3_BUCKET,
    S3_RAW_PREFIX,
    SCANNER_MIN_ADTV,
    SCANNER_MIN_MAGNITUDE,
    SCANNER_STRATEGY,
    DISABLE_LLM_ANALYSIS,
)
from src.modules.events.eight_k_scanner.extract.filter import filter_filing
from src.modules.events.eight_k_scanner.financials import get_financial_snapshot, lookup_adtv
from src.modules.events.eight_k_scanner.storage.s3 import et_now_iso, read_json_from_s3, write_json_to_s3

logger = logging.getLogger(__name__)


def lambda_handler(event, context=None):
    """Handle S3 PutObject events for extracted.json files.

    Runs LLM screening + full analysis, writes analysis.json.
    """
    logging.basicConfig(level=logging.INFO)

    records = event.get("Records", [])
    results = []

    for record in records:
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", S3_BUCKET)
        key = s3_info.get("object", {}).get("key", "")

        if not key.endswith("/extracted.json"):
            logger.debug(f"Ignoring non-extracted key: {key}")
            continue

        # Parse cik and accession from key: data/raw/8k/{cik}/{accession}/extracted.json
        parts = key.split("/")
        try:
            raw_idx = parts.index("8k")
            cik = parts[raw_idx + 1]
            accession = parts[raw_idx + 2]
        except (ValueError, IndexError):
            logger.warning(f"Cannot parse cik/accession from key: {key}")
            continue

        result = _analyze_one(bucket, cik, accession)
        results.append(result)

    return {"processed": len(results), "results": results}


def _analyze_one(bucket: str, cik: str, accession: str) -> dict:
    """Run filter -> LLM analyze -> alert for a single filing."""
    prefix = f"{S3_RAW_PREFIX}/{cik}/{accession}"
    status: dict = {"cik": cik, "accession": accession, "action": "skipped"}

    # Read index.json for ticker and metadata
    try:
        index_data = read_json_from_s3(bucket, f"{prefix}/index.json")
    except Exception:
        logger.exception(f"Cannot read index.json for {accession}")
        status["action"] = "error"
        status["reason"] = "missing index.json"
        return status

    ticker = index_data.get("ticker", "")
    if not ticker:
        logger.warning(f"No ticker in index.json for {accession}")
        status["action"] = "error"
        status["reason"] = "no ticker"
        return status

    status["ticker"] = ticker
    index_needs_write = False
    warnings: list[str] = []

    # ADTV check (warning only)
    adtv = lookup_adtv(ticker)
    if adtv is not None and adtv < SCANNER_MIN_ADTV:
        warning = f"ADTV ${adtv:,.0f} below ${SCANNER_MIN_ADTV:,.0f} threshold"
        warnings.append(warning)
    elif adtv is None:
        warnings.append("ADTV unavailable")

    # Read extracted data
    extracted_key = f"{prefix}/extracted.json"
    try:
        extracted = ExtractedFiling.model_validate(read_json_from_s3(bucket, extracted_key))
    except Exception:
        logger.error(f"Cannot read extracted.json for {accession}")
        status["action"] = "error"
        status["reason"] = "missing extracted.json"
        return status

    # Item filter
    items_detected = list(extracted.items.keys())
    passes, matched_items = filter_filing(items_detected, strategy=SCANNER_STRATEGY)
    if not passes:
        warning = f"Items {items_detected or ['?']} don't match strategy={SCANNER_STRATEGY}"
        warnings.append(warning)
        logger.info(f"WARNING {ticker} ({accession}): {warning}")
    else:
        logger.info(f"{ticker} ({accession}): matched items {matched_items}")

    # Check if already analyzed
    analysis_key = f"{prefix}/analysis.json"
    analysis_data = None
    try:
        analysis_data = read_json_from_s3(bucket, analysis_key)
        logger.info(f"Already analyzed: {accession}")
    except Exception:
        pass

    if analysis_data is None:
        if DISABLE_LLM_ANALYSIS:
            warning = "LLM disabled via DISABLE_LLM_ANALYSIS (analysis skipped)"
            warnings.append(warning)
            status["action"] = "analysis_skipped"
            status["reason"] = warning
            status["warnings"] = warnings
            return status

        # Financial snapshot + LLM analysis
        snapshot = get_financial_snapshot(ticker)
        try:
            result = analyze_filing_with_usage(extracted, snapshot, ticker)
            analysis_data = result.analysis.model_dump()
            analysis_data["token_usage"] = result.token_usage.model_dump()
            analyzed_at = et_now_iso()
            analysis_data["analyzed_at"] = analyzed_at
            write_json_to_s3(bucket, analysis_key, analysis_data)
            if not index_data.get("analyzed_at"):
                index_data["analyzed_at"] = analyzed_at
                index_needs_write = True
            logger.info(f"Analysis: {ticker} -> {analysis_data['classification']} mag={analysis_data['magnitude']}")
        except Exception:
            logger.exception(f"LLM analysis failed for {ticker} ({accession})")
            status["action"] = "error"
            status["reason"] = "llm failed"
            return status

    # Alert decision
    classification = analysis_data.get("classification", "NEUTRAL")
    magnitude = analysis_data.get("magnitude", 0.0)

    status["classification"] = classification
    status["magnitude"] = magnitude
    status["warnings"] = warnings

    if magnitude >= SCANNER_MIN_MAGNITUDE:
        if index_data.get("alert_sent_at"):
            status["action"] = "alert_already_sent"
            status["reason"] = f"already alerted at {index_data['alert_sent_at']}"
        else:
            sent = send_alert(ticker, analysis_data, index_data, warnings=warnings)
            status["action"] = "alerted" if sent else "alert_failed"
            if sent:
                index_data["alert_sent_at"] = et_now_iso()
                index_needs_write = True
    else:
        status["action"] = "analyzed"
        status["reason"] = f"{classification} mag={magnitude:.2f}"

    if index_needs_write:
        write_json_to_s3(bucket, f"{prefix}/index.json", index_data)

    return status
