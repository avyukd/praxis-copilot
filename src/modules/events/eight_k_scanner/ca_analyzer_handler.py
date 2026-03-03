"""CA Analyzer Lambda: S3 trigger on ca-pr index.json -> LLM analyze -> SNS alert."""
from __future__ import annotations

import logging

from src.modules.events.eight_k_scanner.alerts import send_alert
from src.modules.events.eight_k_scanner.analyze.llm import analyze_filing_with_usage
from src.modules.events.eight_k_scanner.config import (
    DISABLE_LLM_ANALYSIS,
    S3_BUCKET,
    S3_CA_RAW_PREFIX,
    SCANNER_MIN_ADTV,
    SCANNER_MIN_MAGNITUDE,
)
from src.modules.events.eight_k_scanner.financials import get_financial_snapshot, lookup_adtv
from src.modules.events.eight_k_scanner.newswire.prompt import build_pr_messages
from src.modules.events.eight_k_scanner.storage.s3 import et_now_iso, get_s3_client, read_json_from_s3, write_json_to_s3

logger = logging.getLogger(__name__)


def lambda_handler(event, context=None):
    logging.basicConfig(level=logging.INFO)

    records = event.get("Records", [])
    results = []

    for record in records:
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", S3_BUCKET)
        key = s3_info.get("object", {}).get("key", "")

        if not key.endswith("/index.json"):
            continue

        parts = key.split("/")
        try:
            pr_idx = parts.index("ca-pr")
            ticker = parts[pr_idx + 1]
            release_id = parts[pr_idx + 2]
        except (ValueError, IndexError):
            logger.warning(f"Cannot parse key: {key}")
            continue

        result = _process_one(bucket, ticker, release_id)
        results.append(result)

    return {"processed": len(results), "results": results}


def _process_one(bucket: str, ticker: str, release_id: str) -> dict:
    prefix = f"{S3_CA_RAW_PREFIX}/{ticker}/{release_id}"
    status: dict = {"ticker": ticker, "release_id": release_id, "action": "skipped"}
    index_needs_write = False

    try:
        index_data = read_json_from_s3(bucket, f"{prefix}/index.json")
    except Exception:
        logger.exception(f"Cannot read index.json for {release_id}")
        status["action"] = "error"
        return status

    symbol = index_data.get("symbol", ticker)
    warnings: list[str] = []

    adtv = lookup_adtv(symbol)
    if adtv is not None and adtv < SCANNER_MIN_ADTV:
        warnings.append(f"ADTV ${adtv:,.0f} below threshold")
    elif adtv is None:
        warnings.append("ADTV unavailable")

    try:
        resp = get_s3_client().get_object(Bucket=bucket, Key=f"{prefix}/release.txt")
        release_text = resp["Body"].read().decode("utf-8")
    except Exception:
        logger.exception(f"Cannot read release.txt for {release_id}")
        status["action"] = "error"
        return status

    analysis_key = f"{prefix}/analysis.json"
    analysis_data = None
    try:
        analysis_data = read_json_from_s3(bucket, analysis_key)
        logger.info(f"Already analyzed: {release_id}")
    except Exception:
        pass

    if analysis_data is None:
        if DISABLE_LLM_ANALYSIS:
            status["action"] = "analysis_skipped"
            status["warnings"] = warnings
            return status

        snapshot = get_financial_snapshot(symbol)
        messages = build_pr_messages(release_text, snapshot, symbol)
        try:
            result = analyze_filing_with_usage({}, snapshot, symbol, messages=messages)
            analysis_data = result.analysis.model_dump()
            analysis_data["token_usage"] = result.token_usage.model_dump()
            analyzed_at = et_now_iso()
            analysis_data["analyzed_at"] = analyzed_at
            write_json_to_s3(bucket, analysis_key, analysis_data)
            if not index_data.get("analyzed_at"):
                index_data["analyzed_at"] = analyzed_at
                index_needs_write = True
        except Exception:
            logger.exception(f"LLM analysis failed for {ticker} ({release_id})")
            status["action"] = "error"
            return status

    classification = analysis_data.get("classification", "NEUTRAL")
    magnitude = analysis_data.get("magnitude", 0.0)
    status["classification"] = classification
    status["magnitude"] = magnitude
    status["warnings"] = warnings

    if magnitude >= SCANNER_MIN_MAGNITUDE:
        if index_data.get("alert_sent_at"):
            status["action"] = "alert_already_sent"
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
