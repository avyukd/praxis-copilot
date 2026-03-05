"""Generic filing alerts: analysis.json -> SNS policy decision."""
from __future__ import annotations

import logging

from src.modules.events.eight_k_scanner.alerts import send_alert
from src.modules.events.eight_k_scanner.config import (
    FILING_ANALYZER_ENABLED_FORMS,
    S3_BUCKET,
    SCANNER_MIN_MAGNITUDE,
)
from src.modules.events.eight_k_scanner.storage.s3 import et_now_iso, read_json_from_s3, write_json_to_s3

logger = logging.getLogger(__name__)

FILINGS_PREFIX = "data/raw/filings"


def lambda_handler(event, context=None):
    logging.basicConfig(level=logging.INFO)
    records = event.get("Records", [])
    results = []

    for record in records:
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", S3_BUCKET)
        key = s3_info.get("object", {}).get("key", "")
        if not (key.startswith(f"{FILINGS_PREFIX}/") and key.endswith("/analysis.json")):
            continue

        parts = key.split("/")
        try:
            raw_idx = parts.index("filings")
            cik = parts[raw_idx + 1]
            accession = parts[raw_idx + 2]
        except (ValueError, IndexError):
            logger.warning("Cannot parse cik/accession from key: %s", key)
            continue
        results.append(_alert_one(bucket, cik, accession))

    return {"processed": len(results), "results": results}


def _alert_one(bucket: str, cik: str, accession: str) -> dict:
    prefix = f"{FILINGS_PREFIX}/{cik}/{accession}"
    status: dict = {"cik": cik, "accession": accession, "action": "skipped"}

    try:
        index_data = read_json_from_s3(bucket, f"{prefix}/index.json")
        analysis = read_json_from_s3(bucket, f"{prefix}/analysis.json")
    except Exception:
        logger.exception("Cannot read index/analysis for %s", accession)
        return {**status, "action": "error", "reason": "missing index/analysis"}

    ticker = index_data.get("ticker", "")
    form_type = (index_data.get("form_type") or analysis.get("form_type") or "").upper()
    classification = analysis.get("classification", "NEUTRAL")
    magnitude = float(analysis.get("magnitude", 0.0) or 0.0)

    status.update({
        "ticker": ticker,
        "form_type": form_type,
        "classification": classification,
        "magnitude": magnitude,
    })

    enabled_forms = {f.upper() for f in FILING_ANALYZER_ENABLED_FORMS}
    if form_type not in enabled_forms:
        return {**status, "action": "skipped", "reason": f"form {form_type} not enabled"}

    if str(classification).upper() == "SELL":
        return {**status, "action": "skipped", "reason": "classification SELL suppressed"}

    if magnitude < SCANNER_MIN_MAGNITUDE:
        return {**status, "action": "below_threshold", "reason": f"mag={magnitude:.2f}"}

    if index_data.get("alert_sent_at"):
        return {**status, "action": "alert_already_sent"}

    if not ticker:
        return {**status, "action": "error", "reason": "no ticker"}

    sent = send_alert(ticker, analysis, index_data)
    if sent:
        index_data["alert_sent_at"] = et_now_iso()
        write_json_to_s3(bucket, f"{prefix}/index.json", index_data)
        return {**status, "action": "alerted"}
    return {**status, "action": "alert_failed"}
