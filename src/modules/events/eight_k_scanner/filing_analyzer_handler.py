"""Generic filing analyzer: extracted.json -> analysis.json for enabled forms."""
from __future__ import annotations

import logging
from typing import Literal

from litellm import completion
from pydantic import BaseModel

from src.modules.events.eight_k_scanner.analyze.llm import analyze_filing_with_usage
from src.modules.events.eight_k_scanner.config import (
    DISABLE_LLM_ANALYSIS,
    ENABLE_8K_HAIKU_SCREEN,
    ENABLE_PRESS_RELEASE_HAIKU_SCREEN,
    FILING_ANALYZER_ENABLED_FORMS,
    HAIKU_PRESCREEN_MODEL,
    S3_BUCKET,
    SCANNER_MIN_ADTV,
    SCANNER_STRATEGY,
)
from src.modules.events.eight_k_scanner.extract.filter import filter_filing
from src.modules.events.eight_k_scanner.financials import get_financial_snapshot, lookup_adtv
from src.modules.events.eight_k_scanner.models import ExtractedFiling
from src.modules.events.eight_k_scanner.storage.s3 import et_now_iso, read_json_from_s3, write_json_to_s3

logger = logging.getLogger(__name__)

FILINGS_PREFIX = "data/raw/filings"
PRESS_RELEASES_PREFIX = "data/raw/press_releases"

SCREENING_SYSTEM_PROMPT = (
    "Classify this filing or press-release excerpt as one token only: POSITIVE, NEUTRAL, or NEGATIVE.\n"
    "When evidence is ambiguous, mixed, or insufficient, prefer POSITIVE over NEUTRAL.\n"
    "Return only valid JSON object: {\"outcome\":\"POSITIVE|NEUTRAL|NEGATIVE\"}."
)


class PrescreenResult(BaseModel):
    outcome: Literal["POSITIVE", "NEUTRAL", "NEGATIVE"]


def lambda_handler(event, context=None):
    """Handle extracted filing objects from canonical filings path."""
    logging.basicConfig(level=logging.INFO)
    records = event.get("Records", [])
    results = []

    for record in records:
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", S3_BUCKET)
        key = s3_info.get("object", {}).get("key", "")
        if key.startswith(f"{FILINGS_PREFIX}/") and key.endswith("/extracted.json"):
            parts = key.split("/")
            try:
                raw_idx = parts.index("filings")
                cik = parts[raw_idx + 1]
                accession = parts[raw_idx + 2]
            except (ValueError, IndexError):
                logger.warning("Cannot parse cik/accession from key: %s", key)
                continue
            results.append(_analyze_one(bucket, cik, accession))
            continue

        if key.startswith(f"{PRESS_RELEASES_PREFIX}/") and key.endswith("/extracted.json"):
            parts = key.split("/")
            try:
                raw_idx = parts.index("press_releases")
                source = parts[raw_idx + 1]
                ticker = parts[raw_idx + 2]
                release_id = parts[raw_idx + 3]
            except (ValueError, IndexError):
                logger.warning("Cannot parse source/ticker/release_id from key: %s", key)
                continue
            results.append(_analyze_press_release_one(bucket, source, ticker, release_id))
            continue

    return {"processed": len(results), "results": results}


def _analyze_one(bucket: str, cik: str, accession: str) -> dict:
    prefix = f"{FILINGS_PREFIX}/{cik}/{accession}"
    status: dict = {"cik": cik, "accession": accession, "action": "skipped"}

    try:
        index_data = read_json_from_s3(bucket, f"{prefix}/index.json")
    except Exception:
        logger.exception("Cannot read index.json for %s", accession)
        return {**status, "action": "error", "reason": "missing index.json"}

    ticker = index_data.get("ticker", "")
    form_type = (index_data.get("form_type") or "").upper()
    if not ticker:
        return {**status, "action": "error", "reason": "no ticker"}

    status["ticker"] = ticker
    status["form_type"] = form_type

    # Policy gate: analyzer is generic, enabled forms are currently 8-K only.
    enabled_forms = {f.upper() for f in FILING_ANALYZER_ENABLED_FORMS}
    if form_type not in enabled_forms:
        return {**status, "action": "skipped", "reason": f"form {form_type} not enabled"}

    warnings: list[str] = []
    adtv = lookup_adtv(ticker)
    if adtv is not None and adtv < SCANNER_MIN_ADTV:
        warnings.append(f"ADTV ${adtv:,.0f} below ${SCANNER_MIN_ADTV:,.0f} threshold")
    elif adtv is None:
        warnings.append("ADTV unavailable")

    try:
        extracted = ExtractedFiling.model_validate(read_json_from_s3(bucket, f"{prefix}/extracted.json"))
    except Exception:
        return {**status, "action": "error", "reason": "missing extracted.json"}

    items_detected = list(extracted.items.keys())
    if form_type.startswith("8-K"):
        passes, matched_items = filter_filing(items_detected, strategy=SCANNER_STRATEGY)
        if not passes:
            warnings.append(f"Items {items_detected or ['?']} don't match strategy={SCANNER_STRATEGY}")
        else:
            logger.info("%s (%s): matched items %s", ticker, accession, matched_items)

        # Optional 8-K Haiku screen on truncated extracted content.
        if ENABLE_8K_HAIKU_SCREEN and form_type in ("8-K", "8-K/A"):
            screening: PrescreenResult | None = None
            screening_error: str | None = None
            try:
                screening = _run_8k_prescreen(extracted)
            except Exception as exc:
                logger.warning("8-K Haiku prescreen failed for %s/%s: %s", ticker, accession, exc)
                screening_error = exc.__class__.__name__

            screening_data = {"outcome": screening.outcome if screening else "ERROR"}
            if screening_error:
                screening_data["error"] = screening_error
            write_json_to_s3(bucket, f"{prefix}/screening.json", screening_data)

            if screening:
                status["screening_outcome"] = screening.outcome
            if screening and screening.outcome in ("NEGATIVE", "NEUTRAL"):
                return {
                    **status,
                    "action": "screened_out",
                    "reason": f"haiku_outcome={screening.outcome}",
                    "warnings": warnings,
                }

    analysis_key = f"{prefix}/analysis.json"
    try:
        existing = read_json_from_s3(bucket, analysis_key)
        return {
            **status,
            "action": "already_analyzed",
            "classification": existing.get("classification", "NEUTRAL"),
            "magnitude": existing.get("magnitude", 0.0),
            "warnings": warnings,
        }
    except Exception:
        pass

    if DISABLE_LLM_ANALYSIS:
        return {
            **status,
            "action": "analysis_skipped",
            "reason": "LLM disabled via DISABLE_LLM_ANALYSIS",
            "warnings": warnings,
        }

    snapshot = get_financial_snapshot(ticker)
    try:
        result = analyze_filing_with_usage(extracted, snapshot, ticker)
        analysis_data = result.analysis.model_dump()
        analysis_data["token_usage"] = result.token_usage.model_dump()
        analysis_data["analyzed_at"] = et_now_iso()
        analysis_data["source_type"] = "filings"
        analysis_data["form_type"] = form_type
        write_json_to_s3(bucket, analysis_key, analysis_data)
    except Exception:
        logger.exception("LLM analysis failed for %s (%s)", ticker, accession)
        return {**status, "action": "error", "reason": "llm failed"}

    return {
        **status,
        "action": "analyzed",
        "classification": analysis_data.get("classification", "NEUTRAL"),
        "magnitude": analysis_data.get("magnitude", 0.0),
        "warnings": warnings,
    }


def _analyze_press_release_one(bucket: str, source: str, ticker: str, release_id: str) -> dict:
    prefix = f"{PRESS_RELEASES_PREFIX}/{source}/{ticker}/{release_id}"
    status: dict = {
        "source": source,
        "ticker": ticker,
        "release_id": release_id,
        "action": "skipped",
    }

    try:
        index_data = read_json_from_s3(bucket, f"{prefix}/index.json")
    except Exception:
        logger.exception("Cannot read index.json for press release %s/%s", ticker, release_id)
        return {**status, "action": "error", "reason": "missing index.json"}

    ticker = (index_data.get("ticker") or ticker or "").upper()
    if not ticker:
        return {**status, "action": "error", "reason": "no ticker"}
    status["ticker"] = ticker

    try:
        extracted_data = read_json_from_s3(bucket, f"{prefix}/extracted.json")
        extracted = ExtractedFiling(
            cik="",
            accession_number=release_id,
            ticker=ticker,
            form_type="PRESS_RELEASE",
            text=extracted_data.get("text", ""),
            total_chars=int(extracted_data.get("total_chars", 0) or 0),
        )
    except Exception:
        logger.exception("Cannot read extracted.json for press release %s/%s", ticker, release_id)
        return {**status, "action": "error", "reason": "missing extracted.json"}

    analysis_key = f"{prefix}/analysis.json"
    try:
        existing = read_json_from_s3(bucket, analysis_key)
        return {
            **status,
            "action": "already_analyzed",
            "classification": existing.get("classification", "NEUTRAL"),
            "magnitude": existing.get("magnitude", 0.0),
        }
    except Exception:
        pass

    if ENABLE_PRESS_RELEASE_HAIKU_SCREEN:
        screening: PrescreenResult | None = None
        screening_error: str | None = None
        try:
            screening = _run_8k_prescreen(extracted)
        except Exception as exc:
            logger.warning(
                "PR Haiku prescreen failed for %s/%s/%s: %s",
                source,
                ticker,
                release_id,
                exc,
            )
            screening_error = exc.__class__.__name__

        screening_data = {"outcome": screening.outcome if screening else "ERROR"}
        if screening_error:
            screening_data["error"] = screening_error
        write_json_to_s3(bucket, f"{prefix}/screening.json", screening_data)

        if screening:
            status["screening_outcome"] = screening.outcome
        if screening and screening.outcome in ("NEGATIVE", "NEUTRAL"):
            return {
                **status,
                "action": "screened_out",
                "reason": f"haiku_outcome={screening.outcome}",
            }

    if DISABLE_LLM_ANALYSIS:
        return {
            **status,
            "action": "analysis_skipped",
            "reason": "LLM disabled via DISABLE_LLM_ANALYSIS",
        }

    snapshot = get_financial_snapshot(ticker)
    try:
        result = analyze_filing_with_usage(extracted, snapshot, ticker)
        analysis_data = result.analysis.model_dump()
        analysis_data["token_usage"] = result.token_usage.model_dump()
        analysis_data["analyzed_at"] = et_now_iso()
        analysis_data["source_type"] = "press_releases"
        analysis_data["form_type"] = "PRESS_RELEASE"
        write_json_to_s3(bucket, analysis_key, analysis_data)
    except Exception:
        logger.exception("LLM analysis failed for press release %s (%s)", ticker, release_id)
        return {**status, "action": "error", "reason": "llm failed"}

    return {
        **status,
        "action": "analyzed",
        "classification": analysis_data.get("classification", "NEUTRAL"),
        "magnitude": analysis_data.get("magnitude", 0.0),
    }


def _run_8k_prescreen(extracted: ExtractedFiling) -> PrescreenResult:
    """Haiku prescreen for 8-K using first half of extracted content."""
    excerpt = _build_8k_screen_text(extracted)
    response = completion(
        model=HAIKU_PRESCREEN_MODEL,
        messages=[
            {"role": "system", "content": SCREENING_SYSTEM_PROMPT},
            {"role": "user", "content": excerpt},
        ],
        response_format=PrescreenResult,
    )
    content = response.choices[0].message.content
    return PrescreenResult.model_validate_json(content)


def _build_8k_screen_text(extracted: ExtractedFiling) -> str:
    parts: list[str] = []
    for item_num, text in extracted.items.items():
        parts.append(f"Item {item_num}\n{text}")
    if not parts and extracted.text:
        parts.append(extracted.text)
    raw = "\n\n".join(parts).strip()
    if not raw:
        return ""
    half = max(1, len(raw) // 2)
    return raw[:half]
