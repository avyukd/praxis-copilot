"""SEC filings poller: EventBridge cron -> poll EDGAR -> fetch -> store to S3.

Triggered by EventBridge cron during market hours.
Stores all SEC filings to:
  - s3://.../data/raw/filings/{cik}/{accession}/
"""
from __future__ import annotations

import logging
from collections import defaultdict

import boto3
import yaml
from botocore.exceptions import ClientError

from src.modules.events.eight_k_scanner.edgar.poller import poll_new_8k_filings
from src.modules.events.eight_k_scanner.edgar.fetcher import fetch_filing
from src.modules.events.eight_k_scanner.universe.builder import is_in_universe
from src.modules.events.eight_k_scanner.config import (
    S3_BUCKET,
    SCANNER_LOOKBACK_MINUTES,
    S3_FILINGS_RAW_PREFIX,
)
from src.modules.events.eight_k_scanner.storage.s3 import filing_exists

logger = logging.getLogger(__name__)

FILINGS_RAW_PREFIX = S3_FILINGS_RAW_PREFIX


def lambda_handler(event=None, context=None):
    """Lambda entry point: poll -> filter -> fetch -> store under unified filings path."""
    logging.getLogger().setLevel(logging.INFO)

    # Build the set of form types to poll from monitor subscriptions
    monitor_subs = _load_monitor_subscriptions()
    extra_forms = set()
    for forms_list in monitor_subs.values():
        extra_forms.update(forms_list)

    # Always poll 8-K for the universe; add monitor-subscribed forms
    all_forms = sorted({"8-K"} | extra_forms)

    filings = poll_new_8k_filings(
        lookback_minutes=SCANNER_LOOKBACK_MINUTES,
        forms=all_forms,
    )
    logger.info(f"Discovered {len(filings)} new filings (forms={all_forms})")

    stored = 0
    errors = 0
    skipped = 0
    filtered_out = 0

    for filing_meta in filings:
        cik = filing_meta.cik
        accession = filing_meta.accession_number
        form_type = filing_meta.form_type or "8-K"

        in_universe, info = is_in_universe(cik)

        # For 8-K: must be in universe (existing behavior)
        # For non-8-K: must match a monitor subscription
        is_8k = form_type in ("8-K", "8-K/A")
        is_monitor_sub = _matches_monitor_subscription(cik, form_type, monitor_subs)

        if not in_universe and not is_monitor_sub:
            filtered_out += 1
            continue

        # All SEC filings now use the unified filings path.
        if _filing_exists_unified(cik, accession):
            skipped += 1
            continue

        try:
            result = fetch_filing(cik, accession)

            meta = result.metadata
            if info:
                meta.ticker = info.ticker
                meta.company_name = info.company_name or filing_meta.company_name
                meta.market_cap = info.market_cap
                meta.exchange = info.exchange
            else:
                meta.ticker = filing_meta.ticker
                meta.company_name = filing_meta.company_name
            meta.filed_date = filing_meta.filed_date
            if not meta.acceptance_datetime:
                meta.acceptance_datetime = filing_meta.acceptance_datetime

            # Add form_type to index metadata
            index_data = meta.model_dump()
            index_data["form_type"] = form_type

            _store_filing_unified(cik, accession, index_data, result.documents)
            stored += 1

        except Exception:
            logger.exception(f"Failed to process filing {accession} for CIK {cik}")
            errors += 1

    logger.info(
        f"Done: {stored} stored, {skipped} skipped, {filtered_out} filtered out, "
        f"{errors} errors (out of {len(filings)} discovered)"
    )

    return {
        "total_discovered": len(filings),
        "forms_polled": all_forms,
        "filtered_out": filtered_out,
        "stored": stored,
        "skipped": skipped,
        "errors": errors,
    }


def _load_monitor_subscriptions() -> dict[str, list[str]]:
    """Load filing monitors from S3 and build {cik: [form_types]} map.

    Reads monitor configs, filters to type=filing, resolves tickers to CIKs
    via ticker_registry, returns subscription map.
    """
    s3 = boto3.client("s3")
    subs: dict[str, list[str]] = defaultdict(list)

    # Load ticker registry for ticker -> CIK mapping
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key="config/ticker_registry.yaml")
        registry = yaml.safe_load(resp["Body"].read()) or {}
        ticker_to_cik = {}
        for ticker, entry in registry.get("tickers", {}).items():
            cik = (entry.get("cik") or "").lstrip("0")
            if cik:
                ticker_to_cik[ticker] = cik
    except (ClientError, Exception):
        logger.warning("Failed to load ticker registry for monitor subscriptions")
        return {}

    # Load monitor configs
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="config/monitors/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".yaml"):
                    continue
                try:
                    resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
                    data = yaml.safe_load(resp["Body"].read()) or {}
                    if data.get("type") != "filing":
                        continue
                    filing_types = data.get("filing_types", [])
                    for ticker in data.get("tickers", []):
                        cik = ticker_to_cik.get(ticker)
                        if cik:
                            for ft in filing_types:
                                if ft not in subs[cik]:
                                    subs[cik].append(ft)
                except Exception:
                    logger.warning(f"Failed to parse monitor config: {key}")
    except Exception:
        logger.warning("Failed to list monitor configs")

    return dict(subs)


def _matches_monitor_subscription(
    cik: str,
    form_type: str,
    subs: dict[str, list[str]],
) -> bool:
    """Check if a filing matches any monitor subscription."""
    cik_normalized = cik.lstrip("0")
    for sub_cik, forms in subs.items():
        if sub_cik.lstrip("0") == cik_normalized:
            # Match form type (including amendments)
            base_form = form_type.rstrip("/A").rstrip("/")
            if base_form in forms or form_type in forms:
                return True
    return False


def _store_filing_unified(
    cik: str,
    accession: str,
    index_data: dict,
    documents: dict[str, bytes],
) -> None:
    """Store a filing under the unified data/raw/filings/ path."""
    import json
    s3 = boto3.client("s3")
    prefix = f"{FILINGS_RAW_PREFIX}/{cik}/{accession}"

    # Write index.json
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}/index.json",
        Body=json.dumps(index_data, default=str).encode(),
        ContentType="application/json",
    )

    # Write document files
    for filename, content in documents.items():
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"{prefix}/{filename}",
            Body=content,
        )

    logger.info(f"Stored filing {accession} at {prefix}/ ({len(documents)} docs)")


def _filing_exists_unified(cik: str, accession: str) -> bool:
    """Check if filing already exists under canonical filings path.

    Falls back to legacy 8-K existence check for safety during cutover.
    """
    s3 = boto3.client("s3")
    key = f"{FILINGS_RAW_PREFIX}/{cik}/{accession}/index.json"
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in ("404", "NoSuchKey", "NotFound"):
            logger.warning(f"head_object failed for {key}: {code}")
    except Exception:
        logger.warning(f"Unexpected error checking {key}", exc_info=True)

    return filing_exists(cik, accession)
