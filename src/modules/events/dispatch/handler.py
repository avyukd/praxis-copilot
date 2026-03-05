"""Event Dispatch Lambda: routes data from event-stage pipelines to monitors.

Triggered by S3 PUTs on:
  - data/raw/filings/{cik}/{accession}/extracted.json
  - data/raw/press_releases/{source}/{ticker}/{release_id}/extracted.json
  - data/news/{date}/digest/{hour}.yaml

Pure routing -- deterministic, no LLM.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError

from src.cli.models import TickerRegistry
from src.modules.events.dispatch.models import (
    DispatchResult,
    EventRecord,
    MonitorDefinition,
    ParsedTrigger,
    S3Event,
)

logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get("S3_BUCKET", "praxis-copilot")
S3_CONFIG_PREFIX = "config"
S3_DATA_PREFIX = "data"

MONITOR_EVALUATOR_LAMBDA = os.environ.get("MONITOR_EVALUATOR_LAMBDA", "praxis-monitor-evaluator")
FILING_ANALYZER_LAMBDA = os.environ.get("FILING_ANALYZER_LAMBDA", "filing-analyzer")

_s3_client = None
_lambda_client = None


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _get_lambda_client():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client("lambda")
    return _lambda_client


def lambda_handler(event: dict, context=None) -> dict:
    """Route S3 PUT events to subscribed monitors."""
    logging.basicConfig(level=logging.INFO)

    s3_event = S3Event.model_validate(event)
    dispatched = 0
    skipped = 0

    for record in s3_event.Records:
        bucket = record.s3.bucket.name or S3_BUCKET
        key = record.s3.object.key

        parsed = _parse_trigger(key)
        if parsed is None:
            logger.debug(f"Ignoring unrecognized key: {key}")
            skipped += 1
            continue

        # For canonical filings extracted events, trigger analyzer asynchronously.
        if parsed.source == "sec-filings-extractor":
            _invoke_filing_analyzer(bucket, key)

        tickers = _resolve_tickers(bucket, key, parsed)

        if not tickers:
            logger.info(f"No tickers resolved for {key}")
            skipped += 1
            continue

        # Load monitor registry
        monitors = _load_monitor_registry(bucket)

        # Match monitors
        for ticker in tickers:
            matching_monitors = _match_monitors(ticker, parsed.data_type, monitors)
            if not matching_monitors:
                logger.debug(f"No monitors matched for {ticker}:{parsed.data_type}")
                continue

            event_id = f"evt-{uuid.uuid4().hex[:12]}"
            event_record = EventRecord(
                event_id=event_id,
                timestamp=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                source=parsed.source,
                ticker=ticker,
                cik=parsed.cik,
                data_type=parsed.data_type,
                s3_path=key,
                monitors_triggered=[m.id for m in matching_monitors],
            )

            # Emit event record
            _emit_event_record(bucket, event_record)

            # Invoke matching monitors
            for monitor in matching_monitors:
                _invoke_monitor_collector(monitor, event_record, parsed)
                dispatched += 1

    logger.info(f"Dispatch complete: {dispatched} monitor invocations, {skipped} skipped")
    return DispatchResult(dispatched=dispatched, skipped=skipped).model_dump()


def _parse_trigger(key: str) -> ParsedTrigger | None:
    """Parse an S3 key to determine source, data type, and identifiers."""
    parts = key.split("/")

    # Canonical filing path: data/raw/filings/{cik}/{accession}/extracted.json
    if key.startswith("data/raw/filings/") and key.endswith("/extracted.json"):
        try:
            form_type = _read_form_type_from_extracted(key)
            return ParsedTrigger(
                source="sec-filings-extractor",
                data_type=f"filings:{form_type}" if form_type else "filings",
                cik=parts[3],
                accession=parts[4],
                form_type=form_type,
            )
        except IndexError:
            return None

    # Canonical press releases path: data/raw/press_releases/{source}/{ticker}/{release_id}/extracted.json
    if key.startswith("data/raw/press_releases/") and key.endswith("/extracted.json"):
        try:
            return ParsedTrigger(
                source="press-releases-extractor",
                data_type="press_releases",
                ticker_direct=parts[4],
                release_id=parts[5],
            )
        except IndexError:
            return None

    # Legacy 8-K extracted trigger compatibility
    if key.startswith("data/raw/8k/") and key.endswith("/extracted.json"):
        try:
            return ParsedTrigger(
                source="8k-extractor",
                data_type="filings:8-K",
                cik=parts[3],
                accession=parts[4],
                form_type="8-K",
            )
        except IndexError:
            return None

    # Legacy CA PR compatibility
    if key.startswith("data/raw/ca-pr/") and key.endswith("/analysis.json"):
        try:
            return ParsedTrigger(
                source="ca-pr-scanner",
                data_type="press_releases",
                ticker_direct=parts[3],
                release_id=parts[4],
            )
        except IndexError:
            return None

    # Legacy US PR compatibility
    if key.startswith("data/raw/us-pr/") and key.endswith("/analysis.json"):
        try:
            return ParsedTrigger(
                source="us-pr-scanner",
                data_type="press_releases",
                ticker_direct=parts[3],
                release_id=parts[4],
            )
        except IndexError:
            return None

    # data/news/{date}/digest/{hour}.yaml
    if key.startswith("data/news/") and "/digest/" in key and key.endswith(".yaml"):
        try:
            return ParsedTrigger(
                source="news-scanner",
                data_type="news",
                date=parts[2],
            )
        except IndexError:
            return None

    return None


def _read_form_type_from_extracted(key: str) -> str:
    """Read form_type from an extracted.json file in S3."""
    try:
        data = _read_json(S3_BUCKET, key)
        return data.get("form_type", "")
    except Exception:
        return ""


def _resolve_tickers(bucket: str, key: str, parsed: ParsedTrigger) -> list[str]:
    """Resolve tickers from the event source."""
    # PR scanners have ticker directly in the path
    if parsed.ticker_direct:
        return [parsed.ticker_direct]

    # Filing scanners: resolve CIK -> ticker via registry
    if parsed.cik:
        ticker = _resolve_cik_to_ticker(bucket, parsed.cik)
        if ticker:
            return [ticker]
        # Fall back to reading the extracted/analysis JSON itself
        try:
            data = _read_json(bucket, key)
            ticker = data.get("ticker")
            if ticker:
                return [ticker]
        except Exception:
            pass
        # Fall back to index.json
        try:
            prefix = key.rsplit("/", 1)[0]
            index_data = _read_json(bucket, f"{prefix}/index.json")
            ticker = index_data.get("ticker")
            if ticker:
                return [ticker]
        except Exception:
            pass
        return []

    # News scanner: parse digest for material tickers
    if parsed.source == "news-scanner":
        try:
            resp = _get_s3_client().get_object(Bucket=bucket, Key=key)
            content = resp["Body"].read().decode("utf-8")
            digest = yaml.safe_load(content) or {}
            tickers = []
            for item in digest.get("material", []):
                single_ticker = item.get("ticker")
                if isinstance(single_ticker, str) and single_ticker:
                    tickers.append(single_ticker)

                item_tickers = item.get("tickers", [])
                if isinstance(item_tickers, list):
                    tickers.extend(t for t in item_tickers if isinstance(t, str) and t)
            return list(set(tickers))
        except Exception:
            logger.exception(f"Failed to parse news digest: {key}")
            return []

    return []


def _resolve_cik_to_ticker(bucket: str, cik: str) -> str | None:
    """Resolve CIK to ticker using the ticker registry from S3 config."""
    try:
        resp = _get_s3_client().get_object(
            Bucket=bucket, Key=f"{S3_CONFIG_PREFIX}/ticker_registry.yaml"
        )
        content = resp["Body"].read().decode("utf-8")
        raw = yaml.safe_load(content) or {}
    except Exception:
        logger.warning("Failed to read ticker registry from S3")
        return None

    registry = TickerRegistry.model_validate(raw)

    # Normalize CIK for comparison (strip leading zeros)
    cik_normalized = cik.lstrip("0")
    for ticker_symbol, entry in registry.tickers.items():
        registry_cik = (entry.cik or "").lstrip("0")
        if registry_cik == cik_normalized:
            return ticker_symbol

    return None


def _load_monitor_registry(bucket: str) -> list[MonitorDefinition]:
    """Load all monitor definitions from S3 config/monitors/."""
    monitors: list[MonitorDefinition] = []
    prefix = f"{S3_CONFIG_PREFIX}/monitors/"
    try:
        paginator = _get_s3_client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith((".yaml", ".yml")):
                    continue
                try:
                    resp = _get_s3_client().get_object(Bucket=bucket, Key=key)
                    content = resp["Body"].read().decode("utf-8")
                    raw = yaml.safe_load(content)
                    if raw and isinstance(raw, dict):
                        # Use filename stem as monitor ID if not specified
                        if "id" not in raw:
                            raw["id"] = key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
                        # Build listen keys from the new schema
                        if "tickers" in raw and raw.get("type") == "filing":
                            listen = _build_listen_keys(raw)
                            raw["listen"] = listen
                        elif isinstance(raw.get("listen"), str):
                            raw["listen"] = [raw["listen"]]
                        monitors.append(MonitorDefinition.model_validate(raw))
                except Exception:
                    logger.warning(f"Failed to parse monitor: {key}")
    except Exception:
        logger.exception("Failed to list monitors from S3")

    return monitors


def _build_listen_keys(raw: dict) -> list[str]:
    """Build listen keys from the new monitor schema."""
    keys = []
    tickers = raw.get("tickers", [])
    filing_types = raw.get("filing_types", [])
    for ticker in tickers:
        if filing_types:
            for ft in filing_types:
                keys.append(f"{ticker}:filings:{ft}")
        else:
            keys.append(f"{ticker}:filings")
    return keys


def _match_monitors(
    ticker: str, data_type: str, monitors: list[MonitorDefinition]
) -> list[MonitorDefinition]:
    """Match monitors whose listen fields include {ticker}:{data_type}.

    Supports hierarchical matching:
      - "AGM:filings" matches any filing type (data_type="filings:10-K", etc.)
      - "AGM:filings:10-K" matches only 10-K
      - "*:filings" matches any ticker's filings
    """
    matched = []

    for monitor in monitors:
        for listen in monitor.listen:
            if _listen_entry_matches(listen, ticker, data_type):
                matched.append(monitor)
                break

    return matched


def _listen_entry_matches(listen: str, ticker: str, data_type: str) -> bool:
    """Check if a single listen entry matches a ticker + data_type."""
    listen_parts = listen.split(":", 1)
    if len(listen_parts) != 2:
        return False

    listen_ticker, listen_dtype = listen_parts

    # Ticker match (exact or wildcard)
    ticker_matches = listen_ticker == ticker or listen_ticker == "*"
    if not ticker_matches:
        return False

    # Data type match (exact or hierarchical prefix)
    # "filings" matches "filings:8-K", "filings:10-K", etc.
    if listen_dtype == data_type:
        return True
    if data_type.startswith(f"{listen_dtype}:"):
        return True
    if listen_dtype.startswith(f"{data_type}:"):
        # More specific listen matches broader data_type (shouldn't normally happen)
        return False

    return False


def _emit_event_record(bucket: str, event_record: EventRecord) -> None:
    """Write event record to data/events/{date}/{event_id}.json."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"{S3_DATA_PREFIX}/events/{date_str}/{event_record.event_id}.json"
    try:
        _get_s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=event_record.model_dump_json(indent=2),
            ContentType="application/json",
        )
        logger.info(f"Event record emitted: {key}")
    except Exception:
        logger.exception(f"Failed to emit event record: {key}")


def _invoke_monitor_collector(
    monitor: MonitorDefinition,
    event_record: EventRecord,
    parsed: ParsedTrigger,
) -> None:
    """Invoke the monitor evaluator Lambda for a matched monitor."""
    # Use the specific collector_lambda if set, otherwise use the shared evaluator
    function_name = monitor.collector_lambda or MONITOR_EVALUATOR_LAMBDA
    if not function_name:
        logger.debug(f"Monitor {monitor.id} has no collector_lambda, skipping invoke")
        return

    payload = {
        "trigger_type": "event",
        "trigger_sources": [f"{event_record.ticker}:{event_record.data_type}"],
        "monitor_id": monitor.id,
        "event_data": {
            "event_id": event_record.event_id,
            "s3_path": event_record.s3_path,
            "ticker": event_record.ticker,
            "cik": event_record.cik,
            "data_type": event_record.data_type,
            "form_type": parsed.form_type or "",
        },
    }

    try:
        _get_lambda_client().invoke(
            FunctionName=function_name,
            InvocationType="Event",  # async
            Payload=json.dumps(payload),
        )
        logger.info(f"Invoked collector {function_name} for monitor {monitor.id}")
    except Exception:
        logger.exception(f"Failed to invoke collector for monitor {monitor.id}")


def _invoke_filing_analyzer(bucket: str, key: str) -> None:
    """Invoke the filing analyzer for canonical filings extracted events."""
    if not FILING_ANALYZER_LAMBDA:
        return
    payload = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                }
            }
        ]
    }
    try:
        _get_lambda_client().invoke(
            FunctionName=FILING_ANALYZER_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps(payload),
        )
        logger.info(f"Invoked filing analyzer {FILING_ANALYZER_LAMBDA} for {key}")
    except Exception:
        logger.exception(f"Failed to invoke filing analyzer for {key}")


def _read_json(bucket: str, key: str) -> dict:
    resp = _get_s3_client().get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read())
