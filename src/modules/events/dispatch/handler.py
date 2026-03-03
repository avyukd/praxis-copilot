"""Event Dispatch Lambda: routes data from event-stage pipelines to monitors.

Triggered by S3 PUTs on:
  - data/raw/8k/{cik}/{accession}/analysis.json
  - data/raw/ca-pr/{ticker}/{release_id}/analysis.json
  - data/raw/us-pr/{ticker}/{release_id}/analysis.json
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

logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get("S3_BUCKET", "praxis-copilot")
S3_CONFIG_PREFIX = "config"
S3_DATA_PREFIX = "data"

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

    records = event.get("Records", [])
    dispatched = 0
    skipped = 0

    for record in records:
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", S3_BUCKET)
        key = s3_info.get("object", {}).get("key", "")

        parsed = _parse_trigger(key)
        if parsed is None:
            logger.debug(f"Ignoring unrecognized key: {key}")
            skipped += 1
            continue

        source = parsed["source"]
        data_type = parsed["data_type"]
        tickers = _resolve_tickers(bucket, key, parsed)

        if not tickers:
            logger.info(f"No tickers resolved for {key}")
            skipped += 1
            continue

        # Load monitor registry
        monitors = _load_monitor_registry(bucket)

        # Match monitors
        for ticker in tickers:
            matching_monitors = _match_monitors(ticker, data_type, monitors)
            if not matching_monitors:
                logger.debug(f"No monitors matched for {ticker}:{data_type}")
                continue

            event_id = f"evt-{uuid.uuid4().hex[:12]}"
            event_record = _build_event_record(
                event_id=event_id,
                source=source,
                ticker=ticker,
                cik=parsed.get("cik"),
                data_type=data_type,
                s3_path=key,
                monitors_triggered=[m["id"] for m in matching_monitors],
            )

            # Emit event record
            _emit_event_record(bucket, event_record)

            # Invoke matching monitors
            for monitor in matching_monitors:
                _invoke_monitor_collector(monitor, event_record)
                dispatched += 1

    logger.info(f"Dispatch complete: {dispatched} monitor invocations, {skipped} skipped")
    return {"dispatched": dispatched, "skipped": skipped}


def _parse_trigger(key: str) -> dict[str, Any] | None:
    """Parse an S3 key to determine source, data type, and identifiers."""
    parts = key.split("/")

    # data/raw/8k/{cik}/{accession}/analysis.json
    if "8k" in parts and key.endswith("/analysis.json"):
        try:
            idx = parts.index("8k")
            return {
                "source": "8k-scanner",
                "data_type": "filings",
                "cik": parts[idx + 1],
                "accession": parts[idx + 2],
            }
        except IndexError:
            return None

    # data/raw/ca-pr/{ticker}/{release_id}/analysis.json
    if "ca-pr" in parts and key.endswith("/analysis.json"):
        try:
            idx = parts.index("ca-pr")
            return {
                "source": "ca-pr-scanner",
                "data_type": "press_releases",
                "ticker_direct": parts[idx + 1],
                "release_id": parts[idx + 2],
            }
        except IndexError:
            return None

    # data/raw/us-pr/{ticker}/{release_id}/analysis.json
    if "us-pr" in parts and key.endswith("/analysis.json"):
        try:
            idx = parts.index("us-pr")
            return {
                "source": "us-pr-scanner",
                "data_type": "press_releases",
                "ticker_direct": parts[idx + 1],
                "release_id": parts[idx + 2],
            }
        except IndexError:
            return None

    # data/news/{date}/digest/{hour}.yaml
    if "digest" in parts and key.endswith(".yaml"):
        try:
            idx = parts.index("news")
            return {
                "source": "news-scanner",
                "data_type": "news",
                "date": parts[idx + 1],
            }
        except IndexError:
            return None

    return None


def _resolve_tickers(bucket: str, key: str, parsed: dict) -> list[str]:
    """Resolve tickers from the event source."""
    # PR scanners have ticker directly in the path
    if "ticker_direct" in parsed:
        return [parsed["ticker_direct"]]

    # 8k-scanner: resolve CIK -> ticker via registry
    if parsed.get("cik"):
        ticker = _resolve_cik_to_ticker(bucket, parsed["cik"])
        if ticker:
            return [ticker]
        # Fall back to reading the analysis.json itself
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
    if parsed["source"] == "news-scanner":
        try:
            resp = _get_s3_client().get_object(Bucket=bucket, Key=key)
            content = resp["Body"].read().decode("utf-8")
            digest = yaml.safe_load(content) or {}
            tickers = []
            for item in digest.get("material", []):
                item_tickers = item.get("tickers", [])
                tickers.extend(item_tickers)
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
        registry = yaml.safe_load(content) or {}
    except Exception:
        logger.warning("Failed to read ticker registry from S3")
        return None

    tickers = registry.get("tickers", {})
    # Normalize CIK for comparison (strip leading zeros)
    cik_normalized = cik.lstrip("0")
    for ticker_symbol, info in tickers.items():
        registry_cik = (info.get("cik") or "").lstrip("0")
        if registry_cik == cik_normalized:
            return ticker_symbol

    return None


def _load_monitor_registry(bucket: str) -> list[dict]:
    """Load all monitor definitions from S3 config/monitors/."""
    monitors = []
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
                    monitor = yaml.safe_load(content)
                    if monitor and isinstance(monitor, dict):
                        # Use filename stem as monitor ID if not specified
                        if "id" not in monitor:
                            monitor["id"] = key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
                        monitors.append(monitor)
                except Exception:
                    logger.warning(f"Failed to parse monitor: {key}")
    except Exception:
        logger.exception("Failed to list monitors from S3")

    return monitors


def _match_monitors(ticker: str, data_type: str, monitors: list[dict]) -> list[dict]:
    """Match monitors whose listen fields include {ticker}:{data_type}."""
    matched = []
    listen_key = f"{ticker}:{data_type}"

    for monitor in monitors:
        listen_fields = monitor.get("listen", [])
        if isinstance(listen_fields, str):
            listen_fields = [listen_fields]

        for listen in listen_fields:
            if listen == listen_key:
                matched.append(monitor)
                break
            # Also match wildcard patterns like *:filings
            if listen.startswith("*:") and listen.split(":", 1)[1] == data_type:
                matched.append(monitor)
                break

    return matched


def _build_event_record(
    event_id: str,
    source: str,
    ticker: str,
    cik: str | None,
    data_type: str,
    s3_path: str,
    monitors_triggered: list[str],
) -> dict:
    return {
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source": source,
        "ticker": ticker,
        "cik": cik,
        "data_type": data_type,
        "s3_path": s3_path,
        "monitors_triggered": monitors_triggered,
    }


def _emit_event_record(bucket: str, event_record: dict) -> None:
    """Write event record to data/events/{date}/{event_id}.json."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    event_id = event_record["event_id"]
    key = f"{S3_DATA_PREFIX}/events/{date_str}/{event_id}.json"
    try:
        _get_s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(event_record, indent=2),
            ContentType="application/json",
        )
        logger.info(f"Event record emitted: {key}")
    except Exception:
        logger.exception(f"Failed to emit event record: {key}")


def _invoke_monitor_collector(monitor: dict, event_record: dict) -> None:
    """Invoke a monitor's collector Lambda asynchronously."""
    collector_fn = monitor.get("collector_lambda")
    if not collector_fn:
        logger.debug(f"Monitor {monitor.get('id')} has no collector_lambda, skipping invoke")
        return

    payload = {
        "event": event_record,
        "monitor_id": monitor.get("id"),
    }

    try:
        _get_lambda_client().invoke(
            FunctionName=collector_fn,
            InvocationType="Event",  # async
            Payload=json.dumps(payload),
        )
        logger.info(f"Invoked collector {collector_fn} for monitor {monitor.get('id')}")
    except Exception:
        logger.exception(f"Failed to invoke collector for monitor {monitor.get('id')}")


def _read_json(bucket: str, key: str) -> dict:
    resp = _get_s3_client().get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read())
