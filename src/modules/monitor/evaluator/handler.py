"""Monitor Evaluator Lambda handler.

Triggered by EventBridge (daily schedule) or by event dispatch (reactive).
Loads monitor configs from S3, runs collectors, writes snapshot artifacts.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError

from src.modules.monitor.evaluator import collector, snapshot
from src.modules.monitor.evaluator.alerts import send_monitor_alert
from src.modules.monitor.evaluator.models import EvaluatorResult, MonitorConfig, cadence_to_hours

logger = logging.getLogger(__name__)

BUCKET = os.environ.get("S3_BUCKET", "praxis-copilot")
CONFIG_PREFIX = "config/monitors/"


def _load_monitor_configs(s3_client: boto3.client) -> list[MonitorConfig]:
    """Load all monitor config YAML files from S3 config/monitors/."""
    configs: list[MonitorConfig] = []
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=CONFIG_PREFIX):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".yaml"):
                    continue
                try:
                    raw = s3_client.get_object(Bucket=BUCKET, Key=key)
                    data = yaml.safe_load(raw["Body"].read().decode())
                    if data is None:
                        continue
                    if isinstance(data, list):
                        for entry in data:
                            configs.append(MonitorConfig(**entry))
                    elif isinstance(data, dict):
                        configs.append(MonitorConfig(**data))
                except (ClientError, yaml.YAMLError, ValueError) as e:
                    logger.warning("Failed to load monitor config %s: %s", key, e)
    except ClientError as e:
        logger.error("Failed to list monitor configs: %s", e)
    return configs


def _filter_monitors(
    configs: list[MonitorConfig],
    trigger_type: str | None,
    trigger_sources: list[str] | None,
    s3_client: boto3.client | None = None,
    now: datetime | None = None,
) -> list[MonitorConfig]:
    """Filter monitors based on trigger type, sources, and cadence.

    For event triggers: match filing monitors whose listen_keys overlap with trigger_sources.
    For scheduled triggers: return scraper and search monitors whose cadence has elapsed.
    """
    if trigger_type == "event" and trigger_sources:
        source_set = set(trigger_sources)
        return [
            c for c in configs
            if c.type == "filing" and bool(set(c.listen_keys) & source_set)
        ]

    # Scheduled run: filter scraper/search monitors by cadence
    candidates = [c for c in configs if c.type in ("scraper", "search")]
    if not s3_client or not now:
        return candidates

    due: list[MonitorConfig] = []
    for config in candidates:
        cadence_hours = cadence_to_hours(config.cadence, config.frequency)
        prev = snapshot.load_previous_snapshot(s3_client, config.id)
        if prev is None:
            due.append(config)
            continue
        try:
            last_run = datetime.strptime(prev.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            hours_since = (now - last_run).total_seconds() / 3600
            if hours_since >= cadence_hours:
                due.append(config)
            else:
                logger.debug(
                    "Skipping monitor %s: %.1fh since last run, cadence is %dh",
                    config.id, hours_since, cadence_hours,
                )
        except (ValueError, TypeError):
            due.append(config)
    return due


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entry point.

    Event payload:
      - trigger_type: "event" | "scheduled" (defaults to "scheduled")
      - trigger_sources: list of data source keys like ["NVDA:filings:8-K"]
      - event_data: dict with s3_path, ticker, form_type etc. (for filing triggers)
      - monitor_id: optional, to evaluate a single specific monitor
    """
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")

    trigger_type = event.get("trigger_type", "scheduled")
    trigger_sources = event.get("trigger_sources", [])
    event_data = event.get("event_data") or event.get("event")

    s3 = boto3.client("s3")

    # Load all monitor configs
    all_configs = _load_monitor_configs(s3)
    logger.info("Loaded %d monitor configs", len(all_configs))

    # If a specific monitor is requested, evaluate only that one
    monitor_id = event.get("monitor_id")
    if monitor_id:
        configs = [c for c in all_configs if c.id == monitor_id]
    else:
        configs = _filter_monitors(all_configs, trigger_type, trigger_sources, s3_client=s3, now=now)

    logger.info(
        "Evaluating %d monitors (trigger_type=%s, sources=%s)",
        len(configs),
        trigger_type,
        trigger_sources,
    )

    result = EvaluatorResult(monitors_evaluated=len(configs))

    for config in configs:
        try:
            # Load previous snapshot for delta detection
            previous = snapshot.load_previous_snapshot(s3, config.id)

            # Run collector with event data
            collected = collector.collect(config, previous, event_data=event_data)

            # Build and store snapshot
            snap = snapshot.build_snapshot(config.id, date_str, collected, previous)
            key = snapshot.store_snapshot(s3, snap)
            result.snapshots_written.append(key)

            # Alert if significant and updated
            if (
                snap.status == "updated"
                and snap.significance in ("medium", "high")
            ):
                sent = send_monitor_alert(
                    monitor_id=config.id,
                    description=config.description,
                    tickers=config.tickers,
                    significance=snap.significance,
                    delta_summary=snap.delta_from_previous,
                    current_state=snap.current_state,
                )
                if sent:
                    result.alerts_sent += 1

        except Exception as e:
            msg = f"Error evaluating monitor {config.id}: {e}"
            logger.exception(msg)
            result.errors.append(msg)

    logger.info(
        "Evaluator complete: %d evaluated, %d snapshots, %d alerts, %d errors",
        result.monitors_evaluated,
        len(result.snapshots_written),
        result.alerts_sent,
        len(result.errors),
    )

    return result.model_dump()
