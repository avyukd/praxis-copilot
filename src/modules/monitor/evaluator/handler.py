"""Monitor Evaluator Lambda handler.

Triggered by EventBridge (daily schedule) or by event dispatch (reactive).
Loads monitor configs from S3, runs collectors, writes snapshot artifacts.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError

from . import collector, snapshot
from .models import EvaluatorResult, MonitorConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "praxis-copilot"
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
                    # Config file may contain a single monitor dict or a list
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
    configs: list[MonitorConfig], trigger_type: str | None, trigger_sources: list[str] | None
) -> list[MonitorConfig]:
    """Filter monitors based on trigger type and sources.

    For scheduled/periodic invocations (trigger_type is None or "scheduled"),
    return all monitors with matching trigger. For event-triggered invocations,
    filter to monitors whose listen fields overlap with trigger_sources.
    """
    if trigger_type == "event" and trigger_sources:
        return [
            c for c in configs
            if c.trigger == "event" and _listen_matches(c.listen, trigger_sources)
        ]

    # Daily scheduled run: process scheduled and periodic monitors
    return [c for c in configs if c.trigger in ("scheduled", "periodic")]


def _listen_matches(listen: list[str], sources: list[str]) -> bool:
    """Check if any listen entry matches any trigger source."""
    listen_set = set(listen)
    source_set = set(sources)
    return bool(listen_set & source_set)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entry point.

    Event payload:
      - trigger_type: "event" | "scheduled" (optional, defaults to "scheduled")
      - trigger_sources: list of data source keys like ["NVDA:8k"] (for event triggers)
    """
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")

    trigger_type = event.get("trigger_type", "scheduled")
    trigger_sources = event.get("trigger_sources", [])

    s3 = boto3.client("s3")

    # Load all monitor configs
    all_configs = _load_monitor_configs(s3)
    logger.info("Loaded %d monitor configs", len(all_configs))

    # Filter to relevant monitors for this invocation
    configs = _filter_monitors(all_configs, trigger_type, trigger_sources)
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

            # Run collector
            collected = collector.collect(config, previous)

            # Build and store snapshot
            snap = snapshot.build_snapshot(config.id, date_str, collected, previous)
            key = snapshot.store_snapshot(s3, snap)
            result.snapshots_written.append(key)

        except Exception as e:
            msg = f"Error evaluating monitor {config.id}: {e}"
            logger.exception(msg)
            result.errors.append(msg)

    logger.info(
        "Evaluator complete: %d evaluated, %d snapshots, %d errors",
        result.monitors_evaluated,
        len(result.snapshots_written),
        len(result.errors),
    )

    return result.model_dump()
