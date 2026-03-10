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
UNIVERSE_KEY = "config/universe.yaml"


def _load_universe(s3_client: boto3.client) -> set[str]:
    """Load active ticker universe from S3. Returns empty set on failure (runs all)."""
    try:
        obj = s3_client.get_object(Bucket=BUCKET, Key=UNIVERSE_KEY)
        data = yaml.safe_load(obj["Body"].read().decode())
        tickers = data.get("tickers", []) if isinstance(data, dict) else []
        universe = {t.upper() for t in tickers if isinstance(t, str)}
        logger.info("Loaded universe with %d tickers", len(universe))
        return universe
    except ClientError:
        logger.warning("Failed to load universe from %s, running all monitors", UNIVERSE_KEY)
        return set()


def _filter_by_universe(configs: list[MonitorConfig], universe: set[str]) -> list[MonitorConfig]:
    """Keep only monitors where at least one ticker is in the universe."""
    if not universe:
        return configs
    filtered = [c for c in configs if not c.tickers or any(t.upper() in universe for t in c.tickers)]
    skipped = len(configs) - len(filtered)
    if skipped:
        logger.info("Skipped %d monitors for tickers not in universe", skipped)
    return filtered


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
) -> tuple[list[MonitorConfig], dict[str, snapshot.MonitorSnapshot | None]]:
    """Filter monitors based on trigger type, sources, and cadence.

    Returns (configs_to_run, {monitor_id: previous_snapshot}) so callers
    can reuse the snapshots already loaded for cadence checks.

    For event triggers: match filing monitors whose listen_keys overlap with trigger_sources.
    For scheduled triggers: return scraper and search monitors whose cadence has elapsed.
    """
    prev_snapshots: dict[str, snapshot.MonitorSnapshot | None] = {}

    if trigger_type == "event" and trigger_sources:
        source_set = set(trigger_sources)
        matched = [
            c for c in configs
            if c.type == "filing" and bool(set(c.listen_keys) & source_set)
        ]
        return matched, prev_snapshots

    # Scheduled run: run all search monitors (EventBridge schedule is the cadence).
    # Scraper monitors still use per-monitor cadence gating.
    search_monitors = [c for c in configs if c.type == "search"]
    scraper_candidates = [c for c in configs if c.type == "scraper"]

    due: list[MonitorConfig] = list(search_monitors)

    if not s3_client or not now:
        return due + scraper_candidates, prev_snapshots

    for config in scraper_candidates:
        cadence_hours = cadence_to_hours(config.cadence, config.frequency)
        prev = snapshot.load_previous_snapshot(s3_client, config.id)
        prev_snapshots[config.id] = prev
        if prev is None:
            due.append(config)
            continue
        try:
            date_fmt = "%Y-%m-%dT%H:%M:%S" if "T" in prev.date else "%Y-%m-%d"
            last_run = datetime.strptime(prev.date, date_fmt).replace(tzinfo=timezone.utc)
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
    return due, prev_snapshots


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entry point.

    Event payload:
      - trigger_type: "event" | "scheduled" (defaults to "scheduled")
      - trigger_sources: list of data source keys like ["NVDA:filings:8-K"]
      - event_data: dict with s3_path, ticker, form_type etc. (for filing triggers)
      - monitor_id: optional, to evaluate a single specific monitor
    """
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%dT%H:%M:%S")

    trigger_type = event.get("trigger_type", "scheduled")
    trigger_sources = event.get("trigger_sources", [])
    event_data = event.get("event_data") or event.get("event")

    s3 = boto3.client("s3")

    # Load all monitor configs, filtered to active universe
    all_configs = _load_monitor_configs(s3)
    universe = _load_universe(s3)
    all_configs = _filter_by_universe(all_configs, universe)
    logger.info("Loaded %d monitor configs (after universe filter)", len(all_configs))

    # If a specific monitor is requested, evaluate only that one
    monitor_id = event.get("monitor_id")
    cached_snapshots: dict[str, snapshot.MonitorSnapshot | None] = {}
    if monitor_id:
        configs = [c for c in all_configs if c.id == monitor_id]
    else:
        configs, cached_snapshots = _filter_monitors(all_configs, trigger_type, trigger_sources, s3_client=s3, now=now)

    logger.info(
        "Evaluating %d monitors (trigger_type=%s, sources=%s)",
        len(configs),
        trigger_type,
        trigger_sources,
    )

    result = EvaluatorResult(monitors_evaluated=len(configs))

    for config in configs:
        try:
            # Reuse snapshot from cadence check if available, else load
            if config.id in cached_snapshots:
                previous = cached_snapshots[config.id]
            else:
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
