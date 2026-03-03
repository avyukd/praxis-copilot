"""Snapshot artifact creation and S3 storage."""
from __future__ import annotations

import logging
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError

from .models import MonitorSnapshot

logger = logging.getLogger(__name__)

BUCKET = "praxis-copilot"
MONITORS_PREFIX = "data/monitors"


def load_previous_snapshot(
    s3_client: boto3.client, monitor_id: str
) -> MonitorSnapshot | None:
    """Load the latest snapshot for a monitor from S3.

    Returns None if no previous snapshot exists.
    """
    key = f"{MONITORS_PREFIX}/{monitor_id}/latest.yaml"
    try:
        obj = s3_client.get_object(Bucket=BUCKET, Key=key)
        raw = yaml.safe_load(obj["Body"].read().decode()) or {}
        return MonitorSnapshot(**raw)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            logger.info("No previous snapshot for %s", monitor_id)
        else:
            logger.warning("S3 error loading snapshot for %s: %s", monitor_id, code)
        return None
    except (yaml.YAMLError, ValueError) as e:
        logger.warning("Failed to parse previous snapshot for %s: %s", monitor_id, e)
        return None


def build_snapshot(
    monitor_id: str,
    date_str: str,
    collected_data: dict[str, Any],
    previous: MonitorSnapshot | None,
) -> MonitorSnapshot:
    """Build a snapshot from collected data and previous snapshot.

    The collector populates collected_data with keys like:
      source, current_state, significance, delta_from_previous, status
    """
    previous_data = previous.current_state if previous else None

    return MonitorSnapshot(
        monitor_id=monitor_id,
        date=date_str,
        source=collected_data.get("source", ""),
        status=collected_data.get("status", "unchanged"),
        current_state=collected_data.get("current_state", ""),
        delta_from_previous=collected_data.get("delta_from_previous", ""),
        significance=collected_data.get("significance", "low"),
        previous_data=previous_data,
    )


def store_snapshot(
    s3_client: boto3.client, snapshot: MonitorSnapshot
) -> str:
    """Write snapshot to S3 as dated file and update latest.yaml.

    Returns the S3 key of the dated snapshot.
    """
    body = yaml.dump(
        snapshot.model_dump(), default_flow_style=False, allow_unicode=True
    )
    dated_key = f"{MONITORS_PREFIX}/{snapshot.monitor_id}/{snapshot.date}.yaml"
    latest_key = f"{MONITORS_PREFIX}/{snapshot.monitor_id}/latest.yaml"

    s3_client.put_object(Bucket=BUCKET, Key=dated_key, Body=body.encode())
    s3_client.put_object(Bucket=BUCKET, Key=latest_key, Body=body.encode())

    logger.info("Stored snapshot: %s", dated_key)
    return dated_key
