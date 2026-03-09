"""S3-backed intraday state persistence for the manage module."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import boto3
import yaml
from botocore.exceptions import ClientError

from .models import IntradayState

logger = logging.getLogger(__name__)

BUCKET = "praxis-copilot"
STATE_KEY = "data/manage/intraday_state.yaml"


def load_intraday_state(s3_client: boto3.client) -> IntradayState:
    """Load intraday state from S3.  Returns fresh state if missing or stale."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    try:
        resp = s3_client.get_object(Bucket=BUCKET, Key=STATE_KEY)
        raw = yaml.safe_load(resp["Body"].read().decode("utf-8")) or {}
        state = IntradayState(**raw)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            logger.info("No existing intraday state, starting fresh")
        else:
            logger.warning("Failed to load intraday state: %s", e)
        return IntradayState(date=today)
    except Exception as e:
        logger.warning("Failed to parse intraday state: %s", e)
        return IntradayState(date=today)

    if state.date != today:
        logger.info("Intraday state is from %s, resetting for %s", state.date, today)
        return IntradayState(date=today)

    return state


def save_intraday_state(s3_client: boto3.client, state: IntradayState) -> None:
    """Write intraday state back to S3."""
    body = yaml.dump(
        state.model_dump(mode="json"),
        default_flow_style=False,
        sort_keys=False,
    )
    try:
        s3_client.put_object(
            Bucket=BUCKET,
            Key=STATE_KEY,
            Body=body.encode("utf-8"),
            ContentType="application/x-yaml",
        )
    except Exception as e:
        logger.error("Failed to save intraday state: %s", e)
        raise
