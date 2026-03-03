"""Alert creation and storage to S3."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import boto3
import yaml

from src.modules.manage.models import Alert

logger = logging.getLogger(__name__)

BUCKET = "praxis-copilot"


def store_alerts(s3_client: boto3.client, alerts: list[Alert]) -> list[str]:
    """Write alerts to S3 at data/manage/{ticker}/{date}/alerts.yaml.

    Groups alerts by ticker and writes one file per ticker per day.
    Returns list of S3 keys written.
    """
    if not alerts:
        return []

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Group alerts by ticker
    by_ticker: dict[str, list[Alert]] = {}
    for alert in alerts:
        by_ticker.setdefault(alert.ticker, []).append(alert)

    keys_written: list[str] = []

    for ticker, ticker_alerts in by_ticker.items():
        key = f"data/manage/{ticker}/{date_str}/alerts.yaml"

        # Load existing alerts for this ticker/date to append
        existing_alerts = _load_existing_alerts(s3_client, key)
        all_alerts = existing_alerts + [_alert_to_dict(a) for a in ticker_alerts]

        body = yaml.dump(
            {"alerts": all_alerts},
            default_flow_style=False,
            sort_keys=False,
        )

        try:
            s3_client.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/x-yaml",
            )
            logger.info("Wrote %d alerts to %s", len(ticker_alerts), key)
            keys_written.append(key)
        except Exception as e:
            logger.error("Failed to write alerts to %s: %s", key, e)

    return keys_written


def _load_existing_alerts(s3_client: boto3.client, key: str) -> list[dict]:
    """Load existing alerts from S3, returning empty list if not found."""
    try:
        resp = s3_client.get_object(Bucket=BUCKET, Key=key)
        content = resp["Body"].read().decode("utf-8")
        data = yaml.safe_load(content) or {}
        return data.get("alerts", [])
    except s3_client.exceptions.NoSuchKey:
        return []
    except Exception:
        return []


def _alert_to_dict(alert: Alert) -> dict:
    """Convert Alert model to a serializable dict."""
    return {
        "ticker": alert.ticker,
        "timestamp": alert.timestamp.isoformat(),
        "alert_type": alert.alert_type.value,
        "severity": alert.severity.value,
        "details": alert.details,
    }
