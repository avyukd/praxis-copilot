"""SNS alerting for monitor snapshots."""
from __future__ import annotations

import logging
import os

import boto3

logger = logging.getLogger(__name__)

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")

_sns_client = None


def _get_sns_client():
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns")
    return _sns_client


def send_monitor_alert(
    monitor_id: str,
    description: str,
    tickers: list[str],
    significance: str,
    delta_summary: str,
    current_state: str,
) -> bool:
    """Publish a monitor alert to SNS. Returns True if sent."""
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not set, skipping monitor alert")
        return False

    tickers_str = ", ".join(tickers)
    message_lines = [
        f"MONITOR ALERT: {monitor_id} [{significance.upper()}]",
        f"Tickers: {tickers_str}",
        f"Description: {description}",
        "",
        f"What changed:",
        delta_summary or "(no delta summary)",
        "",
        f"Current state:",
        current_state[:2000] if current_state else "(empty)",
    ]

    message = "\n".join(message_lines)

    try:
        _get_sns_client().publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject=f"Monitor {significance.upper()}: {monitor_id} ({tickers_str})",
        )
        logger.info("Monitor alert sent for %s", monitor_id)
        return True
    except Exception:
        logger.exception("Failed to send monitor alert for %s", monitor_id)
        return False
