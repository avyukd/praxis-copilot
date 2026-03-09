"""Optional alert delivery for CLI-originated market alerts."""
from __future__ import annotations

import json
import os

import boto3
import click


_sns_client = None


def _get_sns_client():
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns")
    return _sns_client


def build_cli_alert_payload(event: dict) -> tuple[str, str]:
    """Build SNS subject/message for a CLI market alert."""
    if event["kind"] == "default":
        subject = f"Market {event['alert_type']}: {event['ticker']}"
        message = "\n".join(
            [
                f"Default {event['source']} market alert",
                f"Ticker: {event['ticker']}",
                f"Type: {event['alert_type']}",
                f"Severity: {event['severity']}",
                "",
                json.dumps(event["details"], indent=2, sort_keys=True),
            ]
        )
    else:
        subject = f"Market custom {event['source']}: {event['ticker']}"
        message = "\n".join(
            [
                f"Custom {event['source']} market alert",
                f"Ticker: {event['ticker']}",
                f"Rule: {event['field']} {event['op']} {event['target']}",
                f"Actual: {event['actual']}",
                f"Cooldown: {event['cooldown_minutes']} minute(s)",
                f"Note: {event.get('note') or '-'}",
            ]
        )
    return subject[:100], message


def send_cli_alert(event: dict, *, dry_run: bool = False) -> bool:
    """Publish a market alert to SNS when configured."""
    subject, message = build_cli_alert_payload(event)

    if dry_run:
        click.echo("SNS dry run")
        click.echo(f"Subject: {subject}")
        click.echo(message)
        return True

    topic_arn = os.environ.get("SNS_TOPIC_ARN", "").strip()
    if not topic_arn:
        raise click.ClickException("SNS_TOPIC_ARN is not set.")

    _get_sns_client().publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message,
    )
    return True
