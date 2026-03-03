"""SMS/email alerting via AWS SNS."""
from __future__ import annotations

import json
import logging

import boto3

from src.modules.events.eight_k_scanner.config import SNS_TOPIC_ARN

logger = logging.getLogger(__name__)

_sns_client = None


def _get_sns_client():
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns")
    return _sns_client


def _alert_source_labels(index_data: dict) -> tuple[str, str]:
    exchange = (index_data.get("exchange") or "").strip().upper() or "N/A"
    feed_source = (index_data.get("source") or "").strip().upper()

    if index_data.get("accession_number"):
        source_label = "8-K"
        if feed_source:
            source_label = f"{source_label} ({feed_source})"
        else:
            source_label = "8-K (SEC)"
        return source_label, exchange

    if index_data.get("release_id"):
        if exchange in {"TSX", "TSXV"}:
            kind = "CA PR"
        elif exchange in {"NYSE", "NASDAQ", "AMEX"}:
            kind = "US PR"
        else:
            kind = "PR"
        source_label = f"{kind} ({feed_source})" if feed_source else kind
        return source_label, exchange

    if feed_source:
        return feed_source, exchange
    return "Unknown", exchange


def send_alert(ticker: str, analysis: dict, index_data: dict, warnings: list[str] | None = None) -> bool:
    """Publish an alert to the SNS topic. Returns True if sent."""
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not set, skipping alert")
        return False

    warnings = warnings or []
    mcap = index_data.get("market_cap")
    mcap_str = f"${mcap / 1e6:.0f}M" if mcap else "N/A"
    items_list = index_data.get("items_detected") or analysis.get("items_detected") or []
    items = ", ".join(items_list)
    filed = index_data.get("acceptance_datetime", "N/A")
    classification = analysis.get("classification", "NEUTRAL")
    magnitude = analysis.get("magnitude", "?")
    source_label, exchange_label = _alert_source_labels(index_data)

    message_lines = [
        f"{classification} ALERT: {ticker} (mag={magnitude})",
        f"Source: {source_label}",
        f"Exchange: {exchange_label}",
        f"Items: {items or 'N/A'}",
        f"Mcap: {mcap_str}",
        f"Filed: {filed}",
        "",
        f"Analysis: {analysis.get('explanation', '')}",
        "",
        f"Analysis JSON: {json.dumps(analysis, separators=(',', ':'))}",
    ]

    if warnings:
        message_lines.insert(4, f"Warnings: {'; '.join(warnings)}")

    message = "\n".join(message_lines)

    try:
        _get_sns_client().publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject=f"8K {classification}: {ticker}",
        )
        logger.info(f"Alert sent for {ticker}")
        return True
    except Exception:
        logger.exception(f"Failed to send alert for {ticker}")
        return False
