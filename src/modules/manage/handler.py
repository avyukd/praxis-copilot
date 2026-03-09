"""Manage module Lambda handler.

Two modes:
  - intraday: stateful threshold checks (zigzag reversals, stepped close
    moves, volume velocity/anomaly, valuation anchors).  Triggered by
    EventBridge every 15 minutes during market hours.
  - eod: end-of-day Sonnet contextual assessment.  Triggered once after
    market close.  Stubbed for now.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError

from .alerts import store_alerts
from .intraday import run_all_checks
from .models import Alert, IntradayTickerState, ManageConfig, ManageResult, PriceData
from .price import fetch_price_data_batch
from .state import load_intraday_state, save_intraday_state
from .thresholds import load_manage_config, load_valuation_anchors

logger = logging.getLogger(__name__)

BUCKET = "praxis-copilot"

_s3_client = None
_sns_client = None


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _get_sns_client():
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns")
    return _sns_client


def _load_universe(s3_client: boto3.client) -> list[str]:
    """Load ticker universe from S3 config/universe.yaml."""
    try:
        resp = s3_client.get_object(Bucket=BUCKET, Key="config/universe.yaml")
        content = resp["Body"].read().decode("utf-8")
        data = yaml.safe_load(content) or {}
        tickers = data.get("tickers", [])
        return [t for t in tickers if isinstance(t, str)]
    except ClientError as e:
        logger.error("Failed to load universe.yaml: %s", e)
        return []


def _publish_alerts_sns(alerts: list[Alert]) -> None:
    """Publish alerts to SNS for email delivery."""
    topic_arn = os.environ.get("SNS_TOPIC_ARN", "").strip()
    if not topic_arn or not alerts:
        return

    sns = _get_sns_client()

    # Group alerts by ticker for a cleaner email
    by_ticker: dict[str, list[Alert]] = {}
    for alert in alerts:
        by_ticker.setdefault(alert.ticker, []).append(alert)

    lines = [f"Praxis Market Alerts — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"]
    lines.append("")

    for ticker, ticker_alerts in sorted(by_ticker.items()):
        for a in ticker_alerts:
            lines.append(f"[{a.severity.value.upper()}] {ticker} {a.alert_type.value}")
            for k, v in a.details.items():
                lines.append(f"  {k}: {v}")
            lines.append("")

    subject = f"Praxis: {len(alerts)} alert(s) across {len(by_ticker)} ticker(s)"
    message = "\n".join(lines)

    try:
        sns.publish(
            TopicArn=topic_arn,
            Subject=subject[:100],
            Message=message,
        )
        logger.info("Published %d alerts to SNS", len(alerts))
    except Exception as e:
        logger.error("Failed to publish to SNS: %s", e)


def lambda_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Lambda entry point. Dispatches to intraday or eod mode."""
    logging.basicConfig(level=logging.INFO)

    mode = event.get("mode", "intraday")

    if mode == "intraday":
        return _handle_intraday()
    elif mode == "eod":
        return _handle_eod()
    else:
        logger.error("Unknown mode: %s", mode)
        return ManageResult(mode=mode, errors=[f"Unknown mode: {mode}"]).model_dump()


def _handle_intraday() -> dict[str, Any]:
    """Intraday pass: stateful threshold checks with batch price fetching."""
    s3 = _get_s3_client()

    tickers = _load_universe(s3)
    if not tickers:
        logger.info("No tickers in universe, nothing to check")
        return ManageResult(mode="intraday").model_dump()

    config, overrides = load_manage_config(s3)
    intraday_state = load_intraday_state(s3)

    # Batch fetch all prices at once (3 API calls for 120 tickers vs 120)
    price_map = fetch_price_data_batch(tickers)

    all_alerts: list[Alert] = []
    errors: list[str] = []

    for ticker in tickers:
        try:
            price_data = price_map.get(ticker)
            if price_data is None:
                logger.warning("No price data for %s, skipping", ticker)
                continue

            logger.info(
                "%s: price=%.2f change=%.2f%% vol_ratio=%.2f",
                ticker, price_data.price, price_data.change_pct, price_data.volume_ratio,
            )

            anchors = load_valuation_anchors(s3, ticker)
            ticker_overrides = overrides.get(ticker, {})

            # Get or create per-ticker state
            if ticker not in intraday_state.tickers:
                intraday_state.tickers[ticker] = IntradayTickerState()
            ticker_state = intraday_state.tickers[ticker]

            alerts = run_all_checks(
                price_data, config, anchors, ticker_state, ticker_overrides,
            )

            if alerts:
                logger.info("%s: %d alerts triggered", ticker, len(alerts))
                all_alerts.extend(alerts)

        except Exception as e:
            msg = f"Error checking {ticker}: {e}"
            logger.error(msg)
            errors.append(msg)

    # Persist state, store alerts, notify
    try:
        save_intraday_state(s3, intraday_state)
    except Exception as e:
        errors.append(f"Failed to save intraday state: {e}")

    if all_alerts:
        try:
            store_alerts(s3, all_alerts)
        except Exception as e:
            errors.append(f"Failed to store alerts: {e}")

        _publish_alerts_sns(all_alerts)

    result = ManageResult(
        mode="intraday",
        tickers_checked=len(tickers),
        alerts_generated=len(all_alerts),
        errors=errors,
    )
    logger.info(
        "Intraday complete: %d checked, %d alerts, %d errors",
        result.tickers_checked, result.alerts_generated, len(result.errors),
    )
    return result.model_dump()


def _handle_eod() -> dict[str, Any]:
    """EOD pass: Sonnet contextual assessment. Stubbed for now."""
    s3 = _get_s3_client()

    tickers = _load_universe(s3)
    logger.info("EOD assessment stub invoked for %d tickers", len(tickers))

    result = ManageResult(
        mode="eod",
        tickers_checked=len(tickers),
        alerts_generated=0,
        errors=[],
    )
    logger.info("EOD stub complete: %d tickers in universe", len(tickers))
    return result.model_dump()
