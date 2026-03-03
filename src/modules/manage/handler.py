"""Manage module Lambda handler.

Two modes:
  - intraday: deterministic threshold checks, no LLM. Triggered by EventBridge
    every N minutes during market hours.
  - eod: end-of-day Sonnet contextual assessment. Triggered once after market close.
    Stubbed for now.
"""
from __future__ import annotations

import logging
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError

from src.modules.manage.alerts import store_alerts
from src.modules.manage.models import Alert, ManageConfig, ManageResult
from src.modules.manage.price import fetch_price_data
from src.modules.manage.thresholds import (
    check_thresholds,
    load_manage_config,
    load_ticker_overrides,
    load_valuation_anchors,
)

logger = logging.getLogger(__name__)

BUCKET = "praxis-copilot"

_s3_client = None


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


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
    """Intraday pass: deterministic threshold checks, no LLM."""
    s3 = _get_s3_client()

    tickers = _load_universe(s3)
    if not tickers:
        logger.info("No tickers in universe, nothing to check")
        return ManageResult(mode="intraday").model_dump()

    config = load_manage_config(s3)
    all_alerts: list[Alert] = []
    errors: list[str] = []

    for ticker in tickers:
        try:
            # Fetch price data
            price_data = fetch_price_data(ticker)
            logger.info(
                "%s: price=%.2f change=%.2f%% vol_ratio=%.2f",
                ticker, price_data.price, price_data.change_pct, price_data.volume_ratio,
            )

            # Load valuation anchors (may be None if no memo exists)
            anchors = load_valuation_anchors(s3, ticker)

            # Load per-ticker overrides
            overrides = load_ticker_overrides(s3, ticker)

            # Check thresholds
            alerts = check_thresholds(price_data, config, anchors, overrides)

            if alerts:
                logger.info("%s: %d alerts triggered", ticker, len(alerts))
                all_alerts.extend(alerts)

        except Exception as e:
            msg = f"Error checking {ticker}: {e}"
            logger.error(msg)
            errors.append(msg)

    # Store alerts to S3
    if all_alerts:
        try:
            store_alerts(s3, all_alerts)
        except Exception as e:
            errors.append(f"Failed to store alerts: {e}")

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

    # TODO: Implement EOD Sonnet assessment
    # 1. Load day's intraday alerts for each ticker
    # 2. Load memo.yaml thesis context
    # 3. Load recent monitor snapshots
    # 4. Call Sonnet to classify and contextualize
    # 5. Produce assessment artifact and SNS notification

    result = ManageResult(
        mode="eod",
        tickers_checked=len(tickers),
        alerts_generated=0,
        errors=[],
    )
    logger.info("EOD stub complete: %d tickers in universe", len(tickers))
    return result.model_dump()
