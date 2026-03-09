"""Threshold checker: valuation anchor checks and config loading from S3."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import boto3
import yaml
from botocore.exceptions import ClientError

from .models import (
    Alert,
    AlertType,
    ManageConfig,
    PriceData,
    Severity,
    ValuationAnchors,
)

logger = logging.getLogger(__name__)

BUCKET = "praxis-copilot"


def load_valuation_anchors(s3_client: boto3.client, ticker: str) -> ValuationAnchors | None:
    """Load valuation anchors from data/research/{ticker}/memo.yaml on S3."""
    key = f"data/research/{ticker}/memo.yaml"
    try:
        resp = s3_client.get_object(Bucket=BUCKET, Key=key)
        content = resp["Body"].read().decode("utf-8")
        memo = yaml.safe_load(content) or {}
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.info("No memo.yaml found for %s", ticker)
        else:
            logger.warning("Failed to load memo.yaml for %s: %s", ticker, e)
        return None
    except Exception as e:
        logger.warning("Failed to parse memo.yaml for %s: %s", ticker, e)
        return None

    # Extract valuation section from memo
    valuation = memo.get("valuation", {})
    if not valuation:
        logger.info("No valuation section in memo.yaml for %s", ticker)
        return None

    entry_range = valuation.get("entry_range", {})
    exit_range = valuation.get("exit_range", {})

    return ValuationAnchors(
        fair_value_estimate=valuation.get("fair_value_estimate"),
        entry_price=entry_range.get("low") if isinstance(entry_range, dict) else None,
        entry_range_low=entry_range.get("low") if isinstance(entry_range, dict) else None,
        entry_range_high=entry_range.get("high") if isinstance(entry_range, dict) else None,
        exit_range_low=exit_range.get("low") if isinstance(exit_range, dict) else None,
        exit_range_high=exit_range.get("high") if isinstance(exit_range, dict) else None,
        stop_loss=valuation.get("stop_loss"),
        target_price=valuation.get("target_price"),
    )


def load_manage_config(s3_client: boto3.client) -> tuple[ManageConfig, dict]:
    """Load manage config and overrides from S3 config/manage.yaml.

    Returns (config, overrides_dict).
    """
    try:
        resp = s3_client.get_object(Bucket=BUCKET, Key="config/manage.yaml")
        content = resp["Body"].read().decode("utf-8")
        raw = yaml.safe_load(content) or {}
    except Exception as e:
        logger.warning("Failed to load manage.yaml, using defaults: %s", e)
        return ManageConfig(), {}

    defaults = raw.get("defaults", {})
    overrides = raw.get("overrides", {}) or {}

    # Support legacy key name
    if "price_move_pct" in defaults and "move_from_close_pct" not in defaults:
        defaults["move_from_close_pct"] = defaults.pop("price_move_pct")

    return ManageConfig(**{
        k: v for k, v in defaults.items()
        if k in ManageConfig.model_fields
    }), overrides


def check_valuation_anchors(
    price_data: PriceData,
    anchors: ValuationAnchors,
) -> list[Alert]:
    """Check price against valuation anchors (stop loss, target, entry/exit)."""
    alerts: list[Alert] = []
    now = datetime.now(timezone.utc)

    # Stop loss breach
    if anchors.stop_loss is not None and price_data.price <= anchors.stop_loss:
        alerts.append(Alert(
            ticker=price_data.ticker,
            timestamp=now,
            alert_type=AlertType.STOP_LOSS_BREACH,
            severity=Severity.CRITICAL,
            details={
                "price": price_data.price,
                "stop_loss": anchors.stop_loss,
            },
        ))

    # Target price reached
    target = anchors.target_price or anchors.fair_value_estimate
    if target is not None and price_data.price >= target:
        alerts.append(Alert(
            ticker=price_data.ticker,
            timestamp=now,
            alert_type=AlertType.TARGET_REACHED,
            severity=Severity.HIGH,
            details={
                "price": price_data.price,
                "target_price": target,
            },
        ))

    # Entry opportunity (price drops into or below entry range)
    entry_low = anchors.entry_range_low or anchors.entry_price
    if entry_low is not None and price_data.price <= entry_low:
        alerts.append(Alert(
            ticker=price_data.ticker,
            timestamp=now,
            alert_type=AlertType.ENTRY_OPPORTUNITY,
            severity=Severity.HIGH,
            details={
                "price": price_data.price,
                "entry_range_low": entry_low,
                "entry_range_high": anchors.entry_range_high,
            },
        ))

    # Exit signal (price moves above exit range)
    if anchors.exit_range_high is not None and price_data.price >= anchors.exit_range_high:
        alerts.append(Alert(
            ticker=price_data.ticker,
            timestamp=now,
            alert_type=AlertType.EXIT_SIGNAL,
            severity=Severity.HIGH,
            details={
                "price": price_data.price,
                "exit_range_high": anchors.exit_range_high,
            },
        ))

    return alerts


def check_thresholds(
    price_data: PriceData,
    config: ManageConfig,
    anchors: ValuationAnchors | None,
    ticker_overrides: dict | None = None,
) -> list[Alert]:
    """Legacy stateless threshold check (used by CLI delayed scan)."""
    alerts: list[Alert] = []
    now = datetime.now(timezone.utc)

    price_threshold = config.move_from_close_pct
    volume_threshold = config.volume_anomaly_multiplier
    if ticker_overrides:
        price_threshold = ticker_overrides.get("move_from_close_pct", price_threshold)
        # Support legacy key
        price_threshold = ticker_overrides.get("price_move_pct", price_threshold)
        volume_threshold = ticker_overrides.get("volume_anomaly_multiplier", volume_threshold)

    if abs(price_data.change_pct) >= price_threshold:
        direction = "up" if price_data.change_pct > 0 else "down"
        alert_type = AlertType.PRICE_BREACH_UP if direction == "up" else AlertType.PRICE_BREACH_DOWN
        severity = Severity.HIGH if abs(price_data.change_pct) >= price_threshold * 2 else Severity.MEDIUM
        alerts.append(Alert(
            ticker=price_data.ticker,
            timestamp=now,
            alert_type=alert_type,
            severity=severity,
            details={
                "change_pct": price_data.change_pct,
                "threshold_pct": price_threshold,
                "price": price_data.price,
                "previous_close": price_data.previous_close,
            },
        ))

    if price_data.volume_ratio >= volume_threshold:
        severity = Severity.HIGH if price_data.volume_ratio >= volume_threshold * 2 else Severity.MEDIUM
        alerts.append(Alert(
            ticker=price_data.ticker,
            timestamp=now,
            alert_type=AlertType.VOLUME_SPIKE,
            severity=severity,
            details={
                "volume": price_data.volume,
                "adtv": price_data.adtv,
                "volume_ratio": price_data.volume_ratio,
                "threshold_multiplier": volume_threshold,
            },
        ))

    if anchors:
        alerts.extend(check_valuation_anchors(price_data, anchors))

    return alerts
