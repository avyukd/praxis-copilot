"""Threshold checker: valuation anchor checks and config loading from S3."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError

from .models import (
    Alert,
    AlertType,
    IntradayTickerState,
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

    # Handle both dict {"low": x, "high": y} and list [low, high] formats
    def _safe_float(v: Any) -> float | None:
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    if isinstance(entry_range, list) and len(entry_range) >= 2:
        entry_low, entry_high = _safe_float(entry_range[0]), _safe_float(entry_range[1])
    elif isinstance(entry_range, dict):
        entry_low = _safe_float(entry_range.get("low"))
        entry_high = _safe_float(entry_range.get("high"))
    else:
        entry_low, entry_high = None, None

    if isinstance(exit_range, list) and len(exit_range) >= 2:
        exit_low, exit_high = _safe_float(exit_range[0]), _safe_float(exit_range[1])
    elif isinstance(exit_range, dict):
        exit_low = _safe_float(exit_range.get("low"))
        exit_high = _safe_float(exit_range.get("high"))
    else:
        exit_low, exit_high = None, None

    return ValuationAnchors(
        fair_value_estimate=valuation.get("fair_value_estimate"),
        entry_price=entry_low,
        entry_range_low=entry_low,
        entry_range_high=entry_high,
        exit_range_low=exit_low,
        exit_range_high=exit_high,
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


def _classify_valuation_zone(
    price: float,
    anchors: ValuationAnchors,
) -> str:
    """Classify which valuation zone the current price falls into.

    Zones (checked in order from lowest to highest):
      below_stop  – price ≤ stop_loss
      deep_value  – price ≤ entry_range_low
      entry_range – entry_range_low < price ≤ entry_range_high
      fair_value  – between entry_range_high and exit_range_low
      exit_range  – exit_range_low ≤ price < exit_range_high
      overvalued  – price ≥ exit_range_high
      above_target – price ≥ target_price (if set and above exit_range_high)

    When boundaries are missing we collapse adjacent zones.
    """
    if anchors.stop_loss is not None and price <= anchors.stop_loss:
        return "below_stop"

    target = anchors.target_price or anchors.fair_value_estimate
    entry_low = anchors.entry_range_low or anchors.entry_price

    if entry_low is not None and price <= entry_low:
        return "deep_value"

    if anchors.entry_range_high is not None and price <= anchors.entry_range_high:
        return "entry_range"

    if anchors.exit_range_low is not None and price < anchors.exit_range_low:
        return "fair_value"

    if anchors.exit_range_high is not None and price < anchors.exit_range_high:
        return "exit_range"

    if target is not None and price >= target:
        return "above_target"

    if anchors.exit_range_high is not None and price >= anchors.exit_range_high:
        return "overvalued"

    return "fair_value"


# Map zone transitions to the alert type that should fire
_ZONE_ALERT_MAP: dict[str, AlertType] = {
    "below_stop": AlertType.STOP_LOSS_BREACH,
    "deep_value": AlertType.ENTRY_OPPORTUNITY,
    "entry_range": AlertType.ENTRY_OPPORTUNITY,
    "exit_range": AlertType.EXIT_SIGNAL,
    "overvalued": AlertType.EXIT_SIGNAL,
    "above_target": AlertType.TARGET_REACHED,
}

_ZONE_SEVERITY: dict[str, Severity] = {
    "below_stop": Severity.CRITICAL,
    "deep_value": Severity.HIGH,
    "entry_range": Severity.MEDIUM,
    "fair_value": Severity.LOW,
    "exit_range": Severity.MEDIUM,
    "overvalued": Severity.HIGH,
    "above_target": Severity.HIGH,
}


def check_valuation_anchors(
    price_data: PriceData,
    anchors: ValuationAnchors,
    ticker_state: IntradayTickerState,
) -> list[Alert]:
    """Alert only on zone transitions — fires once per boundary crossing."""
    alerts: list[Alert] = []
    now = datetime.now(timezone.utc)

    new_zone = _classify_valuation_zone(price_data.price, anchors)
    old_zone = ticker_state.valuation_zone

    # Update state
    ticker_state.valuation_zone = new_zone

    # First observation or no change — no alert
    if old_zone is None or old_zone == new_zone:
        return alerts

    alert_type = _ZONE_ALERT_MAP.get(new_zone)
    if alert_type is None:
        # Transitioning back to fair_value — no alert needed
        return alerts

    severity = _ZONE_SEVERITY.get(new_zone, Severity.MEDIUM)
    alerts.append(Alert(
        ticker=price_data.ticker,
        timestamp=now,
        alert_type=alert_type,
        severity=severity,
        details={
            "price": price_data.price,
            "from_zone": old_zone,
            "to_zone": new_zone,
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
        # CLI one-shot: use a fresh state so all zone alerts fire once
        tmp_state = IntradayTickerState(valuation_zone="fair_value")
        alerts.extend(check_valuation_anchors(price_data, anchors, tmp_state))

    return alerts
