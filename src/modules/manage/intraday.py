"""Stateful intraday threshold checks: zigzag reversals, stepped close
moves, volume velocity, and volume anomaly."""
from __future__ import annotations

import math
from datetime import datetime, timezone

from .models import (
    Alert,
    AlertType,
    IntradayTickerState,
    ManageConfig,
    PriceData,
    Severity,
    ValuationAnchors,
)
from .thresholds import check_valuation_anchors


def _effective_config(config: ManageConfig, overrides: dict | None) -> ManageConfig:
    """Apply per-ticker overrides on top of defaults."""
    if not overrides:
        return config
    return config.model_copy(update={
        k: v for k, v in overrides.items()
        if k in ManageConfig.model_fields
    })


def check_move_from_close(
    price_data: PriceData,
    config: ManageConfig,
    ticker_state: IntradayTickerState,
) -> list[Alert]:
    """Fire an alert at each new threshold band crossing from previous close.

    With move_from_close_pct=5, bands are at 5%, 10%, 15%, etc.
    Each band fires at most once per day (tracked in ticker_state.close_bands_fired).
    """
    alerts: list[Alert] = []
    now = datetime.now(timezone.utc)
    step = config.move_from_close_pct
    if step <= 0:
        return alerts

    band = int(math.floor(abs(price_data.change_pct) / step))
    if band < 1:
        return alerts

    direction = "up" if price_data.change_pct > 0 else "down"
    alert_type = AlertType.PRICE_BREACH_UP if direction == "up" else AlertType.PRICE_BREACH_DOWN

    for b in range(1, band + 1):
        if b in ticker_state.close_bands_fired:
            continue
        ticker_state.close_bands_fired.append(b)
        threshold_pct = b * step
        severity = Severity.HIGH if threshold_pct >= step * 2 else Severity.MEDIUM
        alerts.append(Alert(
            ticker=price_data.ticker,
            timestamp=now,
            alert_type=alert_type,
            severity=severity,
            details={
                "change_pct": price_data.change_pct,
                "threshold_pct": threshold_pct,
                "price": price_data.price,
                "previous_close": price_data.previous_close,
            },
        ))

    return alerts


def check_zigzag_reversal(
    price_data: PriceData,
    config: ManageConfig,
    ticker_state: IntradayTickerState,
) -> list[Alert]:
    """Zigzag reversal detection.

    Tracks the current trend direction and running extreme.  When price
    reverses by >= reversal_pct from the extreme, fires an alert and
    flips direction.
    """
    alerts: list[Alert] = []
    now = datetime.now(timezone.utc)
    threshold = config.reversal_pct
    if threshold <= 0:
        return alerts

    price = price_data.price

    # First data point: initialize direction from change_pct
    if ticker_state.direction is None or ticker_state.extreme_price is None:
        ticker_state.direction = "up" if price_data.change_pct >= 0 else "down"
        ticker_state.extreme_price = price
        ticker_state.extreme_time = now
        return alerts

    extreme = ticker_state.extreme_price

    if ticker_state.direction == "up":
        # Trending up: update high
        if price >= extreme:
            ticker_state.extreme_price = price
            ticker_state.extreme_time = now
            return alerts
        # Check for reversal down
        if extreme > 0:
            drop_pct = ((extreme - price) / extreme) * 100.0
            if drop_pct >= threshold:
                alerts.append(Alert(
                    ticker=price_data.ticker,
                    timestamp=now,
                    alert_type=AlertType.REVERSAL,
                    severity=Severity.HIGH if drop_pct >= threshold * 2 else Severity.MEDIUM,
                    details={
                        "direction": "down",
                        "from_price": extreme,
                        "to_price": price,
                        "reversal_pct": round(drop_pct, 2),
                        "threshold_pct": threshold,
                    },
                ))
                ticker_state.direction = "down"
                ticker_state.extreme_price = price
                ticker_state.extreme_time = now
    else:
        # Trending down: update low
        if price <= extreme:
            ticker_state.extreme_price = price
            ticker_state.extreme_time = now
            return alerts
        # Check for reversal up
        if extreme > 0:
            rise_pct = ((price - extreme) / extreme) * 100.0
            if rise_pct >= threshold:
                alerts.append(Alert(
                    ticker=price_data.ticker,
                    timestamp=now,
                    alert_type=AlertType.REVERSAL,
                    severity=Severity.HIGH if rise_pct >= threshold * 2 else Severity.MEDIUM,
                    details={
                        "direction": "up",
                        "from_price": extreme,
                        "to_price": price,
                        "reversal_pct": round(rise_pct, 2),
                        "threshold_pct": threshold,
                    },
                ))
                ticker_state.direction = "up"
                ticker_state.extreme_price = price
                ticker_state.extreme_time = now

    return alerts


def check_volume_anomaly(
    price_data: PriceData,
    config: ManageConfig,
    ticker_state: IntradayTickerState,
) -> list[Alert]:
    """Alert once per day when total day volume first crosses multiplier × ADTV."""
    alerts: list[Alert] = []
    now = datetime.now(timezone.utc)
    threshold = config.volume_anomaly_multiplier

    if ticker_state.volume_anomaly_fired:
        return alerts

    if price_data.volume_ratio >= threshold:
        ticker_state.volume_anomaly_fired = True
        severity = Severity.HIGH if price_data.volume_ratio >= threshold * 2 else Severity.MEDIUM
        alerts.append(Alert(
            ticker=price_data.ticker,
            timestamp=now,
            alert_type=AlertType.VOLUME_SPIKE,
            severity=severity,
            details={
                "volume": price_data.volume,
                "adtv": price_data.adtv,
                "volume_ratio": price_data.volume_ratio,
                "threshold_multiplier": threshold,
            },
        ))

    return alerts


# ~26 fifteen-minute intervals in a 6.5-hour US trading day
PERIODS_PER_DAY = 26


def check_volume_velocity(
    price_data: PriceData,
    config: ManageConfig,
    ticker_state: IntradayTickerState,
) -> list[Alert]:
    """Alert when volume in the last polling period is unusually high.

    Compares the volume delta since last check against the expected
    per-period volume (ADTV / 26).
    """
    alerts: list[Alert] = []
    now = datetime.now(timezone.utc)
    threshold = config.volume_velocity_multiplier

    prev_volume = ticker_state.last_volume
    ticker_state.last_volume = price_data.volume

    # Skip first invocation — no prior volume to compare against
    if prev_volume == 0:
        return alerts

    volume_delta = price_data.volume - prev_volume
    if volume_delta <= 0 or price_data.adtv <= 0:
        return alerts

    expected_per_period = price_data.adtv / PERIODS_PER_DAY
    if expected_per_period <= 0:
        return alerts

    velocity_ratio = volume_delta / expected_per_period
    if velocity_ratio >= threshold:
        severity = Severity.HIGH if velocity_ratio >= threshold * 2 else Severity.MEDIUM
        alerts.append(Alert(
            ticker=price_data.ticker,
            timestamp=now,
            alert_type=AlertType.VOLUME_VELOCITY,
            severity=severity,
            details={
                "volume_delta": volume_delta,
                "expected_per_period": round(expected_per_period, 0),
                "velocity_ratio": round(velocity_ratio, 2),
                "threshold_multiplier": threshold,
            },
        ))

    return alerts


def run_all_checks(
    price_data: PriceData,
    config: ManageConfig,
    anchors: ValuationAnchors | None,
    ticker_state: IntradayTickerState,
    ticker_overrides: dict | None = None,
) -> list[Alert]:
    """Run all intraday threshold checks for a single ticker."""
    effective = _effective_config(config, ticker_overrides)
    alerts: list[Alert] = []

    alerts.extend(check_move_from_close(price_data, effective, ticker_state))
    alerts.extend(check_zigzag_reversal(price_data, effective, ticker_state))
    alerts.extend(check_volume_anomaly(price_data, effective, ticker_state))
    alerts.extend(check_volume_velocity(price_data, effective, ticker_state))

    if anchors:
        alerts.extend(check_valuation_anchors(price_data, anchors, ticker_state))

    return alerts
