"""Pydantic models for the manage module."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel


class PriceData(BaseModel):
    """Current price and volume data for a ticker."""

    ticker: str
    price: float
    previous_close: float
    change_pct: float
    volume: int
    adtv: float  # 20-day average daily trading volume
    volume_ratio: float  # volume / adtv
    timestamp: datetime


class ValuationAnchors(BaseModel):
    """Valuation anchors from a ticker's memo.yaml."""

    entry_price: float | None = None
    stop_loss: float | None = None
    target_price: float | None = None
    fair_value_estimate: float | None = None
    entry_range_low: float | None = None
    entry_range_high: float | None = None
    exit_range_low: float | None = None
    exit_range_high: float | None = None


class AlertType(str, Enum):
    PRICE_BREACH_DOWN = "price_breach_down"
    PRICE_BREACH_UP = "price_breach_up"
    VOLUME_SPIKE = "volume_spike"
    STOP_LOSS_BREACH = "stop_loss_breach"
    TARGET_REACHED = "target_reached"
    ENTRY_OPPORTUNITY = "entry_opportunity"
    EXIT_SIGNAL = "exit_signal"


class Severity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Alert(BaseModel):
    """A triggered alert for a ticker."""

    ticker: str
    timestamp: datetime
    alert_type: AlertType
    severity: Severity
    details: dict[str, Any]


class ManageConfig(BaseModel):
    """Parsed manage.yaml config."""

    price_move_pct: float = 5.0
    volume_anomaly_multiplier: float = 3.0


class ManageResult(BaseModel):
    """Return value from the manage handler."""

    mode: str
    tickers_checked: int = 0
    alerts_generated: int = 0
    errors: list[str] = []
