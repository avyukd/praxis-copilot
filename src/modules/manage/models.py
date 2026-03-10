"""Pydantic models for the manage module."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


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
    VOLUME_VELOCITY = "volume_velocity"
    REVERSAL = "reversal"
    STOP_LOSS_BREACH = "stop_loss_breach"
    TARGET_REACHED = "target_reached"
    ENTRY_OPPORTUNITY = "entry_opportunity"
    EXIT_SIGNAL = "exit_signal"
    OPTIONS_LARGE_BET = "options_large_bet"


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

    move_from_close_pct: float = 5.0
    reversal_pct: float = 5.0
    volume_anomaly_multiplier: float = 3.0
    volume_velocity_multiplier: float = 2.0


class IntradayTickerState(BaseModel):
    """Per-ticker intraday tracking state for zigzag and velocity."""

    # Zigzag reversal tracking
    direction: Literal["up", "down"] | None = None
    extreme_price: float | None = None
    extreme_time: datetime | None = None

    # Volume velocity: volume at last check for computing deltas
    last_volume: int = 0

    # Volume anomaly: fired once per day when first crossing threshold
    volume_anomaly_fired: bool = False

    # Move-from-close: which threshold bands have fired (e.g. [1, 2] = 5%, 10%)
    close_bands_fired: list[int] = Field(default_factory=list)

    # Valuation zone tracking for transition-based alerts.
    # Zones: "below_stop", "deep_value", "entry_range", "fair_value",
    #         "exit_range", "overvalued", "above_target", or None (unknown)
    valuation_zone: str | None = None


class IntradayState(BaseModel):
    """Persisted intraday state across all tickers.  Resets daily."""

    date: str = ""  # YYYY-MM-DD, auto-resets when date changes
    tickers: dict[str, IntradayTickerState] = Field(default_factory=dict)
    # Options flow dedup: set of contract keys ("TICKER:STRIKE:TYPE:EXP") already alerted today
    options_alerts_fired: set[str] = Field(default_factory=set)


class ManageResult(BaseModel):
    """Return value from the manage handler."""

    mode: str
    tickers_checked: int = 0
    alerts_generated: int = 0
    errors: list[str] = []
