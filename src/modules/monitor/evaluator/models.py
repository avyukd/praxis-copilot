"""Pydantic models for the monitor evaluator module."""
from __future__ import annotations

from pydantic import BaseModel, Field


class MonitorConfig(BaseModel):
    """A single monitor definition loaded from config/monitors/."""

    id: str
    description: str
    relevant_to: list[str]
    trigger: str  # "event", "scheduled", "periodic"
    listen: list[str] = Field(default_factory=list)
    extract: str
    collector: str = "agent"  # "scraper" or "agent"
    fallback: str | None = None
    frequency: str | None = None  # for periodic: "daily", "weekly", "quarterly"
    schedule_from: str | None = None  # for scheduled
    universe_status: str | None = None  # "external" if listen includes non-universe tickers


class MonitorSnapshot(BaseModel):
    """A point-in-time snapshot produced by a collector run."""

    monitor_id: str
    date: str
    source: str = ""
    status: str = "unchanged"  # "updated" or "unchanged"
    current_state: str = ""
    delta_from_previous: str = ""
    significance: str = "low"  # "low", "medium", "high"
    previous_data: str | None = None


class EvaluatorResult(BaseModel):
    """Summary returned by the evaluator Lambda."""

    monitors_evaluated: int = 0
    snapshots_written: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
