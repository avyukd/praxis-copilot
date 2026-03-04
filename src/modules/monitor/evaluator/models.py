"""Pydantic models for the monitor evaluator module."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class MonitorConfig(BaseModel):
    """A single monitor definition loaded from config/monitors/.

    Unified schema supporting filing, scraper, and search monitor types.
    Type-specific fields are optional and validated by type.
    """

    id: str
    type: Literal["filing", "scraper", "search"]
    tickers: list[str]
    description: str
    extract: str
    threshold: str = ""

    # Filing-specific
    filing_types: list[str] = Field(default_factory=list)

    # Scraper-specific
    source_url: str | None = None
    scraper_script: str | None = None

    # Search-specific
    queries: list[str] = Field(default_factory=list)
    search_backend: str = "brave"

    # Scheduling
    frequency: str = "daily"  # "daily", "weekly", "quarterly"

    @property
    def listen_keys(self) -> list[str]:
        """Generate listen keys for dispatch matching.

        Filing monitors: {ticker}:filings:{form_type} for each ticker/form_type combo.
        Scraper/search monitors don't use listen keys (they're cron-driven).
        """
        if self.type != "filing":
            return []
        keys = []
        for ticker in self.tickers:
            if self.filing_types:
                for ft in self.filing_types:
                    keys.append(f"{ticker}:filings:{ft}")
            else:
                keys.append(f"{ticker}:filings")
        return keys


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
    alerts_sent: int = 0
    errors: list[str] = Field(default_factory=list)
