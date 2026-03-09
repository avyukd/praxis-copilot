"""Pydantic models for the monitor evaluator module."""
from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field

_CADENCE_RE = re.compile(r"^(\d+)\s*(h|d)$", re.IGNORECASE)

FREQUENCY_TO_HOURS: dict[str, int] = {
    "hourly": 1,
    "daily": 24,
    "weekly": 168,
    "quarterly": 2160,
}


def cadence_to_hours(cadence: str, frequency: str = "daily") -> int:
    """Convert a cadence string like '1h', '6h', '1d' to hours.

    Falls back to frequency if cadence is empty.
    """
    if cadence:
        m = _CADENCE_RE.match(cadence.strip())
        if m:
            val, unit = int(m.group(1)), m.group(2).lower()
            return val if unit == "h" else val * 24
    return FREQUENCY_TO_HOURS.get(frequency, 24)


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
    search_backend: str = "tavily"

    # Scheduling — cadence supports interval strings: "1h", "6h", "1d", "7d"
    # Kept for backward compat with existing configs; cadence takes precedence.
    frequency: str = "daily"  # "daily", "weekly", "quarterly"
    cadence: str = ""  # e.g. "1h", "6h", "1d" — empty falls back to frequency

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
    seen_urls: list[str] = Field(default_factory=list)


class EvaluatorResult(BaseModel):
    """Summary returned by the evaluator Lambda."""

    monitors_evaluated: int = 0
    snapshots_written: list[str] = Field(default_factory=list)
    alerts_sent: int = 0
    errors: list[str] = Field(default_factory=list)
