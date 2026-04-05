"""IPC data models — generic, no project-specific dependencies."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field

ET = ZoneInfo("America/New_York")


class IPCTask(BaseModel):
    id: str = ""
    created_at: str = ""
    created_by: str = ""
    status: str = "pending"  # pending, in_progress, done, failed, expired
    priority: str = "medium"  # low, medium, high

    type: str = "browse"  # browse, screenshot, search, monitor, research, custom
    ticker: str = ""
    description: str = ""

    url: str = ""
    search_query: str = ""
    sources: list[str] = Field(default_factory=list)

    output_format: str = "markdown"
    timeout_minutes: int = 30


class IPCResult(BaseModel):
    task_id: str = ""
    completed_at: str = ""
    status: str = "done"
    ticker: str = ""

    finding: str = ""
    actionability: str = "none"  # none, monitor, research_deeper, trade_idea, custom
    urgency: str = "low"  # low, medium, high

    screenshots: list[str] = Field(default_factory=list)
    sources_checked: list[str] = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)


class IPCFinding(BaseModel):
    ticker: str = ""
    timestamp: str = ""
    source: str = ""  # desktop, user, agent, custom
    content: str = ""
    actionability: str = "none"
    urgency: str = "low"
    metadata: dict = Field(default_factory=dict)
