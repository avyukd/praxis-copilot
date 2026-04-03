"""Pydantic models for the research queue system."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class QueueTaskType(str, Enum):
    TICKER_RESEARCH = "ticker_research"
    COMPARATIVE = "comparative"
    THEMATIC = "thematic"
    DOCUMENT_REVIEW = "document_review"
    FREEFORM = "freeform"


class QueueTaskStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


class QueueTask(BaseModel):
    issue_number: int
    title: str
    body: str
    task_type: QueueTaskType = QueueTaskType.FREEFORM
    tickers: list[str] = Field(default_factory=list)
    labels: list[str] = Field(default_factory=list)
    image_urls: list[str] = Field(default_factory=list)
    link_urls: list[str] = Field(default_factory=list)
    created_at: datetime
    status: QueueTaskStatus = QueueTaskStatus.QUEUED
    started_at: datetime | None = None
    finished_at: datetime | None = None
    session_id: str | None = None
    artifacts: list[str] = Field(default_factory=list)
    summary: str = ""
    error: str = ""


class QueueState(BaseModel):
    last_poll_at: datetime | None = None
    tasks: dict[int, QueueTask] = Field(default_factory=dict)
