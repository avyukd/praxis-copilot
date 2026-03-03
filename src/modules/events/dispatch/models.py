"""Pydantic models for the event dispatch module."""

from __future__ import annotations

from pydantic import BaseModel


class ParsedTrigger(BaseModel):
    """Result of parsing an S3 trigger key."""

    source: str
    data_type: str
    cik: str | None = None
    accession: str | None = None
    ticker_direct: str | None = None
    release_id: str | None = None
    date: str | None = None


class MonitorDefinition(BaseModel):
    """A monitor loaded from S3 config/monitors/."""

    id: str
    listen: list[str] = []
    collector_lambda: str | None = None

    class Config:
        extra = "allow"


class EventRecord(BaseModel):
    """Dispatched event record written to S3."""

    event_id: str
    timestamp: str
    source: str
    ticker: str
    cik: str | None = None
    data_type: str
    s3_path: str
    monitors_triggered: list[str] = []


class S3EventRecord(BaseModel):
    """Minimal S3 event notification record."""

    class S3BucketInfo(BaseModel):
        name: str = ""

    class S3ObjectInfo(BaseModel):
        key: str = ""

    class S3Info(BaseModel):
        bucket: "S3EventRecord.S3BucketInfo" = S3EventRecord.S3BucketInfo()
        object: "S3EventRecord.S3ObjectInfo" = S3EventRecord.S3ObjectInfo()

    s3: S3Info = S3Info()


class S3Event(BaseModel):
    """Top-level S3 event notification."""

    Records: list[S3EventRecord] = []


class DispatchResult(BaseModel):
    """Return value from lambda_handler."""

    dispatched: int
    skipped: int
