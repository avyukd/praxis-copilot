"""Tests for monitor cadence filtering in the handler."""
from __future__ import annotations

from datetime import datetime, timezone

from src.modules.monitor.evaluator import handler, snapshot
from src.modules.monitor.evaluator.models import MonitorConfig, MonitorSnapshot


def _make_config(id: str, cadence: str = "", frequency: str = "daily") -> MonitorConfig:
    return MonitorConfig(
        id=id,
        type="search",
        tickers=["TEST"],
        description="test",
        extract="test",
        queries=["test"],
        cadence=cadence,
        frequency=frequency,
    )


def test_cadence_filter_skips_recently_run(monkeypatch):
    """Monitor with 6h cadence should be skipped if run 2h ago."""
    config = _make_config("test-6h", cadence="6h")
    now = datetime(2025, 3, 15, 14, 0, 0, tzinfo=timezone.utc)

    prev = MonitorSnapshot(
        monitor_id="test-6h",
        date="2025-03-15T12:00:00",
        status="updated",
    )

    monkeypatch.setattr(
        snapshot, "load_previous_snapshot",
        lambda s3_client, mid: prev if mid == "test-6h" else None,
    )

    result = handler._filter_monitors(
        [config], "scheduled", None, s3_client=object(), now=now,
    )
    assert len(result) == 0


def test_cadence_filter_includes_due_monitor(monkeypatch):
    """Monitor with 6h cadence should run if last run was 7h ago."""
    config = _make_config("test-6h", cadence="6h")
    now = datetime(2025, 3, 15, 19, 0, 0, tzinfo=timezone.utc)

    prev = MonitorSnapshot(
        monitor_id="test-6h",
        date="2025-03-15T12:00:00",
        status="updated",
    )

    monkeypatch.setattr(
        snapshot, "load_previous_snapshot",
        lambda s3_client, mid: prev if mid == "test-6h" else None,
    )

    result = handler._filter_monitors(
        [config], "scheduled", None, s3_client=object(), now=now,
    )
    assert len(result) == 1
    assert result[0].id == "test-6h"


def test_cadence_filter_includes_never_run(monkeypatch):
    """Monitor that has never run should always be included."""
    config = _make_config("test-new", cadence="1d")
    now = datetime(2025, 3, 15, 14, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        snapshot, "load_previous_snapshot",
        lambda s3_client, mid: None,
    )

    result = handler._filter_monitors(
        [config], "scheduled", None, s3_client=object(), now=now,
    )
    assert len(result) == 1


def test_cadence_filter_legacy_date_format(monkeypatch):
    """Legacy date-only format should still work (treated as midnight UTC)."""
    config = _make_config("test-daily", cadence="1d")
    now = datetime(2025, 3, 16, 14, 0, 0, tzinfo=timezone.utc)

    prev = MonitorSnapshot(
        monitor_id="test-daily",
        date="2025-03-15",
        status="updated",
    )

    monkeypatch.setattr(
        snapshot, "load_previous_snapshot",
        lambda s3_client, mid: prev,
    )

    result = handler._filter_monitors(
        [config], "scheduled", None, s3_client=object(), now=now,
    )
    assert len(result) == 1
