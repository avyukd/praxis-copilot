from datetime import datetime, timezone

from src.cli import pipeline_status as ps


def test_parse_day_window_uses_et_calendar_day():
    start_utc, end_utc, target_day = ps.parse_day_window("2026-03-05")

    assert target_day.isoformat() == "2026-03-05"
    assert start_utc.isoformat() == "2026-03-05T05:00:00+00:00"
    assert end_utc.isoformat() == "2026-03-06T05:00:00+00:00"


def test_collect_pipeline_items_stage_classification(monkeypatch):
    now = datetime(2026, 3, 6, 2, 0, tzinfo=timezone.utc)

    objects = [
        {
            "Key": "data/raw/filings/0001/a1/index.json",
            "LastModified": datetime(2026, 3, 5, 14, 0, tzinfo=timezone.utc),
        },
        {
            "Key": "data/raw/filings/0001/a1/extracted.json",
            "LastModified": datetime(2026, 3, 5, 14, 3, tzinfo=timezone.utc),
        },
        {
            "Key": "data/raw/filings/0001/a1/analysis.json",
            "LastModified": datetime(2026, 3, 5, 14, 5, tzinfo=timezone.utc),
        },
        {
            "Key": "data/raw/filings/0002/a2/index.json",
            "LastModified": datetime(2026, 3, 5, 20, 0, tzinfo=timezone.utc),
        },
        {
            "Key": "data/raw/filings/0003/a3/index.json",
            "LastModified": datetime(2026, 3, 5, 21, 0, tzinfo=timezone.utc),
        },
        {
            "Key": "data/raw/filings/0003/a3/extracted.json",
            "LastModified": datetime(2026, 3, 5, 21, 1, tzinfo=timezone.utc),
        },
        {
            "Key": "data/raw/filings/0003/a3/screening.json",
            "LastModified": datetime(2026, 3, 5, 21, 2, tzinfo=timezone.utc),
        },
    ]

    def _list_prefix_objects(_s3, prefix):
        assert prefix == "data/raw/filings/"
        return objects

    def _download(_s3, key):
        payloads = {
            "data/raw/filings/0001/a1/index.json": b'{"ticker":"NVDA","form_type":"8-K","alert_sent_at":"2026-03-05T10:10:00-05:00"}',
            "data/raw/filings/0002/a2/index.json": b'{"ticker":"APP","form_type":"8-K"}',
            "data/raw/filings/0003/a3/index.json": b'{"ticker":"SHOP","form_type":"8-K"}',
        }
        return payloads[key]

    monkeypatch.setattr(ps, "list_prefix_objects", _list_prefix_objects)
    monkeypatch.setattr(ps, "download_file", _download)

    items = ps.collect_pipeline_items(
        object(),
        day_start_utc=datetime(2026, 3, 5, 5, 0, tzinfo=timezone.utc),
        day_end_utc=datetime(2026, 3, 6, 5, 0, tzinfo=timezone.utc),
        source="filings",
        stuck_minutes=30,
        now_utc=now,
    )

    by_id = {item.item_id: item for item in items}
    assert by_id["a1"].stage == "alerted"
    assert by_id["a2"].stage == "stuck_extract"
    assert by_id["a3"].stage == "screened_out"

    summary = ps.summarize_pipeline_items(items)
    assert summary["total"] == 3
    assert summary["stage_counts"]["alerted"] == 1
    assert summary["stage_counts"]["stuck_extract"] == 1
    assert summary["stage_counts"]["screened_out"] == 1
    assert summary["alerts_sent"] == 1


def test_collect_pipeline_items_filters_day_window(monkeypatch):
    objects = [
        {
            "Key": "data/raw/press_releases/gnw/NVDA/r1/index.json",
            "LastModified": datetime(2026, 3, 4, 20, 0, tzinfo=timezone.utc),
        },
        {
            "Key": "data/raw/press_releases/gnw/NVDA/r2/index.json",
            "LastModified": datetime(2026, 3, 5, 20, 0, tzinfo=timezone.utc),
        },
    ]

    monkeypatch.setattr(ps, "list_prefix_objects", lambda _s3, prefix: objects)
    monkeypatch.setattr(ps, "download_file", lambda _s3, key: b'{"ticker":"NVDA","source":"gnw"}')

    items = ps.collect_pipeline_items(
        object(),
        day_start_utc=datetime(2026, 3, 5, 5, 0, tzinfo=timezone.utc),
        day_end_utc=datetime(2026, 3, 6, 5, 0, tzinfo=timezone.utc),
        source="press_releases",
        stuck_minutes=30,
        now_utc=datetime(2026, 3, 5, 21, 0, tzinfo=timezone.utc),
    )

    assert len(items) == 1
    assert items[0].item_id == "r2"
