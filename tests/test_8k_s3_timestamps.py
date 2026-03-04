import json
import re

from src.modules.events.eight_k_scanner.storage import s3


def test_et_now_iso_has_eastern_offset():
    ts = s3.et_now_iso()
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}-(04|05):00$", ts)


def test_store_filing_sets_timestamp_defaults(monkeypatch):
    writes = []

    class FakeS3Client:
        def put_object(self, **kwargs):
            writes.append(kwargs)

    monkeypatch.setattr(s3, "get_s3_client", lambda: FakeS3Client())
    monkeypatch.setattr(s3, "filing_exists", lambda cik, accession_number, bucket=None: False)
    monkeypatch.setattr(s3, "et_now_iso", lambda: "2026-02-19T00:00:00-05:00")

    stored = s3.store_filing(
        cik="0001234567",
        accession_number="0001234567-26-000001",
        metadata={"ticker": "TEST"},
        documents={"primary.htm": b"<html>ok</html>"},
        bucket="test-bucket",
        force=False,
    )

    assert stored is True
    index_write = writes[0]
    assert index_write["Key"].endswith("/index.json")
    body = json.loads(index_write["Body"])
    assert body["extracted_at"] == "2026-02-19T00:00:00-05:00"
    assert body["analyzed_at"] is None

    keys = [w["Key"] for w in writes]
    assert "data/raw/8k/0001234567/0001234567-26-000001/index.json" in keys
    assert "data/raw/8k/0001234567/0001234567-26-000001/primary.htm" in keys


def test_store_filing_preserves_existing_timestamps(monkeypatch):
    writes = []

    class FakeS3Client:
        def put_object(self, **kwargs):
            writes.append(kwargs)

    monkeypatch.setattr(s3, "get_s3_client", lambda: FakeS3Client())
    monkeypatch.setattr(s3, "filing_exists", lambda cik, accession_number, bucket=None: False)
    monkeypatch.setattr(s3, "et_now_iso", lambda: "2026-02-19T00:00:00-05:00")

    stored = s3.store_filing(
        cik="0001234567",
        accession_number="0001234567-26-000001",
        metadata={
            "ticker": "TEST",
            "extracted_at": "2026-02-18T23:00:00-05:00",
            "analyzed_at": "2026-02-19T01:00:00-05:00",
        },
        documents={"primary.htm": b"<html>ok</html>"},
        bucket="test-bucket",
        force=False,
    )

    assert stored is True
    index_write = writes[0]
    body = json.loads(index_write["Body"])
    assert body["extracted_at"] == "2026-02-18T23:00:00-05:00"
    assert body["analyzed_at"] == "2026-02-19T01:00:00-05:00"
