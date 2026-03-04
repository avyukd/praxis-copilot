from datetime import datetime, timedelta, timezone

from botocore.exceptions import ClientError

from src.modules.events.eight_k_scanner.edgar import poller
from src.modules.events.eight_k_scanner.models import PolledFiling


def _filing(accession: str, filed_date: str, acceptance_datetime: str) -> PolledFiling:
    return PolledFiling(
        cik="1234567",
        accession_number=accession,
        filed_date=filed_date,
        acceptance_datetime=acceptance_datetime,
    )


def test_poller_seen_set_does_not_skip_unseen_out_of_order_accession(monkeypatch):
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    filings = [
        _filing("0000000000-26-000002", today, today),
        _filing("0000000000-26-000001", today, today),
    ]
    saved = {}

    monkeypatch.setattr(poller, "_fetch_efts", lambda start, end: filings)
    monkeypatch.setattr(poller, "_fetch_rss", lambda: [])
    monkeypatch.setattr(
        poller,
        "_load_state",
        lambda: {
            "seen_accessions": {"0000000000-26-000002": now.isoformat()},
            "last_poll_started_at": None,
        },
    )
    monkeypatch.setattr(poller, "_save_state", lambda state: saved.update(state))

    discovered = poller.poll_new_8k_filings(lookback_minutes=24 * 60)

    assert [f.accession_number for f in discovered] == ["0000000000-26-000001"]
    assert "0000000000-26-000001" in saved["seen_accessions"]


def test_poller_uses_datetime_lookback_when_timestamp_available(monkeypatch):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    too_old = (now - timedelta(minutes=120)).isoformat()
    in_window = (now - timedelta(minutes=10)).isoformat()

    monkeypatch.setattr(
        poller,
        "_fetch_efts",
        lambda start, end: [
            _filing("A-OLD", now.date().isoformat(), too_old),
            _filing("A-NEW", now.date().isoformat(), in_window),
        ],
    )
    monkeypatch.setattr(poller, "_fetch_rss", lambda: [])
    monkeypatch.setattr(
        poller,
        "_load_state",
        lambda: {"seen_accessions": {}, "last_poll_started_at": None},
    )
    monkeypatch.setattr(poller, "_save_state", lambda state: None)

    discovered = poller.poll_new_8k_filings(lookback_minutes=60)
    assert [f.accession_number for f in discovered] == ["A-NEW"]


def test_load_state_bootstraps_on_missing_key(monkeypatch):
    missing = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
        "GetObject",
    )
    monkeypatch.setattr(
        poller,
        "read_json_from_s3",
        lambda bucket, key: (_ for _ in ()).throw(missing),
    )

    state = poller._load_state()

    assert state["seen_accessions"] == {}
    assert state["last_poll_started_at"] is None


def test_poller_date_only_fallback_still_includes_unseen_filings(monkeypatch):
    today = datetime.now(timezone.utc).date().isoformat()
    monkeypatch.setattr(
        poller,
        "_fetch_efts",
        lambda start, end: [_filing("DATE-ONLY", today, today)],
    )
    monkeypatch.setattr(poller, "_fetch_rss", lambda: [])
    monkeypatch.setattr(
        poller,
        "_load_state",
        lambda: {"seen_accessions": {}, "last_poll_started_at": None},
    )
    monkeypatch.setattr(poller, "_save_state", lambda state: None)

    discovered = poller.poll_new_8k_filings(lookback_minutes=60)
    assert [f.accession_number for f in discovered] == ["DATE-ONLY"]
