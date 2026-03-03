"""Poll Canadian newswire feeds for press releases."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from botocore.exceptions import ClientError

from src.modules.events.eight_k_scanner.config import S3_BUCKET, CA_LOOKBACK_MINUTES, CA_POLLER_STATE_KEY
from src.modules.events.eight_k_scanner.newswire.cnw import poll_cnw
from src.modules.events.eight_k_scanner.newswire.dedup import dedup_releases
from src.modules.events.eight_k_scanner.newswire.gnw import poll_gnw
from src.modules.events.eight_k_scanner.newswire.newsfile import poll_newsfile
from src.modules.events.eight_k_scanner.storage.s3 import read_json_from_s3, write_json_to_s3

logger = logging.getLogger(__name__)

GNW_CA_FEEDS = [
    "https://www.globenewswire.com/RssFeed/exchange/TSX",
    "https://www.globenewswire.com/RssFeed/exchange/TSXV",
]


def poll_canadian_releases(lookback_minutes: int = CA_LOOKBACK_MINUTES) -> list[dict]:
    last_seen = _load_last_seen()
    min_published_at = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)

    all_releases: list[dict] = []
    all_releases.extend(poll_gnw(GNW_CA_FEEDS))
    all_releases.extend(poll_newsfile())
    all_releases.extend(poll_cnw())

    all_releases = [r for r in all_releases if r.get("exchange") in ("TSX", "TSXV")]
    all_releases = dedup_releases(all_releases)

    all_releases = [
        r for r in all_releases
        if _in_lookback_window(r.get("published_at", ""), min_published_at)
    ]
    all_releases.sort(
        key=lambda r: _release_position(r.get("published_at", ""), r.get("release_id", ""))
    )

    new_releases = []
    newest: dict[str, dict[str, str]] = {
        source: _normalize_state_entry(state_entry)
        for source, state_entry in (last_seen or {}).items()
    }

    for release in all_releases:
        source = release["source"]
        rid = release.get("release_id", "")
        published_at = release.get("published_at", "")
        release_pos = _release_position(published_at, rid)

        last_entry = newest.get(source, {"published_at": "", "release_id": ""})
        last_pos = _release_position(last_entry.get("published_at", ""), last_entry.get("release_id", ""))
        if release_pos <= last_pos:
            continue

        new_releases.append(release)
        newest[source] = {"published_at": published_at, "release_id": rid}

    if newest != {
        source: _normalize_state_entry(state_entry)
        for source, state_entry in (last_seen or {}).items()
    }:
        _save_last_seen(newest)

    logger.info(f"Found {len(new_releases)} new Canadian press releases")
    return new_releases


def _load_last_seen() -> dict | None:
    try:
        data = read_json_from_s3(S3_BUCKET, CA_POLLER_STATE_KEY)
        return data.get("last_seen")
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "NoSuchKey":
            return None
        raise
    except Exception:
        return None


def _save_last_seen(last_seen: dict) -> None:
    write_json_to_s3(S3_BUCKET, CA_POLLER_STATE_KEY, {"last_seen": last_seen})


def _normalize_state_entry(value: dict | str | None) -> dict[str, str]:
    if isinstance(value, dict):
        return {
            "published_at": str(value.get("published_at", "")),
            "release_id": str(value.get("release_id", "")),
        }
    if isinstance(value, str):
        return {"published_at": "", "release_id": value}
    return {"published_at": "", "release_id": ""}


def _release_position(published_at: str, release_id: str) -> tuple[str, str]:
    return (published_at or "", release_id or "")


def _in_lookback_window(published_at: str, min_published_at: datetime) -> bool:
    if not published_at:
        return True
    dt = _parse_iso_datetime(published_at)
    if dt is None:
        return True
    return dt >= min_published_at


def _parse_iso_datetime(value: str) -> datetime | None:
    try:
        v = value.strip()
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None
