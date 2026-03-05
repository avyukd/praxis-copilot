"""Pipeline day-status helpers for filings and press releases."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from cli.s3 import download_file, list_prefix_objects

ET = ZoneInfo("America/New_York")


@dataclass
class PipelineItem:
    source_type: str
    key_prefix: str
    ticker: str
    cik: str
    form_type: str
    source: str
    item_id: str
    arrived_at: datetime
    stage: str
    alert_sent_at: str | None
    age_minutes: int


def parse_day_window(date_str: str | None) -> tuple[datetime, datetime, date]:
    """Return UTC [start, end) bounds for a local ET day."""
    if date_str:
        target_day = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        target_day = datetime.now(ET).date()

    start_et = datetime.combine(target_day, time.min, tzinfo=ET)
    end_et = start_et + timedelta(days=1)
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc), target_day


def collect_pipeline_items(
    s3_client,
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
    source: str,
    stuck_minutes: int,
    now_utc: datetime | None = None,
) -> list[PipelineItem]:
    """Collect day-window pipeline items across canonical raw prefixes."""
    now_utc = now_utc or datetime.now(timezone.utc)
    items: list[PipelineItem] = []

    if source in ("all", "filings"):
        items.extend(
            _collect_for_prefix(
                s3_client,
                prefix="data/raw/filings/",
                source_type="filings",
                day_start_utc=day_start_utc,
                day_end_utc=day_end_utc,
                stuck_minutes=stuck_minutes,
                now_utc=now_utc,
            )
        )

    if source in ("all", "press_releases"):
        items.extend(
            _collect_for_prefix(
                s3_client,
                prefix="data/raw/press_releases/",
                source_type="press_releases",
                day_start_utc=day_start_utc,
                day_end_utc=day_end_utc,
                stuck_minutes=stuck_minutes,
                now_utc=now_utc,
            )
        )

    items.sort(key=lambda x: x.arrived_at, reverse=True)
    return items


def summarize_pipeline_items(items: list[PipelineItem]) -> dict:
    stage_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    form_counts: dict[str, int] = {}
    total_alerts = 0

    for item in items:
        stage_counts[item.stage] = stage_counts.get(item.stage, 0) + 1
        source_counts[item.source_type] = source_counts.get(item.source_type, 0) + 1
        if item.form_type:
            form_counts[item.form_type] = form_counts.get(item.form_type, 0) + 1
        if item.alert_sent_at:
            total_alerts += 1

    return {
        "total": len(items),
        "stage_counts": stage_counts,
        "source_counts": source_counts,
        "form_counts": form_counts,
        "alerts_sent": total_alerts,
    }


def _collect_for_prefix(
    s3_client,
    *,
    prefix: str,
    source_type: str,
    day_start_utc: datetime,
    day_end_utc: datetime,
    stuck_minutes: int,
    now_utc: datetime,
) -> list[PipelineItem]:
    objects = list_prefix_objects(s3_client, prefix)
    by_prefix: dict[str, dict] = {}

    for obj in objects:
        key = obj.get("Key", "")
        if not key:
            continue
        base, name = _split_key(key)
        if not base or not name:
            continue
        bucket = by_prefix.setdefault(base, {"files": set(), "last_modified": {}})
        bucket["files"].add(name)
        last_mod = obj.get("LastModified")
        if isinstance(last_mod, datetime):
            bucket["last_modified"][name] = last_mod.astimezone(timezone.utc)

    rows: list[PipelineItem] = []
    for key_prefix, meta in by_prefix.items():
        files = meta["files"]
        if "index.json" not in files:
            continue

        arrived_at = meta["last_modified"].get("index.json")
        if not arrived_at:
            continue
        if not (day_start_utc <= arrived_at < day_end_utc):
            continue

        index = _read_index_json(s3_client, key_prefix)
        alert_sent_at = _as_str(index.get("alert_sent_at"))

        has_extracted = "extracted.json" in files
        has_analysis = "analysis.json" in files
        has_screening = "screening.json" in files

        age_basis = (
            meta["last_modified"].get("analysis.json")
            or meta["last_modified"].get("extracted.json")
            or arrived_at
        )
        age_minutes = int(max(0, (now_utc - age_basis).total_seconds() // 60))

        if has_analysis:
            stage = "alerted" if alert_sent_at else "analyzed"
        elif has_screening:
            stage = "screened_out"
        elif has_extracted:
            extracted_at = meta["last_modified"].get("extracted.json") or arrived_at
            lag_minutes = int(max(0, (now_utc - extracted_at).total_seconds() // 60))
            stage = "stuck_analyze" if lag_minutes >= stuck_minutes else "extracted"
        else:
            lag_minutes = int(max(0, (now_utc - arrived_at).total_seconds() // 60))
            stage = "stuck_extract" if lag_minutes >= stuck_minutes else "arrived"

        rows.append(
            PipelineItem(
                source_type=source_type,
                key_prefix=key_prefix,
                ticker=_as_str(index.get("ticker")),
                cik=_as_str(index.get("cik")),
                form_type=_as_str(index.get("form_type")),
                source=_as_str(index.get("source")),
                item_id=_item_id_from_key(key_prefix),
                arrived_at=arrived_at,
                stage=stage,
                alert_sent_at=alert_sent_at,
                age_minutes=age_minutes,
            )
        )

    return rows


def _split_key(key: str) -> tuple[str, str]:
    if "/" not in key:
        return "", ""
    base, name = key.rsplit("/", 1)
    return base, name


def _item_id_from_key(key_prefix: str) -> str:
    return key_prefix.rsplit("/", 1)[-1]


def _read_index_json(s3_client, key_prefix: str) -> dict:
    try:
        body = download_file(s3_client, f"{key_prefix}/index.json")
        return json.loads(body)
    except Exception:
        return {}


def _as_str(value) -> str:
    if value is None:
        return ""
    return str(value)
