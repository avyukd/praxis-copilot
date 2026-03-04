"""EDGAR EFTS poller for 8-K filings."""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Dict

from botocore.exceptions import ClientError

from src.modules.events.eight_k_scanner.config import (
    S3_BUCKET,
    S3_POLLER_STATE_KEY,
    SCANNER_POLLER_SEEN_TTL_DAYS,
)
from src.modules.events.eight_k_scanner.edgar.client import edgar_get
from src.modules.events.eight_k_scanner.models import PolledFiling
from src.modules.events.eight_k_scanner.storage.s3 import read_json_from_s3, write_json_to_s3

logger = logging.getLogger(__name__)

EFTS_URL = "https://efts.sec.gov/LATEST/search-index"


def poll_new_8k_filings(
    lookback_minutes: int = 60,
    forms: list[str] | None = None,
) -> list[PolledFiling]:
    """Discover new filings via EFTS. Returns list of PolledFiling.

    Args:
        lookback_minutes: How far back to look for filings.
        forms: List of form types to poll (e.g. ["8-K", "10-K", "10-Q"]).
               Defaults to ["8-K"] for backward compatibility.
    """
    if forms is None:
        forms = ["8-K"]

    now_utc = datetime.now(timezone.utc)
    start_dt = now_utc - timedelta(minutes=lookback_minutes)
    fetch_start = start_dt.date()
    fetch_end = now_utc.date()

    state = _load_state()
    seen_accessions = set(state.get("seen_accessions", {}).keys())

    filings = _fetch_efts(fetch_start, fetch_end, forms=forms)
    if not filings and "8-K" in forms:
        logger.info("EFTS returned no results, falling back to RSS")
        filings = _fetch_rss()

    new_filings: list[PolledFiling] = []
    for filing in filings:
        accession = filing.accession_number
        if not accession:
            continue
        if accession in seen_accessions:
            continue
        if not _in_lookback_window(filing, start_dt, now_utc):
            continue

        new_filings.append(filing)
        seen_accessions.add(accession)

    _save_state(
        {
            "seen_accessions": _prune_and_stamp_seen_accessions(
                state.get("seen_accessions", {}),
                new_accessions=[f.accession_number for f in new_filings],
                now_utc=now_utc,
                ttl_days=SCANNER_POLLER_SEEN_TTL_DAYS,
            ),
            "last_poll_started_at": now_utc.replace(microsecond=0).isoformat(),
        }
    )

    logger.info(f"Found {len(new_filings)} new filings (forms={forms})")
    return new_filings


def _in_lookback_window(filing: PolledFiling, start_dt: datetime, end_dt: datetime) -> bool:
    filing_dt = _parse_filing_datetime(filing)
    if filing_dt is None:
        filed_date = (filing.filed_date or "")[:10]
        if not filed_date:
            return True
        return start_dt.date().isoformat() <= filed_date <= end_dt.date().isoformat()
    return start_dt <= filing_dt <= end_dt


def _parse_filing_datetime(filing: PolledFiling) -> datetime | None:
    raw_acceptance = (filing.acceptance_datetime or "").strip()
    if raw_acceptance and "T" in raw_acceptance:
        parsed = _parse_datetime(raw_acceptance)
        if parsed is not None:
            return parsed
    return None


def _parse_datetime(raw: str) -> datetime | None:
    value = raw.strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        if len(raw) >= 10:
            try:
                d = date.fromisoformat(raw[:10])
                return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            except ValueError:
                return None
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _prune_and_stamp_seen_accessions(
    seen_accessions: Dict[str, str],
    new_accessions: list[str],
    now_utc: datetime,
    ttl_days: int,
) -> Dict[str, str]:
    cutoff = now_utc - timedelta(days=max(1, ttl_days))
    keep: Dict[str, str] = {}
    for accession, ts in (seen_accessions or {}).items():
        parsed = _parse_datetime(ts or "")
        if parsed is None:
            continue
        if parsed >= cutoff:
            keep[accession] = parsed.replace(microsecond=0).isoformat()

    stamp = now_utc.replace(microsecond=0).isoformat()
    for accession in new_accessions:
        keep[accession] = stamp
    return keep


def fetch_filings_by_date(
    start_date: date,
    end_date: date,
    forms: list[str] | None = None,
) -> list[PolledFiling]:
    """Fetch filings in a date range (no state-based dedupe)."""
    if forms is None:
        forms = ["8-K"]
    filings = _fetch_efts(start_date, end_date, forms=forms)
    if not filings and "8-K" in forms:
        logger.info("EFTS returned no results for date range, falling back to RSS")
        filings = _fetch_rss()
    logger.info(f"fetch_filings_by_date({start_date} -> {end_date}): {len(filings)} filings")
    return filings


def _fetch_efts(
    start_date: date,
    end_date: date,
    forms: list[str] | None = None,
) -> list[PolledFiling]:
    if forms is None:
        forms = ["8-K"]
    forms_query = ",".join(forms)

    all_filings: list[PolledFiling] = []
    page_from = 0
    page_size = 100

    while True:
        params = {
            "forms": forms_query,
            "startdt": start_date.isoformat(),
            "enddt": end_date.isoformat(),
            "from": page_from,
            "size": page_size,
        }
        try:
            resp = edgar_get(EFTS_URL, params=params)
            data = resp.json()
            filings = _parse_efts_response(data, allowed_forms=forms)
            all_filings.extend(filings)

            total = data.get("hits", {}).get("total", {}).get("value", 0)
            page_from += page_size
            if page_from >= total:
                break
        except Exception:
            logger.exception("EFTS query failed")
            break

    return all_filings


def _parse_efts_response(
    data: dict,
    allowed_forms: list[str] | None = None,
) -> list[PolledFiling]:
    if allowed_forms is None:
        allowed_forms = ["8-K"]
    # Build set of allowed forms including amendments
    allowed_set = set()
    for f in allowed_forms:
        allowed_set.add(f)
        allowed_set.add(f"{f}/A")

    filings: list[PolledFiling] = []
    hits = data.get("hits", {}).get("hits", [])
    for hit in hits:
        src = hit.get("_source", {})

        if src.get("form") not in allowed_set:
            continue

        ciks = src.get("ciks", [])
        if not ciks:
            continue
        cik = ciks[0].lstrip("0")

        accession = src.get("adsh", "")
        if not accession:
            continue

        display = src.get("display_names", [""])[0] if src.get("display_names") else ""
        company_name = display.split("  (")[0] if display else ""
        ticker = ""
        if "  (" in display:
            parts = display.split("  (")
            for part in parts[1:]:
                token = part.rstrip(")")
                if not token.startswith("CIK"):
                    ticker = token
                    break

        filings.append(PolledFiling(
            cik=cik,
            accession_number=accession,
            company_name=company_name,
            ticker=ticker,
            form_type=src.get("form", ""),
            filed_date=src.get("file_date", ""),
            acceptance_datetime=src.get("file_date", ""),
            items=src.get("items", []),
        ))

    logger.info(f"EFTS returned {len(filings)} filings (from {len(hits)} hits, forms={allowed_forms})")
    return filings


def _fetch_rss() -> list[PolledFiling]:
    url = (
        "https://www.sec.gov/cgi-bin/browse-edgar"
        "?action=getcurrent&type=8-K&count=100&output=atom"
    )
    try:
        resp = edgar_get(url)
        return _parse_rss(resp.text)
    except Exception:
        logger.exception("RSS feed fetch failed")
        return []


def _parse_rss(xml_text: str) -> list[PolledFiling]:
    import xml.etree.ElementTree as ET

    filings: list[PolledFiling] = []
    root = ET.fromstring(xml_text)
    ns = {"atom": "http://www.w3.org/2005/Atom"}

    for entry in root.findall("atom:entry", ns):
        title = entry.findtext("atom:title", "", ns)
        link_el = entry.find("atom:link", ns)
        updated = entry.findtext("atom:updated", "", ns)

        if "8-K" not in title:
            continue

        link = link_el.get("href", "") if link_el is not None else ""

        cik, accession = "", ""
        parts = link.rstrip("/").split("/")
        if "data" in parts:
            idx = parts.index("data")
            if idx + 2 < len(parts):
                cik = parts[idx + 1]
                accession = parts[idx + 2]

        if cik and accession:
            if "-" not in accession and len(accession) >= 10:
                accession = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
            filings.append(PolledFiling(
                cik=cik,
                accession_number=accession,
                company_name=title.replace("8-K", "").strip(" -"),
                filed_date=updated[:10] if updated else "",
                acceptance_datetime=updated,
            ))

    return filings


def _load_state() -> Dict[str, object]:
    default_state: Dict[str, object] = {"seen_accessions": {}, "last_poll_started_at": None}
    try:
        data = read_json_from_s3(S3_BUCKET, S3_POLLER_STATE_KEY)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code == "NoSuchKey":
            return default_state
        raise
    except Exception:
        return default_state

    if not isinstance(data, dict):
        return default_state

    raw_seen = data.get("seen_accessions")
    if isinstance(raw_seen, dict):
        seen_accessions = {
            str(accession): str(ts)
            for accession, ts in raw_seen.items()
            if accession
        }
    else:
        seen_accessions = {}

    legacy_last_seen = data.get("last_seen")
    if isinstance(legacy_last_seen, dict):
        legacy_accession = legacy_last_seen.get("accession_number")
        if legacy_accession and legacy_accession not in seen_accessions:
            seen_accessions[str(legacy_accession)] = datetime.now(timezone.utc).replace(
                microsecond=0
            ).isoformat()

    return {
        "seen_accessions": seen_accessions,
        "last_poll_started_at": data.get("last_poll_started_at"),
    }


def _save_state(state: Dict[str, object]) -> None:
    write_json_to_s3(S3_BUCKET, S3_POLLER_STATE_KEY, state)
