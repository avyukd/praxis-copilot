"""US GNW Poller Lambda: poll GlobeNewsWire NYSE/NASDAQ -> filter -> fetch -> store."""
from __future__ import annotations

import logging

from botocore.exceptions import ClientError

from src.modules.events.eight_k_scanner.config import (
    MARKET_CAP_THRESHOLD,
    S3_BUCKET,
    S3_US_PR_RAW_PREFIX,
    US_GNW_POLLER_STATE_KEY,
)
from src.modules.events.eight_k_scanner.financials import lookup_market_cap
from src.modules.events.eight_k_scanner.models import PRIndexMeta, PressRelease
from src.modules.events.eight_k_scanner.newswire.fetcher import fetch_release
from src.modules.events.eight_k_scanner.newswire.gnw import poll_gnw
from src.modules.events.eight_k_scanner.storage.s3 import (
    et_now_iso,
    get_s3_client,
    read_json_from_s3,
    write_json_to_s3,
)

logger = logging.getLogger(__name__)

GNW_US_FEEDS = [
    "https://www.globenewswire.com/RssFeed/exchange/NYSE",
    "https://www.globenewswire.com/RssFeed/exchange/NASDAQ",
]


def _load_last_seen() -> dict | None:
    try:
        data = read_json_from_s3(S3_BUCKET, US_GNW_POLLER_STATE_KEY)
        return data.get("last_seen")
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "NoSuchKey":
            return None
        raise
    except Exception:
        return None


def _save_last_seen(last_seen: dict) -> None:
    write_json_to_s3(S3_BUCKET, US_GNW_POLLER_STATE_KEY, {"last_seen": last_seen})


def lambda_handler(event=None, context=None):
    logging.basicConfig(level=logging.INFO)

    last_seen = _load_last_seen()

    all_releases = poll_gnw(GNW_US_FEEDS)
    all_releases = [r for r in all_releases if r.exchange in ("NYSE", "NASDAQ")]

    new_releases: list[PressRelease] = []
    newest_ids: dict[str, str] = dict(last_seen) if last_seen else {}
    for release in all_releases:
        source = release.source
        rid = release.release_id
        if last_seen and rid <= last_seen.get(source, ""):
            continue
        new_releases.append(release)
        if rid > newest_ids.get(source, ""):
            newest_ids[source] = rid

    if newest_ids != (last_seen or {}):
        _save_last_seen(newest_ids)

    logger.info(f"Discovered {len(new_releases)} new US GNW press releases")

    stored = 0
    errors = 0
    filtered_out = 0
    skipped = 0

    for release in new_releases:
        ticker = release.ticker
        release_id = release.release_id

        if not ticker:
            filtered_out += 1
            continue

        mcap = lookup_market_cap(ticker)
        if mcap is not None and mcap > MARKET_CAP_THRESHOLD:
            filtered_out += 1
            continue

        prefix = f"{S3_US_PR_RAW_PREFIX}/{ticker}/{release_id}"
        try:
            read_json_from_s3(S3_BUCKET, f"{prefix}/index.json")
            skipped += 1
            continue
        except Exception:
            pass

        try:
            result = fetch_release(release.url, release.source)
            extracted_at = et_now_iso()

            meta = PRIndexMeta(
                ticker=ticker,
                exchange=release.exchange,
                market_cap=mcap,
                release_id=release_id,
                title=release.title,
                url=release.url,
                published_at=release.published_at,
                source=release.source,
                extracted_at=extracted_at,
            )

            write_json_to_s3(S3_BUCKET, f"{prefix}/index.json", meta.model_dump())
            get_s3_client().put_object(
                Bucket=S3_BUCKET,
                Key=f"{prefix}/release.txt",
                Body=result.text,
                ContentType="text/plain",
            )
            stored += 1

        except Exception:
            logger.exception(f"Failed to process release {release_id} for {ticker}")
            errors += 1

    logger.info(
        f"Done: {stored} stored, {skipped} skipped, {filtered_out} filtered, "
        f"{errors} errors (out of {len(new_releases)} discovered)"
    )

    return {
        "total_discovered": len(new_releases),
        "stored": stored,
        "skipped": skipped,
        "filtered_out": filtered_out,
        "errors": errors,
    }
