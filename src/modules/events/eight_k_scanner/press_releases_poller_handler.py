"""Press releases poller: GNW + CA newswires -> unified S3 raw path."""
from __future__ import annotations

import logging

from botocore.exceptions import ClientError

from src.modules.events.eight_k_scanner.canada.poller import poll_canadian_releases
from src.modules.events.eight_k_scanner.canada.universe import is_in_ca_universe
from src.modules.events.eight_k_scanner.config import (
    MARKET_CAP_THRESHOLD,
    S3_BUCKET,
    S3_PRESS_RELEASES_RAW_PREFIX,
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


def lambda_handler(event=None, context=None):
    """Poll CA + US press releases and store under canonical press_releases path."""
    logging.basicConfig(level=logging.INFO)

    ca_stats = _process_ca_releases()
    us_stats = _process_us_gnw_releases()

    result = {
        "ca": ca_stats,
        "us_gnw": us_stats,
        "total_discovered": ca_stats["total_discovered"] + us_stats["total_discovered"],
        "stored": ca_stats["stored"] + us_stats["stored"],
        "skipped": ca_stats["skipped"] + us_stats["skipped"],
        "filtered_out": ca_stats["filtered_out"] + us_stats["filtered_out"],
        "errors": ca_stats["errors"] + us_stats["errors"],
    }
    logger.info("Press release poller complete: %s", result)
    return result


def _process_ca_releases() -> dict[str, int]:
    releases = poll_canadian_releases()
    logger.info("Discovered %d new Canadian press releases", len(releases))

    stored = 0
    errors = 0
    filtered_out = 0
    skipped = 0

    for release in releases:
        ticker = release.ticker
        exchange = release.exchange
        release_id = release.release_id

        if not ticker:
            filtered_out += 1
            continue

        in_universe, info = is_in_ca_universe(ticker, exchange)
        if not in_universe:
            filtered_out += 1
            continue

        source = _normalize_source(release.source or "ca")
        prefix = f"{S3_PRESS_RELEASES_RAW_PREFIX}/{source}/{ticker}/{release_id}"
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
                symbol=info.symbol,
                exchange=exchange,
                market_cap=info.market_cap,
                release_id=release_id,
                title=release.title,
                url=release.url,
                published_at=release.published_at,
                filed_date=(release.published_at or "")[:10],
                acceptance_datetime=release.published_at,
                source=source,
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
            logger.exception("Failed to process CA release %s for %s", release_id, ticker)
            errors += 1

    return {
        "total_discovered": len(releases),
        "stored": stored,
        "skipped": skipped,
        "filtered_out": filtered_out,
        "errors": errors,
    }


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


def _process_us_gnw_releases() -> dict[str, int]:
    last_seen = _load_last_seen()
    all_releases = poll_gnw(GNW_US_FEEDS)
    all_releases = [r for r in all_releases if r.exchange in ("NYSE", "NASDAQ")]

    new_releases: list[PressRelease] = []
    newest_ids: dict[str, str] = dict(last_seen) if last_seen else {}
    for release in all_releases:
        source = _normalize_source(release.source or "gnw")
        rid = release.release_id
        if last_seen and rid <= last_seen.get(source, ""):
            continue
        new_releases.append(release)
        if rid > newest_ids.get(source, ""):
            newest_ids[source] = rid

    if newest_ids != (last_seen or {}):
        _save_last_seen(newest_ids)

    logger.info("Discovered %d new US GNW press releases", len(new_releases))

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

        source = _normalize_source(release.source or "gnw")
        prefix = f"{S3_PRESS_RELEASES_RAW_PREFIX}/{source}/{ticker}/{release_id}"
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
                source=source,
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
            logger.exception("Failed to process US release %s for %s", release_id, ticker)
            errors += 1

    return {
        "total_discovered": len(new_releases),
        "stored": stored,
        "skipped": skipped,
        "filtered_out": filtered_out,
        "errors": errors,
    }


def _normalize_source(source: str) -> str:
    value = (source or "unknown").strip().lower().replace(" ", "_")
    value = value.replace("-", "_")
    return value or "unknown"
