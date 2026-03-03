"""CA Poller Lambda: poll Canadian newswires -> filter -> fetch -> store in S3."""
from __future__ import annotations

import logging

from src.modules.events.eight_k_scanner.canada.poller import poll_canadian_releases
from src.modules.events.eight_k_scanner.canada.universe import is_in_ca_universe
from src.modules.events.eight_k_scanner.config import S3_BUCKET, S3_CA_RAW_PREFIX
from src.modules.events.eight_k_scanner.models import PRIndexMeta
from src.modules.events.eight_k_scanner.newswire.fetcher import fetch_release
from src.modules.events.eight_k_scanner.storage.s3 import (
    et_now_iso,
    get_s3_client,
    read_json_from_s3,
    write_json_to_s3,
)

logger = logging.getLogger(__name__)


def lambda_handler(event=None, context=None):
    logging.basicConfig(level=logging.INFO)

    releases = poll_canadian_releases()
    logger.info(f"Discovered {len(releases)} new Canadian press releases")

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

        prefix = f"{S3_CA_RAW_PREFIX}/{ticker}/{release_id}"
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
        f"{errors} errors (out of {len(releases)} discovered)"
    )

    return {
        "total_discovered": len(releases),
        "stored": stored,
        "skipped": skipped,
        "filtered_out": filtered_out,
        "errors": errors,
    }
