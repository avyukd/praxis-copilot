"""Content hashing and change detection for news results."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict

import boto3
from botocore.exceptions import ClientError

from .serp import SerpResponse

logger = logging.getLogger(__name__)

BUCKET = "praxis-copilot"
HASHES_KEY = "data/state/news_hashes.json"


def compute_hash(response: SerpResponse) -> str:
    """Compute a deterministic content hash for a ticker's SERP results.

    Hashes the sorted list of (headline, url) tuples to detect content changes
    while being insensitive to ordering changes.
    """
    items = sorted((r.headline, r.url) for r in response.results)
    payload = json.dumps(items, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def load_previous_hashes(s3_client: boto3.client) -> dict[str, str]:
    """Load previous sweep's content hashes from S3."""
    try:
        obj = s3_client.get_object(Bucket=BUCKET, Key=HASHES_KEY)
        return json.loads(obj["Body"].read().decode())
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            logger.info("No previous hashes found at %s — first sweep", HASHES_KEY)
        else:
            logger.error("S3 error loading hashes (%s): %s", code, e)
        return {}
    except (json.JSONDecodeError, ValueError) as e:
        logger.error("Corrupt hashes file at %s: %s", HASHES_KEY, e)
        return {}


def save_hashes(s3_client: boto3.client, hashes: dict[str, str]) -> None:
    """Save current sweep's content hashes to S3."""
    s3_client.put_object(
        Bucket=BUCKET,
        Key=HASHES_KEY,
        Body=json.dumps(hashes, indent=2).encode(),
        ContentType="application/json",
    )


def detect_changes(
    responses: dict[str, SerpResponse],
    previous_hashes: dict[str, str],
) -> tuple[dict[str, str], set[str]]:
    """Compare current SERP results against previous hashes.

    Returns:
        (current_hashes, changed_tickers)
    """
    current_hashes: dict[str, str] = {}
    changed_tickers: set[str] = set()

    for ticker, resp in responses.items():
        h = compute_hash(resp)
        current_hashes[ticker] = h
        prev = previous_hashes.get(ticker)
        if prev != h:
            changed_tickers.add(ticker)
            logger.info(
                "Ticker %s hash changed: %s -> %s", ticker, prev, h
            )

    return current_hashes, changed_tickers


def store_raw_results(
    s3_client: boto3.client,
    response: SerpResponse,
    date_str: str,
    hour: int,
    content_hash: str,
    previous_hash: str | None,
) -> str:
    """Store raw SERP results to S3.

    Returns the S3 key where stored.
    """
    key = f"data/news/{date_str}/raw/{response.ticker}/{hour:02d}.json"
    payload = {
        "ticker": response.ticker,
        "date": date_str,
        "hour": hour,
        "content_hash": content_hash,
        "previous_hash": previous_hash,
        "changed": content_hash != previous_hash,
        "query": response.query,
        "results": [
            {
                "headline": r.headline,
                "url": r.url,
                "snippet": r.snippet,
                "source": r.source,
                "published": r.published,
            }
            for r in response.results
        ],
    }
    s3_client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(payload, indent=2).encode(),
        ContentType="application/json",
    )
    return key
