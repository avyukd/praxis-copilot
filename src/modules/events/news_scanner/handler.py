"""News Scanner Lambda handler.

Two-layer architecture:
  Layer 1 (SERP Sweep): Runs hourly, fetches news for all tickers, hashes and deduplicates.
  Layer 2 (Agent Triage): If any ticker's content changed, runs Sonnet triage on changed headlines.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError

from src.cli.models import TickerRegistry, TickerRegistryEntry

from . import dedup, serp, triage
from .models import NewsScannerConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = os.environ.get("S3_BUCKET", "praxis-copilot")
CONFIG_KEY = "config/news.yaml"
REGISTRY_KEY = "config/ticker_registry.yaml"


def _load_yaml_from_s3(s3_client: boto3.client, key: str) -> dict[str, Any]:
    """Load and parse a YAML file from S3."""
    obj = s3_client.get_object(Bucket=BUCKET, Key=key)
    return yaml.safe_load(obj["Body"].read().decode()) or {}


def _load_config(s3_client: boto3.client) -> NewsScannerConfig:
    """Load news scanner config from S3, with defaults."""
    try:
        config = _load_yaml_from_s3(s3_client, CONFIG_KEY)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            logger.info("No config at %s, using defaults", CONFIG_KEY)
        else:
            logger.warning("S3 error loading %s (%s), using defaults", CONFIG_KEY, code)
        config = {}
    except (yaml.YAMLError, ValueError) as e:
        logger.warning("Failed to parse %s (%s), using defaults", CONFIG_KEY, e)
        config = {}
    return NewsScannerConfig(**config)


def _load_ticker_registry(s3_client: boto3.client) -> dict[str, TickerRegistryEntry]:
    """Load ticker registry from S3. Returns dict of ticker -> config."""
    raw = _load_yaml_from_s3(s3_client, REGISTRY_KEY)
    registry = TickerRegistry.model_validate(raw)
    return registry.tickers


def _get_serp_api_key(ssm_client: boto3.client, param_name: str) -> str:
    """Retrieve SERP API key from SSM Parameter Store."""
    resp = ssm_client.get_parameter(Name=param_name, WithDecryption=True)
    return resp["Parameter"]["Value"]


def _is_market_hours(now: datetime) -> bool:
    """Check if current time is within extended US market hours (7am-5pm ET).

    Uses US Eastern DST rules: second Sunday of March to first Sunday of November.
    """
    # Determine ET offset: UTC-4 during DST, UTC-5 otherwise
    year = now.year
    # Second Sunday of March
    march_1 = datetime(year, 3, 1, tzinfo=timezone.utc)
    dst_start = datetime(year, 3, 14 - (march_1.weekday() + 1) % 7, 2, tzinfo=timezone.utc)
    # First Sunday of November
    nov_1 = datetime(year, 11, 1, tzinfo=timezone.utc)
    dst_end = datetime(year, 11, 7 - (nov_1.weekday() + 1) % 7, 2, tzinfo=timezone.utc)

    is_dst = dst_start <= now < dst_end
    et_offset = -4 if is_dst else -5
    et_hour = (now.hour + et_offset) % 24

    return 7 <= et_hour <= 17


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entry point.

    Triggered by EventBridge hourly during market hours.
    """
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    hour = now.hour

    s3 = boto3.client("s3")
    ssm = boto3.client("ssm")

    # Load config
    config = _load_config(s3)
    if not config.enabled:
        logger.info("News scanner disabled via config")
        return {"status": "disabled"}

    # Check market hours
    if config.market_hours_only and not _is_market_hours(now):
        logger.info("Outside market hours, skipping sweep")
        return {"status": "skipped", "reason": "outside_market_hours"}

    # Load ticker registry
    tickers = _load_ticker_registry(s3)
    if not tickers:
        logger.warning("No tickers in registry")
        return {"status": "skipped", "reason": "no_tickers"}

    # Get API key: try env var first (TAVILY_API_KEY or SERPAPI_KEY), then SSM
    api_key = ""
    if config.serp_api == "tavily":
        api_key = os.environ.get("TAVILY_API_KEY", "")
    elif config.serp_api == "serpapi":
        api_key = os.environ.get("SERPAPI_KEY", "")

    if not api_key:
        try:
            api_key = _get_serp_api_key(ssm, config.serp_api_key_param)
        except ClientError as e:
            logger.error("Failed to retrieve SERP API key from SSM: %s", e)
            return {"status": "error", "reason": "ssm_key_retrieval_failed"}

    if not api_key:
        logger.error("No API key available for provider %s", config.serp_api)
        return {"status": "error", "reason": "no_api_key"}

    # Initialize SERP provider
    provider = serp.get_provider(config.serp_api, api_key)

    # --- Layer 1: SERP Sweep ---
    logger.info("Starting SERP sweep for %d tickers", len(tickers))

    responses: dict[str, serp.SerpResponse] = {}
    for ticker, ticker_config in tickers.items():
        try:
            resp = serp.sweep_ticker(
                provider, ticker, ticker_config, num_results=config.results_per_ticker
            )
            responses[ticker] = resp
        except Exception:
            logger.exception("SERP sweep failed for %s", ticker)

    # Load previous hashes and detect changes
    previous_hashes = dedup.load_previous_hashes(s3)
    current_hashes, changed_tickers = dedup.detect_changes(responses, previous_hashes)

    # Store raw results and update hashes
    for ticker, resp in responses.items():
        try:
            dedup.store_raw_results(
                s3,
                resp,
                date_str,
                hour,
                content_hash=current_hashes.get(ticker, ""),
                previous_hash=previous_hashes.get(ticker),
            )
        except Exception:
            logger.exception("Failed to store raw results for %s", ticker)

    dedup.save_hashes(s3, current_hashes)

    logger.info(
        "SERP sweep complete. %d/%d tickers changed: %s",
        len(changed_tickers),
        len(tickers),
        sorted(changed_tickers),
    )

    # --- Layer 2: Agent Triage (only if changes detected) ---
    if not changed_tickers:
        return {
            "status": "ok",
            "layer1": "complete",
            "layer2": "skipped",
            "tickers_scanned": len(tickers),
            "tickers_changed": 0,
        }

    changed_responses = {t: responses[t] for t in changed_tickers if t in responses}

    try:
        digest = triage.run_triage(s3, changed_responses)
        digest_key = triage.store_digest(s3, digest, date_str, hour)
        logger.info("Triage complete, digest at %s", digest_key)
    except Exception:
        logger.exception("Triage failed")
        digest_key = None

    return {
        "status": "ok",
        "layer1": "complete",
        "layer2": "complete" if digest_key else "failed",
        "tickers_scanned": len(tickers),
        "tickers_changed": len(changed_tickers),
        "changed_tickers": sorted(changed_tickers),
        "digest_key": digest_key,
    }
