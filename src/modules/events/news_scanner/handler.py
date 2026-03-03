"""News Scanner Lambda handler.

Two-layer architecture:
  Layer 1 (SERP Sweep): Runs hourly, fetches news for all tickers, hashes and deduplicates.
  Layer 2 (Agent Triage): If any ticker's content changed, runs Sonnet triage on changed headlines.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import boto3
import yaml

from . import dedup, serp, triage

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUCKET = "praxis-copilot"
CONFIG_KEY = "config/news.yaml"
REGISTRY_KEY = "config/ticker_registry.yaml"

# Default config values
DEFAULTS = {
    "enabled": True,
    "serp_api": "serpapi",
    "serp_api_key_param": "/praxis/serpapi_key",
    "results_per_ticker": 10,
    "lookback_hours": 24,
    "market_hours_only": True,
}


def _load_yaml_from_s3(s3_client: boto3.client, key: str) -> dict[str, Any]:
    """Load and parse a YAML file from S3."""
    obj = s3_client.get_object(Bucket=BUCKET, Key=key)
    return yaml.safe_load(obj["Body"].read().decode()) or {}


def _load_config(s3_client: boto3.client) -> dict[str, Any]:
    """Load news scanner config from S3, with defaults."""
    try:
        config = _load_yaml_from_s3(s3_client, CONFIG_KEY)
    except Exception:
        logger.warning("Could not load %s, using defaults", CONFIG_KEY)
        config = {}
    merged = {**DEFAULTS, **config}
    return merged


def _load_ticker_registry(s3_client: boto3.client) -> dict[str, dict[str, Any]]:
    """Load ticker registry from S3. Returns dict of ticker -> config."""
    registry = _load_yaml_from_s3(s3_client, REGISTRY_KEY)
    return registry.get("tickers", {})


def _get_serp_api_key(ssm_client: boto3.client, param_name: str) -> str:
    """Retrieve SERP API key from SSM Parameter Store."""
    resp = ssm_client.get_parameter(Name=param_name, WithDecryption=True)
    return resp["Parameter"]["Value"]


def _is_market_hours(now: datetime) -> bool:
    """Check if current time is within US market hours (roughly 7am-5pm ET).

    Approximation — doesn't handle holidays or DST precisely.
    """
    # Convert to ET (UTC-5 or UTC-4 for DST)
    # Simple approximation: market hours are 12:00-22:00 UTC
    hour_utc = now.hour
    return 12 <= hour_utc <= 22


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
    if not config["enabled"]:
        logger.info("News scanner disabled via config")
        return {"status": "disabled"}

    # Check market hours
    if config["market_hours_only"] and not _is_market_hours(now):
        logger.info("Outside market hours, skipping sweep")
        return {"status": "skipped", "reason": "outside_market_hours"}

    # Load ticker registry
    tickers = _load_ticker_registry(s3)
    if not tickers:
        logger.warning("No tickers in registry")
        return {"status": "skipped", "reason": "no_tickers"}

    # Get SERP API key from SSM
    api_key = _get_serp_api_key(ssm, config["serp_api_key_param"])

    # Initialize SERP provider
    provider = serp.get_provider(config["serp_api"], api_key)

    # --- Layer 1: SERP Sweep ---
    logger.info("Starting SERP sweep for %d tickers", len(tickers))

    responses: dict[str, serp.SerpResponse] = {}
    for ticker, ticker_config in tickers.items():
        try:
            resp = serp.sweep_ticker(
                provider, ticker, ticker_config, num_results=config["results_per_ticker"]
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
