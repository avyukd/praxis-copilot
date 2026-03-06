"""Configuration for 8k-scanner module.

Reads from S3 config/ prefix at runtime. Falls back to env vars / defaults
for local development.
"""
from __future__ import annotations

import logging
import os
import re
from functools import lru_cache
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# --- Core S3 settings (always from env) ---
S3_BUCKET = os.environ.get("S3_BUCKET", "praxis-copilot")
S3_CONFIG_PREFIX = "config"
S3_DATA_PREFIX = "data"

# S3 paths (praxis-copilot layout)
S3_RAW_PREFIX = f"{S3_DATA_PREFIX}/raw/8k"
S3_FILINGS_RAW_PREFIX = f"{S3_DATA_PREFIX}/raw/filings"
S3_PRESS_RELEASES_RAW_PREFIX = f"{S3_DATA_PREFIX}/raw/press_releases"
S3_CA_RAW_PREFIX = f"{S3_DATA_PREFIX}/raw/ca-pr"
S3_US_PR_RAW_PREFIX = f"{S3_DATA_PREFIX}/raw/us-pr"
S3_POLLER_STATE_KEY = f"{S3_DATA_PREFIX}/state/poller_last_seen.json"
CA_POLLER_STATE_KEY = f"{S3_DATA_PREFIX}/state/ca_poller_last_seen.json"
US_GNW_POLLER_STATE_KEY = f"{S3_DATA_PREFIX}/state/us_gnw_poller_last_seen.json"

# EDGAR
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "PraxisCopilot admin@example.com")
EDGAR_MAX_RPS = 5

# API keys (always from env -- never stored in S3 config)
FMP_API_KEY = os.environ.get("FMP_API_KEY", "")
EODHD_API_KEY = os.environ.get("EODHD_API_KEY", "")

# --- S3 config reader ---

_s3_client = None


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


@lru_cache(maxsize=16)
def _read_s3_config(filename: str) -> dict[str, Any]:
    """Read a YAML config file from S3 config/ prefix. Cached per Lambda invocation."""
    key = f"{S3_CONFIG_PREFIX}/{filename}"
    try:
        resp = _get_s3_client().get_object(Bucket=S3_BUCKET, Key=key)
        content = resp["Body"].read().decode("utf-8")
        return yaml.safe_load(content) or {}
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "NoSuchKey":
            logger.warning(f"Config file not found in S3: {key}")
            return {}
        raise
    except Exception:
        logger.exception(f"Failed to read config from S3: {key}")
        return {}


def get_scanner_config() -> dict[str, Any]:
    """Read config/8k_scanner.yaml from S3."""
    return _read_s3_config("8k_scanner.yaml")


def get_ticker_registry() -> dict[str, Any]:
    """Read config/ticker_registry.yaml from S3."""
    return _read_s3_config("ticker_registry.yaml")


# --- Derived config (env vars / defaults, overridable by S3 config at runtime) ---

MARKET_CAP_THRESHOLD = int(os.environ.get("MARKET_CAP_THRESHOLD", 2_000_000_000))
CA_MARKET_CAP_THRESHOLD = int(os.environ.get("CA_MARKET_CAP_THRESHOLD", 500_000_000))
WATCHLIST_TICKERS: list[str] = [
    t.strip()
    for t in os.environ.get("WATCHLIST_TICKERS", "").split(",")
    if t.strip()
]

# LLM
LLM_MODEL = os.environ.get("LLM_MODEL", "anthropic/claude-sonnet-4-5-20250929")
LLM_TOKENS_PER_MINUTE = int(os.environ.get("LLM_TOKENS_PER_MINUTE", "30000"))
DISABLE_LLM_ANALYSIS = os.environ.get("DISABLE_LLM_ANALYSIS", "0") in {"1", "true", "TRUE"}

# Scanner / alerting
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
SCANNER_MIN_ADTV = float(os.environ.get("SCANNER_MIN_ADTV", "10000"))
SCANNER_STRATEGY = os.environ.get("SCANNER_STRATEGY", "long")
SCANNER_MIN_MAGNITUDE = float(os.environ.get("SCANNER_MIN_MAGNITUDE", "0.5"))
SCANNER_LOOKBACK_MINUTES = int(os.environ.get("SCANNER_LOOKBACK_MINUTES", "1440"))
CA_LOOKBACK_MINUTES = int(os.environ.get("CA_LOOKBACK_MINUTES", "1440"))

# Price data source order
PRICE_SOURCES: list[str] = [
    s.strip()
    for s in os.environ.get("PRICE_SOURCES", "yfinance,fmp,eodhd").split(",")
    if s.strip()
]

SCANNER_POLLER_SEEN_TTL_DAYS = int(os.environ.get("SCANNER_POLLER_SEEN_TTL_DAYS", "7"))

# Generic filing analyzer policy
FILING_ANALYZER_ENABLED_FORMS: list[str] = [
    form.strip()
    for form in re.split(
        r"[;,]",
        os.environ.get("FILING_ANALYZER_ENABLED_FORMS", "8-K;8-K/A"),
    )
    if form.strip()
]

# 8-K cost saver: optional Haiku prescreen before Sonnet deep analysis.
ENABLE_8K_HAIKU_SCREEN = os.environ.get("ENABLE_8K_HAIKU_SCREEN", "1") in {"1", "true", "TRUE"}
ENABLE_PRESS_RELEASE_HAIKU_SCREEN = os.environ.get(
    "ENABLE_PRESS_RELEASE_HAIKU_SCREEN", "1"
) in {"1", "true", "TRUE"}
HAIKU_PRESCREEN_MODEL = os.environ.get(
    "HAIKU_PRESCREEN_MODEL",
    "anthropic/claude-3-haiku-20240307",
)
