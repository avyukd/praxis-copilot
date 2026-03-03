"""Earnings call transcript ingestion — best-effort pull from ROIC.AI v2 API."""

import json
import logging
import os
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

ROICAI_TRANSCRIPT_URL = "https://api.roic.ai/v2/company/earnings-calls/transcript/{ticker}"


def _get_recent_quarters(count: int = 8) -> list[tuple[int, int]]:
    """Generate (year, quarter) tuples for the most recent N quarters."""
    now = datetime.now()
    year = now.year
    quarter = (now.month - 1) // 3 + 1

    quarters = []
    for _ in range(count):
        quarters.append((year, quarter))
        quarter -= 1
        if quarter == 0:
            quarter = 4
            year -= 1
    return quarters


def ingest_transcripts(ticker: str) -> list[dict]:
    """Pull last 8 quarters of earnings call transcripts from ROIC.AI.

    Returns list of dicts with keys: quarter, text.
    Best-effort — returns whatever is available.
    """
    api_key = os.environ.get("ROICAI_API_KEY", "")
    if not api_key:
        logger.warning("ROICAI_API_KEY not set, skipping transcript ingestion")
        return []

    transcripts = []

    for year, quarter in _get_recent_quarters(8):
        url = ROICAI_TRANSCRIPT_URL.format(ticker=ticker)
        params = {"year": year, "quarter": quarter, "apikey": api_key}

        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 404:
                logger.debug(f"No transcript for {ticker} {year} Q{quarter}")
                continue
            if resp.status_code == 403:
                logger.warning("ROIC.AI transcripts require a premium subscription")
                return transcripts
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch transcript for {ticker} {year} Q{quarter}: {e}")
            continue
        except (ValueError, json.JSONDecodeError) as e:
            logger.warning(f"Invalid transcript response for {ticker} {year} Q{quarter}: {e}")
            continue

        if not data:
            continue

        # Extract transcript text — handle both string and structured responses
        if isinstance(data, str):
            text = data
        elif isinstance(data, dict):
            text = data.get("transcript") or data.get("text") or data.get("content", "")
        elif isinstance(data, list):
            text = "\n\n".join(
                entry.get("text") or entry.get("transcript", "")
                for entry in data if isinstance(entry, dict)
            )
        else:
            continue

        if text and len(text.strip()) > 100:
            label = f"{year}_Q{quarter}"
            transcripts.append({"quarter": label, "text": text.strip()})
            logger.info(f"  Fetched transcript for {ticker} {year} Q{quarter}")

    logger.info(f"Fetched {len(transcripts)} transcript(s) for {ticker}")
    return transcripts
