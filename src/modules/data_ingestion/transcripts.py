"""Earnings call transcript ingestion — best-effort pull from ROIC.AI."""

import json
import logging
import os

import requests

logger = logging.getLogger(__name__)


def ingest_transcripts(ticker: str) -> list[dict]:
    """Pull earnings call transcripts from ROIC.AI.

    Returns list of dicts with keys: quarter, text.
    This is best-effort — returns empty list if unavailable.
    """
    api_key = os.environ.get("ROICAI_API_KEY", "")
    if not api_key:
        logger.warning("ROICAI_API_KEY not set, skipping transcript ingestion")
        return []

    url = f"https://roic.ai/api/transcripts/{ticker}"
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch transcripts for {ticker}: {e}")
        return []
    except (ValueError, json.JSONDecodeError) as e:
        logger.warning(f"Invalid transcript response for {ticker}: {e}")
        return []

    if not data or not isinstance(data, list):
        logger.warning(f"No transcripts available for {ticker}")
        return []

    transcripts = []
    for entry in data:
        quarter = entry.get("quarter") or entry.get("period", "unknown")
        text = entry.get("text") or entry.get("transcript", "")
        if text:
            transcripts.append({"quarter": str(quarter), "text": str(text)})

    logger.info(f"Fetched {len(transcripts)} transcript(s) for {ticker}")
    return transcripts
