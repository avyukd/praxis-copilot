"""Pull key financial fundamentals from SEC XBRL API and store to S3."""
from __future__ import annotations

import json
import logging
import time

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

SEC_USER_AGENT = "PraxisCopilot/0.1 (research-tool)"
XBRL_COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
S3_BUCKET = "praxis-copilot"

# Key XBRL concepts to extract
KEY_CONCEPTS = {
    "us-gaap": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
        "NetIncomeLoss",
        "EarningsPerShareBasic",
        "EarningsPerShareDiluted",
        "Assets",
        "Liabilities",
        "StockholdersEquity",
        "OperatingIncomeLoss",
        "GrossProfit",
        "CashAndCashEquivalentsAtCarryingValue",
        "LongTermDebt",
        "CommonStockSharesOutstanding",
    ],
}

_last_request_time = 0.0
_MIN_INTERVAL = 0.11


def _rate_limit():
    global _last_request_time
    now = time.monotonic()
    elapsed = now - _last_request_time
    if elapsed < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - elapsed)
    _last_request_time = time.monotonic()


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": SEC_USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
    })
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


_session = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = _build_session()
    return _session


def _fetch_company_facts(cik: str) -> dict:
    """Fetch the full company facts JSON from SEC XBRL API."""
    padded_cik = cik.zfill(10)
    url = XBRL_COMPANY_FACTS_URL.format(cik=padded_cik)
    _rate_limit()
    resp = _get_session().get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _extract_key_metrics(facts: dict) -> dict:
    """Extract key financial metrics from company facts data.

    Returns a dict keyed by concept name, each containing a list of
    data points (value, period, form, filed date).
    """
    extracted = {}
    facts_data = facts.get("facts", {})

    for taxonomy, concepts in KEY_CONCEPTS.items():
        taxonomy_data = facts_data.get(taxonomy, {})
        for concept in concepts:
            concept_data = taxonomy_data.get(concept)
            if concept_data is None:
                continue

            units = concept_data.get("units", {})
            # Most financial data is in USD or shares
            for unit_type, entries in units.items():
                key = f"{concept}_{unit_type}" if len(units) > 1 else concept
                # Keep only 10-K and 10-Q entries, take the most recent ones
                filtered = [
                    {
                        "value": e.get("val"),
                        "period_end": e.get("end"),
                        "period_start": e.get("start"),
                        "form": e.get("form"),
                        "filed": e.get("filed"),
                        "fiscal_year": e.get("fy"),
                        "fiscal_period": e.get("fp"),
                    }
                    for e in entries
                    if e.get("form") in ("10-K", "10-Q")
                ]
                # Sort by filing date descending, keep recent entries
                filtered.sort(key=lambda x: x.get("filed", ""), reverse=True)
                if filtered:
                    extracted[key] = filtered[:20]  # Keep last ~5 years of quarterly data

    return extracted


def ingest_fundamentals(ticker: str, cik: str, s3_client=None) -> dict:
    """Pull key financial data from SEC XBRL API and store to S3.

    Returns summary dict.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    s3_prefix = f"data/research/{ticker}/data/fundamentals"

    try:
        logger.info(f"Fetching company facts for {ticker} (CIK: {cik})")
        facts = _fetch_company_facts(cik)
    except Exception:
        logger.exception(f"Failed to fetch company facts for {ticker}")
        return {"fundamentals_stored": False, "error": "Failed to fetch company facts"}

    # Store the raw company facts (full JSON)
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f"{s3_prefix}/company_facts_raw.json",
            Body=json.dumps(facts, indent=2),
            ContentType="application/json",
        )
        logger.info(f"Stored raw company facts for {ticker}")
    except Exception:
        logger.exception(f"Failed to store raw company facts for {ticker}")
        return {"fundamentals_stored": False, "error": "Failed to store raw facts"}

    # Extract and store key metrics
    try:
        metrics = _extract_key_metrics(facts)
        entity_name = facts.get("entityName", ticker)

        summary = {
            "ticker": ticker,
            "cik": cik,
            "entity_name": entity_name,
            "metrics_available": list(metrics.keys()),
            "metrics": metrics,
        }

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f"{s3_prefix}/key_metrics.json",
            Body=json.dumps(summary, indent=2),
            ContentType="application/json",
        )
        logger.info(
            f"Stored key metrics for {ticker}: {len(metrics)} concept(s)"
        )

        return {
            "fundamentals_stored": True,
            "concepts_extracted": len(metrics),
        }

    except Exception:
        logger.exception(f"Failed to extract/store key metrics for {ticker}")
        return {"fundamentals_stored": False, "error": "Failed to extract metrics"}
