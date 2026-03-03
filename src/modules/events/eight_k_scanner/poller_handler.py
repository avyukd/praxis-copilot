"""Poller Lambda: EventBridge cron -> poll EDGAR EFTS -> fetch -> store to S3.

Triggered by EventBridge cron during market hours.
Stores raw docs to s3://praxis-copilot/data/raw/8k/{cik}/{accession}/
"""
from __future__ import annotations

import logging

from src.modules.events.eight_k_scanner.edgar.poller import poll_new_8k_filings
from src.modules.events.eight_k_scanner.edgar.fetcher import fetch_filing
from src.modules.events.eight_k_scanner.universe.builder import is_in_universe
from src.modules.events.eight_k_scanner.config import SCANNER_LOOKBACK_MINUTES
from src.modules.events.eight_k_scanner.storage.s3 import store_filing, filing_exists

logger = logging.getLogger(__name__)


def lambda_handler(event=None, context=None):
    """Lambda entry point: poll -> filter by universe -> fetch -> store."""
    logging.getLogger().setLevel(logging.INFO)

    filings = poll_new_8k_filings(lookback_minutes=SCANNER_LOOKBACK_MINUTES)
    logger.info(f"Discovered {len(filings)} new 8-K filings")

    stored = 0
    errors = 0
    skipped = 0
    filtered_out = 0

    for filing_meta in filings:
        cik = filing_meta["cik"]
        accession = filing_meta["accession_number"]

        in_universe, info = is_in_universe(cik)
        if not in_universe:
            filtered_out += 1
            continue

        if filing_exists(cik, accession):
            skipped += 1
            continue

        try:
            result = fetch_filing(cik, accession)

            meta = result["metadata"]
            meta["ticker"] = info.get("ticker", "")
            meta["company_name"] = info.get("company_name") or filing_meta.get("company_name", "")
            meta["market_cap"] = info.get("market_cap")
            meta["exchange"] = info.get("exchange", "")
            meta["filed_date"] = filing_meta.get("filed_date", "")
            if not meta.get("acceptance_datetime"):
                meta["acceptance_datetime"] = filing_meta.get("acceptance_datetime", "")
            else:
                meta["acceptance_datetime"] = meta["acceptance_datetime"] or filing_meta.get("acceptance_datetime", "")

            store_filing(cik, accession, meta, result["documents"])
            stored += 1

        except Exception:
            logger.exception(f"Failed to process filing {accession} for CIK {cik}")
            errors += 1

    logger.info(
        f"Done: {stored} stored, {skipped} skipped, {filtered_out} filtered out, "
        f"{errors} errors (out of {len(filings)} discovered)"
    )

    return {
        "total_discovered": len(filings),
        "filtered_out": filtered_out,
        "stored": stored,
        "skipped": skipped,
        "errors": errors,
    }
