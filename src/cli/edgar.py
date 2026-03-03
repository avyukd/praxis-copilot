"""EDGAR API utilities for resolving tickers to CIK and company info."""

import json
import os
from datetime import datetime, timedelta

import requests

from cli.models import TickerInfo

# SEC requires a User-Agent header with company name and contact email
# See: https://www.sec.gov/os/accessing-edgar-data
_email = os.environ.get("SEC_CONTACT_EMAIL", "praxis.copilot.tool@gmail.com")

# Use a Session for connection pooling and proper default headers
_session = requests.Session()
_session.headers.update({
    "User-Agent": f"PraxisCopilot/0.1 ({_email})",
    "Accept": "*/*",
})

# SEC's full-text search endpoint
EFTS_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"

# SEC's company tickers JSON (more reliable for direct lookup)
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"


def resolve_ticker(ticker: str) -> TickerInfo | None:
    """Resolve a ticker symbol to CIK, company name, and exchange.

    Returns TickerInfo or None if not found.
    """
    ticker_upper = ticker.upper()

    # Try the SEC company tickers JSON first (most reliable)
    try:
        resp = _session.get(COMPANY_TICKERS_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for entry in data.values():
            if entry.get("ticker", "").upper() == ticker_upper:
                cik = str(entry["cik_str"]).zfill(10)
                name = entry.get("title", "")
                # This endpoint doesn't include exchange, try to get it
                exchange = _lookup_exchange(ticker_upper, cik)
                return TickerInfo(
                    cik=cik,
                    name=name,
                    exchange=exchange or "UNKNOWN",
                )
    except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
        # Fall through to EFTS search
        pass

    # Fallback: EFTS search
    try:
        now = datetime.now()
        params = {
            "q": ticker_upper,
            "dateRange": "custom",
            "startdt": (now - timedelta(days=365)).strftime("%Y-%m-%d"),
            "enddt": now.strftime("%Y-%m-%d"),
        }
        resp = _session.get(EFTS_SEARCH_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        if hits:
            source = hits[0].get("_source", {})
            entity_id = source.get("entity_id", "")
            if entity_id and str(entity_id).strip("0"):
                cik = str(entity_id).zfill(10)
                name = source.get("entity_name", "")
                return TickerInfo(
                    cik=cik,
                    name=name,
                    exchange="UNKNOWN",
                )
    except (requests.RequestException, json.JSONDecodeError, KeyError):
        pass

    return None


def _lookup_exchange(ticker: str, cik: str) -> str | None:
    """Try to look up exchange for a ticker from SEC's exchange-aware endpoint."""
    try:
        resp = _session.get(COMPANY_TICKERS_EXCHANGE_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        # Format: {"fields": [...], "data": [[cik, name, ticker, exchange], ...]}
        fields = data.get("fields", [])
        rows = data.get("data", [])
        ticker_idx = fields.index("ticker") if "ticker" in fields else 2
        exchange_idx = fields.index("exchange") if "exchange" in fields else 3
        for row in rows:
            if row[ticker_idx].upper() == ticker.upper():
                return row[exchange_idx]
    except (requests.RequestException, json.JSONDecodeError, KeyError, ValueError, IndexError):
        pass
    return None
