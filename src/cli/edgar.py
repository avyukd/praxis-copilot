"""EDGAR API utilities for resolving tickers to CIK and company info."""

import json

import requests

# SEC requires a User-Agent header with contact info
HEADERS = {
    "User-Agent": "PraxisCopilot/0.1 (research-tool)",
    "Accept": "application/json",
}

# SEC's full-text search endpoint
EFTS_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"

# SEC's company tickers JSON (more reliable for direct lookup)
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_TICKERS_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"


def resolve_ticker(ticker: str) -> dict | None:
    """Resolve a ticker symbol to CIK, company name, and exchange.

    Returns dict with keys: cik, name, exchange, or None if not found.
    """
    ticker_upper = ticker.upper()

    # Try the SEC company tickers JSON first (most reliable)
    try:
        resp = requests.get(COMPANY_TICKERS_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        for entry in data.values():
            if entry.get("ticker", "").upper() == ticker_upper:
                cik = str(entry["cik_str"]).zfill(10)
                name = entry.get("title", "")
                # This endpoint doesn't include exchange, try to get it
                exchange = _lookup_exchange(ticker_upper, cik)
                return {
                    "cik": cik,
                    "name": name,
                    "exchange": exchange or "UNKNOWN",
                }
    except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
        # Fall through to EFTS search
        pass

    # Fallback: EFTS search
    try:
        params = {
            "q": ticker_upper,
            "dateRange": "custom",
            "startdt": "2024-01-01",
            "enddt": "2025-01-01",
        }
        resp = requests.get(EFTS_SEARCH_URL, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        if hits:
            source = hits[0].get("_source", {})
            cik = str(source.get("entity_id", "")).zfill(10)
            name = source.get("entity_name", "")
            return {
                "cik": cik,
                "name": name,
                "exchange": "UNKNOWN",
            }
    except (requests.RequestException, json.JSONDecodeError, KeyError):
        pass

    return None


def _lookup_exchange(ticker: str, cik: str) -> str | None:
    """Try to look up exchange for a ticker from SEC's exchange-aware endpoint."""
    try:
        resp = requests.get(COMPANY_TICKERS_EXCHANGE_URL, headers=HEADERS, timeout=10)
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
