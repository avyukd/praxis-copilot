"""Universe filtering: checks CIK against ticker registry and market cap thresholds."""
from __future__ import annotations

import logging

import requests

from src.modules.events.eight_k_scanner.config import (
    MARKET_CAP_THRESHOLD,
    SEC_USER_AGENT,
    WATCHLIST_TICKERS,
    get_ticker_registry,
)
from src.modules.events.eight_k_scanner.financials import lookup_market_cap
from src.modules.events.eight_k_scanner.models import UniverseInfo

logger = logging.getLogger(__name__)

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"


def _is_non_common(ticker: str) -> bool:
    t = ticker.upper()
    if "-" in t:
        suffix = t.split("-", 1)[1]
        if suffix.startswith(("WT", "WS", "W", "P", "U", "R")):
            return True
    if len(t) >= 5 and t[-1] in ("W", "U"):
        return True
    return False


_cik_map = None


def get_cik_map() -> dict:
    """Get the SEC CIK->ticker mapping, cached in memory."""
    global _cik_map
    if _cik_map is None:
        _cik_map = _fetch_sec_ticker_map()
        logger.info(f"Loaded {len(_cik_map)} tickers from SEC")
    return _cik_map


def _build_registry_cik_map() -> dict[str, dict]:
    """Build a CIK -> ticker info map from the S3 ticker registry."""
    registry = get_ticker_registry()
    tickers = registry.get("tickers", {})
    cik_map: dict[str, dict] = {}
    for ticker_symbol, info in tickers.items():
        cik = (info.get("cik") or "").lstrip("0")
        if cik:
            cik_map[cik] = {
                "ticker": ticker_symbol,
                "company_name": info.get("name", ""),
                "exchange": info.get("exchange", ""),
            }
    return cik_map


def is_in_universe(cik: str) -> tuple[bool, UniverseInfo]:
    """Check if a CIK belongs to our universe.

    First checks the ticker registry from S3 config. Falls back to SEC ticker map
    with market cap filtering.

    Returns (in_universe, info).
    """
    # Check ticker registry first (all registered tickers are in-universe)
    registry_map = _build_registry_cik_map()
    if cik in registry_map:
        reg = registry_map[cik]
        ticker = reg["ticker"]
        mcap = lookup_market_cap(ticker)
        return True, UniverseInfo(
            ticker=ticker,
            company_name=reg["company_name"],
            market_cap=mcap,
            exchange=reg.get("exchange", ""),
        )

    # Fall back to SEC ticker map + market cap filter
    cik_map = get_cik_map()
    info = cik_map.get(cik)
    if not info:
        return False, UniverseInfo()

    ticker = info["ticker"]

    if _is_non_common(ticker):
        return False, UniverseInfo()

    if ticker.upper() in [t.upper() for t in WATCHLIST_TICKERS]:
        mcap = lookup_market_cap(ticker)
        return True, UniverseInfo(ticker=ticker, company_name=info["company_name"], market_cap=mcap)

    mcap = lookup_market_cap(ticker)
    if mcap is None:
        logger.warning(f"Including {ticker} (CIK {cik}) despite unknown market cap")
        return True, UniverseInfo(ticker=ticker, company_name=info["company_name"], market_cap=None)

    if mcap <= MARKET_CAP_THRESHOLD:
        return True, UniverseInfo(ticker=ticker, company_name=info["company_name"], market_cap=mcap)

    return False, UniverseInfo(ticker=ticker, company_name=info["company_name"], market_cap=mcap)


def _fetch_sec_ticker_map() -> dict:
    resp = requests.get(
        SEC_TICKERS_URL,
        headers={"User-Agent": SEC_USER_AGENT},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    fields = data.get("fields", [])
    rows = data.get("data", [])

    cik_idx = fields.index("cik") if "cik" in fields else 0
    name_idx = fields.index("name") if "name" in fields else 1
    ticker_idx = fields.index("ticker") if "ticker" in fields else 2
    exchange_idx = fields.index("exchange") if "exchange" in fields else 3

    result = {}
    for row in rows:
        cik = str(row[cik_idx])
        result[cik] = {
            "ticker": row[ticker_idx],
            "company_name": row[name_idx],
            "exchange": row[exchange_idx] if exchange_idx < len(row) else "",
        }
    return result
