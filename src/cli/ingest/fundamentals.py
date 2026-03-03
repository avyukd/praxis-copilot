"""Fundamentals data ingestion — three-tier fallback: EODHD -> ROIC.AI -> yfinance."""

import json
import logging
import os

import requests
import yfinance as yf

from cli.ingest.models import FundamentalsData

logger = logging.getLogger(__name__)


def _try_eodhd(ticker: str) -> FundamentalsData | None:
    """Try fetching fundamentals from EODHD API."""
    api_key = os.environ.get("EODHD_API_KEY", "")
    if not api_key:
        logger.info("EODHD_API_KEY not set, skipping EODHD")
        return None

    url = f"https://eodhd.com/api/fundamentals/{ticker}.US"
    params = {"api_token": api_key, "fmt": "json"}

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if not data or isinstance(data, list):
            logger.warning(f"EODHD returned empty/unexpected data for {ticker}")
            return None

        return FundamentalsData(source="eodhd", raw=data)
    except requests.RequestException as e:
        logger.warning(f"EODHD request failed for {ticker}: {e}")
        return None
    except (ValueError, json.JSONDecodeError) as e:
        logger.warning(f"EODHD returned invalid JSON for {ticker}: {e}")
        return None


def _try_roicai(ticker: str) -> FundamentalsData | None:
    """Try fetching fundamentals from ROIC.AI API."""
    api_key = os.environ.get("ROICAI_API_KEY", "")
    if not api_key:
        logger.info("ROICAI_API_KEY not set, skipping ROIC.AI")
        return None

    url = f"https://roic.ai/api/fundamentals/{ticker}"
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            logger.warning(f"ROIC.AI returned empty data for {ticker}")
            return None

        return FundamentalsData(source="roicai", raw=data)
    except requests.RequestException as e:
        logger.warning(f"ROIC.AI request failed for {ticker}: {e}")
        return None
    except (ValueError, json.JSONDecodeError) as e:
        logger.warning(f"ROIC.AI returned invalid JSON for {ticker}: {e}")
        return None


def _try_yfinance(ticker: str) -> FundamentalsData | None:
    """Try fetching fundamentals from yfinance (worst-case fallback)."""
    try:
        yticker = yf.Ticker(ticker)
        info = yticker.info or {}

        # Pull all available financial data
        data: dict = {"info": info}

        try:
            financials = yticker.financials
            if financials is not None and not financials.empty:
                data["financials"] = json.loads(financials.to_json())
        except Exception:
            pass

        try:
            balance_sheet = yticker.balance_sheet
            if balance_sheet is not None and not balance_sheet.empty:
                data["balance_sheet"] = json.loads(balance_sheet.to_json())
        except Exception:
            pass

        try:
            cashflow = yticker.cashflow
            if cashflow is not None and not cashflow.empty:
                data["cashflow"] = json.loads(cashflow.to_json())
        except Exception:
            pass

        if not info and "financials" not in data:
            logger.warning(f"yfinance returned no data for {ticker}")
            return None

        return FundamentalsData(source="yfinance", raw=data)
    except Exception as e:
        logger.warning(f"yfinance failed for {ticker}: {e}")
        return None


def ingest_fundamentals(ticker: str) -> FundamentalsData | None:
    """Pull fundamental data using three-tier fallback.

    Tries EODHD first, then ROIC.AI, then yfinance. Returns the first
    successful result, or None if all sources fail.
    """
    sources = [
        ("EODHD", _try_eodhd),
        ("ROIC.AI", _try_roicai),
        ("yfinance", _try_yfinance),
    ]

    for name, fetch_fn in sources:
        logger.info(f"Trying {name} for {ticker} fundamentals...")
        result = fetch_fn(ticker)
        if result is not None:
            logger.info(f"Got fundamentals for {ticker} from {name}")
            return result
        logger.info(f"{name} unavailable for {ticker}, trying next source")

    logger.warning(f"All fundamental data sources failed for {ticker}")
    return None
