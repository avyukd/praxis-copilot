"""Price and volume data fetcher with EODHD delayed quotes as primary source."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import requests

try:
    import yfinance as yf
except ImportError:
    yf = None  # Lambda deployment doesn't include yfinance

from .models import PriceData

logger = logging.getLogger(__name__)

EODHD_DELAYED_QUOTES_URL = "https://eodhd.com/api/us-quote-delayed"


def _to_eodhd_symbol(ticker: str) -> str:
    ticker = ticker.strip().upper()
    return ticker if "." in ticker else f"{ticker}.US"


def _from_eodhd_symbol(symbol: str) -> str:
    return symbol.split(".", 1)[0].upper()


def fetch_price_data(ticker: str) -> PriceData:
    """Fetch current price, volume, and ADTV for a single ticker."""
    api_key = os.environ.get("EODHD_API_KEY", "").strip()
    if api_key:
        try:
            return _fetch_price_data_eodhd(ticker, api_key)
        except Exception as exc:
            logger.warning("EODHD delayed quote failed for %s, falling back to yfinance: %s", ticker, exc)
    return _fetch_price_data_yfinance(ticker)


def fetch_price_data_batch(tickers: list[str]) -> dict[str, PriceData]:
    """Fetch delayed quotes for multiple tickers in batches of 50.

    Returns a dict of ticker -> PriceData.  Tickers that fail are omitted.
    """
    api_key = os.environ.get("EODHD_API_KEY", "").strip()
    if not api_key:
        # Fall back to per-ticker yfinance
        result = {}
        for ticker in tickers:
            try:
                result[ticker] = _fetch_price_data_yfinance(ticker)
            except Exception as e:
                logger.warning("yfinance failed for %s: %s", ticker, e)
        return result

    result: dict[str, PriceData] = {}
    symbols = [_to_eodhd_symbol(t) for t in tickers]

    for i in range(0, len(symbols), 50):
        batch = symbols[i:i + 50]
        try:
            response = requests.get(
                EODHD_DELAYED_QUOTES_URL,
                params={"api_token": api_key, "fmt": "json", "s": ",".join(batch)},
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data") or {}

            for symbol, quote in data.items():
                ticker = _from_eodhd_symbol(symbol)
                try:
                    result[ticker] = _parse_eodhd_quote(ticker, quote)
                except Exception as e:
                    logger.warning("Failed to parse EODHD quote for %s: %s", ticker, e)
        except Exception as e:
            logger.warning("EODHD batch request failed: %s", e)
            # Fall back to yfinance for this batch
            for symbol in batch:
                ticker = _from_eodhd_symbol(symbol)
                try:
                    result[ticker] = _fetch_price_data_yfinance(ticker)
                except Exception as yf_e:
                    logger.warning("yfinance fallback failed for %s: %s", ticker, yf_e)

    return result


def _parse_eodhd_quote(ticker: str, quote: dict) -> PriceData:
    price = float(quote.get("lastTradePrice") or quote.get("close") or 0.0)
    previous_close = float(quote.get("previousClosePrice") or quote.get("previousClose") or price)
    volume = int(quote.get("volume") or 0)
    adtv = float(quote.get("averageVolume") or 0.0)
    change_pct = float(quote.get("changePercent") or 0.0)
    volume_ratio = (volume / adtv) if adtv > 0 else 0.0

    return PriceData(
        ticker=ticker,
        price=price,
        previous_close=previous_close,
        change_pct=round(change_pct, 2),
        volume=volume,
        adtv=round(adtv, 0),
        volume_ratio=round(volume_ratio, 2),
        timestamp=datetime.now(timezone.utc),
    )


def _fetch_price_data_eodhd(ticker: str, api_key: str) -> PriceData:
    symbol = _to_eodhd_symbol(ticker)
    response = requests.get(
        EODHD_DELAYED_QUOTES_URL,
        params={"api_token": api_key, "fmt": "json", "s": symbol},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    quote = (payload.get("data") or {}).get(symbol)
    if not quote:
        raise ValueError(f"No delayed quote returned for {symbol}")
    return _parse_eodhd_quote(ticker.upper(), quote)


def _fetch_price_data_yfinance(ticker: str) -> PriceData:
    """Fallback quote source when EODHD is unavailable."""
    if yf is None:
        raise ImportError("yfinance is not installed; EODHD_API_KEY must be set")
    yf_ticker = yf.Ticker(ticker)
    hist = yf_ticker.history(period="1mo")
    if hist.empty:
        raise ValueError(f"No price history available for {ticker}")

    latest = hist.iloc[-1]
    current_price = float(latest["Close"])
    current_volume = int(latest["Volume"])

    if len(hist) < 2:
        raise ValueError(f"Insufficient history for {ticker} (need at least 2 days)")
    previous_close = float(hist.iloc[-2]["Close"])

    volume_history = hist["Volume"].iloc[:-1]
    adtv_window = volume_history.tail(20)
    adtv = float(adtv_window.mean()) if len(adtv_window) > 0 else 0.0
    change_pct = ((current_price - previous_close) / previous_close) * 100.0
    volume_ratio = (current_volume / adtv) if adtv > 0 else 0.0

    return PriceData(
        ticker=ticker,
        price=current_price,
        previous_close=previous_close,
        change_pct=round(change_pct, 2),
        volume=current_volume,
        adtv=round(adtv, 0),
        volume_ratio=round(volume_ratio, 2),
        timestamp=datetime.now(timezone.utc),
    )
