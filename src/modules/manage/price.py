"""Price and volume data fetcher with EODHD delayed quotes as primary source."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import requests
import yfinance as yf

from .models import PriceData

logger = logging.getLogger(__name__)

EODHD_DELAYED_QUOTES_URL = "https://eodhd.com/api/us-quote-delayed"


def fetch_price_data(ticker: str) -> PriceData:
    """Fetch current price, volume, and ADTV for a ticker."""
    api_key = os.environ.get("EODHD_API_KEY", "").strip()
    if api_key:
        try:
            return _fetch_price_data_eodhd(ticker, api_key)
        except Exception as exc:
            logger.warning("EODHD delayed quote failed for %s, falling back to yfinance: %s", ticker, exc)
    return _fetch_price_data_yfinance(ticker)


def _fetch_price_data_eodhd(ticker: str, api_key: str) -> PriceData:
    symbol = f"{ticker.upper()}.US" if "." not in ticker else ticker.upper()
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

    price = float(quote.get("lastTradePrice") or quote.get("close") or 0.0)
    previous_close = float(quote.get("previousClosePrice") or quote.get("previousClose") or price)
    volume = int(quote.get("volume") or 0)
    adtv = float(quote.get("averageVolume") or 0.0)
    change_pct = float(quote.get("changePercent") or 0.0)
    volume_ratio = (volume / adtv) if adtv > 0 else 0.0

    return PriceData(
        ticker=ticker.upper(),
        price=price,
        previous_close=previous_close,
        change_pct=round(change_pct, 2),
        volume=volume,
        adtv=round(adtv, 0),
        volume_ratio=round(volume_ratio, 2),
        timestamp=datetime.now(timezone.utc),
    )


def _fetch_price_data_yfinance(ticker: str) -> PriceData:
    """Fallback quote source when EODHD is unavailable."""
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
