"""Price and volume data fetcher using yfinance."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import yfinance as yf

from src.modules.manage.models import PriceData

logger = logging.getLogger(__name__)


def fetch_price_data(ticker: str) -> PriceData:
    """Fetch current price, volume, and ADTV for a ticker.

    Uses yfinance to get the latest quote and 20-day volume average.
    Raises ValueError if data cannot be fetched.
    """
    yf_ticker = yf.Ticker(ticker)

    # Get recent history for ADTV calculation (need ~30 days to get 20 trading days)
    hist = yf_ticker.history(period="1mo")
    if hist.empty:
        raise ValueError(f"No price history available for {ticker}")

    # Current data from the most recent row
    latest = hist.iloc[-1]
    current_price = float(latest["Close"])
    current_volume = int(latest["Volume"])

    # Previous close
    if len(hist) < 2:
        raise ValueError(f"Insufficient history for {ticker} (need at least 2 days)")
    previous_close = float(hist.iloc[-2]["Close"])

    # 20-day ADTV (use available history, up to 20 days excluding today)
    volume_history = hist["Volume"].iloc[:-1]  # exclude today
    adtv_window = volume_history.tail(20)
    adtv = float(adtv_window.mean()) if len(adtv_window) > 0 else 0.0

    # Calculate derived metrics
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
