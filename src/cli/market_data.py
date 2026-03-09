"""Shared market data helpers for delayed and real-time watch commands."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import click
import requests
from pydantic import BaseModel


EODHD_BASE = "https://eodhd.com/api"
US_DELAYED_QUOTES_URL = f"{EODHD_BASE}/us-quote-delayed"


class MarketSnapshot(BaseModel):
    """Normalized price snapshot used by delayed scans and real-time watch."""

    ticker: str
    price: float
    previous_close: float
    change_pct: float
    volume: int
    adtv: float
    volume_ratio: float
    bid: float | None = None
    ask: float | None = None
    bid_size: int | None = None
    ask_size: int | None = None
    timestamp: datetime
    source: str


def get_eodhd_api_key() -> str:
    """Return the configured EODHD API key or raise."""
    api_key = os.environ.get("EODHD_API_KEY", "").strip()
    if not api_key:
        raise click.ClickException("EODHD_API_KEY is not set.")
    return api_key


def to_eodhd_symbol(ticker: str) -> str:
    """Normalize tickers for EODHD US endpoints."""
    ticker = ticker.strip().upper()
    return ticker if "." in ticker else f"{ticker}.US"


def to_display_ticker(symbol: str) -> str:
    """Strip exchange suffix for CLI display."""
    return symbol.split(".", 1)[0].upper()


def _iso_to_datetime(timestamp_seconds: int | float | None) -> datetime:
    if timestamp_seconds:
        return datetime.fromtimestamp(float(timestamp_seconds), tz=timezone.utc)
    return datetime.now(timezone.utc)


def _build_snapshot_from_delayed_quote(symbol: str, payload: dict[str, Any]) -> MarketSnapshot:
    price = float(payload.get("lastTradePrice") or payload.get("close") or payload.get("price") or 0.0)
    previous_close = float(payload.get("previousClosePrice") or payload.get("previousClose") or price)
    volume = int(payload.get("volume") or 0)
    adtv = float(payload.get("averageVolume") or 0.0)
    volume_ratio = (volume / adtv) if adtv > 0 else 0.0
    change_pct = float(payload.get("changePercent") or 0.0)

    return MarketSnapshot(
        ticker=to_display_ticker(symbol),
        price=price,
        previous_close=previous_close,
        change_pct=change_pct,
        volume=volume,
        adtv=adtv,
        volume_ratio=round(volume_ratio, 2),
        bid=_maybe_float(payload.get("bidPrice")),
        ask=_maybe_float(payload.get("askPrice")),
        bid_size=_maybe_int(payload.get("bidSize")),
        ask_size=_maybe_int(payload.get("askSize")),
        timestamp=_iso_to_datetime(payload.get("timestamp")),
        source="delayed",
    )


def _build_snapshot_from_realtime(symbol: str, payload: dict[str, Any]) -> MarketSnapshot:
    price = float(payload.get("close") or payload.get("price") or 0.0)
    previous_close = float(payload.get("previousClose") or payload.get("previousClosePrice") or price)
    volume = int(payload.get("volume") or 0)
    change_pct = float(payload.get("change_p") or payload.get("changePercent") or 0.0)

    return MarketSnapshot(
        ticker=to_display_ticker(symbol),
        price=price,
        previous_close=previous_close,
        change_pct=change_pct,
        volume=volume,
        adtv=0.0,
        volume_ratio=0.0,
        timestamp=_iso_to_datetime(payload.get("timestamp")),
        source="realtime",
    )


def _maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def fetch_delayed_snapshots(tickers: list[str], api_key: str | None = None) -> dict[str, MarketSnapshot]:
    """Fetch delayed US quote snapshots in batches."""
    if not tickers:
        return {}

    api_key = api_key or get_eodhd_api_key()
    snapshots: dict[str, MarketSnapshot] = {}
    symbols = [to_eodhd_symbol(ticker) for ticker in tickers]

    for batch in _chunked(symbols, 50):
        response = requests.get(
            US_DELAYED_QUOTES_URL,
            params={"api_token": api_key, "fmt": "json", "s": ",".join(batch)},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        for symbol, raw in (payload.get("data") or {}).items():
            snapshots[to_display_ticker(symbol)] = _build_snapshot_from_delayed_quote(symbol, raw)

    return snapshots


def fetch_realtime_snapshot(ticker: str, api_key: str | None = None) -> MarketSnapshot:
    """Fetch a single current snapshot for a real-time watch symbol."""
    api_key = api_key or get_eodhd_api_key()
    symbol = to_eodhd_symbol(ticker)
    response = requests.get(
        f"{EODHD_BASE}/real-time/{symbol}",
        params={"api_token": api_key, "fmt": "json"},
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    return _build_snapshot_from_realtime(symbol, payload)
