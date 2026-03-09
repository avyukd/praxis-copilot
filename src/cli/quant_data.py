"""OHLCV data fetching for quant workspaces."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import click
import pandas as pd
import requests
import yfinance as yf

from cli.market_data import EODHD_BASE, get_eodhd_api_key, to_eodhd_symbol


def _fetch_eodhd_daily(ticker: str, api_key: str, days: int = 365) -> pd.DataFrame | None:
    """Fetch daily OHLCV from EODHD. Returns None on failure."""
    sym = to_eodhd_symbol(ticker)
    end = datetime.now()
    start = end - timedelta(days=days)
    try:
        resp = requests.get(
            f"{EODHD_BASE}/eod/{sym}",
            params={
                "period": "d",
                "from": start.strftime("%Y-%m-%d"),
                "to": end.strftime("%Y-%m-%d"),
                "fmt": "json",
                "api_token": api_key,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return None
        df = pd.DataFrame(data)
        df = df.rename(columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "adjusted_close": "adjusted_close",
            "volume": "volume",
        })
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        df = df[["open", "high", "low", "close", "volume"]]
        return df
    except Exception as e:
        click.echo(f"    EODHD daily failed for {ticker}: {e}")
        return None


def _fetch_eodhd_intraday(ticker: str, api_key: str, days: int = 120) -> pd.DataFrame | None:
    """Fetch 1min intraday from EODHD. Returns None on failure."""
    sym = to_eodhd_symbol(ticker)
    end = int(datetime.now().timestamp())
    start = int((datetime.now() - timedelta(days=days)).timestamp())
    try:
        resp = requests.get(
            f"{EODHD_BASE}/intraday/{sym}",
            params={
                "interval": "1m",
                "from": start,
                "to": end,
                "fmt": "json",
                "api_token": api_key,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return None
        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["datetime"] if "datetime" in df.columns else df["timestamp"], utc=True)
        df = df.set_index("date")
        cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
        df = df[cols]
        if len(df) < 100:
            return None
        return df
    except Exception as e:
        click.echo(f"    EODHD intraday failed for {ticker}: {e}")
        return None


def _fetch_yfinance_daily(ticker: str, period: str = "1y") -> pd.DataFrame | None:
    """Fetch daily OHLCV from yfinance."""
    try:
        t = yf.Ticker(ticker)
        df = t.history(period=period, interval="1d")
        if df.empty:
            return None
        df.index.name = "date"
        df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
        df = df[["open", "high", "low", "close", "volume"]]
        return df
    except Exception as e:
        click.echo(f"    yfinance daily failed for {ticker}: {e}")
        return None


def _fetch_yfinance_intraday(ticker: str) -> pd.DataFrame | None:
    """Fetch 1min intraday from yfinance (max 7 days)."""
    try:
        t = yf.Ticker(ticker)
        df = t.history(period="7d", interval="1m")
        if df.empty or len(df) < 100:
            return None
        df.index.name = "date"
        df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
        df = df[["open", "high", "low", "close", "volume"]]
        return df
    except Exception as e:
        click.echo(f"    yfinance intraday failed for {ticker}: {e}")
        return None


def fetch_ohlcv(ticker: str, data_dir: Path) -> dict[str, int]:
    """Fetch daily + intraday OHLCV for a ticker, save to data_dir.

    Returns dict of filename -> row count for summary.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, int] = {}

    # Try EODHD first, fallback to yfinance
    api_key = None
    try:
        api_key = get_eodhd_api_key()
    except click.ClickException:
        pass

    # Daily
    df_daily = None
    if api_key:
        df_daily = _fetch_eodhd_daily(ticker, api_key)
    if df_daily is None:
        click.echo(f"    Falling back to yfinance for {ticker} daily...")
        df_daily = _fetch_yfinance_daily(ticker)

    if df_daily is not None and not df_daily.empty:
        fname = f"{ticker}_1d.csv"
        df_daily.to_csv(data_dir / fname)
        results[fname] = len(df_daily)
    else:
        click.echo(f"    WARNING: No daily data for {ticker}")

    # Intraday
    df_intra = None
    if api_key:
        df_intra = _fetch_eodhd_intraday(ticker, api_key)
    if df_intra is None:
        df_intra = _fetch_yfinance_intraday(ticker)

    if df_intra is not None and not df_intra.empty:
        fname = f"{ticker}_1m.csv"
        df_intra.to_csv(data_dir / fname)
        results[fname] = len(df_intra)
    else:
        click.echo(f"    (no intraday data for {ticker} — not critical)")

    return results
