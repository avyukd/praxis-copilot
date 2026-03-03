"""Unified financial data fetching. FMP -> EODHD -> yfinance fallback chain."""
from __future__ import annotations

import logging

import requests
import yfinance as yf

from src.modules.events.eight_k_scanner.config import FMP_API_KEY, EODHD_API_KEY

logger = logging.getLogger(__name__)

FMP_BASE = "https://financialmodelingprep.com/stable"
EODHD_BASE = "https://eodhd.com/api"


def _eodhd_symbol(ticker: str) -> str:
    if "." in ticker:
        return ticker
    return f"{ticker}.US"


def lookup_market_cap(ticker: str) -> int | None:
    """Look up market cap. Tries FMP -> EODHD -> yfinance -> price*shares fallback."""
    mcap = _fmp_market_cap(ticker)
    if mcap is not None:
        return mcap

    mcap = _eodhd_market_cap(ticker)
    if mcap is not None:
        return mcap

    mcap = _yfinance_market_cap(ticker)
    if mcap is not None:
        return mcap

    mcap = _infer_market_cap(ticker)
    if mcap is not None:
        return mcap

    logger.warning(f"MARKET_CAP_UNAVAILABLE: Could not determine market cap for {ticker}")
    return None


def get_financial_snapshot(ticker: str) -> dict:
    """Full financial snapshot for LLM context."""
    snap = _fmp_snapshot(ticker)
    if snap:
        return snap
    snap = _eodhd_snapshot(ticker)
    if snap:
        return snap
    return _yfinance_snapshot(ticker)


def _fmp_market_cap(ticker: str) -> int | None:
    if not FMP_API_KEY:
        return None
    try:
        resp = requests.get(
            f"{FMP_BASE}/profile",
            params={"symbol": ticker, "apikey": FMP_API_KEY},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            return data[0].get("mktCap") or data[0].get("marketCap")
        elif isinstance(data, dict):
            return data.get("mktCap") or data.get("marketCap")
    except Exception as e:
        logger.debug(f"  {ticker} FMP: {e}")
    return None


def _fmp_snapshot(ticker: str) -> dict | None:
    if not FMP_API_KEY:
        return None
    try:
        profile_resp = requests.get(
            f"{FMP_BASE}/profile",
            params={"symbol": ticker, "apikey": FMP_API_KEY},
            timeout=10,
        )
        profile_resp.raise_for_status()
        profile_data = profile_resp.json()
        if isinstance(profile_data, list) and profile_data:
            profile = profile_data[0]
        elif isinstance(profile_data, dict):
            profile = profile_data
        else:
            return None

        market_cap = profile.get("mktCap") or profile.get("marketCap")
        if not market_cap:
            return None

        snap = {
            "market_cap": market_cap,
            "revenue_ttm": None,
            "net_income_ttm": None,
            "cash": None,
            "total_debt": None,
            "source": "fmp",
        }

        try:
            is_resp = requests.get(
                f"{FMP_BASE}/income-statement",
                params={"symbol": ticker, "period": "annual", "limit": 1, "apikey": FMP_API_KEY},
                timeout=10,
            )
            is_resp.raise_for_status()
            is_data = is_resp.json()
            if isinstance(is_data, list) and is_data:
                snap["revenue_ttm"] = is_data[0].get("revenue")
                snap["net_income_ttm"] = is_data[0].get("netIncome")
        except Exception:
            pass

        try:
            bs_resp = requests.get(
                f"{FMP_BASE}/balance-sheet-statement",
                params={"symbol": ticker, "period": "annual", "limit": 1, "apikey": FMP_API_KEY},
                timeout=10,
            )
            bs_resp.raise_for_status()
            bs_data = bs_resp.json()
            if isinstance(bs_data, list) and bs_data:
                snap["cash"] = bs_data[0].get("cashAndCashEquivalents")
                snap["total_debt"] = bs_data[0].get("totalDebt")
        except Exception:
            pass

        return snap
    except Exception:
        pass
    return None


def _eodhd_market_cap(ticker: str) -> int | None:
    if not EODHD_API_KEY:
        return None
    try:
        resp = requests.get(
            f"{EODHD_BASE}/fundamentals/{_eodhd_symbol(ticker)}",
            params={"api_token": EODHD_API_KEY, "fmt": "json", "filter": "Highlights"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        mcap = data.get("MarketCapitalization")
        return int(mcap) if mcap else None
    except Exception as e:
        logger.debug(f"  {ticker} EODHD: {e}")
    return None


def _eodhd_snapshot(ticker: str) -> dict | None:
    if not EODHD_API_KEY:
        return None
    try:
        resp = requests.get(
            f"{EODHD_BASE}/fundamentals/{_eodhd_symbol(ticker)}",
            params={"api_token": EODHD_API_KEY, "fmt": "json"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        highlights = data.get("Highlights", {})
        market_cap = highlights.get("MarketCapitalization")
        if not market_cap:
            return None

        snap = {
            "market_cap": int(market_cap),
            "revenue_ttm": None,
            "net_income_ttm": None,
            "cash": None,
            "total_debt": None,
            "source": "eodhd",
        }

        revenue = highlights.get("RevenueTTM")
        if revenue:
            snap["revenue_ttm"] = int(float(revenue))

        financials = data.get("Financials", {})
        income_stmt = financials.get("Income_Statement", {}).get("quarterly", {})
        if income_stmt:
            latest = next(iter(income_stmt.values()), {})
            ni = latest.get("netIncome")
            if ni:
                snap["net_income_ttm"] = int(float(ni))

        balance_sheet = financials.get("Balance_Sheet", {}).get("quarterly", {})
        if balance_sheet:
            latest_bs = next(iter(balance_sheet.values()), {})
            cash = latest_bs.get("cashAndShortTermInvestments") or latest_bs.get("cash")
            if cash:
                snap["cash"] = int(float(cash))
            debt = latest_bs.get("shortLongTermDebtTotal") or latest_bs.get("longTermDebt")
            if debt:
                snap["total_debt"] = int(float(debt))

        return snap
    except Exception:
        pass
    return None


def _yfinance_market_cap(ticker: str) -> int | None:
    try:
        t = yf.Ticker(ticker)
        mcap = t.info.get("marketCap")
        if mcap:
            return mcap
    except Exception as e:
        logger.debug(f"  {ticker} yfinance: {e}")
    return None


def _infer_market_cap(ticker: str) -> int | None:
    """Infer market cap from price * shares outstanding as last resort."""
    shares = None

    if EODHD_API_KEY:
        try:
            resp = requests.get(
                f"{EODHD_BASE}/fundamentals/{_eodhd_symbol(ticker)}",
                params={"api_token": EODHD_API_KEY, "fmt": "json", "filter": "SharesStats"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            shares = data.get("SharesOutstanding")
            if shares:
                shares = int(float(shares))
        except Exception:
            pass

    if shares is None:
        try:
            t = yf.Ticker(ticker)
            shares = t.info.get("sharesOutstanding")
            if shares:
                shares = int(shares)
        except Exception:
            pass

    if not shares:
        return None

    price = None
    try:
        t = yf.Ticker(ticker)
        price = t.info.get("currentPrice") or t.info.get("regularMarketPrice")
    except Exception:
        pass

    if price is None and EODHD_API_KEY:
        try:
            resp = requests.get(
                f"{EODHD_BASE}/real-time/{_eodhd_symbol(ticker)}",
                params={"api_token": EODHD_API_KEY, "fmt": "json"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            price = data.get("close") or data.get("previousClose")
            if price:
                price = float(price)
        except Exception:
            pass

    if not price:
        return None

    return int(price * shares)


def lookup_adtv(ticker: str) -> float | None:
    """Look up average daily trading volume in dollars (last 5 trading days)."""
    adtv = _yfinance_adtv(ticker)
    if adtv is not None:
        return adtv

    adtv = _eodhd_adtv(ticker)
    if adtv is not None:
        return adtv

    logger.warning(f"ADTV_UNAVAILABLE: Could not determine ADTV for {ticker}")
    return None


def _yfinance_adtv(ticker: str) -> float | None:
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="5d", interval="1d")
        if hist.empty or len(hist) == 0:
            return None
        dollar_volumes = hist["Volume"] * hist["Close"]
        adtv = float(dollar_volumes.mean())
        return adtv if adtv > 0 else None
    except Exception:
        return None


def _eodhd_adtv(ticker: str) -> float | None:
    if not EODHD_API_KEY:
        return None
    try:
        from datetime import date, timedelta
        end = date.today()
        start = end - timedelta(days=10)
        resp = requests.get(
            f"{EODHD_BASE}/eod/{_eodhd_symbol(ticker)}",
            params={
                "api_token": EODHD_API_KEY,
                "period": "d",
                "from": start.isoformat(),
                "to": end.isoformat(),
                "fmt": "json",
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or not data:
            return None
        recent = data[-5:]
        dollar_volumes = [
            d.get("volume", 0) * d.get("close", 0)
            for d in recent
            if d.get("volume") and d.get("close")
        ]
        if not dollar_volumes:
            return None
        adtv = sum(dollar_volumes) / len(dollar_volumes)
        return adtv if adtv > 0 else None
    except Exception:
        return None


def _yfinance_snapshot(ticker: str) -> dict:
    snap = {
        "market_cap": None,
        "revenue_ttm": None,
        "net_income_ttm": None,
        "cash": None,
        "total_debt": None,
        "source": "yfinance",
    }
    try:
        t = yf.Ticker(ticker)
        info = t.info
        snap["market_cap"] = info.get("marketCap")
        snap["revenue_ttm"] = info.get("totalRevenue")
        snap["net_income_ttm"] = info.get("netIncomeToCommon")
        snap["cash"] = info.get("totalCash")
        snap["total_debt"] = info.get("totalDebt")
    except Exception:
        pass
    return snap
