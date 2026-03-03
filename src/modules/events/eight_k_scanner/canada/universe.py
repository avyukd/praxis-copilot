"""Canadian stock universe filter."""
from __future__ import annotations

import logging

from src.modules.events.eight_k_scanner.config import CA_MARKET_CAP_THRESHOLD
from src.modules.events.eight_k_scanner.financials import lookup_market_cap

logger = logging.getLogger(__name__)


def ca_ticker_symbol(ticker: str, exchange: str) -> str:
    if exchange == "TSXV":
        return f"{ticker}.V"
    return f"{ticker}.TO"


def is_in_ca_universe(ticker: str, exchange: str) -> tuple[bool, dict]:
    symbol = ca_ticker_symbol(ticker, exchange)
    mcap = lookup_market_cap(symbol)

    info = {
        "ticker": ticker,
        "symbol": symbol,
        "exchange": exchange,
        "market_cap": mcap,
    }

    if mcap is None:
        logger.warning(f"Including {ticker} ({exchange}) despite unknown market cap")
        return True, info

    if mcap <= CA_MARKET_CAP_THRESHOLD:
        return True, info

    return False, info
