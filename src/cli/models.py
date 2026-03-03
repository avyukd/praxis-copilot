"""Pydantic models for CLI data structures."""

from pydantic import BaseModel


class TickerInfo(BaseModel):
    """Result from EDGAR ticker resolution."""

    cik: str
    name: str
    exchange: str


class TickerRegistryEntry(BaseModel):
    """Entry in ticker_registry.yaml."""

    cik: str
    exchange: str
    name: str
    news_queries: list[str]
    universe_status: str | None = None


class TickerRegistry(BaseModel):
    """Full ticker_registry.yaml structure."""

    tickers: dict[str, TickerRegistryEntry] = {}


class UniverseConfig(BaseModel):
    """universe.yaml structure."""

    tickers: list[str] = []
