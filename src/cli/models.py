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
    edgar_supported: bool = True
    universe_status: str | None = None
    research_priority: int = 5  # 0-10, controls depth of research analysis
    interest: int = 5  # 0-10, how interesting/relevant this ticker is (used for scan prioritization)


class TickerRegistry(BaseModel):
    """Full ticker_registry.yaml structure."""

    tickers: dict[str, TickerRegistryEntry] = {}


class UniverseConfig(BaseModel):
    """universe.yaml structure."""

    tickers: list[str] = []


class PortfolioPosition(BaseModel):
    """A position in the portfolio."""

    ticker: str
    shares: int = 0
    avg_cost: float = 0.0
    date_entered: str = ""


class PortfolioConfig(BaseModel):
    """config/portfolio.yaml structure."""

    positions: list[PortfolioPosition] = []
    watchlist: list[str] = []
