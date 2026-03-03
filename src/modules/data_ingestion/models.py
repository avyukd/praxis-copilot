"""Pydantic models for data ingestion results."""

from pydantic import BaseModel, Field


class FilingSection(BaseModel):
    """An extracted narrative section from an SEC filing."""

    filing_type: str = Field(description="10-K or 10-Q")
    period: str = Field(description="Filing period, e.g. 2024-12-31")
    section_name: str = Field(description="e.g. item1_business, item7_mda")
    text: str = Field(description="Extracted plain text content")


class FundamentalsData(BaseModel):
    """Key fundamental metrics with source attribution."""

    source: str = Field(description="eodhd, roicai, or yfinance")
    raw: dict = Field(default_factory=dict, description="Raw API response data")


class IngestionResult(BaseModel):
    """Summary of what was pulled during data ingestion."""

    ticker: str
    filings_count: int = 0
    fundamentals_source: str | None = None
    transcripts_count: int = 0
    warnings: list[str] = Field(default_factory=list)
