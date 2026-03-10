"""Pydantic models for the news scanner module."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# --- SERP models (replacing dataclasses in serp.py) ---


class SerpResult(BaseModel):
    """A single search result from a SERP query."""

    headline: str
    url: str
    snippet: str
    source: str
    published: str | None = None


class SerpResponse(BaseModel):
    """Response from a SERP query for a single ticker."""

    ticker: str
    query: str
    results: list[SerpResult] = Field(default_factory=list)


# --- Triage models ---


class Monitor(BaseModel):
    """A monitor definition relevant to a ticker."""

    id: str
    description: str = ""
    listen: list[str] = Field(default_factory=list)


class Significance(str, Enum):
    """Significance level for a material news item."""

    high = "high"
    medium = "medium"
    low = "low"


class MaterialItem(BaseModel):
    """A single material news item from the triage digest."""

    ticker: str
    headline: str
    url: str
    relevance: str
    related_monitors: list[str] = Field(default_factory=list)
    significance: Significance


class TriageDigest(BaseModel):
    """Triage digest output from the Sonnet agent."""

    material: list[MaterialItem] = Field(default_factory=list)
    nothing_material: list[str] = Field(default_factory=list)
    # Preserved for error/debug cases
    raw: str | None = Field(None, alias="_raw")
    parse_error: bool = Field(False, alias="_parse_error")
    error: str | None = Field(None, alias="_error")

    model_config = {"populate_by_name": True}


# --- Handler config ---


class NewsScannerConfig(BaseModel):
    """News scanner configuration loaded from S3."""

    enabled: bool = True
    serp_api: str = "tavily"
    serp_api_key_param: str = "/praxis/serpapi_key"
    results_per_ticker: int = 10
    lookback_hours: int = 24
    market_hours_only: bool = True
