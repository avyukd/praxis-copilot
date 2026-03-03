"""SERP API client — abstract provider with SerpAPI implementation."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)

SERPAPI_ENDPOINT = "https://serpapi.com/search"


@dataclass
class SerpResult:
    """A single search result from a SERP query."""

    headline: str
    url: str
    snippet: str
    source: str
    published: str | None = None


@dataclass
class SerpResponse:
    """Response from a SERP query for a single ticker."""

    ticker: str
    query: str
    results: list[SerpResult] = field(default_factory=list)


class SerpProvider(ABC):
    """Abstract SERP provider interface."""

    @abstractmethod
    def search(self, query: str, num_results: int = 10) -> list[SerpResult]:
        ...


class SerpAPIProvider(SerpProvider):
    """SerpAPI (Google News) provider."""

    def __init__(self, api_key: str):
        self.api_key = api_key

    def search(self, query: str, num_results: int = 10) -> list[SerpResult]:
        params = {
            "q": query,
            "api_key": self.api_key,
            "engine": "google_news",
            "num": num_results,
        }
        try:
            resp = requests.get(SERPAPI_ENDPOINT, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.error("SerpAPI request failed for query %r: %s", query, e)
            return []

        results: list[SerpResult] = []
        for item in data.get("news_results", []):
            results.append(
                SerpResult(
                    headline=item.get("title", ""),
                    url=item.get("link", ""),
                    snippet=item.get("snippet", ""),
                    source=item.get("source", {}).get("name", "")
                    if isinstance(item.get("source"), dict)
                    else item.get("source", ""),
                    published=item.get("date"),
                )
            )
        return results[:num_results]


def get_provider(provider_name: str, api_key: str) -> SerpProvider:
    """Factory for SERP providers."""
    providers: dict[str, type[SerpProvider]] = {
        "serpapi": SerpAPIProvider,
    }
    cls = providers.get(provider_name)
    if cls is None:
        raise ValueError(f"Unknown SERP provider: {provider_name}. Available: {list(providers.keys())}")
    return cls(api_key=api_key)


def build_queries(ticker: str, ticker_config: dict[str, Any]) -> list[str]:
    """Build search queries for a ticker from its registry config.

    Uses news_queries from ticker registry if available,
    otherwise falls back to '"{company_name}" OR "{ticker}"'.
    """
    custom_queries = ticker_config.get("news_queries")
    if custom_queries:
        return custom_queries

    company_name = ticker_config.get("name", ticker)
    return [f'"{company_name}" OR "{ticker}"']


def sweep_ticker(
    provider: SerpProvider,
    ticker: str,
    ticker_config: dict[str, Any],
    num_results: int = 10,
) -> SerpResponse:
    """Run SERP sweep for a single ticker. Merges results from all queries."""
    queries = build_queries(ticker, ticker_config)
    all_results: list[SerpResult] = []
    seen_urls: set[str] = set()

    for query in queries:
        results = provider.search(query, num_results=num_results)
        for r in results:
            if r.url not in seen_urls:
                seen_urls.add(r.url)
                all_results.append(r)

    return SerpResponse(
        ticker=ticker,
        query=" | ".join(queries),
        results=all_results[:num_results],
    )
