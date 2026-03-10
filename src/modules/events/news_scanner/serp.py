"""SERP API client — abstract provider with SerpAPI implementation."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import Any

import requests

from src.cli.models import TickerRegistryEntry

from .models import SerpResponse, SerpResult

logger = logging.getLogger(__name__)

SERPAPI_ENDPOINT = "https://serpapi.com/search"


class SerpProvider(ABC):
    """Abstract SERP provider interface."""

    @abstractmethod
    def search(self, query: str, num_results: int = 10) -> list[SerpResult]:
        ...


class SerpAPIProvider(SerpProvider):
    """SerpAPI (Google News) provider."""

    def __init__(self, api_key: str):
        self.api_key = api_key

    def search(self, query: str, num_results: int = 10, max_retries: int = 3) -> list[SerpResult]:
        params = {
            "q": query,
            "api_key": self.api_key,
            "engine": "google_news",
            "num": num_results,
        }
        last_error = None
        for attempt in range(max_retries):
            try:
                resp = requests.get(SERPAPI_ENDPOINT, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.RequestException as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    logger.warning("SerpAPI attempt %d failed for %r, retrying in %ds: %s", attempt + 1, query, wait, e)
                    time.sleep(wait)
        else:
            logger.error("SerpAPI request failed after %d attempts for query %r: %s", max_retries, query, last_error)
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


class TavilyNewsProvider(SerpProvider):
    """Tavily Search API provider configured for news topic."""

    ENDPOINT = "https://api.tavily.com/search"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def search(self, query: str, num_results: int = 10, max_retries: int = 3) -> list[SerpResult]:
        payload = {
            "query": query,
            "max_results": num_results,
            "topic": "news",
        }
        headers = {"Authorization": f"Bearer {self.api_key}"}
        last_error = None
        for attempt in range(max_retries):
            try:
                resp = requests.post(self.ENDPOINT, json=payload, headers=headers, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.RequestException as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    logger.warning("Tavily attempt %d failed for %r, retrying in %ds: %s", attempt + 1, query, wait, e)
                    time.sleep(wait)
        else:
            logger.error("Tavily request failed after %d attempts for query %r: %s", max_retries, query, last_error)
            return []

        results: list[SerpResult] = []
        for item in data.get("results", []):
            results.append(
                SerpResult(
                    headline=item.get("title", ""),
                    url=item.get("url", ""),
                    snippet=item.get("content", ""),
                    source=item.get("url", "").split("/")[2] if item.get("url") else "",
                    published=item.get("published_date"),
                )
            )
        return results[:num_results]


def get_provider(provider_name: str, api_key: str) -> SerpProvider:
    """Factory for SERP providers."""
    providers: dict[str, type[SerpProvider]] = {
        "serpapi": SerpAPIProvider,
        "tavily": TavilyNewsProvider,
    }
    cls = providers.get(provider_name)
    if cls is None:
        raise ValueError(f"Unknown SERP provider: {provider_name}. Available: {list(providers.keys())}")
    return cls(api_key=api_key)


def build_queries(ticker: str, ticker_config: TickerRegistryEntry) -> list[str]:
    """Build search queries for a ticker from its registry config.

    Uses news_queries from ticker registry if available,
    otherwise falls back to '"{company_name}" OR "{ticker}"'.
    """
    if ticker_config.news_queries:
        return ticker_config.news_queries

    company_name = ticker_config.name or ticker
    return [f'"{company_name}" OR "{ticker}"']


def sweep_ticker(
    provider: SerpProvider,
    ticker: str,
    ticker_config: TickerRegistryEntry,
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
