"""Pluggable search backends for search monitors."""
from __future__ import annotations

import logging
import os
from typing import Protocol

import requests

logger = logging.getLogger(__name__)


class SearchBackend(Protocol):
    """Protocol for search backends."""

    def search(self, query: str, max_results: int = 10) -> list[dict[str, str]]:
        """Run a search query. Returns list of {title, url, snippet}."""
        ...


class BraveSearchBackend:
    """Brave Search API backend."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get("BRAVE_API_KEY", "")
        self.base_url = "https://api.search.brave.com/res/v1/web/search"

    def search(self, query: str, max_results: int = 10) -> list[dict[str, str]]:
        if not self.api_key:
            logger.warning("BRAVE_API_KEY not set")
            return []

        resp = requests.get(
            self.base_url,
            params={"q": query, "count": max_results},
            headers={"X-Subscription-Token": self.api_key},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        results = []
        for item in data.get("web", {}).get("results", []):
            results.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "snippet": item.get("description", ""),
            })
        return results


class TavilySearchBackend:
    """Tavily Search API backend."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get("TAVILY_API_KEY", "")
        self.base_url = "https://api.tavily.com/search"

    def search(self, query: str, max_results: int = 10) -> list[dict[str, str]]:
        if not self.api_key:
            logger.warning("TAVILY_API_KEY not set")
            return []

        resp = requests.post(
            self.base_url,
            json={
                "query": query,
                "max_results": max_results,
                "topic": "news",
            },
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        results = []
        for item in data.get("results", []):
            results.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "snippet": item.get("content", ""),
            })
        return results


class SerpApiBackend:
    """SerpAPI backend."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get("SERPAPI_KEY", "")
        self.base_url = "https://serpapi.com/search"

    def search(self, query: str, max_results: int = 10) -> list[dict[str, str]]:
        if not self.api_key:
            logger.warning("SERPAPI_KEY not set")
            return []

        resp = requests.get(
            self.base_url,
            params={
                "q": query,
                "api_key": self.api_key,
                "num": max_results,
                "engine": "google",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        results = []
        for item in data.get("organic_results", []):
            results.append({
                "title": item.get("title", ""),
                "url": item.get("link", ""),
                "snippet": item.get("snippet", ""),
            })
        return results


_BACKENDS: dict[str, type] = {
    "brave": BraveSearchBackend,
    "tavily": TavilySearchBackend,
    "serpapi": SerpApiBackend,
}


def get_backend(name: str, api_key: str | None = None) -> SearchBackend:
    """Factory function to get a search backend by name."""
    cls = _BACKENDS.get(name)
    if cls is None:
        raise ValueError(f"Unknown search backend: {name}. Available: {list(_BACKENDS.keys())}")
    return cls(api_key=api_key)
