"""Unified press release fetcher -- dispatches by source."""
from __future__ import annotations

import logging

from src.modules.events.eight_k_scanner.newswire.gnw import fetch_gnw_text
from src.modules.events.eight_k_scanner.newswire.newsfile import fetch_newsfile_text
from src.modules.events.eight_k_scanner.newswire.cnw import fetch_cnw_text

logger = logging.getLogger(__name__)

_FETCHERS = {
    "gnw": fetch_gnw_text,
    "newsfile": fetch_newsfile_text,
    "cnw": fetch_cnw_text,
}


def fetch_release(url: str, source: str) -> dict:
    """Fetch full press release text. Returns {"text": str, "metadata": dict}."""
    fetcher = _FETCHERS.get(source)
    if not fetcher:
        raise ValueError(f"Unknown newswire source: {source}")
    text = fetcher(url)
    return {"text": text, "metadata": {"url": url, "source": source}}
