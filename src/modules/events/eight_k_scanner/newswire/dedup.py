"""Cross-source deduplication for press releases."""
from __future__ import annotations

import logging
from difflib import SequenceMatcher

from src.modules.events.eight_k_scanner.models import PressRelease

logger = logging.getLogger(__name__)

SIMILARITY_THRESHOLD = 0.75


def dedup_releases(releases: list[PressRelease]) -> list[PressRelease]:
    """Remove duplicate releases across sources (same ticker + similar title)."""
    if not releases:
        return []

    seen: list[tuple[str, str]] = []
    result: list[PressRelease] = []

    for release in releases:
        ticker = release.ticker
        title = release.title

        if not ticker:
            result.append(release)
            continue

        is_dup = False
        for seen_ticker, seen_title in seen:
            if ticker == seen_ticker and _similar(title, seen_title):
                is_dup = True
                break

        if not is_dup:
            seen.append((ticker, title))
            result.append(release)

    if len(result) < len(releases):
        logger.info(f"Dedup: {len(releases)} -> {len(result)} releases")

    return result


def _similar(a: str, b: str) -> bool:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() >= SIMILARITY_THRESHOLD
