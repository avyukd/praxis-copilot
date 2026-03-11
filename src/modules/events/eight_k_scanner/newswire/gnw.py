"""GlobeNewsWire RSS feed polling and HTML fetching."""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup

from src.modules.events.eight_k_scanner.models import PressRelease

logger = logging.getLogger(__name__)

USER_AGENT = "PraxisCopilot/1.0"

TICKER_RE = re.compile(
    r"\((?P<exchange>TSX|TSXV|TSX-V|NYSE|NASDAQ)\s*:\s*(?P<ticker>[A-Za-z][A-Za-z0-9.]*)\)",
    re.IGNORECASE,
)

# Looser pattern for HTML meta tags: "Nasdaq:MTSI" (no parens, no space)
META_TICKER_RE = re.compile(
    r"(?P<exchange>TSX|TSXV|TSX-V|NYSE|NASDAQ)\s*:\s*(?P<ticker>[A-Za-z][A-Za-z0-9.]*)",
    re.IGNORECASE,
)


def poll_gnw(feed_urls: list[str]) -> list[PressRelease]:
    releases: list[PressRelease] = []
    for url in feed_urls:
        # Infer exchange from feed URL (e.g. .../exchange/NASDAQ)
        feed_exchange = _exchange_from_feed_url(url)
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
            resp.raise_for_status()
            items = _parse_rss(resp.text, feed_exchange=feed_exchange)
            releases.extend(items)
        except Exception:
            logger.exception(f"Failed to fetch GNW feed: {url}")
    return releases


def _exchange_from_feed_url(url: str) -> str:
    """Extract exchange name from a GNW feed URL like .../RssFeed/exchange/NASDAQ."""
    parts = url.rstrip("/").split("/")
    for i, part in enumerate(parts):
        if part.lower() == "exchange" and i + 1 < len(parts):
            return parts[i + 1].upper()
    return ""


def _parse_rss(xml_text: str, feed_exchange: str = "") -> list[PressRelease]:
    items: list[PressRelease] = []
    root = ET.fromstring(xml_text)
    for item in root.iter("item"):
        title = item.findtext("title", "")
        link = item.findtext("link", "")
        pub_date = item.findtext("pubDate", "")
        description = item.findtext("description", "")

        release_id = _extract_release_id(link)
        if not release_id:
            continue

        ticker, exchange = _extract_ticker(title + " " + description)

        # If RSS fields didn't contain the ticker, try fetching the full page.
        # The (Exchange: TICKER) tag is often only in the article body or HTML meta tags.
        if not ticker and link:
            try:
                ticker, exchange = _extract_ticker_from_page(link)
            except Exception:
                logger.debug("Failed to fetch page for ticker extraction: %s", link)

        # Use feed-level exchange as fallback (we know the feed is exchange-specific)
        if not exchange and feed_exchange:
            exchange = feed_exchange

        published_at = ""
        if pub_date:
            try:
                dt = parsedate_to_datetime(pub_date)
                published_at = dt.isoformat()
            except Exception:
                published_at = pub_date

        items.append(PressRelease(
            release_id=f"gnw-{release_id}",
            title=title,
            url=link,
            published_at=published_at,
            source="gnw",
            ticker=ticker,
            exchange=exchange,
        ))
    return items


def _extract_release_id(url: str) -> str:
    parts = url.split("/")
    for i, part in enumerate(parts):
        if part == "news-release" and i + 4 < len(parts):
            candidate = parts[i + 4]
            if candidate.isdigit():
                return candidate
    return ""


def _extract_ticker(text: str) -> tuple[str, str]:
    match = TICKER_RE.search(text)
    if match:
        exchange = match.group("exchange").upper()
        ticker = match.group("ticker").upper()
        if exchange == "TSX-V":
            exchange = "TSXV"
        return ticker, exchange
    return "", ""


def _extract_ticker_from_page(url: str) -> tuple[str, str]:
    """Fetch a GNW page and extract ticker from body text or HTML meta tags."""
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    # Try meta keywords first (e.g. <meta name="keywords" content="Nasdaq:MTSI, ...">)
    # Uses looser regex since meta tags don't have parentheses
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    if meta_kw and meta_kw.get("content"):
        match = META_TICKER_RE.search(meta_kw["content"])
        if match:
            exchange = match.group("exchange").upper()
            ticker = match.group("ticker").upper()
            if exchange == "TSX-V":
                exchange = "TSXV"
            return ticker, exchange

    # Try body text
    body = (
        soup.find("div", class_="main-body-container")
        or soup.find("article")
        or soup.find("div", id="main-body-container")
    )
    if body:
        text = body.get_text(separator=" ", strip=True)[:5000]
        ticker, exchange = _extract_ticker(text)
        if ticker:
            return ticker, exchange

    return "", ""


def fetch_gnw_text(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    body = (
        soup.find("div", class_="main-body-container")
        or soup.find("article")
        or soup.find("div", id="main-body-container")
    )
    if not body:
        paragraphs = soup.find_all("p")
        return "\n\n".join(
            p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)
        )

    return body.get_text(separator="\n", strip=True)
