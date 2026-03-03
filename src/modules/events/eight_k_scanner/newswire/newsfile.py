"""Newsfilecorp RSS feed polling and HTML fetching."""
from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENT = "PraxisCopilot/1.0"

DEFAULT_CATEGORIES = [
    "mining-metals",
    "technology",
    "oil-gas",
    "cannabis",
    "biotech-pharma",
    "clean-technology",
]

TICKER_RE = re.compile(
    r"\((?P<exchange>TSX|TSXV|TSX-V)\s*:\s*(?P<ticker>[A-Z][A-Z0-9.]*)\)"
)


def poll_newsfile(categories: list[str] | None = None) -> list[dict]:
    categories = categories or DEFAULT_CATEGORIES
    releases = []
    for cat in categories:
        url = f"https://feeds.newsfilecorp.com/industry/{cat}"
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
            resp.raise_for_status()
            items = _parse_rss(resp.text)
            releases.extend(items)
        except ParseError:
            logger.warning(f"Skipping non-XML Newsfile feed response: {url}")
        except Exception:
            logger.exception(f"Failed to fetch Newsfile feed: {url}")
    return releases


def _parse_rss(xml_text: str) -> list[dict]:
    items = []
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

        published_at = ""
        if pub_date:
            try:
                dt = parsedate_to_datetime(pub_date)
                published_at = dt.isoformat()
            except Exception:
                published_at = pub_date

        items.append({
            "release_id": f"newsfile-{release_id}",
            "title": title,
            "url": link,
            "published_at": published_at,
            "source": "newsfile",
            "ticker": ticker,
            "exchange": exchange,
        })
    return items


def _extract_release_id(url: str) -> str:
    match = re.search(r"/release/(\d+)", url)
    return match.group(1) if match else ""


def _extract_ticker(text: str) -> tuple[str, str]:
    match = TICKER_RE.search(text)
    if match:
        exchange = match.group("exchange")
        ticker = match.group("ticker")
        if exchange == "TSX-V":
            exchange = "TSXV"
        return ticker, exchange
    return "", ""


def fetch_newsfile_text(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    body = soup.find("div", class_="release-body") or soup.find("article")
    if not body:
        paragraphs = soup.find_all("p")
        return "\n\n".join(
            p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)
        )

    return body.get_text(separator="\n", strip=True)
