"""Cision/CNW (newswire.ca) HTML scraping and fetching."""
from __future__ import annotations

import logging
import re

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENT = "PraxisCopilot/1.0"
CNW_BASE = "https://www.newswire.ca"
CNW_LISTING_URL = f"{CNW_BASE}/news-releases/"

TICKER_RE = re.compile(
    r"\((?P<exchange>TSX|TSXV|TSX-V)\s*:\s*(?P<ticker>[A-Z][A-Z0-9.]*)\)"
)


def poll_cnw(pages: int = 2) -> list[dict]:
    releases = []
    for page in range(1, pages + 1):
        url = CNW_LISTING_URL if page == 1 else f"{CNW_LISTING_URL}?page={page}"
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
            resp.raise_for_status()
            items = _parse_listing(resp.text)
            releases.extend(items)
        except Exception:
            logger.exception(f"Failed to scrape CNW listing page {page}")
    return releases


def _parse_listing(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    items = []

    for card in soup.select("div.card, article.news-release, div.news-release"):
        link_el = card.find("a", href=True)
        if not link_el:
            continue

        title = link_el.get_text(strip=True)
        href = link_el["href"]
        if not href.startswith("http"):
            href = CNW_BASE + href

        time_el = card.find("time") or card.find("span", class_=re.compile(r"date|time"))
        published_at = ""
        if time_el:
            published_at = time_el.get("datetime", "") or time_el.get_text(strip=True)

        release_id = _extract_release_id(href)
        if not release_id:
            continue

        ticker, exchange = _extract_ticker(title)

        items.append({
            "release_id": f"cnw-{release_id}",
            "title": title,
            "url": href,
            "published_at": published_at,
            "source": "cnw",
            "ticker": ticker,
            "exchange": exchange,
        })

    return items


def _extract_release_id(url: str) -> str:
    last_segment = url.rstrip("/").split("/")[-1] if "/" in url else url
    match = re.search(r"(\d{6,})", last_segment)
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


def fetch_cnw_text(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    body = (
        soup.find("div", class_="release-body")
        or soup.find("div", class_="content-body")
        or soup.find("article")
    )
    if not body:
        paragraphs = soup.find_all("p")
        return "\n\n".join(
            p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)
        )

    return body.get_text(separator="\n", strip=True)
