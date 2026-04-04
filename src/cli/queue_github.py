"""GitHub Issues integration for the research queue."""

from __future__ import annotations

import json
import logging
import re
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from cli.queue_models import QueueTask, QueueTaskStatus, QueueTaskType

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

REPO = "avyukd/praxis-copilot"
QUEUE_LABEL = "research-queue"


# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------


def poll_issues() -> list[dict]:
    """Fetch open issues with the research-queue label. Returns raw JSON dicts."""
    result = subprocess.run(
        [
            "gh", "issue", "list",
            "--repo", REPO,
            "--label", QUEUE_LABEL,
            "--state", "open",
            "--json", "number,title,body,labels,createdAt",
            "--limit", "50",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        logger.error("gh issue list failed: %s", result.stderr)
        return []
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        logger.error("Failed to parse gh output: %s", result.stdout[:200])
        return []


def parse_issue(raw: dict) -> QueueTask:
    """Parse a raw GitHub issue dict into a QueueTask."""
    title = raw.get("title", "")
    body = raw.get("body", "") or ""
    labels = [l.get("name", "") for l in raw.get("labels", [])]
    created_str = raw.get("createdAt", "")

    tickers = extract_tickers(title + " " + body)
    image_urls = extract_image_urls(body)
    link_urls = extract_link_urls(body)
    task_type = classify_task(title, body, tickers, image_urls)

    created_at = datetime.now(ET)
    if created_str:
        try:
            created_at = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        except ValueError:
            pass

    return QueueTask(
        issue_number=raw["number"],
        title=title,
        body=body,
        task_type=task_type,
        tickers=tickers,
        labels=labels,
        image_urls=image_urls,
        link_urls=link_urls,
        created_at=created_at,
    )


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


def extract_tickers(text: str) -> list[str]:
    """Extract ticker symbols from text. Matches $TICKER or 'ticker: XYZ'."""
    tickers: list[str] = []

    # $TICKER pattern (1-6 uppercase letters, optional .suffix for exchanges)
    for m in re.finditer(r"\$([A-Z]{1,6}(?:\.[A-Z]{1,3})?)\b", text.upper()):
        t = m.group(1)
        if t not in tickers:
            tickers.append(t)

    # "ticker: XYZ" or "tickers: XYZ, ABC"
    for m in re.finditer(r"tickers?:\s*([A-Z][A-Z0-9.,\s]+)", text, re.IGNORECASE):
        for t in re.split(r"[,\s]+", m.group(1).strip().upper()):
            t = t.strip(".")
            if t and t not in tickers and re.match(r"^[A-Z]{1,6}(\.[A-Z]{1,3})?$", t):
                tickers.append(t)

    return tickers


def extract_image_urls(body: str) -> list[str]:
    """Extract GitHub-hosted image URLs from markdown body."""
    urls = []
    # Markdown images: ![alt](url)
    for m in re.finditer(r"!\[[^\]]*\]\(([^)]+)\)", body):
        url = m.group(1)
        if url not in urls:
            urls.append(url)
    # Bare image URLs on their own line
    for m in re.finditer(r"(https://[^\s]+\.(?:png|jpg|jpeg|gif|webp))", body, re.IGNORECASE):
        url = m.group(1)
        if url not in urls:
            urls.append(url)
    return urls


def extract_link_urls(body: str) -> list[str]:
    """Extract non-image external URLs from body."""
    image_exts = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
    urls = []
    for m in re.finditer(r"(https?://[^\s\)]+)", body):
        url = m.group(1).rstrip(".,;:)")
        # Skip image URLs (handled separately)
        if any(url.lower().endswith(ext) for ext in image_exts):
            continue
        # Skip GitHub attachment URLs (already in images)
        if "user-images.githubusercontent.com" in url:
            continue
        if url not in urls:
            urls.append(url)
    return urls


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def classify_task(title: str, body: str, tickers: list[str], images: list[str]) -> QueueTaskType:
    """Classify a queue task by its content."""
    text = (title + " " + body).lower()

    # Comparative: two tickers + comparison language
    if len(tickers) >= 2 and any(kw in text for kw in ["compare", "vs", "versus", "relative"]):
        return QueueTaskType.COMPARATIVE

    # Document review: has images or mentions filings/documents
    if images and any(kw in text for kw in ["8-k", "10-k", "10-q", "filing", "screenshot", "document"]):
        return QueueTaskType.DOCUMENT_REVIEW

    # Thematic: macro/sector keywords without specific tickers
    if any(kw in text for kw in ["macro", "sector", "theme", "industry", "supply chain", "rates", "fed"]):
        return QueueTaskType.THEMATIC

    # Ticker research: has tickers
    if tickers:
        return QueueTaskType.TICKER_RESEARCH

    # Document review fallback: has images
    if images:
        return QueueTaskType.DOCUMENT_REVIEW

    return QueueTaskType.FREEFORM


# ---------------------------------------------------------------------------
# Posting results
# ---------------------------------------------------------------------------


def post_result(issue_number: int, summary: str, success: bool) -> None:
    """Post research results as a comment and close the issue."""
    status_label = "done" if success else "failed"
    prefix = "Research complete." if success else "Research failed."

    comment_body = f"**[praxis-queue]** {prefix}\n\n{summary}"

    # Post comment
    subprocess.run(
        [
            "gh", "issue", "comment", str(issue_number),
            "--repo", REPO,
            "--body", comment_body,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )

    # Add status label
    subprocess.run(
        [
            "gh", "issue", "edit", str(issue_number),
            "--repo", REPO,
            "--add-label", status_label,
            "--remove-label", "in-progress",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )

    # Close issue
    subprocess.run(
        [
            "gh", "issue", "close", str(issue_number),
            "--repo", REPO,
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )


def mark_in_progress(issue_number: int) -> None:
    """Add in-progress label to an issue."""
    subprocess.run(
        [
            "gh", "issue", "edit", str(issue_number),
            "--repo", REPO,
            "--add-label", "in-progress",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )


def download_image(url: str, dest: Path) -> bool:
    """Download an image URL to a local path."""
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(resp.content)
        return True
    except Exception:
        logger.debug("Failed to download image: %s", url, exc_info=True)
        return False
