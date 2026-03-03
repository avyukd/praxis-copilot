"""Sonnet triage agent — prompt construction, API call, response parsing.

Layer 2: receives changed headlines, thesis summaries, and monitor descriptions.
Produces a triaged digest identifying material news.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import anthropic
import boto3
import yaml
from botocore.exceptions import ClientError

from .serp import SerpResponse

logger = logging.getLogger(__name__)

BUCKET = "praxis-copilot"
MODEL = "claude-sonnet-4-20250514"


TRIAGE_SYSTEM_PROMPT = """\
You are a financial news triage agent for an investment research system.

Your job: given a batch of new/changed headlines across multiple tickers, determine which are MATERIAL to the investment theses for those tickers.

Material means: the news could meaningfully affect the thesis, valuation drivers, key risks, or monitored datapoints. Noise (routine coverage, republished stories, tangential mentions) should be filtered out.

You will receive:
1. New/changed headlines per ticker (with snippets)
2. Thesis summaries from memo.yaml for each affected ticker (if available)
3. Active monitor descriptions for affected tickers (if available)

Output YAML with this exact schema:
```yaml
material:
  - ticker: TICKER
    headline: "exact headline"
    url: "url"
    relevance: |
      Why this matters to the thesis. Be specific — reference thesis points,
      valuation drivers, or monitors.
    related_monitors: [monitor-id-1, monitor-id-2]
    significance: high|medium|low

nothing_material: [TICKER1, TICKER2]  # changed tickers with no material news
```

Rules:
- Be selective. Most news is noise. Only flag genuinely material items.
- significance: high = could change thesis direction or valuation meaningfully. medium = relevant data point, worth tracking. low = tangentially relevant.
- related_monitors: list monitor IDs whose tracked datapoints this news relates to. Empty list if none.
- nothing_material: list all tickers that had changed headlines but nothing material.
- Output ONLY the YAML, no preamble or explanation.
"""


def _load_thesis_summary(s3_client: boto3.client, ticker: str) -> str | None:
    """Load thesis summary from memo.yaml for a ticker."""
    key = f"data/research/{ticker}/memo.yaml"
    try:
        obj = s3_client.get_object(Bucket=BUCKET, Key=key)
        memo = yaml.safe_load(obj["Body"].read().decode())
        # Extract the thesis summary section
        if isinstance(memo, dict):
            thesis = memo.get("thesis_summary") or memo.get("thesis") or memo.get("summary")
            if thesis:
                return str(thesis)
            # Fall back to dumping the whole memo if it's small
            dumped = yaml.dump(memo, default_flow_style=False)
            if len(dumped) < 2000:
                return dumped
        return None
    except Exception:
        logger.debug("No memo.yaml found for %s", ticker)
        return None


def _load_monitors(s3_client: boto3.client, ticker: str) -> list[dict[str, Any]]:
    """Load active monitor definitions that listen to this ticker."""
    monitors: list[dict[str, Any]] = []
    try:
        # List all monitor config files (paginated)
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix="config/monitors/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith((".yaml", ".yml")):
                    continue
                body = s3_client.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()
                monitor = yaml.safe_load(body)
                if not isinstance(monitor, dict):
                    continue
                # Check if this monitor listens to this ticker
                listen_list = monitor.get("listen", [])
                for listen_item in listen_list:
                    if isinstance(listen_item, str) and listen_item.startswith(f"{ticker}:"):
                        monitors.append({
                            "id": monitor.get("id", key.split("/")[-1].replace(".yaml", "")),
                            "description": monitor.get("description", ""),
                            "listen": listen_list,
                        })
                        break
    except ClientError as e:
        logger.error("S3 error loading monitors for %s: %s", ticker, e)
    except Exception:
        logger.exception("Failed to load monitors for %s", ticker)
    return monitors


def _build_user_prompt(
    changed_responses: dict[str, SerpResponse],
    thesis_summaries: dict[str, str | None],
    monitors: dict[str, list[dict[str, Any]]],
) -> str:
    """Build the user prompt with all context for the triage agent."""
    sections: list[str] = []

    for ticker, resp in changed_responses.items():
        section = f"## {ticker}\n\n"

        # Headlines
        section += "### New/Changed Headlines\n"
        for r in resp.results:
            section += f"- **{r.headline}**\n"
            section += f"  URL: {r.url}\n"
            section += f"  Source: {r.source}\n"
            if r.snippet:
                section += f"  Snippet: {r.snippet}\n"
            if r.published:
                section += f"  Published: {r.published}\n"
            section += "\n"

        # Thesis summary
        thesis = thesis_summaries.get(ticker)
        if thesis:
            section += f"### Thesis Summary\n{thesis}\n\n"
        else:
            section += "### Thesis Summary\nNo memo.yaml available for this ticker.\n\n"

        # Monitors
        ticker_monitors = monitors.get(ticker, [])
        if ticker_monitors:
            section += "### Active Monitors\n"
            for m in ticker_monitors:
                section += f"- **{m['id']}**: {m['description']}\n"
            section += "\n"

        sections.append(section)

    return "\n---\n\n".join(sections)


def run_triage(
    s3_client: boto3.client,
    changed_responses: dict[str, SerpResponse],
) -> dict[str, Any]:
    """Run Sonnet triage on changed headlines.

    Args:
        s3_client: boto3 S3 client
        changed_responses: dict of ticker -> SerpResponse for tickers with changed content

    Returns:
        Parsed triage digest as a dict
    """
    if not changed_responses:
        return {"material": [], "nothing_material": []}

    # Gather context
    thesis_summaries: dict[str, str | None] = {}
    monitors: dict[str, list[dict[str, Any]]] = {}

    for ticker in changed_responses:
        thesis_summaries[ticker] = _load_thesis_summary(s3_client, ticker)
        monitors[ticker] = _load_monitors(s3_client, ticker)

    user_prompt = _build_user_prompt(changed_responses, thesis_summaries, monitors)

    # Call Sonnet
    client = anthropic.Anthropic()
    logger.info(
        "Calling Sonnet triage for %d changed tickers: %s",
        len(changed_responses),
        list(changed_responses.keys()),
    )

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=TRIAGE_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
    except anthropic.APIError as e:
        logger.error("Anthropic API call failed: %s", e)
        return {"material": [], "nothing_material": list(changed_responses.keys()), "_error": str(e)}

    if not response.content or not hasattr(response.content[0], "text"):
        logger.error("Anthropic API returned empty content")
        return {"material": [], "nothing_material": list(changed_responses.keys()), "_error": "empty_response"}

    raw_text = response.content[0].text

    # Parse YAML response
    # Strip markdown code fences if present
    text = raw_text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first and last lines (fences)
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    try:
        digest = yaml.safe_load(text)
        if not isinstance(digest, dict):
            digest = {"material": [], "nothing_material": [], "_raw": raw_text}
    except yaml.YAMLError:
        logger.error("Failed to parse triage YAML response")
        digest = {"material": [], "nothing_material": [], "_raw": raw_text, "_parse_error": True}

    return digest


def store_digest(
    s3_client: boto3.client,
    digest: dict[str, Any],
    date_str: str,
    hour: int,
) -> str:
    """Store triage digest to S3. Returns the S3 key."""
    key = f"data/news/{date_str}/digest/{hour:02d}.yaml"
    payload = {
        "date": date_str,
        "hour": hour,
        **digest,
    }
    s3_client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=yaml.dump(payload, default_flow_style=False, sort_keys=False).encode(),
        ContentType="application/x-yaml",
    )
    logger.info("Stored triage digest to %s", key)
    return key
