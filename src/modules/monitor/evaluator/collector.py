"""Collector dispatcher. Routes monitor configs to type-specific collection logic."""
from __future__ import annotations

import json
import logging
import os
from typing import Any

import boto3
import yaml
from botocore.exceptions import ClientError

from src.modules.common.llm import call_haiku, call_sonnet
from src.modules.monitor.evaluator.models import MonitorConfig, MonitorSnapshot

logger = logging.getLogger(__name__)

BUCKET = os.environ.get("S3_BUCKET", "praxis-copilot")

_s3_client = None


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def collect(
    config: MonitorConfig,
    previous: MonitorSnapshot | None,
    event_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Dispatch to the appropriate collector based on config.type.

    Returns a dict with keys consumed by snapshot.build_snapshot:
      source, current_state, status, delta_from_previous, significance
    """
    if config.type == "filing":
        return _collect_filing(config, previous, event_data)
    elif config.type == "scraper":
        return _collect_scraper(config, previous)
    elif config.type == "search":
        return _collect_search(config, previous)
    else:
        logger.error("Unknown monitor type '%s' for monitor %s", config.type, config.id)
        return {"status": "unchanged", "source": "", "current_state": ""}


def _collect_filing(
    config: MonitorConfig,
    previous: MonitorSnapshot | None,
    event_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Filing monitor: read extracted text from S3, call Sonnet with extract prompt."""
    if not event_data or not event_data.get("s3_path"):
        logger.warning("Filing monitor %s invoked without event_data/s3_path", config.id)
        return {"status": "unchanged", "source": "filing:no_event", "current_state": ""}

    s3_path = event_data["s3_path"]
    ticker = event_data.get("ticker", config.tickers[0] if config.tickers else "?")

    # Prefer extracted.json content. Some legacy dispatch paths pass analysis.json,
    # which lacks filing text and would otherwise make the monitor a no-op.
    extracted = None
    loaded_key = None
    candidate_keys = [s3_path]
    if s3_path.endswith("/analysis.json"):
        candidate_keys = [s3_path.replace("/analysis.json", "/extracted.json"), s3_path]

    for key in candidate_keys:
        try:
            resp = _get_s3_client().get_object(Bucket=BUCKET, Key=key)
            extracted = json.loads(resp["Body"].read())
            loaded_key = key
            break
        except Exception:
            logger.debug("Failed to read filing payload at %s", key)

    if extracted is None or loaded_key is None:
        logger.exception("Failed to read filing payload for %s", s3_path)
        return {"status": "unchanged", "source": f"filing:{s3_path}", "current_state": ""}

    # Build filing text from extracted data
    filing_text = _build_filing_text(extracted)
    if not filing_text.strip():
        return {
            "status": "unchanged",
            "source": f"filing:{loaded_key}",
            "current_state": "Empty filing text",
        }

    # Load thesis context if available
    thesis_context = _load_thesis_context(ticker)

    # Build Sonnet prompt
    previous_state = previous.current_state if previous else ""
    system_prompt = _build_filing_system_prompt(config, thesis_context, previous_state)
    user_prompt = f"Filing text for {ticker}:\n\n{filing_text[:30000]}"

    try:
        response = call_sonnet(system=system_prompt, user=user_prompt)
    except Exception:
        logger.exception("Sonnet call failed for monitor %s", config.id)
        return {"status": "unchanged", "source": f"filing:{loaded_key}", "current_state": ""}

    # Parse Sonnet response for significance
    significance = _parse_significance(response)

    return {
        "source": f"filing:{loaded_key}",
        "current_state": response,
        "status": "updated",
        "delta_from_previous": _compute_delta(previous_state, response),
        "significance": significance,
    }


def _collect_scraper(
    config: MonitorConfig,
    previous: MonitorSnapshot | None,
) -> dict[str, Any]:
    """Scraper monitor: load and execute scraper script, detect deltas."""
    if not config.scraper_script:
        logger.warning("Scraper monitor %s has no scraper_script", config.id)
        return {"status": "unchanged", "source": "scraper:no_script", "current_state": ""}

    # Load scraper from S3
    scraper_key = f"config/scrapers/{config.scraper_script}.py"
    try:
        resp = _get_s3_client().get_object(Bucket=BUCKET, Key=scraper_key)
        scraper_code = resp["Body"].read().decode()
    except ClientError:
        logger.warning("Scraper script not found: %s", scraper_key)
        return {"status": "unchanged", "source": f"scraper:{config.scraper_script}", "current_state": ""}

    # Execute scraper
    try:
        namespace: dict[str, Any] = {}
        exec(scraper_code, namespace)  # noqa: S102
        scrape_fn = namespace.get("scrape")
        if not callable(scrape_fn):
            logger.error("Scraper %s has no scrape() function", config.scraper_script)
            return {"status": "unchanged", "source": f"scraper:{config.scraper_script}", "current_state": ""}
        scraped_text = scrape_fn()
    except Exception:
        logger.exception("Scraper %s execution failed", config.scraper_script)
        return {"status": "unchanged", "source": f"scraper:{config.scraper_script}", "current_state": ""}

    previous_state = previous.current_state if previous else ""

    # No change detection
    if scraped_text.strip() == previous_state.strip():
        return {
            "source": f"scraper:{config.scraper_script}",
            "current_state": scraped_text,
            "status": "unchanged",
            "delta_from_previous": "",
            "significance": "low",
        }

    # Changed: call Sonnet for analysis
    system_prompt = (
        f"You are a financial monitor analyzing scraped data changes.\n"
        f"Monitor: {config.description}\n"
        f"Extract instruction: {config.extract}\n"
        f"Threshold: {config.threshold}\n\n"
        f"Previous state:\n{previous_state[:5000]}\n\n"
        f"Classify the significance as low, medium, or high based on the threshold."
    )
    user_prompt = f"New scraped data:\n\n{scraped_text[:20000]}"

    try:
        response = call_sonnet(system=system_prompt, user=user_prompt)
    except Exception:
        logger.exception("Sonnet call failed for scraper monitor %s", config.id)
        return {
            "source": f"scraper:{config.scraper_script}",
            "current_state": scraped_text,
            "status": "updated",
            "delta_from_previous": "Data changed but Sonnet analysis failed",
            "significance": "medium",
        }

    significance = _parse_significance(response)

    return {
        "source": f"scraper:{config.scraper_script}",
        "current_state": response,
        "status": "updated",
        "delta_from_previous": _compute_delta(previous_state, response),
        "significance": significance,
    }


def _collect_search(
    config: MonitorConfig,
    previous: MonitorSnapshot | None,
) -> dict[str, Any]:
    """Search monitor with delta detection and Haiku pre-filter.

    Flow:
      1. Run SERP queries via pluggable backend
      2. Delta detection: filter to URLs not seen in previous snapshot
      3. Haiku pre-filter: cheap call to decide if each new result is relevant
      4. Sonnet analysis: deep analysis only on Haiku-approved results
    """
    if not config.queries:
        logger.warning("Search monitor %s has no queries", config.id)
        return {"status": "unchanged", "source": "search:no_queries", "current_state": ""}

    # Import search backend
    try:
        from src.modules.monitor.search.backends import get_backend
        backend = get_backend(config.search_backend)
    except Exception:
        logger.exception("Failed to load search backend '%s'", config.search_backend)
        return {"status": "unchanged", "source": f"search:{config.search_backend}", "current_state": ""}

    # 1. Run all queries and deduplicate within this batch
    all_results: list[dict[str, str]] = []
    batch_seen: set[str] = set()
    for query in config.queries:
        try:
            results = backend.search(query)
            for r in results:
                url = r.get("url", "")
                if url and url not in batch_seen:
                    batch_seen.add(url)
                    all_results.append(r)
        except Exception:
            logger.warning("Search query failed for monitor %s: %s", config.id, query)

    if not all_results:
        return {
            "source": f"search:{config.search_backend}",
            "current_state": previous.current_state if previous else "No search results",
            "status": "unchanged",
            "delta_from_previous": "",
            "significance": "low",
            "seen_urls": list(previous.seen_urls) if previous else [],
        }

    # 2. Delta detection: only keep URLs not in previous snapshot
    previously_seen: set[str] = set(previous.seen_urls) if previous else set()
    new_results = [r for r in all_results if r.get("url", "") not in previously_seen]
    # Cap seen_urls to most recent 500 to prevent unbounded growth.
    # batch_seen (current) takes priority over previously_seen.
    merged = list(batch_seen) + [u for u in previously_seen if u not in batch_seen]
    all_seen_urls = merged[:500]

    if not new_results:
        return {
            "source": f"search:{config.search_backend}",
            "current_state": previous.current_state if previous else "No new results",
            "status": "unchanged",
            "delta_from_previous": "",
            "significance": "low",
            "seen_urls": all_seen_urls,
        }

    logger.info(
        "Monitor %s: %d total results, %d new (delta)",
        config.id, len(all_results), len(new_results),
    )

    # 3. Haiku pre-filter: cheap relevance gate
    relevant_results = _haiku_prefilter(config, new_results)

    if not relevant_results:
        logger.info("Monitor %s: Haiku filtered all %d new results as irrelevant", config.id, len(new_results))
        return {
            "source": f"search:{config.search_backend}",
            "current_state": previous.current_state if previous else "No relevant new results",
            "status": "unchanged",
            "delta_from_previous": f"{len(new_results)} new results filtered as irrelevant by pre-screen",
            "significance": "low",
            "seen_urls": all_seen_urls,
        }

    # 4. Sonnet analysis on relevant results only
    results_text = "\n\n".join(
        f"Title: {r.get('title', '')}\nURL: {r.get('url', '')}\nSnippet: {r.get('snippet', '')}"
        for r in relevant_results[:20]
    )

    previous_state = previous.current_state if previous else ""
    system_prompt = (
        f"You are a financial monitor analyzing NEW search results that passed relevance screening.\n"
        f"Monitor: {config.description}\n"
        f"Extract instruction: {config.extract}\n"
        f"Threshold: {config.threshold}\n\n"
        f"Previous state:\n{previous_state[:5000]}\n\n"
        f"Analyze these new results. Extract relevant information per the extract instruction.\n"
        f"Provide an updated current state that incorporates new findings with previous state.\n"
        f"Classify significance as low, medium, or high based on the threshold.\n"
        f"Start your response with SIGNIFICANCE: low|medium|high"
    )
    user_prompt = f"New search results ({len(relevant_results)} of {len(new_results)} passed relevance screen):\n\n{results_text}"

    try:
        response = call_sonnet(system=system_prompt, user=user_prompt)
    except Exception:
        logger.exception("Sonnet call failed for search monitor %s", config.id)
        return {
            "source": f"search:{config.search_backend}",
            "current_state": previous_state or results_text[:2000],
            "status": "updated",
            "delta_from_previous": "New results found but Sonnet analysis failed",
            "significance": "medium",
            "seen_urls": all_seen_urls,
        }

    significance = _parse_significance(response)

    return {
        "source": f"search:{config.search_backend}",
        "current_state": response,
        "status": "updated",
        "delta_from_previous": _compute_delta(previous_state, response),
        "significance": significance,
        "seen_urls": all_seen_urls,
    }


def _haiku_prefilter(
    config: MonitorConfig,
    results: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Use Haiku to cheaply filter search results for relevance.

    Sends all results in a single batch call. Returns only results Haiku
    marks as relevant.
    """
    if not results:
        return []

    numbered_results = "\n".join(
        f"[{i}] {r.get('title', '')} — {r.get('snippet', '')}"
        for i, r in enumerate(results)
    )

    system_prompt = (
        "You are a relevance filter for a financial monitoring system.\n"
        f"Monitor description: {config.description}\n"
        f"Extract instruction: {config.extract}\n\n"
        "For each numbered search result below, decide if it is RELEVANT to the monitor.\n"
        "Respond with ONLY a comma-separated list of the relevant result numbers.\n"
        "If none are relevant, respond with: NONE\n"
        "Example response: 0,2,5"
    )

    try:
        response = call_haiku(system=system_prompt, user=numbered_results)
    except Exception:
        logger.warning("Haiku pre-filter failed for monitor %s, passing all results through", config.id)
        return results

    # Parse response — extract integers
    response_stripped = response.strip()
    if response_stripped.upper() == "NONE":
        return []

    relevant_indices: set[int] = set()
    for token in response_stripped.replace("\n", ",").split(","):
        token = token.strip()
        if token.isdigit():
            relevant_indices.add(int(token))

    return [r for i, r in enumerate(results) if i in relevant_indices]


def _build_filing_text(extracted: dict) -> str:
    """Build readable text from extracted.json data."""
    parts = []
    form_type = extracted.get("form_type", "")
    if form_type:
        parts.append(f"Form type: {form_type}")

    items = extracted.get("items", {})
    if items:
        for item_num, text in items.items():
            parts.append(f"\n--- Item {item_num} ---\n{text}")

    exhibits = extracted.get("exhibits", [])
    for exhibit in exhibits:
        if isinstance(exhibit, dict):
            parts.append(f"\n--- Exhibit: {exhibit.get('filename', '?')} ---\n{exhibit.get('text', '')}")

    # For 10-K/10-Q, there may be a plain text field
    text = extracted.get("text", "")
    if text and not items:
        parts.append(text)

    return "\n".join(parts)


def _build_filing_system_prompt(
    config: MonitorConfig,
    thesis_context: str,
    previous_state: str,
) -> str:
    """Build system prompt for filing monitor Sonnet call."""
    parts = [
        "You are a financial monitor analyzing a new SEC filing.",
        f"Monitor: {config.description}",
        f"Extract instruction: {config.extract}",
    ]
    if config.threshold:
        parts.append(f"Threshold for alerting: {config.threshold}")
    if thesis_context:
        parts.append(f"\nInvestment thesis context:\n{thesis_context[:5000]}")
    if previous_state:
        parts.append(f"\nPrevious monitor state:\n{previous_state[:3000]}")
    parts.append(
        "\nClassify the significance as low, medium, or high based on the threshold.\n"
        "Start your response with SIGNIFICANCE: low|medium|high"
    )
    return "\n".join(parts)


def _load_thesis_context(ticker: str) -> str:
    """Load memo.yaml thesis context from S3 for a ticker."""
    key = f"data/research/{ticker}/memo.yaml"
    try:
        resp = _get_s3_client().get_object(Bucket=BUCKET, Key=key)
        data = yaml.safe_load(resp["Body"].read())
        if isinstance(data, dict):
            return data.get("thesis", data.get("summary", ""))
        return ""
    except Exception:
        return ""


def _parse_significance(response: str) -> str:
    """Extract significance level from Sonnet response."""
    first_line = response.strip().split("\n")[0].lower()
    if "high" in first_line:
        return "high"
    if "medium" in first_line:
        return "medium"
    return "low"


def _compute_delta(previous: str, current: str) -> str:
    """Compute a simple delta summary."""
    if not previous:
        return "Initial observation"
    if previous.strip() == current.strip():
        return ""
    return "State updated from previous observation"
