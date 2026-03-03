"""Collector dispatcher. Routes monitor configs to the appropriate collection tier."""
from __future__ import annotations

import logging
from typing import Any

from .models import MonitorConfig, MonitorSnapshot

logger = logging.getLogger(__name__)


def collect(
    config: MonitorConfig, previous: MonitorSnapshot | None
) -> dict[str, Any]:
    """Dispatch to the appropriate collector based on config.collector.

    Returns a dict with keys consumed by snapshot.build_snapshot:
      source, current_state, status, delta_from_previous, significance
    """
    collector_type = config.collector

    if collector_type == "scraper":
        return _collect_scraper(config, previous)
    elif collector_type == "agent":
        return _collect_agent(config, previous)
    else:
        logger.error("Unknown collector type '%s' for monitor %s", collector_type, config.id)
        return {"status": "unchanged", "source": "", "current_state": ""}


def _collect_scraper(
    config: MonitorConfig, previous: MonitorSnapshot | None
) -> dict[str, Any]:
    """Scraper-tier collection.

    Executes the extract prompt against the data sources in config.listen.
    For now, this is a skeleton that returns a placeholder result.
    Real implementation will call structured APIs or parse known document locations.
    """
    logger.info(
        "Running scraper collector for monitor %s (listen=%s)",
        config.id,
        config.listen,
    )

    # TODO: Implement actual scraper logic per data source type.
    # For each entry in config.listen (e.g. "NVDA:8k", "NVDA:price"),
    # resolve the data source, fetch latest data, and apply config.extract.

    return {
        "source": f"scraper:{','.join(config.listen)}",
        "current_state": "",
        "status": "unchanged",
        "delta_from_previous": "",
        "significance": "low",
    }


def _collect_agent(
    config: MonitorConfig, previous: MonitorSnapshot | None
) -> dict[str, Any]:
    """Agent-tier collection (Sonnet API).

    Not yet implemented. Logs a warning and returns unchanged status.
    """
    logger.warning(
        "Agent-tier collection not yet implemented for monitor %s. Skipping.",
        config.id,
    )

    return {
        "source": "agent:not_implemented",
        "current_state": "",
        "status": "unchanged",
        "delta_from_previous": "",
        "significance": "low",
    }
