"""Tests for the search collector with delta detection and Haiku pre-filter."""
from __future__ import annotations

from src.modules.monitor.evaluator import collector
from src.modules.monitor.evaluator.models import (
    MonitorConfig,
    MonitorSnapshot,
    cadence_to_hours,
)


def _make_search_config(**overrides) -> MonitorConfig:
    defaults = dict(
        id="test-search-monitor",
        type="search",
        tickers=["NVDA"],
        description="Track NVIDIA data center news",
        extract="Data center revenue growth, customer concentration, inference vs training",
        queries=['"NVIDIA" data center revenue'],
        search_backend="brave",
    )
    defaults.update(overrides)
    return MonitorConfig(**defaults)


def _fake_backend_factory(results: list[dict[str, str]]):
    """Return a fake backend class that returns fixed results."""
    class FakeBackend:
        def search(self, query: str, max_results: int = 10) -> list[dict[str, str]]:
            return results
    return FakeBackend()


# --- Delta detection tests ---


def test_search_no_new_results_after_delta(monkeypatch):
    """If all URLs were seen before, status should be unchanged."""
    results = [
        {"title": "Old article", "url": "https://example.com/old", "snippet": "old news"},
    ]
    monkeypatch.setattr(
        "src.modules.monitor.search.backends.get_backend",
        lambda name, api_key=None: _fake_backend_factory(results),
    )

    previous = MonitorSnapshot(
        monitor_id="test-search-monitor",
        date="2025-01-01",
        status="updated",
        current_state="Previous analysis",
        seen_urls=["https://example.com/old"],
    )

    result = collector._collect_search(_make_search_config(), previous)
    assert result["status"] == "unchanged"
    assert "https://example.com/old" in result["seen_urls"]


def test_search_new_urls_detected(monkeypatch):
    """New URLs should pass delta detection and reach Haiku pre-filter."""
    results = [
        {"title": "Old article", "url": "https://example.com/old", "snippet": "old"},
        {"title": "New article", "url": "https://example.com/new", "snippet": "new GPU sales surge"},
    ]
    monkeypatch.setattr(
        "src.modules.monitor.search.backends.get_backend",
        lambda name, api_key=None: _fake_backend_factory(results),
    )
    monkeypatch.setattr(
        collector, "call_haiku",
        lambda system, user: "0",
    )
    monkeypatch.setattr(
        collector, "call_sonnet",
        lambda system, user: "SIGNIFICANCE: medium\nNew GPU sales data.",
    )

    previous = MonitorSnapshot(
        monitor_id="test-search-monitor",
        date="2025-01-01",
        status="updated",
        current_state="Previous analysis",
        seen_urls=["https://example.com/old"],
    )

    result = collector._collect_search(_make_search_config(), previous)
    assert result["status"] == "updated"
    assert result["significance"] == "medium"
    assert "https://example.com/new" in result["seen_urls"]
    assert "https://example.com/old" in result["seen_urls"]


def test_search_first_run_no_previous(monkeypatch):
    """First run with no previous snapshot should process all results."""
    results = [
        {"title": "Article 1", "url": "https://example.com/1", "snippet": "news 1"},
    ]
    monkeypatch.setattr(
        "src.modules.monitor.search.backends.get_backend",
        lambda name, api_key=None: _fake_backend_factory(results),
    )
    monkeypatch.setattr(
        collector, "call_haiku",
        lambda system, user: "0",
    )
    monkeypatch.setattr(
        collector, "call_sonnet",
        lambda system, user: "SIGNIFICANCE: low\nInitial observation.",
    )

    result = collector._collect_search(_make_search_config(), None)
    assert result["status"] == "updated"
    assert "https://example.com/1" in result["seen_urls"]


# --- Haiku pre-filter tests ---


def test_haiku_filters_all_irrelevant(monkeypatch):
    """When Haiku says NONE, all results are filtered and status is unchanged."""
    results = [
        {"title": "Irrelevant", "url": "https://example.com/irrelevant", "snippet": "cat videos"},
    ]
    monkeypatch.setattr(
        "src.modules.monitor.search.backends.get_backend",
        lambda name, api_key=None: _fake_backend_factory(results),
    )
    monkeypatch.setattr(
        collector, "call_haiku",
        lambda system, user: "NONE",
    )

    result = collector._collect_search(_make_search_config(), None)
    assert result["status"] == "unchanged"
    assert "filtered as irrelevant" in result.get("delta_from_previous", "")


def test_haiku_passes_relevant_indices(monkeypatch):
    """Haiku returns specific indices; only those reach Sonnet."""
    results = [
        {"title": "Irrelevant", "url": "https://example.com/0", "snippet": "cats"},
        {"title": "Relevant", "url": "https://example.com/1", "snippet": "NVDA DC revenue up 200%"},
        {"title": "Also irrelevant", "url": "https://example.com/2", "snippet": "dogs"},
    ]
    monkeypatch.setattr(
        "src.modules.monitor.search.backends.get_backend",
        lambda name, api_key=None: _fake_backend_factory(results),
    )

    haiku_calls = []
    def fake_haiku(system, user):
        haiku_calls.append(user)
        return "1"

    sonnet_calls = []
    def fake_sonnet(system, user):
        sonnet_calls.append(user)
        return "SIGNIFICANCE: high\nMassive DC revenue growth."

    monkeypatch.setattr(collector, "call_haiku", fake_haiku)
    monkeypatch.setattr(collector, "call_sonnet", fake_sonnet)

    result = collector._collect_search(_make_search_config(), None)
    assert result["status"] == "updated"
    assert result["significance"] == "high"
    # Sonnet should only see the relevant result
    assert len(sonnet_calls) == 1
    assert "https://example.com/1" in sonnet_calls[0]
    assert "https://example.com/0" not in sonnet_calls[0]


def test_haiku_prose_response_extracts_indices(monkeypatch):
    """Haiku returns prose like 'Results 0, 2, and 5 are relevant'; regex should extract all."""
    results = [
        {"title": "A", "url": "https://example.com/0", "snippet": "a"},
        {"title": "B", "url": "https://example.com/1", "snippet": "b"},
        {"title": "C", "url": "https://example.com/2", "snippet": "c"},
    ]
    monkeypatch.setattr(
        "src.modules.monitor.search.backends.get_backend",
        lambda name, api_key=None: _fake_backend_factory(results),
    )

    monkeypatch.setattr(
        collector, "call_haiku",
        lambda system, user: "Results 0 and 2 are relevant to the monitor.",
    )

    sonnet_calls = []
    def fake_sonnet(system, user):
        sonnet_calls.append(user)
        return "SIGNIFICANCE: low\nAnalysis."

    monkeypatch.setattr(collector, "call_sonnet", fake_sonnet)

    result = collector._collect_search(_make_search_config(), None)
    assert result["status"] == "updated"
    # Sonnet should see results 0 and 2 but NOT 1
    assert len(sonnet_calls) == 1
    assert "https://example.com/0" in sonnet_calls[0]
    assert "https://example.com/2" in sonnet_calls[0]
    assert "https://example.com/1" not in sonnet_calls[0]


def test_haiku_failure_passes_all_through(monkeypatch):
    """If Haiku call fails, all results should pass through to Sonnet."""
    results = [
        {"title": "Article", "url": "https://example.com/1", "snippet": "news"},
    ]
    monkeypatch.setattr(
        "src.modules.monitor.search.backends.get_backend",
        lambda name, api_key=None: _fake_backend_factory(results),
    )
    monkeypatch.setattr(
        collector, "call_haiku",
        lambda system, user: (_ for _ in ()).throw(RuntimeError("API down")),
    )
    monkeypatch.setattr(
        collector, "call_sonnet",
        lambda system, user: "SIGNIFICANCE: low\nAnalysis.",
    )

    result = collector._collect_search(_make_search_config(), None)
    assert result["status"] == "updated"


# --- Cadence tests ---


def test_cadence_parsing():
    assert cadence_to_hours("1h") == 1
    assert cadence_to_hours("6h") == 6
    assert cadence_to_hours("1d") == 24
    assert cadence_to_hours("7d") == 168
    assert cadence_to_hours("") == 24  # default daily
    assert cadence_to_hours("", "weekly") == 168
    assert cadence_to_hours("", "hourly") == 1


def test_cadence_in_config():
    config = _make_search_config(cadence="6h")
    assert config.cadence == "6h"
    assert cadence_to_hours(config.cadence, config.frequency) == 6


# --- No queries test ---


def test_search_no_queries():
    config = _make_search_config(queries=[])
    result = collector._collect_search(config, None)
    assert result["status"] == "unchanged"


# --- Empty SERP results test ---


def test_search_empty_serp_results(monkeypatch):
    monkeypatch.setattr(
        "src.modules.monitor.search.backends.get_backend",
        lambda name, api_key=None: _fake_backend_factory([]),
    )
    result = collector._collect_search(_make_search_config(), None)
    assert result["status"] == "unchanged"
