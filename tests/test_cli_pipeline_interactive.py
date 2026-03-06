from datetime import date, datetime, timezone

from src.cli import pipeline_interactive as pi
from src.cli.pipeline_status import PipelineItem
from src.cli.pipeline_status import PipelineTrace


def _sample_trace() -> PipelineTrace:
    return PipelineTrace(
        source_type="filings",
        key_prefix="data/raw/filings/0001/a1",
        item_id="a1",
        ticker="NVDA",
        cik="0001045810",
        form_type="8-K",
        source="sec",
        files=["index.json", "extracted.json", "analysis.json"],
        stage="analyzed",
        arrived_at="2026-03-05T14:00:00+00:00",
        extracted_at="2026-03-05T14:01:00+00:00",
        analyzed_at="2026-03-05T14:02:00+00:00",
        screening_at="",
        alert_sent_at="",
        analysis_classification="BUY",
        analysis_magnitude=0.9,
        analysis_summary="Raised guidance",
        extracted_total_chars=1200,
        extracted_items=["2.02"],
    )


def test_trace_payload_contains_expected_fields():
    trace = _sample_trace()
    payload = pi.trace_payload(trace)

    assert payload["item_id"] == "a1"
    assert payload["analysis"]["classification"] == "BUY"
    assert payload["extracted"]["items"] == ["2.02"]


def test_build_interactive_trace_text_includes_artifacts(monkeypatch):
    trace = _sample_trace()

    payloads = {
        "data/raw/filings/0001/a1/index.json": b'{"ticker":"NVDA"}',
        "data/raw/filings/0001/a1/extracted.json": b'{"total_chars":1200}',
        "data/raw/filings/0001/a1/analysis.json": b'{"classification":"BUY"}',
    }

    monkeypatch.setattr(pi, "download_file", lambda _s3, key: payloads[key])

    text = pi.build_interactive_trace_text(object(), trace)

    assert "Pipeline trace for id=a1" in text
    assert '"item_id": "a1"' in text
    assert "Artifacts:" in text
    assert "index.json" in text
    assert '{"classification":"BUY"}' in text


def test_move_selection_respects_bounds_and_paging():
    assert pi._move_selection(0, "up", total_items=5, page_size=3) == 0
    assert pi._move_selection(0, "down", total_items=5, page_size=3) == 1
    assert pi._move_selection(1, "page_down", total_items=5, page_size=3) == 4
    assert pi._move_selection(4, "page_up", total_items=5, page_size=3) == 1
    assert pi._move_selection(2, "home", total_items=5, page_size=3) == 0
    assert pi._move_selection(2, "end", total_items=5, page_size=3) == 4


def test_render_day_view_shows_selection_and_details(monkeypatch):
    monkeypatch.setattr(pi, "get_terminal_size", lambda fallback: __import__("os").terminal_size((100, 24)))

    items = [
        PipelineItem(
            source_type="filings",
            key_prefix="data/raw/filings/0001/a1",
            ticker="NVDA",
            cik="0001045810",
            form_type="8-K",
            source="sec",
            item_id="a1",
            arrived_at=datetime(2026, 3, 5, 14, 0, tzinfo=timezone.utc),
            stage="analyzed",
            alert_sent_at=None,
            age_minutes=12,
        ),
        PipelineItem(
            source_type="press_releases",
            key_prefix="data/raw/press_releases/gnw/NVDA/r1",
            ticker="NVDA",
            cik="0001045810",
            form_type="",
            source="gnw",
            item_id="r1",
            arrived_at=datetime(2026, 3, 5, 14, 5, tzinfo=timezone.utc),
            stage="screened_out",
            alert_sent_at=None,
            age_minutes=7,
        ),
    ]

    text = pi._render_day_view(items, date(2026, 3, 5), "all", selected_index=1)

    assert "Navigate: up/down arrows or j/k" in text
    assert "> screened_out" in text
    assert "item_id: r1" in text
    assert "prefix: data/raw/press_releases/gnw/NVDA/r1" in text
