import json

from src.modules.monitor.evaluator import collector
from src.modules.monitor.evaluator.models import MonitorConfig


class _FakeBody:
    def __init__(self, payload: str):
        self._payload = payload

    def read(self):
        return self._payload.encode("utf-8")


class _FakeS3:
    def __init__(self, objects: dict[str, dict]):
        self.objects = objects

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": _FakeBody(json.dumps(self.objects[Key]))}


def test_filing_collector_falls_back_from_analysis_to_extracted(monkeypatch):
    analysis_key = "data/raw/8k/0001045810/0001/analysis.json"
    extracted_key = "data/raw/8k/0001045810/0001/extracted.json"

    fake_s3 = _FakeS3(
        {
            analysis_key: {"classification": "BUY", "magnitude": 0.8},
            extracted_key: {
                "form_type": "8-K",
                "items": {"2.02": "Revenue and guidance were raised."},
            },
        }
    )

    monkeypatch.setattr(collector, "_get_s3_client", lambda: fake_s3)
    monkeypatch.setattr(collector, "_load_thesis_context", lambda ticker: "")
    monkeypatch.setattr(
        collector,
        "call_sonnet",
        lambda system, user: "SIGNIFICANCE: high\nMaterial update detected.",
    )

    config = MonitorConfig(
        id="nvda-filing-monitor",
        type="filing",
        tickers=["NVDA"],
        description="Track material filing updates",
        extract="Find material updates",
        filing_types=["8-K"],
    )

    result = collector.collect(
        config=config,
        previous=None,
        event_data={"s3_path": analysis_key, "ticker": "NVDA"},
    )

    assert result["status"] == "updated"
    assert result["significance"] == "high"
    assert result["source"] == f"filing:{extracted_key}"


def test_filing_collector_returns_unchanged_when_text_is_unavailable(monkeypatch):
    analysis_key = "data/raw/8k/0001045810/0001/analysis.json"
    fake_s3 = _FakeS3({analysis_key: {"classification": "BUY", "magnitude": 0.8}})

    monkeypatch.setattr(collector, "_get_s3_client", lambda: fake_s3)
    monkeypatch.setattr(collector, "_load_thesis_context", lambda ticker: "")

    config = MonitorConfig(
        id="nvda-filing-monitor",
        type="filing",
        tickers=["NVDA"],
        description="Track material filing updates",
        extract="Find material updates",
        filing_types=["8-K"],
    )

    result = collector.collect(
        config=config,
        previous=None,
        event_data={"s3_path": analysis_key, "ticker": "NVDA"},
    )

    assert result["status"] == "unchanged"
    assert result["current_state"] == "Empty filing text"
