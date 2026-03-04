from types import SimpleNamespace

import yaml

from src.modules.events.dispatch import handler
from src.modules.events.dispatch.models import EventRecord, MonitorDefinition, ParsedTrigger


class _FakeBody:
    def __init__(self, text: str):
        self._text = text

    def read(self):
        return self._text.encode("utf-8")


class _FakeS3:
    def __init__(self, objects=None):
        self.objects = objects or {}

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": _FakeBody(self.objects[Key])}


def test_parse_trigger_for_supported_paths():
    t1 = handler._parse_trigger("data/raw/8k/0001045810/0001/analysis.json")
    assert t1 is not None
    assert t1.source == "8k-scanner"
    assert t1.data_type == "filings"
    assert t1.cik == "0001045810"

    t2 = handler._parse_trigger("data/raw/ca-pr/SHOP/abc/analysis.json")
    assert t2 is not None
    assert t2.source == "ca-pr-scanner"
    assert t2.data_type == "press_releases"
    assert t2.ticker_direct == "SHOP"

    t3 = handler._parse_trigger("data/raw/us-pr/NVDA/r1/analysis.json")
    assert t3 is not None
    assert t3.source == "us-pr-scanner"
    assert t3.data_type == "press_releases"
    assert t3.ticker_direct == "NVDA"


def test_match_monitors_supports_exact_and_wildcard():
    monitors = [
        MonitorDefinition(id="m1", listen=["NVDA:filings"]),
        MonitorDefinition(id="m2", listen=["*:filings"]),
        MonitorDefinition(id="m3", listen=["NVDA:news"]),
    ]

    matched = handler._match_monitors("NVDA", "filings", monitors)
    assert [m.id for m in matched] == ["m1", "m2"]


def test_resolve_cik_to_ticker_handles_leading_zeroes(monkeypatch):
    registry = {
        "tickers": {
            "NVDA": {
                "cik": "0001045810",
                "exchange": "NASDAQ",
                "name": "NVIDIA",
                "news_queries": ['"NVDA"'],
            }
        }
    }

    fake_s3 = _FakeS3(objects={"config/ticker_registry.yaml": yaml.dump(registry)})
    monkeypatch.setattr(handler, "_get_s3_client", lambda: fake_s3)
    monkeypatch.setattr(
        handler.TickerRegistry,
        "model_validate",
        lambda raw: SimpleNamespace(
            tickers={"NVDA": SimpleNamespace(cik="0001045810")}
        ),
    )

    assert handler._resolve_cik_to_ticker("praxis-copilot", "1045810") == "NVDA"


def test_resolve_tickers_from_news_digest_supports_ticker_and_tickers(monkeypatch):
    digest = {
        "material": [
            {"ticker": "NVDA", "headline": "h1", "url": "u1", "relevance": "r", "significance": "high"},
            {"tickers": ["TSM", "AMD"], "headline": "h2", "url": "u2", "relevance": "r", "significance": "medium"},
        ]
    }
    fake_s3 = _FakeS3(objects={"data/news/2026-03-04/digest/14.yaml": yaml.dump(digest)})
    monkeypatch.setattr(handler, "_get_s3_client", lambda: fake_s3)

    parsed = ParsedTrigger(source="news-scanner", data_type="news", date="2026-03-04")
    tickers = handler._resolve_tickers("praxis-copilot", "data/news/2026-03-04/digest/14.yaml", parsed)

    assert set(tickers) == {"NVDA", "TSM", "AMD"}


def test_lambda_handler_routes_matching_monitor(monkeypatch):
    emitted: list[EventRecord] = []
    invoked: list[str] = []

    monkeypatch.setattr(handler, "_resolve_tickers", lambda bucket, key, parsed: ["NVDA"])
    monkeypatch.setattr(
        handler.S3Event,
        "model_validate",
        lambda event: SimpleNamespace(
            Records=[
                SimpleNamespace(
                    s3=SimpleNamespace(
                        bucket=SimpleNamespace(name="praxis-copilot"),
                        object=SimpleNamespace(key="data/raw/8k/0001045810/0001/analysis.json"),
                    )
                )
            ]
        ),
    )
    monkeypatch.setattr(
        handler,
        "_load_monitor_registry",
        lambda bucket: [
            MonitorDefinition(id="nvda-filings", listen=["NVDA:filings"], collector_lambda="collector-fn"),
            MonitorDefinition(id="amd-news", listen=["AMD:news"], collector_lambda="collector-fn-2"),
        ],
    )
    monkeypatch.setattr(handler, "_emit_event_record", lambda bucket, event_record: emitted.append(event_record))
    monkeypatch.setattr(handler, "_invoke_monitor_collector", lambda monitor, event_record: invoked.append(monitor.id))

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "praxis-copilot"},
                    "object": {"key": "data/raw/8k/0001045810/0001/analysis.json"},
                }
            }
        ]
    }

    result = handler.lambda_handler(event)

    assert result["dispatched"] == 1
    assert result["skipped"] == 0
    assert invoked == ["nvda-filings"]
    assert len(emitted) == 1
    assert emitted[0].ticker == "NVDA"
    assert emitted[0].data_type == "filings"
