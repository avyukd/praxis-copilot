from src.modules.events.eight_k_scanner import filing_analyzer_handler as analyzer


def test_analyzer_skips_non_enabled_forms(monkeypatch):
    monkeypatch.setattr(analyzer, "FILING_ANALYZER_ENABLED_FORMS", ["8-K", "8-K/A"])
    monkeypatch.setattr(
        analyzer,
        "read_json_from_s3",
        lambda bucket, key: (
            {"ticker": "AGM", "form_type": "10-Q"} if key.endswith("/index.json")
            else {"text": "quarterly update", "items": {}}
        ),
    )

    result = analyzer._analyze_one("praxis-copilot", "0000845877", "a1")
    assert result["action"] == "skipped"
    assert "not enabled" in result["reason"]


def test_analyzer_processes_enabled_forms(monkeypatch):
    monkeypatch.setattr(analyzer, "FILING_ANALYZER_ENABLED_FORMS", ["8-K", "8-K/A"])
    def _read(bucket, key):
        if key.endswith("/index.json"):
            return {"ticker": "NVDA", "form_type": "8-K"}
        if key.endswith("/extracted.json"):
            return {"cik": "0001045810", "accession_number": "a2", "items": {"2.02": "Raised guidance"}}
        raise RuntimeError("missing")

    monkeypatch.setattr(
        analyzer,
        "read_json_from_s3",
        _read,
    )
    monkeypatch.setattr(analyzer, "lookup_adtv", lambda ticker: 1000000.0)
    monkeypatch.setattr(analyzer, "get_financial_snapshot", lambda ticker: object())

    class _TokenUsage:
        def model_dump(self):
            return {"usage_available": True}

    class _Result:
        def __init__(self):
            self.analysis = type("A", (), {"model_dump": lambda self: {"classification": "BUY", "magnitude": 0.7}})()
            self.token_usage = _TokenUsage()

    monkeypatch.setattr(analyzer, "analyze_filing_with_usage", lambda extracted, snapshot, ticker: _Result())
    monkeypatch.setattr(analyzer, "write_json_to_s3", lambda bucket, key, payload: None)
    monkeypatch.setattr(analyzer, "et_now_iso", lambda: "2026-03-05T00:00:00-06:00")

    result = analyzer._analyze_one("praxis-copilot", "0001045810", "a2")
    assert result["action"] == "analyzed"
    assert result["classification"] == "BUY"
