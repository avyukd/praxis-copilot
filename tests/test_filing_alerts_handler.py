from src.modules.events.eight_k_scanner import filing_alerts_handler as alerts_handler


def test_alert_one_skips_sell(monkeypatch):
    monkeypatch.setattr(alerts_handler, "FILING_ANALYZER_ENABLED_FORMS", ["8-K", "8-K/A"])
    monkeypatch.setattr(alerts_handler, "SCANNER_MIN_MAGNITUDE", 0.5)

    def _read(bucket, key):
        if key.endswith("/index.json"):
            return {"ticker": "NVDA", "form_type": "8-K"}
        if key.endswith("/analysis.json"):
            return {"classification": "SELL", "magnitude": 0.9, "form_type": "8-K"}
        raise RuntimeError("missing")

    sent_calls = []
    monkeypatch.setattr(alerts_handler, "read_json_from_s3", _read)
    monkeypatch.setattr(alerts_handler, "send_alert", lambda *args, **kwargs: sent_calls.append(args) or True)

    result = alerts_handler._alert_one("praxis-copilot", "0001045810", "a1")

    assert result["action"] == "skipped"
    assert "SELL suppressed" in result["reason"]
    assert sent_calls == []


def test_alert_one_sends_buy(monkeypatch):
    monkeypatch.setattr(alerts_handler, "FILING_ANALYZER_ENABLED_FORMS", ["8-K", "8-K/A"])
    monkeypatch.setattr(alerts_handler, "SCANNER_MIN_MAGNITUDE", 0.5)

    def _read(bucket, key):
        if key.endswith("/index.json"):
            return {"ticker": "NVDA", "form_type": "8-K"}
        if key.endswith("/analysis.json"):
            return {"classification": "BUY", "magnitude": 0.9, "form_type": "8-K"}
        raise RuntimeError("missing")

    writes = []
    monkeypatch.setattr(alerts_handler, "read_json_from_s3", _read)
    monkeypatch.setattr(alerts_handler, "send_alert", lambda *args, **kwargs: True)
    monkeypatch.setattr(alerts_handler, "write_json_to_s3", lambda bucket, key, payload: writes.append((key, payload)))
    monkeypatch.setattr(alerts_handler, "et_now_iso", lambda: "2026-03-06T08:00:00-06:00")

    result = alerts_handler._alert_one("praxis-copilot", "0001045810", "a2")

    assert result["action"] == "alerted"
    assert any(k.endswith("/index.json") for k, _ in writes)
