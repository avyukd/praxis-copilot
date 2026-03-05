from src.modules.events.eight_k_scanner import filing_alerts_handler as alerts_handler


def test_alert_press_release_respects_threshold(monkeypatch):
    def _read(bucket, key):
        if key.endswith("/index.json"):
            return {"ticker": "SHOP", "source": "gnw", "release_id": "abc123"}
        if key.endswith("/analysis.json"):
            return {"classification": "BUY", "magnitude": 0.2}
        raise RuntimeError("missing")

    monkeypatch.setattr(alerts_handler, "read_json_from_s3", _read)
    monkeypatch.setattr(alerts_handler, "SCANNER_MIN_MAGNITUDE", 0.5)

    result = alerts_handler._alert_press_release_one("praxis-copilot", "gnw", "SHOP", "abc123")
    assert result["action"] == "below_threshold"


def test_alert_press_release_suppresses_sell(monkeypatch):
    def _read(bucket, key):
        if key.endswith("/index.json"):
            return {"ticker": "SHOP", "source": "gnw", "release_id": "abc123"}
        if key.endswith("/analysis.json"):
            return {"classification": "SELL", "magnitude": 0.9}
        raise RuntimeError("missing")

    monkeypatch.setattr(alerts_handler, "read_json_from_s3", _read)

    result = alerts_handler._alert_press_release_one("praxis-copilot", "gnw", "SHOP", "abc123")
    assert result["action"] == "suppressed_sell"
