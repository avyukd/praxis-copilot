from src.modules.events.eight_k_scanner import alerts


def test_send_alert_stub_analysis_omits_reasoning_text(monkeypatch):
    published = {}

    class FakeSNS:
        def publish(self, **kwargs):
            published.update(kwargs)

    monkeypatch.setattr(alerts, "SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123:test")
    monkeypatch.setattr(alerts, "_get_sns_client", lambda: FakeSNS())

    sent = alerts.send_alert(
        "TEST",
        {
            "classification": "SELL",
            "magnitude": 0.7,
            "new_information": "",
            "materiality": "",
            "explanation": "",
        },
        {
            "acceptance_datetime": "2026-02-25T10:00:00Z",
            "accession_number": "0000000000-26-000001",
            "exchange": "NASDAQ",
        },
    )

    assert sent is True
    assert published["Subject"] == "8K SELL: TEST"
    assert "SELL ALERT: TEST (mag=0.7)" in published["Message"]
    assert "Source: 8-K (SEC)" in published["Message"]
    assert "Exchange: NASDAQ" in published["Message"]
    assert "Analysis: \n" in published["Message"]
    assert "materiality" in published["Message"]
    assert "because management said" not in published["Message"]


def test_send_alert_includes_press_release_source_and_exchange(monkeypatch):
    published = {}

    class FakeSNS:
        def publish(self, **kwargs):
            published.update(kwargs)

    monkeypatch.setattr(alerts, "SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123:test")
    monkeypatch.setattr(alerts, "_get_sns_client", lambda: FakeSNS())

    sent = alerts.send_alert(
        "ABCD",
        {"classification": "BUY", "magnitude": 0.9, "explanation": "Strong contract."},
        {
            "acceptance_datetime": "2026-02-25T11:00:00Z",
            "release_id": "20260225-1",
            "source": "gnw",
            "exchange": "TSXV",
        },
    )

    assert sent is True
    assert "Source: CA PR (GNW)" in published["Message"]
    assert "Exchange: TSXV" in published["Message"]
