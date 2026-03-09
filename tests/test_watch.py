from datetime import UTC, datetime

from cli.notifications import build_cli_alert_payload
from cli.market_data import _build_snapshot_from_delayed_quote
from cli.watch import (
    AlertState,
    WatchAlertRule,
    WatchConfig,
    _is_suppressed,
    evaluate_alert_rule,
)


def test_build_snapshot_from_delayed_quote_uses_average_volume():
    snapshot = _build_snapshot_from_delayed_quote(
        "AAPL.US",
        {
            "lastTradePrice": 210.0,
            "previousClosePrice": 200.0,
            "changePercent": 5.0,
            "volume": 4_000_000,
            "averageVolume": 2_000_000,
            "bidPrice": 209.9,
            "askPrice": 210.1,
            "timestamp": 1_772_832_540,
        },
    )

    assert snapshot.ticker == "AAPL"
    assert snapshot.price == 210.0
    assert snapshot.previous_close == 200.0
    assert snapshot.volume_ratio == 2.0
    assert snapshot.bid == 209.9
    assert snapshot.ask == 210.1


def test_evaluate_alert_rule_triggers_for_realtime_price():
    snapshot = _build_snapshot_from_delayed_quote(
        "NVDA.US",
        {
            "lastTradePrice": 121.0,
            "previousClosePrice": 100.0,
            "changePercent": 21.0,
            "volume": 9_000_000,
            "averageVolume": 3_000_000,
            "timestamp": 1_772_832_540,
        },
    ).model_copy(update={"source": "realtime", "timestamp": datetime.now(UTC)})

    rule = WatchAlertRule(
        id="rule123",
        ticker="NVDA",
        source="realtime",
        field="price",
        op=">=",
        value=120.0,
    )

    event = evaluate_alert_rule(rule, snapshot)

    assert event is not None
    assert event["rule_id"] == "rule123"
    assert event["actual"] == 121.0


def test_evaluate_alert_rule_skips_when_quote_field_missing():
    snapshot = _build_snapshot_from_delayed_quote(
        "MSFT.US",
        {
            "lastTradePrice": 400.0,
            "previousClosePrice": 398.0,
            "changePercent": 0.5,
            "volume": 1_000_000,
            "averageVolume": 2_000_000,
            "timestamp": 1_772_832_540,
        },
    )
    snapshot = snapshot.model_copy(update={"bid": None, "ask": None})

    rule = WatchAlertRule(
        id="spread1",
        ticker="MSFT",
        source="delayed",
        field="spread_pct",
        op=">=",
        value=0.05,
    )

    assert evaluate_alert_rule(rule, snapshot) is None


def test_custom_alert_cooldown_suppresses_repeat_event():
    snapshot = _build_snapshot_from_delayed_quote(
        "AAPL.US",
        {
            "lastTradePrice": 260.0,
            "previousClosePrice": 250.0,
            "changePercent": 4.0,
            "volume": 4_000_000,
            "averageVolume": 2_000_000,
            "timestamp": 1_772_832_540,
        },
    )
    rule = WatchAlertRule(
        id="cool1",
        ticker="AAPL",
        source="delayed",
        field="price",
        op=">=",
        value=250.0,
        cooldown_minutes=15,
    )
    event = evaluate_alert_rule(rule, snapshot)
    assert event is not None

    state = AlertState(last_triggered={"custom:cool1": event["timestamp"]})
    config = WatchConfig()

    assert _is_suppressed(event, state, config) is True


def test_build_cli_alert_payload_for_custom_rule():
    subject, message = build_cli_alert_payload(
        {
            "kind": "custom",
            "ticker": "AAPL",
            "source": "delayed",
            "field": "price",
            "op": ">=",
            "target": 250.0,
            "actual": 256.98,
            "cooldown_minutes": 30,
            "note": "test",
        }
    )

    assert subject == "Market custom delayed: AAPL"
    assert "Rule: price >= 250.0" in message
    assert "Actual: 256.98" in message
