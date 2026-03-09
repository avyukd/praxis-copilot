"""Tests for intraday threshold checks."""
from datetime import datetime, timezone

from modules.manage.intraday import (
    check_move_from_close,
    check_volume_velocity,
    check_zigzag_reversal,
    run_all_checks,
)
from modules.manage.models import (
    AlertType,
    IntradayTickerState,
    ManageConfig,
    PriceData,
)


def _make_price(ticker: str = "AAPL", price: float = 100.0,
                previous_close: float = 100.0, volume: int = 1_000_000,
                adtv: float = 1_000_000.0) -> PriceData:
    change_pct = ((price - previous_close) / previous_close) * 100.0 if previous_close else 0.0
    return PriceData(
        ticker=ticker,
        price=price,
        previous_close=previous_close,
        change_pct=round(change_pct, 2),
        volume=volume,
        adtv=adtv,
        volume_ratio=round(volume / adtv, 2) if adtv else 0.0,
        timestamp=datetime.now(timezone.utc),
    )


def test_move_from_close_fires_at_each_band():
    config = ManageConfig(move_from_close_pct=5.0)
    state = IntradayTickerState()

    # 7% move — should fire band 1 (5%)
    price = _make_price(price=107.0)
    alerts = check_move_from_close(price, config, state)
    assert len(alerts) == 1
    assert alerts[0].details["threshold_pct"] == 5.0

    # 12% move — should fire band 2 (10%) only (band 1 already fired)
    price = _make_price(price=112.0)
    alerts = check_move_from_close(price, config, state)
    assert len(alerts) == 1
    assert alerts[0].details["threshold_pct"] == 10.0

    # Same 12% — nothing new
    alerts = check_move_from_close(price, config, state)
    assert len(alerts) == 0


def test_move_from_close_no_alert_below_threshold():
    config = ManageConfig(move_from_close_pct=5.0)
    state = IntradayTickerState()
    price = _make_price(price=103.0)
    alerts = check_move_from_close(price, config, state)
    assert len(alerts) == 0


def test_zigzag_reversal_detects_whipsaw():
    config = ManageConfig(reversal_pct=5.0)
    state = IntradayTickerState()

    # Initialize: price at 100, trending down (-2%)
    check_zigzag_reversal(_make_price(price=98.0, previous_close=100.0), config, state)
    assert state.direction == "down"

    # Drop to 92 — extends the low, no alert
    alerts = check_zigzag_reversal(_make_price(price=92.0), config, state)
    assert len(alerts) == 0
    assert state.extreme_price == 92.0

    # Bounce to 105 — reversal of 14.1% from 92, should fire
    alerts = check_zigzag_reversal(_make_price(price=105.0), config, state)
    assert len(alerts) == 1
    assert alerts[0].alert_type == AlertType.REVERSAL
    assert alerts[0].details["direction"] == "up"
    assert state.direction == "up"

    # Drop to 95 — reversal of 9.5% from 105, should fire
    alerts = check_zigzag_reversal(_make_price(price=95.0), config, state)
    assert len(alerts) == 1
    assert alerts[0].details["direction"] == "down"


def test_zigzag_no_alert_for_small_move():
    config = ManageConfig(reversal_pct=5.0)
    state = IntradayTickerState()

    check_zigzag_reversal(_make_price(price=100.0), config, state)
    # Small dip — not enough to trigger
    alerts = check_zigzag_reversal(_make_price(price=97.0), config, state)
    assert len(alerts) == 0


def test_volume_velocity_fires_on_burst():
    config = ManageConfig(volume_velocity_multiplier=2.0)
    state = IntradayTickerState(last_volume=1_000_000)

    # ADTV 50M → expected per period = 50M/26 ≈ 1.92M
    # Delta = 5M - 1M = 4M → ratio = 4M / 1.92M ≈ 2.08 → fires
    price = _make_price(volume=5_000_000, adtv=50_000_000.0)
    alerts = check_volume_velocity(price, config, state)
    assert len(alerts) == 1
    assert alerts[0].alert_type == AlertType.VOLUME_VELOCITY
    assert state.last_volume == 5_000_000


def test_volume_velocity_no_alert_normal_period():
    config = ManageConfig(volume_velocity_multiplier=2.0)
    state = IntradayTickerState(last_volume=1_000_000)

    # Delta = 2M - 1M = 1M, expected ~1.92M → ratio = 0.52 → no alert
    price = _make_price(volume=2_000_000, adtv=50_000_000.0)
    alerts = check_volume_velocity(price, config, state)
    assert len(alerts) == 0


def test_volume_velocity_skips_first_invocation():
    config = ManageConfig(volume_velocity_multiplier=2.0)
    state = IntradayTickerState(last_volume=0)

    # First poll: last_volume=0, should skip even with large volume
    price = _make_price(volume=10_000_000, adtv=50_000_000.0)
    alerts = check_volume_velocity(price, config, state)
    assert len(alerts) == 0
    assert state.last_volume == 10_000_000


def test_run_all_checks_with_overrides():
    config = ManageConfig(move_from_close_pct=5.0, reversal_pct=5.0)
    state = IntradayTickerState()
    overrides = {"move_from_close_pct": 20.0}  # very high threshold

    # 10% move — would fire with default 5%, but not with 20% override
    price = _make_price(price=110.0)
    alerts = run_all_checks(price, config, anchors=None,
                            ticker_state=state, ticker_overrides=overrides)
    # Should have no move_from_close alerts
    close_alerts = [a for a in alerts if a.alert_type in
                    (AlertType.PRICE_BREACH_UP, AlertType.PRICE_BREACH_DOWN)]
    assert len(close_alerts) == 0
