"""CLI commands for delayed scans, real-time streaming, and custom alerts."""
from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import click
from pydantic import BaseModel, Field

from cli.config_utils import get_config_dir, load_yaml, save_yaml
from cli.market_data import (
    MarketSnapshot,
    fetch_delayed_snapshots,
    fetch_realtime_snapshot,
    get_eodhd_api_key,
)
from cli.models import UniverseConfig
from cli.notifications import send_cli_alert
from modules.manage.models import ManageConfig, PriceData
from modules.manage.thresholds import check_thresholds


ET = ZoneInfo("America/New_York")

WATCH_FILE = "watch.yaml"
WATCH_STATE_FILE = "watch_state.yaml"
MAX_REALTIME_SYMBOLS = 50
ALERT_LOG_DIR = Path("data/market/alerts")
DEFAULT_DELAYED_ALERT_COOLDOWN_MINUTES = 15
DEFAULT_REALTIME_ALERT_COOLDOWN_MINUTES = 5


class WatchAlertRule(BaseModel):
    """Custom delayed or real-time alert rule."""

    id: str
    ticker: str
    source: str = Field(pattern="^(delayed|realtime)$")
    field: str
    op: str
    value: float
    enabled: bool = True
    cooldown_minutes: int = DEFAULT_REALTIME_ALERT_COOLDOWN_MINUTES
    note: str | None = None


class WatchConfig(BaseModel):
    """Persistent local watch config."""

    max_realtime_symbols: int = MAX_REALTIME_SYMBOLS
    delayed_scan_interval_minutes: int = 15
    default_delayed_alert_cooldown_minutes: int = DEFAULT_DELAYED_ALERT_COOLDOWN_MINUTES
    default_realtime_alert_cooldown_minutes: int = DEFAULT_REALTIME_ALERT_COOLDOWN_MINUTES
    alerts: list[WatchAlertRule] = Field(default_factory=list)


class AlertState(BaseModel):
    """Persistent dedupe state for delayed and realtime alert runs."""

    last_triggered: dict[str, str] = Field(default_factory=dict)


@dataclass
class StreamState:
    """Mutable in-memory state for a streaming symbol."""

    ticker: str
    price: float
    previous_close: float
    volume: int
    bid: float | None = None
    ask: float | None = None
    bid_size: int | None = None
    ask_size: int | None = None
    last_trade_size: int | None = None
    event_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    received_time: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def change_pct(self) -> float:
        if not self.previous_close:
            return 0.0
        return ((self.price - self.previous_close) / self.previous_close) * 100.0

    @property
    def spread_pct(self) -> float | None:
        if self.bid is None or self.ask is None or self.price <= 0:
            return None
        return ((self.ask - self.bid) / self.price) * 100.0

    def to_snapshot(self) -> MarketSnapshot:
        return MarketSnapshot(
            ticker=self.ticker,
            price=self.price,
            previous_close=self.previous_close,
            change_pct=round(self.change_pct, 2),
            volume=self.volume,
            adtv=0.0,
            volume_ratio=0.0,
            bid=self.bid,
            ask=self.ask,
            bid_size=self.bid_size,
            ask_size=self.ask_size,
            timestamp=self.event_time,
            source="realtime",
        )


def _watch_config_path() -> Path:
    return get_config_dir() / WATCH_FILE


def _watch_state_path() -> Path:
    return get_config_dir() / WATCH_STATE_FILE


def load_watch_config() -> WatchConfig:
    return WatchConfig(**load_yaml(_watch_config_path()))


def save_watch_config(config: WatchConfig) -> None:
    save_yaml(_watch_config_path(), config.model_dump(exclude_none=True))


def load_alert_state() -> AlertState:
    return AlertState(**load_yaml(_watch_state_path()))


def save_alert_state(state: AlertState) -> None:
    save_yaml(_watch_state_path(), state.model_dump())


def _load_manage_config_local() -> tuple[ManageConfig, dict[str, dict[str, Any]]]:
    raw = load_yaml(get_config_dir() / "manage.yaml")
    defaults = raw.get("defaults", {})
    config = ManageConfig(
        price_move_pct=defaults.get("price_move_pct", 5.0),
        volume_anomaly_multiplier=defaults.get("volume_anomaly_multiplier", 3.0),
    )
    return config, raw.get("overrides", {})


def _snapshot_to_price_data(snapshot: MarketSnapshot) -> PriceData:
    return PriceData(
        ticker=snapshot.ticker,
        price=snapshot.price,
        previous_close=snapshot.previous_close,
        change_pct=snapshot.change_pct,
        volume=snapshot.volume,
        adtv=round(snapshot.adtv, 0),
        volume_ratio=round(snapshot.volume_ratio, 2),
        timestamp=snapshot.timestamp,
    )


def _log_alert(event: dict[str, Any]) -> None:
    ALERT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = ALERT_LOG_DIR / f"{datetime.now(UTC).date().isoformat()}.jsonl"
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, default=str) + "\n")


def _emit_event(event: dict[str, Any], *, notify_sns: bool, sns_dry_run: bool) -> None:
    _log_alert(event)
    if notify_sns:
        send_cli_alert(event, dry_run=sns_dry_run)


def _format_default_alert(alert: Any) -> dict[str, Any]:
    return {
        "kind": "default",
        "ticker": alert.ticker,
        "alert_type": alert.alert_type.value,
        "severity": alert.severity.value,
        "details": alert.details,
        "timestamp": alert.timestamp.isoformat(),
    }


def _alert_state_key(event: dict[str, Any]) -> str:
    if event["kind"] == "default":
        return f"default:{event['ticker']}:{event['alert_type']}"
    return f"custom:{event['rule_id']}"


def _alert_cooldown_minutes(event: dict[str, Any], watch_config: WatchConfig) -> int:
    if event["kind"] == "default":
        if event.get("source") == "realtime":
            return watch_config.default_realtime_alert_cooldown_minutes
        return watch_config.default_delayed_alert_cooldown_minutes
    return int(event.get("cooldown_minutes") or watch_config.default_realtime_alert_cooldown_minutes)


def _is_suppressed(event: dict[str, Any], state: AlertState, watch_config: WatchConfig) -> bool:
    cooldown_minutes = _alert_cooldown_minutes(event, watch_config)
    if cooldown_minutes <= 0:
        return False

    last_triggered = state.last_triggered.get(_alert_state_key(event))
    if not last_triggered:
        return False

    last_ts = datetime.fromisoformat(last_triggered)
    event_ts = datetime.fromisoformat(event["timestamp"])
    return event_ts < last_ts + timedelta(minutes=cooldown_minutes)


def _record_trigger(event: dict[str, Any], state: AlertState) -> None:
    state.last_triggered[_alert_state_key(event)] = event["timestamp"]


def _rule_value(snapshot: MarketSnapshot, field: str) -> float | None:
    field = field.lower()
    if field == "price":
        return snapshot.price
    if field == "change_pct":
        return snapshot.change_pct
    if field == "volume":
        return float(snapshot.volume)
    if field == "volume_ratio":
        return snapshot.volume_ratio
    if field == "bid":
        return snapshot.bid
    if field == "ask":
        return snapshot.ask
    if field == "spread_pct":
        if snapshot.bid is None or snapshot.ask is None or snapshot.price <= 0:
            return None
        return ((snapshot.ask - snapshot.bid) / snapshot.price) * 100.0
    raise click.ClickException(f"Unsupported alert field: {field}")


def evaluate_alert_rule(rule: WatchAlertRule, snapshot: MarketSnapshot) -> dict[str, Any] | None:
    """Evaluate a custom rule against a snapshot."""
    current = _rule_value(snapshot, rule.field)
    if current is None:
        return None

    op = rule.op.lower()
    triggered = (
        (op == ">=" and current >= rule.value)
        or (op == "<=" and current <= rule.value)
        or (op == ">" and current > rule.value)
        or (op == "<" and current < rule.value)
    )
    if not triggered:
        return None

    return {
        "kind": "custom",
        "rule_id": rule.id,
        "ticker": rule.ticker,
        "source": rule.source,
        "field": rule.field,
        "op": rule.op,
        "target": rule.value,
        "actual": round(current, 4),
        "cooldown_minutes": rule.cooldown_minutes,
        "note": rule.note,
        "timestamp": snapshot.timestamp.isoformat(),
    }


def _load_universe_tickers() -> list[str]:
    config = UniverseConfig(**load_yaml(get_config_dir() / "universe.yaml"))
    return config.tickers


def _matching_rules(config: WatchConfig, source: str, tickers: list[str] | None = None) -> list[WatchAlertRule]:
    allowed = set(tickers or [])
    rules = []
    for rule in config.alerts:
        if not rule.enabled or rule.source != source:
            continue
        if allowed and rule.ticker not in allowed:
            continue
        rules.append(rule)
    return rules


def _print_alert(event: dict[str, Any]) -> None:
    if event["kind"] == "default":
        click.echo(
            f"[default] {event['ticker']} {event['alert_type']} "
            f"{json.dumps(event['details'], sort_keys=True)}"
        )
        return
    click.echo(
        f"[custom:{event['source']}] {event['ticker']} "
        f"{event['field']} {event['op']} {event['target']} "
        f"(actual={event['actual']})"
    )


def _run_delayed_scan_once(
    selected: list[str],
    *,
    emit_snapshot_lines: bool = True,
    notify_sns: bool = False,
    sns_dry_run: bool = False,
) -> list[dict[str, Any]]:
    snapshots = fetch_delayed_snapshots(selected)
    config, overrides = _load_manage_config_local()
    watch_config = load_watch_config()
    alert_state = load_alert_state()
    custom_rules = _matching_rules(watch_config, "delayed", selected)

    all_events: list[dict[str, Any]] = []

    for ticker in selected:
        snapshot = snapshots.get(ticker)
        if snapshot is None:
            if emit_snapshot_lines:
                click.echo(f"{ticker}: no delayed quote returned")
            continue

        default_alerts = check_thresholds(
            _snapshot_to_price_data(snapshot),
            config,
            anchors=None,
            ticker_overrides=overrides.get(ticker, {}),
        )
        for alert_obj in default_alerts:
            event = _format_default_alert(alert_obj) | {"source": "delayed"}
            if _is_suppressed(event, alert_state, watch_config):
                continue
            all_events.append(event)
            _record_trigger(event, alert_state)
            _emit_event(event, notify_sns=notify_sns, sns_dry_run=sns_dry_run)

        for rule in custom_rules:
            if rule.ticker != ticker:
                continue
            event = evaluate_alert_rule(rule, snapshot)
            if not event or _is_suppressed(event, alert_state, watch_config):
                continue
            all_events.append(event)
            _record_trigger(event, alert_state)
            _emit_event(event, notify_sns=notify_sns, sns_dry_run=sns_dry_run)

        if emit_snapshot_lines:
            click.echo(
                f"{ticker}: price={snapshot.price:.2f} change={snapshot.change_pct:.2f}% "
                f"volume={snapshot.volume} vol_ratio={snapshot.volume_ratio:.2f}"
            )

    save_alert_state(alert_state)
    return all_events


@click.group()
def market():
    """Delayed market scans and alert evaluation."""
    pass


@market.command("delayed-scan")
@click.argument("tickers", nargs=-1)
@click.option("--notify-sns/--no-notify-sns", default=False, help="Publish triggered alerts to SNS.")
@click.option("--sns-dry-run", is_flag=True, help="Render SNS payloads without publishing.")
def delayed_scan(tickers: tuple[str, ...], notify_sns: bool, sns_dry_run: bool) -> None:
    """Run delayed price/volume alerts for the universe or selected tickers."""
    selected = [ticker.upper() for ticker in tickers] if tickers else _load_universe_tickers()
    if not selected:
        raise click.ClickException("No tickers to scan. Add tickers to config/universe.yaml first.")

    all_events = _run_delayed_scan_once(selected, notify_sns=notify_sns, sns_dry_run=sns_dry_run)

    if not all_events:
        click.echo("No delayed alerts triggered.")
        return

    click.echo("")
    for event in all_events:
        _print_alert(event)


@market.command("run")
@click.argument("tickers", nargs=-1)
@click.option("--interval-minutes", type=int, default=None, help="Polling interval for delayed scans.")
@click.option("--notify-sns/--no-notify-sns", default=False, help="Publish triggered alerts to SNS.")
@click.option("--sns-dry-run", is_flag=True, help="Render SNS payloads without publishing.")
def market_run(
    tickers: tuple[str, ...],
    interval_minutes: int | None,
    notify_sns: bool,
    sns_dry_run: bool,
) -> None:
    """Continuously run delayed scans on the universe or selected tickers."""
    selected = [ticker.upper() for ticker in tickers] if tickers else _load_universe_tickers()
    if not selected:
        raise click.ClickException("No tickers to scan. Add tickers to config/universe.yaml first.")

    watch_config = load_watch_config()
    interval = interval_minutes or watch_config.delayed_scan_interval_minutes
    if interval <= 0:
        raise click.ClickException("interval_minutes must be positive.")

    click.echo(
        f"Running delayed scans every {interval} minute(s) for: {', '.join(selected)}"
    )
    try:
        while True:
            click.echo(f"\n[{datetime.now(ET).strftime('%Y-%m-%d %H:%M:%S ET')}] delayed scan")
            events = _run_delayed_scan_once(selected, notify_sns=notify_sns, sns_dry_run=sns_dry_run)
            if not events:
                click.echo("No delayed alerts triggered.")
            else:
                for event in events:
                    _print_alert(event)
            time.sleep(interval * 60)
    except KeyboardInterrupt:
        click.echo("\nStopped delayed scan runner.")


# ---------------------------------------------------------------------------
# praxis watch TICKER [TICKER ...]
# ---------------------------------------------------------------------------

def _parse_symbol(raw: dict[str, Any]) -> str | None:
    symbol = raw.get("s") or raw.get("symbol") or raw.get("code")
    if not symbol:
        return None
    return str(symbol).split(".", 1)[0].upper()


def _parse_trade_size(raw: dict[str, Any]) -> int | None:
    for key in ("v", "size", "volume"):
        value = raw.get(key)
        if value is not None:
            return int(float(value))
    return None


def _parse_trade_price(raw: dict[str, Any]) -> float | None:
    for key in ("p", "price", "lastTradePrice", "close"):
        value = raw.get(key)
        if value is not None:
            return float(value)
    return None


def _parse_quote_price(raw: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = raw.get(key)
        if value is not None:
            return float(value)
    return None


def _parse_event_time(raw: dict[str, Any]) -> datetime | None:
    """Extract event timestamp from websocket payload (seconds or millis epoch)."""
    for key in ("t", "timestamp", "time"):
        value = raw.get(key)
        if value is not None:
            ts = float(value)
            if ts > 1e12:  # milliseconds
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=UTC)
    return None


def _parse_quote_size(raw: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = raw.get(key)
        if value is not None:
            return int(float(value))
    return None


def _stream_symbols(tickers: list[str]) -> None:
    import websocket

    api_key = get_eodhd_api_key()
    state: dict[str, StreamState] = {}
    lock = threading.Lock()
    stop_event = threading.Event()

    for ticker in tickers:
        snapshot = fetch_realtime_snapshot(ticker, api_key=api_key)
        state[ticker] = StreamState(
            ticker=ticker,
            price=snapshot.price,
            previous_close=snapshot.previous_close,
            volume=snapshot.volume,
            event_time=snapshot.timestamp,
            received_time=datetime.now(UTC),
        )

    def on_trade_message(_ws: websocket.WebSocketApp, message: str) -> None:
        raw = json.loads(message)
        if isinstance(raw, dict) and raw.get("status_code"):
            return
        rows = raw if isinstance(raw, list) else raw.get("data", [raw])
        with lock:
            for row in rows:
                symbol = _parse_symbol(row)
                if not symbol or symbol not in state:
                    continue
                price = _parse_trade_price(row)
                size = _parse_trade_size(row)
                if price is None:
                    continue
                state_row = state[symbol]
                state_row.price = price
                if size is not None:
                    state_row.last_trade_size = size
                    state_row.volume += size
                state_row.event_time = _parse_event_time(row) or state_row.event_time
                state_row.received_time = datetime.now(UTC)

    def on_quote_message(_ws: websocket.WebSocketApp, message: str) -> None:
        raw = json.loads(message)
        if isinstance(raw, dict) and raw.get("status_code"):
            return
        rows = raw if isinstance(raw, list) else raw.get("data", [raw])
        with lock:
            for row in rows:
                symbol = _parse_symbol(row)
                if not symbol or symbol not in state:
                    continue
                state_row = state[symbol]
                state_row.bid = _parse_quote_price(row, ("bp", "bidPrice", "bid", "b"))
                state_row.ask = _parse_quote_price(row, ("ap", "askPrice", "ask", "a"))
                state_row.bid_size = _parse_quote_size(row, ("bs", "bidSize"))
                state_row.ask_size = _parse_quote_size(row, ("as", "askSize"))
                state_row.event_time = _parse_event_time(row) or state_row.event_time
                state_row.received_time = datetime.now(UTC)

    def run_socket(url: str, on_message: Any) -> None:
        def _on_open(ws: websocket.WebSocketApp) -> None:
            ws.send(json.dumps({"action": "subscribe", "symbols": ",".join(tickers)}))

        ws = websocket.WebSocketApp(url, on_open=_on_open, on_message=on_message)
        while not stop_event.is_set():
            ws.run_forever(ping_interval=20, ping_timeout=10)
            if not stop_event.is_set():
                time.sleep(1)

    trade_thread = threading.Thread(
        target=run_socket,
        args=(f"wss://ws.eodhistoricaldata.com/ws/us?api_token={api_key}", on_trade_message),
        daemon=True,
    )
    quote_thread = threading.Thread(
        target=run_socket,
        args=(f"wss://ws.eodhistoricaldata.com/ws/us-quote?api_token={api_key}", on_quote_message),
        daemon=True,
    )
    trade_thread.start()
    quote_thread.start()

    try:
        while True:
            with lock:
                lines = [
                    "Ticker     Last      Chg%       Bid       Ask    Spread%       Vol  LastSz    As Of  Received"
                ]
                for ticker in tickers:
                    row = state[ticker]
                    spread = f"{row.spread_pct:.3f}" if row.spread_pct is not None else "-"
                    bid = f"{row.bid:.2f}" if row.bid is not None else "-"
                    ask = f"{row.ask:.2f}" if row.ask is not None else "-"
                    last_size = str(row.last_trade_size or "-")
                    as_of = row.event_time.astimezone(ET).strftime("%H:%M:%S")
                    received = row.received_time.astimezone(ET).strftime("%H:%M:%S")
                    lines.append(
                        f"{ticker:<8} {row.price:>8.2f} {row.change_pct:>8.2f}% "
                        f"{bid:>9} {ask:>9} {spread:>10} "
                        f"{row.volume:>9} {last_size:>7} {as_of:>8} {received:>9}"
                    )
            click.clear()
            click.echo("\n".join(lines))
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
        click.echo("\nStopped stream.")


@click.command("watch")
@click.argument("tickers", nargs=-1, required=True)
def watch(tickers: tuple[str, ...]) -> None:
    """Stream real-time trades and quotes for the given tickers."""
    selected = [ticker.upper() for ticker in tickers]
    if len(selected) > MAX_REALTIME_SYMBOLS:
        raise click.ClickException(f"Too many tickers (max {MAX_REALTIME_SYMBOLS}).")
    _stream_symbols(selected)


# ---------------------------------------------------------------------------
# praxis alert add/list/rm/enable/disable
# ---------------------------------------------------------------------------

@click.group()
def alert():
    """Manage persistent custom delayed and real-time alert rules."""
    pass


@alert.command("add")
@click.argument("ticker")
@click.argument("field")
@click.argument("op", type=click.Choice([">=", "<=", ">", "<"], case_sensitive=False))
@click.argument("value", type=float)
@click.option("--source", type=click.Choice(["delayed", "realtime"]), default="realtime")
@click.option("--cooldown-minutes", type=int, default=None, help="Minimum minutes between repeat firings.")
@click.option("--note", default=None, help="Optional note stored with the alert.")
def alert_add(
    ticker: str,
    field: str,
    op: str,
    value: float,
    source: str,
    cooldown_minutes: int | None,
    note: str | None,
) -> None:
    """Add a persistent custom alert rule.

    FIELD is one of: price, change_pct, volume, volume_ratio, bid, ask, spread_pct.
    """
    config = load_watch_config()
    rule = WatchAlertRule(
        id=uuid.uuid4().hex[:8],
        ticker=ticker.upper(),
        source=source,
        field=field,
        op=op,
        value=value,
        cooldown_minutes=(
            cooldown_minutes
            if cooldown_minutes is not None
            else (
                config.default_delayed_alert_cooldown_minutes
                if source == "delayed"
                else config.default_realtime_alert_cooldown_minutes
            )
        ),
        note=note,
    )
    config.alerts.append(rule)
    save_watch_config(config)
    click.echo(f"Added alert {rule.id} for {rule.ticker}.")


@alert.command("list")
def alert_list() -> None:
    """List configured custom alert rules."""
    config = load_watch_config()
    if not config.alerts:
        click.echo("No custom alerts configured.")
        return
    for rule in config.alerts:
        status = "on" if rule.enabled else "off"
        click.echo(
            f"{rule.id} {rule.ticker} [{rule.source}] {rule.field} {rule.op} {rule.value} "
            f"{status} cooldown={rule.cooldown_minutes}m"
        )


@alert.command("rm")
@click.argument("rule_id")
def alert_rm(rule_id: str) -> None:
    """Remove a custom alert rule."""
    config = load_watch_config()
    before = len(config.alerts)
    config.alerts = [rule for rule in config.alerts if rule.id != rule_id]
    if len(config.alerts) == before:
        raise click.ClickException(f"Alert rule {rule_id} not found.")
    save_watch_config(config)
    click.echo(f"Removed alert {rule_id}.")


@alert.command("enable")
@click.argument("rule_id")
def alert_enable(rule_id: str) -> None:
    """Enable a custom alert rule."""
    config = load_watch_config()
    for rule in config.alerts:
        if rule.id == rule_id:
            rule.enabled = True
            save_watch_config(config)
            click.echo(f"Enabled alert {rule_id}.")
            return
    raise click.ClickException(f"Alert rule {rule_id} not found.")


@alert.command("disable")
@click.argument("rule_id")
def alert_disable(rule_id: str) -> None:
    """Disable a custom alert rule."""
    config = load_watch_config()
    for rule in config.alerts:
        if rule.id == rule_id:
            rule.enabled = False
            save_watch_config(config)
            click.echo(f"Disabled alert {rule_id}.")
            return
    raise click.ClickException(f"Alert rule {rule_id} not found.")
