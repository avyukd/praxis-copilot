"""Entry trigger watchdog — monitors prices against memo entry ranges.

Reads memo.yaml entry ranges from all workspaces and monitors prices.
When a ticker hits its entry zone, sends an email alert with the
thesis summary and tactical setup.

Runs as part of the analyst agent loop or standalone.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import click
import yaml

from cli.config_utils import find_repo_root

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


EXCHANGE_MAP = {
    ".AX": "ASX (Australia)",
    ".TO": "TSX (Toronto)",
    ".V": "TSXV (TSX Venture)",
    ".L": "LSE (London)",
    ".CO": "Nasdaq Copenhagen",
    ".SW": "SIX Swiss Exchange",
    ".HK": "HKEX (Hong Kong)",
    ".T": "TSE (Tokyo)",
    ".DE": "XETRA (Frankfurt)",
    ".PA": "Euronext Paris",
}


def _exchange_label(ticker: str) -> str:
    """Get human-readable exchange label from ticker suffix."""
    for suffix, label in EXCHANGE_MAP.items():
        if ticker.upper().endswith(suffix):
            return label
    return ""


class EntryTarget:
    """A ticker with an entry range from its memo."""

    def __init__(self, ticker: str, entry_low: float, entry_high: float,
                 fair_value: float | None, decision: str, thesis: str,
                 tactical: dict, scores: dict,
                 exit_range: list | None = None):
        self.ticker = ticker
        self.entry_low = entry_low
        self.entry_high = entry_high
        self.exit_range = exit_range  # [low, high] or None
        self.fair_value = fair_value
        self.decision = decision
        self.thesis = thesis
        self.tactical = tactical
        self.scores = scores


def load_entry_targets() -> list[EntryTarget]:
    """Load all entry targets from workspace memos."""
    repo_root = find_repo_root()
    workspace = repo_root / "workspace"
    targets = []

    if not workspace.exists():
        return targets

    for ticker_dir in workspace.iterdir():
        if not ticker_dir.is_dir() or ticker_dir.name in ("queue", "analyst", "macro"):
            continue

        memo_path = ticker_dir / "memo.yaml"
        if not memo_path.exists():
            continue

        try:
            memo = yaml.safe_load(memo_path.read_text()) or {}
        except Exception:
            continue

        decision = (memo.get("decision") or "").upper().strip()
        # Only track BUY decisions and portfolio holdings for entry/exit alerts
        if decision not in ("BUY", "SPECULATIVE_BUY", "SPECULATIVE BUY"):
            # Also include NEUTRAL if it's a portfolio position
            portfolio_path = get_config_dir() / "portfolio.yaml"
            is_portfolio = False
            if portfolio_path.exists():
                try:
                    port_data = yaml.safe_load(portfolio_path.read_text()) or {}
                    port_tickers = {p.get("ticker", "").upper() for p in port_data.get("positions", [])}
                    is_portfolio = ticker_dir.name.upper() in port_tickers
                except Exception:
                    pass
            if not is_portfolio:
                continue

        valuation = memo.get("valuation", {}) or {}
        entry_range = valuation.get("entry_range", [None, None])

        if not entry_range or entry_range[0] is None or entry_range[1] is None:
            continue

        try:
            entry_low = float(entry_range[0])
            entry_high = float(entry_range[1])
        except (ValueError, TypeError):
            continue

        if entry_low <= 0 or entry_high <= 0:
            continue

        fv = valuation.get("fair_value_estimate")
        exit_range_raw = valuation.get("exit_range", [None, None])
        exit_range = None
        if exit_range_raw and exit_range_raw[0] is not None:
            try:
                exit_range = [float(exit_range_raw[0]), float(exit_range_raw[1])]
            except (ValueError, TypeError):
                pass

        thesis = memo.get("thesis_summary", "")
        tactical = memo.get("tactical", {}) or {}
        scores = memo.get("scores", {}) or {}

        targets.append(EntryTarget(
            ticker=ticker_dir.name,
            entry_low=entry_low,
            entry_high=entry_high,
            fair_value=float(fv) if fv else None,
            decision=decision,
            thesis=thesis,
            tactical=tactical,
            scores=scores,
            exit_range=exit_range,
        ))

    return targets


def check_entries(targets: list[EntryTarget], alerted_today: set[str] | None = None) -> list[dict]:
    """Check current prices against entry targets.

    Returns list of triggered entries with price data.
    """
    if alerted_today is None:
        alerted_today = set()

    triggered = []

    for target in targets:
        if target.ticker in alerted_today:
            continue

        price = _get_price(target.ticker)
        if price is None:
            continue

        # Check zones
        in_entry = target.entry_low <= price <= target.entry_high
        below_entry = price < target.entry_low
        # Exit zone: check exit_range from memo
        in_exit = False
        above_exit = False
        exit_range = target.exit_range
        if exit_range and exit_range[0] is not None:
            in_exit = exit_range[0] <= price <= exit_range[1]
            above_exit = price > exit_range[1]

        discount_pct = None
        if target.fair_value and target.fair_value > 0:
            discount_pct = (1 - price / target.fair_value) * 100

        if in_entry or below_entry:
            zone = "BELOW entry range" if below_entry else "IN entry range"
            triggered.append({
                "ticker": target.ticker,
                "price": price,
                "entry_low": target.entry_low,
                "entry_high": target.entry_high,
                "exit_low": exit_range[0] if exit_range else None,
                "exit_high": exit_range[1] if exit_range else None,
                "fair_value": target.fair_value,
                "zone": zone,
                "discount_pct": discount_pct,
                "decision": target.decision,
                "thesis": target.thesis,
                "tactical": target.tactical,
                "scores": target.scores,
                "signal": "entry",
            })
        elif in_exit or above_exit:
            zone = "ABOVE exit range" if above_exit else "IN exit range"
            triggered.append({
                "ticker": target.ticker,
                "price": price,
                "entry_low": target.entry_low,
                "entry_high": target.entry_high,
                "exit_low": exit_range[0] if exit_range else None,
                "exit_high": exit_range[1] if exit_range else None,
                "fair_value": target.fair_value,
                "zone": zone,
                "discount_pct": discount_pct,
                "decision": target.decision,
                "thesis": target.thesis,
                "tactical": target.tactical,
                "scores": target.scores,
                "signal": "exit",
            })

    return triggered


def _get_price(ticker: str) -> float | None:
    """Get current price for a ticker. Uses yfinance as fallback."""
    # Try EODHD first
    try:
        import requests
        eodhd_key = os.environ.get("EODHD_API_KEY", "")
        if eodhd_key:
            sym = f"{ticker}.US" if "." not in ticker else ticker
            r = requests.get(
                f"https://eodhd.com/api/real-time/{sym}",
                params={"api_token": eodhd_key, "fmt": "json"}, timeout=5,
            )
            data = r.json()
            price = data.get("close") or data.get("previousClose")
            if price and float(price) > 0:
                return float(price)
    except Exception:
        pass

    # Fallback: yfinance
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        hist = t.history(period="1d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass

    return None


def send_entry_alert(entry: dict) -> None:
    """Send email alert for an entry trigger."""
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not topic_arn:
        return

    ticker = entry["ticker"]
    price = entry["price"]
    zone = entry["zone"]
    thesis = entry["thesis"]
    scores = entry["scores"]
    tactical = entry["tactical"]

    exchange = _exchange_label(ticker)
    exchange_str = f" ({exchange})" if exchange else ""
    signal = entry.get("signal", "entry")

    if signal == "exit":
        subject = f"[PRAXIS EXIT] {ticker} at ${price:.2f} — {zone}"
        header = f"EXIT SIGNAL — {ticker}{exchange_str}"
    else:
        subject = f"[PRAXIS ENTRY] {ticker} at ${price:.2f} — {zone}"
        header = f"ENTRY TRIGGER — {ticker}{exchange_str}"

    body = (
        f"{header}\n\n"
        f"Current price: ${price:.2f}\n"
        f"Entry range: ${entry['entry_low']:.2f} – ${entry['entry_high']:.2f}\n"
    )
    if entry.get("exit_low") is not None:
        body += f"Exit range: ${entry['exit_low']:.2f} – ${entry['exit_high']:.2f}\n"
    body += f"Status: {zone}\n"

    if entry.get("fair_value"):
        body += f"Fair value: ${entry['fair_value']:.2f}\n"
    if entry.get("discount_pct") is not None:
        if signal == "exit":
            premium = -entry["discount_pct"]
            body += f"Premium to FV: {premium:.1f}%\n"
        else:
            body += f"Discount to FV: {entry['discount_pct']:.1f}%\n"

    body += (
        f"Decision: {entry['decision']}\n"
        f"Tactical: {scores.get('tactical', '?')}/10 | Fundamental: {scores.get('fundamental', '?')}/10\n"
        f"\nThesis:\n{thesis}\n"
    )

    if signal == "exit":
        body += "\n⚠ Consider trimming or closing this position.\n"

    if tactical:
        body += f"\nTactical Setup:\n"
        for key in ("setup", "entry_trigger", "risk_reward", "catalyst", "invalidation"):
            if tactical.get(key):
                body += f"  {key.replace('_', ' ').title()}: {tactical[key]}\n"

    try:
        import boto3
        sns = boto3.client("sns", region_name="us-east-1")
        sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=body)
        logger.info("Entry alert sent for %s at $%.2f", ticker, price)
    except Exception as e:
        logger.error("Entry alert failed for %s: %s", ticker, e)


# ---------------------------------------------------------------------------
# Standalone check (can be called from analyst agent or CLI)
# ---------------------------------------------------------------------------


def _alert_state_path() -> Path:
    return find_repo_root() / "data" / "entry_watchdog_state.yaml"


def _load_alert_state() -> dict:
    path = _alert_state_path()
    if path.exists():
        try:
            return yaml.safe_load(path.read_text()) or {}
        except Exception:
            return {}
    return {}


def _save_alert_state(state: dict) -> None:
    path = _alert_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(state, default_flow_style=False))


def run_entry_check() -> list[dict]:
    """Check all entry targets against current prices. Returns triggered entries.

    Dedup logic: only re-alert on a ticker if:
    - Never alerted before, OR
    - Price has moved >5% since last alert, OR
    - It's been >3 days since last alert
    """
    targets = load_entry_targets()
    if not targets:
        return []

    state = _load_alert_state()
    alerted = state.get("alerted", {})  # {ticker: {price, date, zone}}

    skip_tickers = set()
    today = date.today().isoformat()

    for ticker, prev in alerted.items():
        prev_date = prev.get("date", "")
        prev_price = prev.get("price", 0)

        # Re-alert if >3 days since last alert
        if prev_date:
            try:
                days_since = (date.today() - date.fromisoformat(prev_date)).days
                if days_since >= 3:
                    continue  # Allow re-alert
            except ValueError:
                pass

        # Skip if alerted recently
        skip_tickers.add(ticker)

    triggered = check_entries(targets, skip_tickers)

    for entry in triggered:
        ticker = entry["ticker"]
        prev = alerted.get(ticker, {})
        prev_price = prev.get("price", 0)

        # Check if price moved >5% since last alert
        if prev_price > 0:
            pct_change = abs(entry["price"] - prev_price) / prev_price * 100
            if pct_change < 5:
                continue  # Not enough movement to re-alert

        send_entry_alert(entry)
        alerted[ticker] = {
            "price": entry["price"],
            "date": today,
            "zone": entry["zone"],
        }
        click.echo(f"  ENTRY: {entry['ticker']} at ${entry['price']:.2f} ({entry['zone']})")

    state["alerted"] = alerted

    # Also check thesis price-level watches (invalidation levels)
    try:
        from cli.thesis_monitors import get_price_watches
        price_watches = get_price_watches()
        watch_alerted = state.get("watch_alerted", {})

        for w in price_watches:
            watch_key = f"{w.ticker}_{w.direction}_{w.level}"
            if watch_key in watch_alerted:
                prev_date = watch_alerted[watch_key].get("date", "")
                try:
                    if prev_date and (date.today() - date.fromisoformat(prev_date)).days < 3:
                        continue
                except ValueError:
                    pass

            price = _get_price(w.ticker)
            if price is None:
                continue

            hit = False
            if w.direction == "below" and price <= w.level:
                hit = True
            elif w.direction == "above" and price >= w.level:
                hit = True

            if hit:
                # Send thesis invalidation alert
                _send_thesis_watch_alert(w, price)
                watch_alerted[watch_key] = {"date": today, "price": price}
                click.echo(f"  THESIS WATCH: {w.ticker} at ${price:.2f} — {w.direction} ${w.level:.2f}")

        state["watch_alerted"] = watch_alerted
    except Exception as e:
        logger.debug("Thesis watch check failed: %s", e)

    _save_alert_state(state)

    return triggered


def _send_thesis_watch_alert(watch, price: float) -> None:
    """Send email for a triggered thesis price watch."""
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not topic_arn:
        return

    exchange = _exchange_label(watch.ticker)
    exchange_str = f" [{exchange}]" if exchange else ""

    subject = f"[PRAXIS THESIS] {watch.ticker}{exchange_str}: {watch.direction} ${watch.level:.2f}"
    body = (
        f"THESIS WATCH TRIGGERED — {watch.ticker}{exchange_str}\n\n"
        f"Current price: ${price:.2f}\n"
        f"Watch: {watch.direction} ${watch.level:.2f}\n"
        f"Urgency: {watch.urgency.upper()}\n\n"
        f"Condition: {watch.description}\n\n"
        f"This was auto-extracted from the investment memo's invalidation conditions.\n"
        f"Review whether this changes the thesis."
    )

    try:
        import boto3
        sns = boto3.client("sns", region_name="us-east-1")
        sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=body)
    except Exception as e:
        logger.error("Thesis watch alert failed: %s", e)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.group("watchdog")
def watchdog():
    """Entry trigger watchdog — monitors prices against memo entry ranges."""
    pass


@watchdog.command("check")
def watchdog_check():
    """Check all entry targets now.

    \b
    Reads entry ranges from workspace memos and checks current prices.
    Sends email alerts for any tickers in their entry zone.
    """
    targets = load_entry_targets()
    click.echo(f"Loaded {len(targets)} entry targets:")

    click.echo(f"\nChecking prices...")
    triggered = run_entry_check()
    if not triggered:
        click.echo("No tickers in entry zone.")


@watchdog.command("list")
def watchdog_list():
    """List all entry targets from workspace memos."""
    targets = load_entry_targets()
    if not targets:
        click.echo("No entry targets found. Run research to generate memos with entry ranges.")
        return

    click.echo(f"Entry targets ({len(targets)}):")
    click.echo(f"  {'Ticker':<12} {'Entry Range':>20} {'Fair Value':>12} {'Decision':<12} {'Tac':>4} {'Fun':>4}")
    click.echo(f"  {'-'*68}")
    for t in sorted(targets, key=lambda x: x.ticker):
        fv = f"${t.fair_value:.2f}" if t.fair_value else "—"
        scores = t.scores or {}
        click.echo(
            f"  {t.ticker:<12} ${t.entry_low:.2f}–${t.entry_high:.2f}  "
            f"{fv:>12}  {t.decision:<12} {scores.get('tactical', '?'):>4} {scores.get('fundamental', '?'):>4}"
        )
