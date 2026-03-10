"""Options flow scanner — detect unusual options activity across the universe."""
from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo

import click
import requests
from pydantic import BaseModel, Field

from cli.config_utils import get_config_dir, load_yaml
from cli.market_data import EODHD_BASE, get_eodhd_api_key, to_eodhd_symbol
from cli.models import UniverseConfig
from cli.notifications import send_cli_alert


ET = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class OptionContract(BaseModel):
    """Single options contract from EODHD API."""

    contractName: str = ""
    contractSize: str | None = None
    currency: str = "USD"
    type: str  # CALL or PUT
    strike: float
    expirationDate: str
    lastTradeDateTime: str | None = None
    lastPrice: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    volume: int = 0
    openInterest: int = 0
    impliedVolatility: float = 0.0
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    rho: float | None = None
    daysBeforeExpiration: int = 0
    updatedAt: str | None = None
    inTheMoney: str | None = None

    @property
    def notional(self) -> float:
        return self.volume * self.lastPrice * 100

    @property
    def vol_oi_ratio(self) -> float:
        if self.openInterest <= 0:
            return 0.0
        return self.volume / self.openInterest

    @property
    def is_otm(self) -> bool:
        """Best-effort OTM check using inTheMoney field."""
        if self.inTheMoney is not None:
            return self.inTheMoney.upper() != "TRUE"
        return True  # assume OTM if we can't tell


class OptionExpiration(BaseModel):
    """All contracts for a single expiration date."""

    expirationDate: str
    options: dict[str, list[OptionContract]] = Field(default_factory=dict)

    @property
    def calls(self) -> list[OptionContract]:
        return self.options.get("CALL", [])

    @property
    def puts(self) -> list[OptionContract]:
        return self.options.get("PUT", [])


class TickerOptionsData(BaseModel):
    """Full options chain for a ticker."""

    ticker: str
    expirations: list[OptionExpiration] = Field(default_factory=list)


class AlertType(str, Enum):
    unusual_vol = "unusual_vol"
    large_bet = "large_bet"
    sweep = "sweep"
    pc_extreme = "pc_extreme"
    iv_spike = "iv_spike"


# What each alert type means — shown in detail output and scan legend.
ALERT_INTERPRETATION: dict[AlertType, str] = {
    AlertType.unusual_vol: "New positions opening aggressively — volume far exceeds existing open interest.",
    AlertType.large_bet: "Single contract with very large notional — institutional-size positioning.",
    AlertType.sweep: "Multiple OTM call strikes hit on the same expiry — strong directional conviction.",
    AlertType.pc_extreme: "Put/call volume ratio at an extreme — signals lopsided sentiment.",
    AlertType.iv_spike: "Near-term IV elevated vs long-term — market pricing an imminent move.",
}


class OptionsAlert(BaseModel):
    """A single options flow alert."""

    ticker: str
    score: int = 0
    alert_type: AlertType
    detail: str
    notional: float = 0.0
    dte: int | None = None

    @property
    def notional_display(self) -> str:
        if self.notional <= 0:
            return "-"
        if self.notional >= 1_000_000:
            return f"${self.notional / 1_000_000:.1f}M"
        return f"${self.notional / 1_000:.0f}K"

    @property
    def dte_display(self) -> str:
        return str(self.dte) if self.dte is not None else "-"


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------


def fetch_options_chain(ticker: str, api_key: str) -> TickerOptionsData:
    """Fetch the full options chain for a ticker from EODHD."""
    symbol = to_eodhd_symbol(ticker)
    response = requests.get(
        f"{EODHD_BASE}/options/{symbol}",
        params={"api_token": api_key, "fmt": "json"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    expirations: list[OptionExpiration] = []
    raw_data = payload.get("data", []) if isinstance(payload, dict) else payload
    if isinstance(raw_data, list):
        for exp_block in raw_data:
            exp_date = exp_block.get("expirationDate", "")
            raw_options = exp_block.get("options", {})
            parsed_options: dict[str, list[OptionContract]] = {}
            for opt_type in ("CALL", "PUT"):
                raw_contracts = raw_options.get(opt_type, [])
                parsed_options[opt_type] = [
                    OptionContract(**c) for c in raw_contracts
                ]
            expirations.append(
                OptionExpiration(expirationDate=exp_date, options=parsed_options)
            )

    return TickerOptionsData(ticker=ticker, expirations=expirations)


def _load_universe_tickers() -> list[str]:
    config = UniverseConfig(**load_yaml(get_config_dir() / "universe.yaml"))
    return config.tickers


# ---------------------------------------------------------------------------
# Alert detection logic
# ---------------------------------------------------------------------------


def _normalize_iv(raw_iv: float) -> float:
    """Normalize EODHD's IV to a decimal (e.g. 0.35 for 35%).

    EODHD reports IV inconsistently — some tickers return 0.35 (already
    decimal), others return 35.0 (percentage points), and occasionally
    values like 3500 (basis-point-ish).  Heuristic: if > 10, assume
    percentage points and divide by 100.
    """
    if raw_iv <= 0:
        return 0.0
    if raw_iv > 10:
        return raw_iv / 100.0
    return raw_iv


def _compute_smart_money_score(contract: OptionContract) -> int:
    """Score 0-100 based on how many 'smart money' criteria are met."""
    score = 0
    # Unusual volume (vol > OI)
    if contract.openInterest > 0 and contract.volume > contract.openInterest:
        score += 25
    # OTM
    if contract.is_otm:
        score += 25
    # 10-60 DTE sweet spot
    if 10 <= contract.daysBeforeExpiration <= 60:
        score += 25
    # Volume > 50 (meaningful activity)
    if contract.volume > 50:
        score += 15
    # Notional > $100K
    if contract.notional > 100_000:
        score += 10
    return min(score, 100)


def detect_unusual_volume(data: TickerOptionsData) -> list[OptionsAlert]:
    """Contracts where volume > 5x OI and volume > 100, notional > $50K."""
    alerts: list[OptionsAlert] = []
    for exp in data.expirations:
        for contract in exp.calls + exp.puts:
            if contract.volume <= 100:
                continue
            if contract.openInterest <= 0:
                continue
            ratio = contract.vol_oi_ratio
            if ratio < 5.0:
                continue
            if contract.notional < 50_000:
                continue
            score = _compute_smart_money_score(contract)
            detail = (
                f"${contract.strike}{contract.type[0]} "
                f"{contract.expirationDate} "
                f"vol={contract.volume} OI={contract.openInterest} "
                f"({ratio:.1f}x)"
            )
            alerts.append(OptionsAlert(
                ticker=data.ticker,
                score=score,
                alert_type=AlertType.unusual_vol,
                detail=detail,
                notional=contract.notional,
                dte=contract.daysBeforeExpiration,
            ))
    return alerts


def detect_large_bets(data: TickerOptionsData) -> list[OptionsAlert]:
    """Single contracts with notional > $1M."""
    alerts: list[OptionsAlert] = []
    for exp in data.expirations:
        for contract in exp.calls + exp.puts:
            if contract.notional < 1_000_000:
                continue
            score = _compute_smart_money_score(contract)
            detail = (
                f"${contract.strike}{contract.type[0]} "
                f"{contract.expirationDate} "
                f"vol={contract.volume} notional={_fmt_notional(contract.notional)}"
            )
            alerts.append(OptionsAlert(
                ticker=data.ticker,
                score=score,
                alert_type=AlertType.large_bet,
                detail=detail,
                notional=contract.notional,
                dte=contract.daysBeforeExpiration,
            ))
    return alerts


def detect_bullish_sweeps(data: TickerOptionsData) -> list[OptionsAlert]:
    """Multiple OTM call strikes on the same expiration with unusual volume."""
    alerts: list[OptionsAlert] = []
    for exp in data.expirations:
        otm_calls = [
            c for c in exp.calls
            if c.is_otm and c.volume > 50
            and c.openInterest > 0 and c.vol_oi_ratio > 2.0
        ]
        if len(otm_calls) < 3:
            continue
        strikes = sorted(c.strike for c in otm_calls)
        total_vol = sum(c.volume for c in otm_calls)
        total_notional = sum(c.notional for c in otm_calls)
        avg_dte = otm_calls[0].daysBeforeExpiration
        strike_range = f"${strikes[0]:.0f}-${strikes[-1]:.0f}"
        detail = (
            f"{len(otm_calls)} OTM calls {exp.expirationDate} "
            f"({strike_range}) total={total_vol}"
        )
        score = min(60 + len(otm_calls) * 5, 100)
        alerts.append(OptionsAlert(
            ticker=data.ticker,
            score=score,
            alert_type=AlertType.sweep,
            detail=detail,
            notional=total_notional,
            dte=avg_dte,
        ))
    return alerts


def detect_pc_ratio_extreme(data: TickerOptionsData) -> list[OptionsAlert]:
    """Flag ticker-level P/C ratio extremes."""
    total_put_vol = 0
    total_call_vol = 0
    for exp in data.expirations:
        total_call_vol += sum(c.volume for c in exp.calls)
        total_put_vol += sum(c.volume for c in exp.puts)

    total_vol = total_put_vol + total_call_vol
    if total_vol < 500:
        return []

    if total_call_vol == 0:
        pc_ratio = float("inf")
    else:
        pc_ratio = total_put_vol / total_call_vol

    alerts: list[OptionsAlert] = []
    if pc_ratio > 3.0:
        detail = f"P/C={pc_ratio:.2f} (bearish) total_vol={total_vol:,}"
        score = min(int(40 + pc_ratio * 5), 90)
        alerts.append(OptionsAlert(
            ticker=data.ticker,
            score=score,
            alert_type=AlertType.pc_extreme,
            detail=detail,
        ))
    elif pc_ratio < 0.2:
        detail = f"P/C={pc_ratio:.2f} (bullish) total_vol={total_vol:,}"
        score = min(int(40 + (1 / max(pc_ratio, 0.01)) * 2), 90)
        alerts.append(OptionsAlert(
            ticker=data.ticker,
            score=score,
            alert_type=AlertType.pc_extreme,
            detail=detail,
        ))

    return alerts


def detect_iv_spike(data: TickerOptionsData) -> list[OptionsAlert]:
    """Near-term IV significantly higher than far-term IV."""
    near_ivs: list[float] = []
    far_ivs: list[float] = []
    for exp in data.expirations:
        for contract in exp.calls + exp.puts:
            iv = _normalize_iv(contract.impliedVolatility)
            if iv <= 0 or contract.volume < 10:
                continue
            if contract.daysBeforeExpiration < 30:
                near_ivs.append(iv)
            elif contract.daysBeforeExpiration > 60:
                far_ivs.append(iv)

    if not near_ivs or not far_ivs:
        return []

    avg_near = sum(near_ivs) / len(near_ivs)
    avg_far = sum(far_ivs) / len(far_ivs)
    if avg_far <= 0:
        return []

    skew = (avg_near - avg_far) / avg_far
    if skew < 0.5:
        return []

    detail = (
        f"Near IV={avg_near:.1%} Far IV={avg_far:.1%} "
        f"skew={skew:.0%}"
    )
    score = min(int(50 + skew * 30), 95)
    return [OptionsAlert(
        ticker=data.ticker,
        score=score,
        alert_type=AlertType.iv_spike,
        detail=detail,
    )]


def scan_ticker(data: TickerOptionsData, *, max_per_ticker: int = 5) -> list[OptionsAlert]:
    """Run all detectors on a ticker's options data, deduplicate, and cap output."""
    alerts: list[OptionsAlert] = []
    alerts.extend(detect_unusual_volume(data))
    alerts.extend(detect_large_bets(data))
    alerts.extend(detect_bullish_sweeps(data))
    alerts.extend(detect_pc_ratio_extreme(data))
    alerts.extend(detect_iv_spike(data))

    # Deduplicate: if same contract triggers unusual_vol and large_bet,
    # keep only the higher-scoring one. Key on (detail prefix → strike+exp).
    seen: dict[str, OptionsAlert] = {}
    deduped: list[OptionsAlert] = []
    for a in alerts:
        # Sweep, pc_extreme, iv_spike are ticker-level — always keep
        if a.alert_type in (AlertType.sweep, AlertType.pc_extreme, AlertType.iv_spike):
            deduped.append(a)
            continue
        # For contract-level alerts, dedup on the strike+exp portion
        key = a.detail.split("vol=")[0].strip() if "vol=" in a.detail else a.detail
        if key in seen:
            if a.score > seen[key].score:
                seen[key] = a
        else:
            seen[key] = a
    deduped.extend(seen.values())

    # Sort by score desc, notional desc, and cap per ticker
    deduped.sort(key=lambda a: (a.score, a.notional), reverse=True)
    return deduped[:max_per_ticker]


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _fmt_notional(value: float) -> str:
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:.0f}"


def _severity_color(score: int) -> str:
    if score >= 80:
        return "red"
    if score >= 60:
        return "yellow"
    return "white"


def _print_scan_table(alerts: list[OptionsAlert], scanned_count: int) -> None:
    """Print formatted scan results."""
    now = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")
    click.echo(f"\nOptions Flow Scan — {now}")
    click.echo(f"Scanned: {scanned_count} tickers | Flags: {len(alerts)}\n")

    if not alerts:
        click.echo("No interesting options flow detected.")
        return

    header = (
        f"{'TICKER':<8} {'Score':>5}  {'Type':<14} "
        f"{'Detail':<48} {'Notional':>10} {'DTE':>5}"
    )
    click.echo(header)
    click.echo("-" * len(header))

    for a in alerts:
        color = _severity_color(a.score)
        line = (
            f"{a.ticker:<8} {a.score:>5}  {a.alert_type.value:<14} "
            f"{a.detail:<48} {a.notional_display:>10} {a.dte_display:>5}"
        )
        click.echo(click.style(line, fg=color))

    # Legend
    seen_types = {a.alert_type for a in alerts}
    if seen_types:
        click.echo(click.style("\nLegend:", dim=True))
        for at in AlertType:
            if at in seen_types:
                click.echo(click.style(f"  {at.value:<14} {ALERT_INTERPRETATION[at]}", dim=True))


def _print_detail(data: TickerOptionsData, alerts: list[OptionsAlert]) -> None:
    """Print detailed options chain info for a single ticker."""
    click.echo(f"\nOptions Detail — {data.ticker}")
    click.echo(f"Expirations: {len(data.expirations)}\n")

    # P/C ratio
    total_call_vol = 0
    total_put_vol = 0
    for exp in data.expirations:
        total_call_vol += sum(c.volume for c in exp.calls)
        total_put_vol += sum(c.volume for c in exp.puts)
    total_vol = total_call_vol + total_put_vol
    pc_ratio = (total_put_vol / total_call_vol) if total_call_vol > 0 else float("inf")
    click.echo(f"Put/Call Ratio: {pc_ratio:.2f}  (Call vol: {total_call_vol:,}  Put vol: {total_put_vol:,}  Total: {total_vol:,})")

    # IV term structure
    click.echo("\nIV Term Structure:")
    buckets: dict[str, list[float]] = {"<30 DTE": [], "30-60 DTE": [], ">60 DTE": []}
    for exp in data.expirations:
        for c in exp.calls + exp.puts:
            if c.impliedVolatility <= 0 or c.volume < 5:
                continue
            if c.daysBeforeExpiration < 30:
                buckets["<30 DTE"].append(c.impliedVolatility)
            elif c.daysBeforeExpiration <= 60:
                buckets["30-60 DTE"].append(c.impliedVolatility)
            else:
                buckets[">60 DTE"].append(c.impliedVolatility)
    for label, ivs in buckets.items():
        if ivs:
            normed = [_normalize_iv(v) for v in ivs]
            avg_iv = sum(normed) / len(normed)
            click.echo(f"  {label}: avg IV = {avg_iv:.1%}  ({len(ivs)} contracts)")
        else:
            click.echo(f"  {label}: no data")

    # Unusual contracts per expiration
    click.echo("\nUnusual Contracts:")
    found_any = False
    for exp in data.expirations:
        unusual = []
        for c in exp.calls + exp.puts:
            if c.volume > 50 and c.openInterest > 0 and c.vol_oi_ratio > 2.0:
                unusual.append(c)
        if not unusual:
            continue
        found_any = True
        click.echo(f"\n  Exp: {exp.expirationDate}")
        unusual.sort(key=lambda c: c.notional, reverse=True)
        for c in unusual[:10]:
            flag = click.style("*", fg="yellow") if c.vol_oi_ratio >= 3.0 else " "
            iv_display = _normalize_iv(c.impliedVolatility)
            click.echo(
                f"  {flag} ${c.strike:<8} {c.type:<4} "
                f"vol={c.volume:<8} OI={c.openInterest:<8} "
                f"ratio={c.vol_oi_ratio:.1f}x  IV={iv_display:.1%}  "
                f"notional={_fmt_notional(c.notional)}"
            )
    if not found_any:
        click.echo("  No unusual contracts found.")

    # Alerts with interpretation
    if alerts:
        click.echo(f"\nAlerts ({len(alerts)}):")
        for a in alerts:
            color = _severity_color(a.score)
            click.echo(click.style(
                f"  [{a.score}] {a.alert_type.value}: {a.detail}",
                fg=color,
            ))
            interp = ALERT_INTERPRETATION.get(a.alert_type)
            if interp:
                click.echo(click.style(f"         ↳ {interp}", dim=True))


def _build_sns_event(alert: OptionsAlert) -> dict[str, Any]:
    """Build an SNS-compatible event dict from an OptionsAlert."""
    return {
        "kind": "custom",
        "rule_id": f"options_{alert.alert_type.value}",
        "ticker": alert.ticker,
        "source": "options_flow",
        "field": alert.alert_type.value,
        "op": ">=",
        "target": 0,
        "actual": alert.score,
        "cooldown_minutes": 0,
        "note": f"{alert.alert_type.value}: {alert.detail}",
        "timestamp": datetime.now(ET).isoformat(),
    }


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


@click.group()
def options():
    """Options flow scanner — detect unusual options activity."""
    pass


@options.command("scan")
@click.argument("tickers", nargs=-1)
@click.option("--min-score", type=int, default=0, help="Minimum smart money score to display.")
@click.option("--notify-sns", is_flag=True, default=False, help="Push alerts via SNS.")
def options_scan(tickers: tuple[str, ...], min_score: int, notify_sns: bool) -> None:
    """Scan universe (or specified tickers) for interesting options flow."""
    api_key = get_eodhd_api_key()
    selected = [t.upper() for t in tickers] if tickers else _load_universe_tickers()
    if not selected:
        raise click.ClickException("No tickers to scan. Add tickers to config/universe.yaml first.")

    click.echo(f"Scanning {len(selected)} tickers for options flow...")

    all_alerts: list[OptionsAlert] = []
    errors = 0

    for i, ticker in enumerate(selected):
        try:
            data = fetch_options_chain(ticker, api_key)
            alerts = scan_ticker(data)
            all_alerts.extend(alerts)
        except requests.HTTPError:
            errors += 1
        except Exception:
            errors += 1

        # Rate limit: ~0.25s between calls
        if i < len(selected) - 1:
            time.sleep(0.25)

    # Filter by min score
    if min_score > 0:
        all_alerts = [a for a in all_alerts if a.score >= min_score]

    # Sort by score descending, then notional descending
    all_alerts.sort(key=lambda a: (a.score, a.notional), reverse=True)

    _print_scan_table(all_alerts, len(selected))

    if errors:
        click.echo(f"\n({errors} tickers failed to fetch)")

    # SNS notification
    if notify_sns and all_alerts:
        for alert in all_alerts:
            event = _build_sns_event(alert)
            send_cli_alert(event)
        click.echo(f"Sent {len(all_alerts)} alerts via SNS.")


@options.command("detail")
@click.argument("ticker")
def options_detail(ticker: str) -> None:
    """Deep dive on a single ticker's options chain."""
    api_key = get_eodhd_api_key()
    ticker = ticker.upper()

    click.echo(f"Fetching options chain for {ticker}...")
    data = fetch_options_chain(ticker, api_key)

    if not data.expirations:
        click.echo(f"No options data found for {ticker}.")
        return

    alerts = scan_ticker(data)
    _print_detail(data, alerts)
