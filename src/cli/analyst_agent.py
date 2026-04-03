"""Reactive analyst agent — responds to price alerts with Claude CLI.

Event-driven agent that:
1. Polls S3 for new price alerts on portfolio/watchlist tickers
2. Triages with haiku (cheap — "is this worth investigating?")
3. Investigates with sonnet (why did it move, does it matter?)
4. If sonnet finds something interesting → queues opus deep research via GitHub Issues
5. Emails urgent findings immediately, compiles daily digest

Three-tier escalation: haiku triage → sonnet investigation → opus research (queued)

Lowest-priority daemon. Zero API cost (Claude Max subscription only).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
import uuid
from datetime import date, datetime, timedelta
from enum import Enum
from pathlib import Path
from zoneinfo import ZoneInfo

import click
import yaml
from pydantic import BaseModel, Field

from cli.config_utils import find_repo_root, get_config_dir, load_yaml
from cli.models import PortfolioConfig, TickerRegistry
from cli.queue_capacity import CapacityTracker
from cli.s3 import download_file, get_s3_client, list_prefix

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

MAX_INVESTIGATIONS_PER_HOUR = 3
DEDUP_WINDOW_HOURS = 2


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class AlertReaction(BaseModel):
    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    ticker: str
    alert_type: str = ""
    severity: str = ""
    alert_details: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(ET))

    # Triage (haiku)
    triage_result: str = ""  # "investigate" or "skip"
    triage_reason: str = ""

    # Investigation (sonnet)
    investigated: bool = False
    finding: str = ""
    actionability: str = ""  # none, monitor, buying_opportunity, thesis_breaker
    urgency: str = ""  # low, medium, high

    # Escalation (opus via queue)
    escalated: bool = False
    github_issue: int | None = None

    # Notification
    emailed: bool = False


class AnalystState(BaseModel):
    last_poll_at: datetime | None = None
    current_date: str = ""
    investigations_today: int = 0
    reactions: list[AlertReaction] = Field(default_factory=list)
    processed_alert_keys: list[str] = Field(default_factory=list)  # dedup
    digest_sent_today: bool = False
    digest_findings: list[dict] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def _state_path() -> Path:
    return find_repo_root() / "data" / "analyst_state.yaml"


def _load_state() -> AnalystState:
    path = _state_path()
    if path.exists():
        try:
            raw = yaml.safe_load(path.read_text()) or {}
            return AnalystState(**raw)
        except Exception:
            return AnalystState()
    return AnalystState()


def _save_state(state: AnalystState) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    data = state.model_dump(mode="json")
    # Keep last 500 reactions and 200 processed keys
    if len(data.get("reactions", [])) > 500:
        data["reactions"] = data["reactions"][-500:]
    if len(data.get("processed_alert_keys", [])) > 200:
        data["processed_alert_keys"] = data["processed_alert_keys"][-200:]
    path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def _load_portfolio() -> PortfolioConfig:
    config_dir = get_config_dir()
    path = config_dir / "portfolio.yaml"
    if path.exists():
        return PortfolioConfig(**load_yaml(path))
    return PortfolioConfig()


def _find_claude() -> str:
    found = shutil.which("claude")
    if found:
        return found
    local = Path.home() / ".local" / "bin" / "claude"
    if local.exists():
        return str(local)
    raise FileNotFoundError("Claude CLI not found")


def _cli_env() -> dict:
    env = os.environ.copy()
    env.pop("ANTHROPIC_API_KEY", None)
    env.pop("CLAUDE_API_KEY", None)
    env.pop("CLAUDE_CODE_ENTRYPOINT", None)
    env.pop("CLAUDECODE", None)
    return env


# ---------------------------------------------------------------------------
# Alert polling
# ---------------------------------------------------------------------------


def _poll_new_alerts(
    s3,
    portfolio: PortfolioConfig,
    state: AnalystState,
) -> list[dict]:
    """Poll S3 for new price alerts on portfolio/watchlist tickers.

    Returns list of alert dicts with ticker, alert_type, severity, details.
    Only returns high/critical severity that haven't been processed yet.
    """
    today = date.today().isoformat()
    priority_tickers = {p.ticker.upper() for p in portfolio.positions}
    priority_tickers |= {t.upper() for t in portfolio.watchlist}

    new_alerts = []
    now = datetime.now(ET)

    for ticker in priority_tickers:
        alert_key = f"data/manage/{ticker}/{today}/alerts.yaml"
        try:
            raw = download_file(s3, alert_key)
            alerts_data = yaml.safe_load(raw)
            if not alerts_data:
                continue

            alerts = alerts_data if isinstance(alerts_data, list) else alerts_data.get("alerts", [])

            for alert in alerts:
                severity = alert.get("severity", "low")
                alert_type = alert.get("alert_type", "")

                # Accept high/critical always, medium for price moves and options
                if severity in ("high", "critical"):
                    pass  # Always accept
                elif severity == "medium" and alert_type in ("options_large_bet", "price_breach_up", "price_breach_down", "reversal", "entry_opportunity", "exit_signal", "target_reached"):
                    pass  # Accept actionable medium alerts
                else:
                    continue

                # Build dedup key
                alert_type = alert.get("alert_type", "")
                details_str = json.dumps(alert.get("details", {}), sort_keys=True, default=str)[:100]
                dedup_key = f"{ticker}_{alert_type}_{details_str[:50]}"

                if dedup_key in state.processed_alert_keys:
                    continue

                # Check dedup window — skip if we investigated this ticker recently
                recent_reactions = [
                    r for r in state.reactions
                    if r.ticker == ticker
                    and r.investigated
                    and (now - r.timestamp).total_seconds() < DEDUP_WINDOW_HOURS * 3600
                ]
                if recent_reactions:
                    continue

                new_alerts.append({
                    "ticker": ticker,
                    "alert_type": alert_type,
                    "severity": severity,
                    "details": alert.get("details", {}),
                    "dedup_key": dedup_key,
                    "timestamp": alert.get("timestamp", ""),
                })

        except Exception:
            continue  # No alerts for this ticker today

    # Sort: critical first, then high, then medium
    severity_order = {"critical": 0, "high": 1, "medium": 2}
    new_alerts.sort(key=lambda a: severity_order.get(a["severity"], 3))

    return new_alerts


# ---------------------------------------------------------------------------
# Three-tier escalation
# ---------------------------------------------------------------------------


def _parse_notional(notional_str: str) -> float:
    """Parse '$12.6M' or '$500K' to a float in dollars."""
    s = notional_str.replace("$", "").replace(",", "").strip()
    if s.upper().endswith("M"):
        return float(s[:-1]) * 1_000_000
    if s.upper().endswith("K"):
        return float(s[:-1]) * 1_000
    try:
        return float(s)
    except ValueError:
        return 0


def _parse_strike(contract: str) -> float:
    """Parse strike from contract string like '$340.0C 2026-07-17 vol=3795'."""
    try:
        return float(contract.split("$")[1].split("C")[0].split("P")[0])
    except (IndexError, ValueError):
        return 0


def score_options_flow(ticker: str, details: dict) -> dict:
    """Score an options flow alert with deterministic heuristics.

    Returns a dict with score (0-100), flags, and context for the LLM.
    Higher score = more interesting / more likely institutional signal.
    """
    import re

    contract = details.get("contract", "")
    notional_str = details.get("notional", "$0")
    dte_str = details.get("dte", "0")
    smart_money = details.get("smart_money_score", 0)

    notional = _parse_notional(notional_str)
    dte = int(dte_str) if dte_str else 0
    strike = _parse_strike(contract)

    # Parse call vs put
    is_put = bool(re.search(r"\d+\.?\d*P\s", contract))
    is_call = bool(re.search(r"\d+\.?\d*C\s", contract))
    direction = "BEARISH" if is_put else "BULLISH" if is_call else "UNKNOWN"

    # Parse volume
    vol_match = re.search(r"vol=(\d+)", contract)
    volume = int(vol_match.group(1)) if vol_match else 0

    # Get market cap for relative sizing
    mcap = None
    current_price = None
    try:
        import sys
        repo_root = str(Path(__file__).resolve().parent.parent.parent)
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)
        from src.modules.events.eight_k_scanner.financials import lookup_market_cap
        mcap = lookup_market_cap(ticker)
    except Exception as e:
        logger.debug("Market cap lookup failed for %s: %s", ticker, e)

    # Try to get current price for OTM calculation
    try:
        import requests
        eodhd_key = os.environ.get("EODHD_API_KEY", "")
        if eodhd_key:
            sym = f"{ticker}.US" if "." not in ticker else ticker
            r = requests.get(
                f"https://eodhd.com/api/real-time/{sym}",
                params={"api_token": eodhd_key, "fmt": "json"}, timeout=5,
            )
            current_price = r.json().get("close")
        else:
            # Fallback: yfinance
            import yfinance as yf
            t = yf.Ticker(ticker)
            current_price = t.info.get("currentPrice") or t.info.get("regularMarketPrice")
    except Exception:
        pass

    # --- SCORING ---
    score = 0
    flags = []

    # 1. Notional relative to market cap (most important signal)
    if mcap and mcap > 0:
        notional_pct = notional / mcap * 100
        if notional_pct > 1.0:
            score += 35
            flags.append(f"HUGE relative size: {notional_pct:.2f}% of market cap")
        elif notional_pct > 0.1:
            score += 25
            flags.append(f"Large relative size: {notional_pct:.2f}% of market cap")
        elif notional_pct > 0.01:
            score += 10
            flags.append(f"Moderate relative size: {notional_pct:.3f}% of market cap")
        else:
            flags.append(f"Small relative to mcap: {notional_pct:.4f}%")
    else:
        # No mcap — score by absolute notional
        if notional >= 5_000_000:
            score += 20
        elif notional >= 2_000_000:
            score += 10

    # 2. Smart money score
    if smart_money >= 100:
        score += 20
        flags.append("Smart money: maximum conviction (100)")
    elif smart_money >= 75:
        score += 15
        flags.append("Smart money: high conviction")
    elif smart_money >= 50:
        score += 5
    else:
        flags.append("Low smart money score — likely MM or retail")

    # 3. OTM-ness (how far out of the money)
    if current_price and strike > 0:
        if is_call:
            otm_pct = (strike - current_price) / current_price * 100
        else:
            otm_pct = (current_price - strike) / current_price * 100

        if otm_pct > 30:
            score += 10
            flags.append(f"Far OTM ({otm_pct:.0f}%) — speculative/lottery ticket")
        elif otm_pct > 10:
            score += 5
            flags.append(f"Moderately OTM ({otm_pct:.0f}%)")
        elif otm_pct < -5:
            flags.append(f"Deep ITM ({abs(otm_pct):.0f}%) — likely hedge or stock replacement")
            score -= 5
        else:
            flags.append(f"Near ATM ({otm_pct:.0f}%) — directional with conviction")
            score += 8

    # 4. DTE interpretation
    if dte <= 7:
        score += 5
        flags.append(f"Very short-dated ({dte}d) — event bet, binary outcome")
    elif dte <= 30:
        flags.append(f"Short-dated ({dte}d) — near-term catalyst expected")
    elif dte <= 90:
        score += 3
        flags.append(f"Medium-dated ({dte}d) — positioned for earnings/catalyst")
    else:
        flags.append(f"Long-dated ({dte}d) — institutional timeframe, patient thesis")

    # 5. Volume relative to typical (if we had OI we'd use that)
    if volume > 5000:
        score += 5
        flags.append(f"Very high volume ({volume:,} contracts)")
    elif volume > 1000:
        score += 3

    # 6. MM vs institutional heuristics
    likely_mm = False
    if smart_money < 25 and dte <= 7:
        likely_mm = True
        score -= 10
        flags.append("LIKELY MARKET MAKER: low smart money + short DTE")
    if notional < 500_000 and smart_money < 50:
        likely_mm = True
        score -= 5
        flags.append("Small bet + low conviction — likely retail or hedging")

    # Clamp
    score = max(0, min(100, score))

    # Build interpretation
    if score >= 60:
        verdict = "HIGHLY INTERESTING — likely institutional directional bet"
    elif score >= 40:
        verdict = "WORTH INVESTIGATING — notable flow with some conviction signals"
    elif score >= 20:
        verdict = "MODERATE — could be hedging, MM, or speculative retail"
    else:
        verdict = "LIKELY NOISE — small relative size, low conviction, or MM activity"

    mcap_str = f"${mcap / 1e9:.1f}B" if mcap and mcap >= 1e9 else f"${mcap / 1e6:.0f}M" if mcap else "unknown"
    price_str = f"${current_price:.2f}" if current_price else "unknown"

    context = (
        f"\nOPTIONS FLOW SCORING (deterministic):\n"
        f"  Score: {score}/100 — {verdict}\n"
        f"  Direction: {direction}\n"
        f"  Contract: {contract}\n"
        f"  Notional: {notional_str} | Volume: {volume:,}\n"
        f"  DTE: {dte} days | Smart money: {smart_money}/100\n"
        f"  Stock price: {price_str} | Market cap: {mcap_str}\n"
        f"  Flags:\n" +
        "\n".join(f"    • {f}" for f in flags) + "\n"
    )

    return {
        "score": score,
        "direction": direction,
        "flags": flags,
        "verdict": verdict,
        "context": context,
        "likely_mm": likely_mm,
        "notional": notional,
        "mcap": mcap,
        "notional_pct": (notional / mcap * 100) if mcap else None,
    }


def _haiku_triage(ticker: str, alert_type: str, details: dict) -> tuple[str, str]:
    """Tier 1: haiku triage. Returns (result, reason).

    result is 'investigate' or 'skip'.
    """
    from cli.telemetry import track_claude_call

    details_str = json.dumps(details, default=str)[:300]

    # For options flow: run deterministic scoring first
    if alert_type == "options_large_bet":
        flow_score = score_options_flow(ticker, details)

        # Auto-skip if score < 20 (noise) — don't waste a haiku call
        if flow_score["score"] < 20:
            return "skip", f"Options flow score {flow_score['score']}/100: {flow_score['verdict']}"

        # Auto-investigate if score >= 60 — clear signal, skip haiku
        if flow_score["score"] >= 60:
            return "investigate", f"Options flow score {flow_score['score']}/100: {flow_score['verdict']}"

        options_context = flow_score["context"]
    else:
        options_context = ""

    prompt = (
        f"You are triaging a price/options alert for investment research.\n\n"
        f"Ticker: {ticker}\n"
        f"Alert type: {alert_type}\n"
        f"Details: {details_str}\n"
        f"{options_context}\n"
        f"Should an analyst investigate this further? Consider:\n"
        f"- Is this a significant move or event?\n"
        f"- Could this represent a buying opportunity or thesis risk?\n"
        f"- For options: the scoring above already filtered obvious noise. Focus on whether the signal is actionable.\n\n"
        f"Respond with EXACTLY one line in this format:\n"
        f"INVESTIGATE: <one sentence reason>\n"
        f"or\n"
        f"SKIP: <one sentence reason>"
    )

    cmd = [_find_claude(), "-p", prompt, "--dangerously-skip-permissions", "--model", "haiku"]
    result = track_claude_call(
        cmd, env=_cli_env(), timeout=30,
        daemon="analyst_triage", task_id=f"triage_{ticker}", ticker=ticker,
    )

    response = result.get("result", "").strip()

    if response.upper().startswith("INVESTIGATE"):
        reason = response.split(":", 1)[1].strip() if ":" in response else response
        return "investigate", reason
    return "skip", response.split(":", 1)[1].strip() if ":" in response else response


def _sonnet_investigate(ticker: str, alert_type: str, details: dict) -> dict:
    """Tier 2: sonnet investigation. Returns finding dict."""
    from cli.telemetry import track_claude_call

    # Load event calendar context
    events_context = ""
    try:
        from cli.event_calendar import format_events_for_prompt
        events_context = format_events_for_prompt(ticker)
    except Exception:
        pass

    # Load existing thesis if available
    thesis_context = ""
    memo_path = find_repo_root() / "workspace" / ticker / "memo.yaml"
    if memo_path.exists():
        try:
            memo = yaml.safe_load(memo_path.read_text()) or {}
            thesis = memo.get("thesis_summary", "")
            decision = memo.get("decision", "")
            invalidation = memo.get("valuation", {}).get("invalidation", [])
            thesis_context = (
                f"\nEXISTING THESIS ({decision}): {thesis}\n"
                f"INVALIDATION CONDITIONS:\n" +
                "\n".join(f"  - {inv}" for inv in (invalidation or []))
            )
        except Exception:
            pass

    details_str = json.dumps(details, default=str)[:500]

    if alert_type == "options_large_bet":
        flow_score = score_options_flow(ticker, details)
        options_context = flow_score["context"]
    else:
        options_context = ""

    prompt = (
        f"A price/options alert fired for {ticker}. Investigate what's happening.\n\n"
        f"Alert type: {alert_type}\n"
        f"Details: {details_str}\n"
        f"{options_context}"
        f"{thesis_context}"
        f"{events_context}\n\n"
        f"Research this using web search. Answer:\n"
        f"1. WHY did this move/alert happen? What's the catalyst?\n"
        f"2. Is the move JUSTIFIED by fundamentals, or is it noise?\n"
        f"3. Is this a BUYING OPPORTUNITY, a thesis breaker, or neither?\n"
        f"4. What should the investor DO? (nothing, buy more, trim, close)\n"
        f"{'5. For the options flow: given the scoring above, who is likely behind this bet? Is it institutional, MM hedging, or speculative? What does it imply about near-term direction?' if alert_type == 'options_large_bet' else ''}\n\n"
        f"Respond with ONLY a JSON object:\n"
        f'{{"finding": "<2-3 sentence summary>", '
        f'"actionability": "<none|buying_opportunity|thesis_breaker|monitor>", '
        f'"urgency": "<low|medium|high>", '
        f'"catalyst": "<what caused this>", '
        f'"recommendation": "<what to do>", '
        f'"upcoming_events": ["{{"date": "YYYY-MM-DD", "type": "earnings|clinical_data|fda_decision|conference|regulatory|other", "description": "..."}}"]}}'
    )

    cmd = [_find_claude(), "-p", prompt, "--dangerously-skip-permissions", "--model", "sonnet"]
    result = track_claude_call(
        cmd, env=_cli_env(), timeout=120,
        daemon="analyst_investigate", task_id=f"investigate_{ticker}", ticker=ticker,
    )

    raw = result.get("result", "").strip()
    import re
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    return {
        "finding": raw[:200],
        "actionability": "none",
        "urgency": "low",
        "catalyst": "",
        "recommendation": "",
    }


def _escalate_to_opus(ticker: str, finding: dict, alert_details: str) -> int | None:
    """Tier 3: create a GitHub issue for opus deep research via queue daemon."""
    finding_text = finding.get("finding", "")
    catalyst = finding.get("catalyst", "")
    recommendation = finding.get("recommendation", "")

    body = (
        f"Tickers: ${ticker}\n\n"
        f"**Analyst agent escalation** — price alert triggered sonnet investigation "
        f"that found something worth deeper research.\n\n"
        f"**Alert:** {alert_details}\n\n"
        f"**Sonnet finding:** {finding_text}\n\n"
        f"**Catalyst:** {catalyst}\n\n"
        f"**Recommendation:** {recommendation}\n\n"
        f"Please run full tactical research on this ticker. Produce memo.md and memo.yaml "
        f"with updated thesis, valuation, and tactical setup."
    )

    try:
        result = subprocess.run(
            [
                "gh", "issue", "create",
                "--repo", "avyukd/praxis-copilot",
                "--title", f"[analyst] {ticker}: {finding_text[:60]}",
                "--body", body,
                "--label", "research-queue",
            ],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            # Extract issue number from URL
            url = result.stdout.strip()
            try:
                return int(url.rstrip("/").split("/")[-1])
            except ValueError:
                return None
    except Exception as e:
        logger.error("Failed to create research issue for %s: %s", ticker, e)
    return None


def _queue_weekly_thesis_review() -> None:
    """Queue a weekly thesis review for all portfolio positions via GitHub Issues."""
    portfolio = _load_portfolio()
    tickers = [p.ticker for p in portfolio.positions]

    # Build a list of positions with existing theses
    reviews = []
    repo_root = find_repo_root()
    for ticker in tickers:
        memo_path = repo_root / "workspace" / ticker / "memo.yaml"
        if memo_path.exists():
            try:
                memo = yaml.safe_load(memo_path.read_text()) or {}
                thesis = memo.get("thesis_summary", "")[:100]
                decision = memo.get("decision", "?")
                reviews.append(f"  - {ticker}: {decision} — {thesis}")
            except Exception:
                reviews.append(f"  - {ticker}: (memo exists but unreadable)")
        else:
            reviews.append(f"  - {ticker}: (no memo)")

    body = (
        f"**Weekly Portfolio Thesis Review**\n\n"
        f"Review all portfolio positions and check if anything has changed.\n\n"
        f"Current portfolio ({len(tickers)} positions):\n"
        + "\n".join(reviews) +
        f"\n\nFor each position with an existing memo:\n"
        f"1. Search for any developments in the past week\n"
        f"2. Check if any invalidation conditions have been triggered\n"
        f"3. Update thesis if needed\n"
        f"4. Flag any positions that should be trimmed or closed\n\n"
        f"Produce a summary email with: position, status (hold/trim/close/add), one-line update."
    )

    try:
        subprocess.run(
            [
                "gh", "issue", "create",
                "--repo", "avyukd/praxis-copilot",
                "--title", f"[weekly] Portfolio thesis review — {datetime.now(ET).strftime('%b %d')}",
                "--body", body,
                "--label", "research-queue",
            ],
            capture_output=True, text=True, timeout=15,
        )
        click.echo(f"    Queued weekly thesis review for {len(tickers)} positions")
    except Exception as e:
        logger.error("Failed to queue weekly review: %s", e)


# ---------------------------------------------------------------------------
# Email notifications
# ---------------------------------------------------------------------------


def _send_urgent_email(reaction: AlertReaction, finding: dict) -> None:
    """Send immediate email for urgent findings."""
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not topic_arn:
        click.echo(f"    [NO SNS] URGENT: {reaction.ticker} — {reaction.finding}")
        return

    try:
        import boto3
        sns = boto3.client("sns")

        # Exchange label for non-US tickers
        _exch_map = {".AX": "ASX", ".TO": "TSX", ".V": "TSXV", ".L": "LSE",
                     ".CO": "Copenhagen", ".SW": "SIX", ".HK": "HKEX"}
        exch = next((v for k, v in _exch_map.items() if reaction.ticker.upper().endswith(k)), "")
        exch_note = f" [{exch}]" if exch else ""

        subject = f"[PRAXIS] {reaction.ticker}{exch_note}: {finding.get('catalyst', reaction.alert_type)[:60]}"
        message = (
            f"PRICE ALERT REACTION — {reaction.ticker}{exch_note}\n\n"
            f"Alert: {reaction.alert_type} ({reaction.severity})\n"
            f"Details: {reaction.alert_details[:200]}\n\n"
            f"Finding: {reaction.finding}\n\n"
            f"Actionability: {reaction.actionability}\n"
            f"Recommendation: {finding.get('recommendation', 'N/A')}\n"
            f"Catalyst: {finding.get('catalyst', 'N/A')}\n"
        )
        if reaction.escalated:
            message += f"\nEscalated to deep research: github.com/avyukd/praxis-copilot/issues/{reaction.github_issue}\n"

        sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)
    except Exception as e:
        logger.error("Urgent email failed: %s", e)


def _send_desktop_finding_email(finding) -> None:
    """Send email for an urgent Desktop finding."""
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not topic_arn:
        return
    try:
        import boto3
        sns = boto3.client("sns", region_name="us-east-1")
        ticker_str = f"{finding.ticker}: " if finding.ticker else ""
        subject = f"[PRAXIS DESKTOP] {ticker_str}{finding.content[:60]}"
        body = (
            f"DESKTOP FINDING\n\n"
            f"Ticker: {finding.ticker or 'N/A'}\n"
            f"Actionability: {finding.actionability}\n"
            f"Urgency: {finding.urgency}\n"
            f"Source: {finding.source}\n\n"
            f"{finding.content}"
        )
        sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=body)
    except Exception as e:
        logger.error("Desktop finding email failed: %s", e)


def _compile_and_send_digest(state: AnalystState) -> None:
    """Compile and send daily digest of all alert reactions."""
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not state.digest_findings:
        return

    today_str = datetime.now(ET).strftime("%B %d, %Y")
    sections = []

    # Group by actionability
    urgent = [f for f in state.digest_findings if f.get("urgency") == "high"]
    actionable = [f for f in state.digest_findings if f.get("actionability") not in ("none", "") and f not in urgent]
    routine = [f for f in state.digest_findings if f not in urgent and f not in actionable]

    if urgent:
        lines = [f"  [{f['ticker']}] {f.get('finding', '')[:100]}" for f in urgent]
        sections.append("URGENT ALERTS\n" + "\n".join(lines))
    if actionable:
        lines = [f"  [{f['ticker']}] {f.get('actionability', '')}: {f.get('finding', '')[:80]}" for f in actionable]
        sections.append("ACTIONABLE\n" + "\n".join(lines))
    if routine:
        lines = [f"  [{f['ticker']}] {f.get('finding', '')[:80]}" for f in routine]
        sections.append("MONITORED\n" + "\n".join(lines))

    body = (
        f"PRAXIS ANALYST DIGEST — {today_str}\n\n"
        + "\n\n".join(sections) +
        f"\n\nTotal alerts processed: {len(state.digest_findings)} | "
        f"Investigations: {state.investigations_today}"
    )

    subject = f"[PRAXIS DIGEST] {today_str} — {len(state.digest_findings)} alert(s)"

    if topic_arn:
        try:
            import boto3
            sns = boto3.client("sns")
            sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=body)
            click.echo(f"    Digest sent ({len(state.digest_findings)} findings)")
        except Exception as e:
            logger.error("Digest failed: %s", e)
    else:
        click.echo(f"    [NO SNS] Would send digest with {len(state.digest_findings)} findings")


# ---------------------------------------------------------------------------
# Daemon
# ---------------------------------------------------------------------------


def run_analyst(
    *,
    poll_interval: int = 600,
    max_per_hour: int = MAX_INVESTIGATIONS_PER_HOUR,
    digest_hour_et: int = 21,
    dry_run: bool = False,
) -> None:
    """Main reactive analyst daemon loop."""
    state = _load_state()
    portfolio = _load_portfolio()
    s3 = get_s3_client()
    capacity = CapacityTracker()

    portfolio_tickers = {p.ticker.upper() for p in portfolio.positions}
    watchlist_tickers = {t.upper() for t in portfolio.watchlist}

    click.echo("Reactive analyst agent started")
    click.echo(f"  Portfolio: {len(portfolio.positions)} | Watchlist: {len(portfolio.watchlist)}")
    click.echo(f"  Poll interval: {poll_interval}s | Max investigations/hr: {max_per_hour}")
    click.echo(f"  Digest hour: {digest_hour_et}:00 ET | Dry run: {dry_run}")
    click.echo()

    # Track hourly investigation count
    hour_investigations: list[datetime] = []

    try:
        while True:
            now_et = datetime.now(ET)
            today = now_et.strftime("%Y-%m-%d")

            # Day rollover
            if state.current_date != today:
                state.current_date = today
                state.investigations_today = 0
                state.digest_sent_today = False
                state.digest_findings = []
                state.processed_alert_keys = []

            # No work on weekends (Sat=5, Sun=6) or outside market hours
            if now_et.weekday() >= 5 or now_et.hour < 6 or now_et.hour >= 23:
                time.sleep(600)
                continue

            # Morning briefing at 8 AM ET
            if now_et.hour == 8 and now_et.minute < 10 and not state.processed_alert_keys:
                # First poll of the day around 8 AM — send briefing
                try:
                    from cli.morning_briefing import send_morning_briefing
                    click.echo(f"\n[{now_et.strftime('%H:%M')}] Sending morning briefing...")
                    send_morning_briefing()
                except Exception as e:
                    logger.debug("Morning briefing failed: %s", e)

            # Weekly thesis review — Sundays at digest hour
            if now_et.weekday() == 6 and now_et.hour == digest_hour_et:
                weekly_key = f"weekly_review_{now_et.strftime('%Y-%W')}"
                if weekly_key not in state.processed_alert_keys:
                    click.echo(f"\n[{now_et.strftime('%H:%M')}] Weekly thesis review...")
                    try:
                        _queue_weekly_thesis_review()
                        state.processed_alert_keys.append(weekly_key)
                    except Exception as e:
                        logger.debug("Weekly review failed: %s", e)

            # Daily digest + pre-earnings scan
            if now_et.hour >= digest_hour_et and not state.digest_sent_today:
                click.echo(f"\n[{now_et.strftime('%H:%M')}] Sending daily digest...")
                _compile_and_send_digest(state)
                # Queue pre-earnings research for upcoming earnings
                try:
                    from cli.earnings_research import find_upcoming_earnings, queue_earnings_research
                    upcoming = find_upcoming_earnings(days_ahead=3)
                    if upcoming:
                        click.echo(f"  Queueing {len(upcoming)} pre-earnings research task(s)")
                        queue_earnings_research(upcoming)
                except Exception as e:
                    logger.debug("Pre-earnings scan failed: %s", e)
                state.digest_sent_today = True
                _save_state(state)

            # Capacity check
            if not capacity.should_run():
                time.sleep(120)
                continue

            # Hourly rate limit
            cutoff = now_et - timedelta(hours=1)
            hour_investigations = [t for t in hour_investigations if t > cutoff]
            if len(hour_investigations) >= max_per_hour:
                time.sleep(poll_interval)
                continue

            # Check entry targets (no LLM cost — just price lookups)
            try:
                from cli.entry_watchdog import run_entry_check
                run_entry_check()
            except Exception as e:
                logger.debug("Entry watchdog error: %s", e)

            # Check IPC inbox for Desktop findings
            try:
                from cli.ipc import get_unprocessed_findings, mark_finding_processed
                desktop_findings = get_unprocessed_findings()
                for path, finding in desktop_findings:
                    if finding.urgency == "high" or finding.actionability in ("trade_idea", "research_deeper"):
                        click.echo(f"  DESKTOP FINDING: [{finding.ticker}] {finding.content[:80]}")
                        # Add to digest
                        state.digest_findings.append({
                            "ticker": finding.ticker,
                            "task_type": "desktop",
                            "finding": finding.content[:200],
                            "actionability": finding.actionability,
                            "urgency": finding.urgency,
                        })
                        # Email if urgent
                        if finding.urgency == "high":
                            _send_desktop_finding_email(finding)
                    mark_finding_processed(path)
            except Exception as e:
                logger.debug("IPC inbox check failed: %s", e)

            # Poll for new alerts
            state.last_poll_at = now_et
            new_alerts = _poll_new_alerts(s3, portfolio, state)

            if new_alerts:
                click.echo(
                    f"[{now_et.strftime('%H:%M')}] {len(new_alerts)} new alert(s) "
                    f"({len(hour_investigations)}/{max_per_hour} investigations this hour)"
                )

            for alert in new_alerts:
                # Re-check hourly limit
                if len(hour_investigations) >= max_per_hour:
                    click.echo(f"    Hourly limit reached, deferring remaining alerts")
                    break

                # Re-check capacity
                if not capacity.should_run():
                    click.echo(f"    Capacity exhausted, stopping")
                    break

                ticker = alert["ticker"]
                alert_type = alert["alert_type"]
                severity = alert["severity"]
                details = alert["details"]
                dedup_key = alert["dedup_key"]

                reaction = AlertReaction(
                    ticker=ticker,
                    alert_type=alert_type,
                    severity=severity,
                    alert_details=json.dumps(details, default=str)[:300],
                )

                if dry_run:
                    click.echo(f"  [DRY RUN] {severity} {alert_type}: {ticker} — {json.dumps(details, default=str)[:60]}")
                    state.processed_alert_keys.append(dedup_key)
                    continue

                # Tier 1: Haiku triage
                click.echo(f"  Triaging: {ticker} ({alert_type}, {severity})")
                triage_result, triage_reason = _haiku_triage(ticker, alert_type, details)
                reaction.triage_result = triage_result
                reaction.triage_reason = triage_reason

                if triage_result == "skip":
                    click.echo(f"    Skip: {triage_reason[:80]}")
                    state.processed_alert_keys.append(dedup_key)
                    state.reactions.append(reaction)
                    continue

                # Tier 2: Sonnet investigation
                click.echo(f"    Investigating: {triage_reason[:80]}")
                finding = _sonnet_investigate(ticker, alert_type, details)
                reaction.investigated = True
                reaction.finding = finding.get("finding", "")[:200]
                reaction.actionability = finding.get("actionability", "none")
                reaction.urgency = finding.get("urgency", "low")
                hour_investigations.append(datetime.now(ET))
                state.investigations_today += 1
                capacity.on_success()

                click.echo(f"    Finding: {reaction.finding[:100]}")
                click.echo(f"    Actionability: {reaction.actionability} | Urgency: {reaction.urgency}")

                # Save any events discovered during investigation
                if finding.get("upcoming_events"):
                    try:
                        from cli.event_calendar import add_event
                        for ev in finding["upcoming_events"]:
                            if isinstance(ev, dict) and ev.get("date"):
                                add_event(
                                    ticker=ticker,
                                    event_date=ev["date"],
                                    event_type=ev.get("type", "other"),
                                    description=ev.get("description", ""),
                                    source="analyst_agent",
                                )
                                click.echo(f"    Event saved: {ev['date']} {ev.get('type', '')} — {ev.get('description', '')[:60]}")
                    except Exception:
                        pass

                # Tier 3: Escalate to opus if interesting
                if reaction.actionability in ("buying_opportunity", "thesis_breaker"):
                    click.echo(f"    *** Escalating to deep research ***")
                    issue_num = _escalate_to_opus(
                        ticker, finding, reaction.alert_details
                    )
                    if issue_num:
                        reaction.escalated = True
                        reaction.github_issue = issue_num
                        click.echo(f"    Created issue #{issue_num}")

                # Email if urgent
                if reaction.urgency == "high" or reaction.actionability in ("buying_opportunity", "thesis_breaker"):
                    _send_urgent_email(reaction, finding)
                    reaction.emailed = True
                    click.echo(f"    *** Emailed ***")

                # Add to digest
                state.digest_findings.append({
                    "ticker": ticker,
                    "alert_type": alert_type,
                    "finding": reaction.finding,
                    "actionability": reaction.actionability,
                    "urgency": reaction.urgency,
                })

                state.processed_alert_keys.append(dedup_key)
                state.reactions.append(reaction)

            _save_state(state)
            time.sleep(poll_interval)

    except KeyboardInterrupt:
        click.echo("\nInterrupted. Saving state...")
    finally:
        _save_state(state)
        click.echo(f"State saved. {state.investigations_today} investigations today.")


# ---------------------------------------------------------------------------
# Status display
# ---------------------------------------------------------------------------


def show_status() -> None:
    """Display analyst agent status."""
    state = _load_state()
    if not state.last_poll_at:
        click.echo("Analyst agent has not run yet. Start with: praxis analyst run")
        return

    click.echo(f"Last poll: {state.last_poll_at.strftime('%Y-%m-%d %H:%M ET') if state.last_poll_at else '—'}")
    click.echo(f"Investigations today: {state.investigations_today}")
    click.echo(f"Digest sent: {'Yes' if state.digest_sent_today else 'No'}")
    click.echo(f"Pending digest: {len(state.digest_findings)} findings")
    click.echo(f"Total reactions: {len(state.reactions)}")

    # Recent reactions
    recent = [r for r in state.reactions if r.investigated][-10:]
    if recent:
        click.echo(f"\nRecent investigations:")
        for r in recent:
            esc = " → QUEUED" if r.escalated else ""
            email = " [EMAILED]" if r.emailed else ""
            click.echo(
                f"  [{r.ticker}] {r.alert_type} ({r.severity}): "
                f"{r.finding[:70]}{esc}{email}"
            )


def show_portfolio() -> None:
    """Show portfolio and watchlist."""
    portfolio = _load_portfolio()
    click.echo(f"Portfolio ({len(portfolio.positions)} positions):")
    for pos in portfolio.positions:
        click.echo(f"  {pos.ticker}")
    click.echo(f"\nWatchlist ({len(portfolio.watchlist)}):")
    for t in portfolio.watchlist:
        click.echo(f"  {t}")


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


@click.group("analyst")
def analyst():
    """Reactive analyst agent — responds to price alerts with Claude CLI."""
    pass


@analyst.command("run")
@click.option("--poll-interval", type=int, default=600, show_default=True, help="Seconds between polls")
@click.option("--max-per-hour", type=int, default=3, show_default=True, help="Max investigations per hour")
@click.option("--digest-hour", type=int, default=21, show_default=True, help="Daily digest hour (ET)")
@click.option("--dry-run", is_flag=True, help="Poll alerts but don't investigate")
def analyst_run(poll_interval: int, max_per_hour: int, digest_hour: int, dry_run: bool):
    """Start the reactive analyst agent.

    \b
    Polls for price alerts on portfolio/watchlist tickers.
    Triages with haiku, investigates with sonnet, escalates to
    opus deep research via the queue daemon.

    \b
    Examples:
      praxis analyst run
      praxis analyst run --dry-run
      praxis analyst run --max-per-hour 5
    """
    run_analyst(
        poll_interval=poll_interval,
        max_per_hour=max_per_hour,
        digest_hour_et=digest_hour,
        dry_run=dry_run,
    )


@analyst.command("status")
def analyst_status():
    """Show analyst agent status and recent investigations."""
    show_status()


@analyst.command("portfolio")
def analyst_portfolio():
    """Show portfolio and watchlist."""
    show_portfolio()


@analyst.command("digest")
def analyst_digest():
    """Force-send the daily digest now."""
    state = _load_state()
    _compile_and_send_digest(state)
    state.digest_sent_today = True
    _save_state(state)


@analyst.command("schedule")
def analyst_schedule():
    """Install the launchd plist to run the analyst daemon."""
    import shutil as _shutil

    plist_name = "com.praxis.analyst.plist"
    source = find_repo_root() / plist_name
    if not source.exists():
        click.echo(f"Plist not found at {source}")
        return

    dest_dir = Path.home() / "Library" / "LaunchAgents"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / plist_name

    _shutil.copy2(source, dest)
    click.echo(f"Installed plist to {dest}")

    result = subprocess.run(["launchctl", "load", str(dest)], capture_output=True, text=True)
    if result.returncode == 0:
        click.echo("Loaded into launchd. Analyst agent is now running.")
    else:
        click.echo(f"launchctl load failed: {result.stderr}")


@analyst.command("unschedule")
def analyst_unschedule():
    """Remove the analyst launchd plist."""
    plist_name = "com.praxis.analyst.plist"
    dest = Path.home() / "Library" / "LaunchAgents" / plist_name

    if not dest.exists():
        click.echo("No plist found. Not currently scheduled.")
        return

    subprocess.run(["launchctl", "unload", str(dest)], capture_output=True, text=True)
    dest.unlink()
    click.echo("Unloaded and removed analyst plist.")
