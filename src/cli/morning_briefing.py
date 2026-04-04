"""Morning briefing email — one email before market open.

Compiles overnight activity into a concise 30-second scan:
- Entry/exit triggers that fired
- New BUY memos from filing research
- Upcoming earnings and events
- Analyst findings from overnight
- Daemon health status

Sent at 8:00 AM ET daily (configurable).
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import click
import yaml

from cli.config_utils import find_repo_root, get_config_dir, load_yaml
from cli.models import PortfolioConfig

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


def _load_portfolio() -> PortfolioConfig:
    config_dir = get_config_dir()
    path = config_dir / "portfolio.yaml"
    if path.exists():
        return PortfolioConfig(**load_yaml(path))
    return PortfolioConfig()


def compile_briefing() -> str:
    """Compile the morning briefing content."""
    today = date.today().isoformat()
    today_label = datetime.now(ET).strftime("%A, %B %d, %Y")
    sections = []

    # 1. Entry/exit triggers
    entry_section = _get_entry_exit_summary()
    if entry_section:
        sections.append(entry_section)

    # 2. New BUY memos from filing research
    buy_section = _get_new_buy_memos()
    if buy_section:
        sections.append(buy_section)

    # 3. Upcoming earnings and events
    events_section = _get_upcoming_events()
    if events_section:
        sections.append(events_section)

    # 4. Analyst findings
    analyst_section = _get_analyst_findings()
    if analyst_section:
        sections.append(analyst_section)

    # 5. Daemon health
    health_section = _get_daemon_health()
    sections.append(health_section)

    # 6. Capacity summary
    capacity_section = _get_capacity_summary()
    if capacity_section:
        sections.append(capacity_section)

    if not any(s for s in sections if "ENTRY" in s or "BUY" in s or "EARNINGS" in s):
        summary_line = "Quiet overnight. No actionable triggers."
    else:
        counts = []
        if entry_section:
            n = entry_section.count("•")
            counts.append(f"{n} entry/exit trigger(s)")
        if buy_section:
            n = buy_section.count("•")
            counts.append(f"{n} new BUY memo(s)")
        if events_section and "EARNINGS" in events_section:
            counts.append("earnings upcoming")
        summary_line = " | ".join(counts)

    body = (
        f"PRAXIS MORNING BRIEFING — {today_label}\n"
        f"{summary_line}\n"
        f"{'=' * 50}\n\n"
        + "\n\n".join(sections)
    )

    return body


def _get_entry_exit_summary() -> str:
    """Get entry/exit triggers from the watchdog state."""
    state_path = find_repo_root() / "data" / "entry_watchdog_state.yaml"
    if not state_path.exists():
        return ""

    try:
        state = yaml.safe_load(state_path.read_text()) or {}
        alerted = state.get("alerted", {})
    except Exception:
        return ""

    today = date.today().isoformat()
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    recent = []
    for ticker, info in alerted.items():
        alert_date = info.get("date", "")
        if alert_date in (today, yesterday):
            zone = info.get("zone", "")
            price = info.get("price", 0)
            recent.append(f"  • {ticker} at ${price:.2f} — {zone}")

    if not recent:
        return ""

    return "ENTRY TRIGGERS\n" + "\n".join(recent[:10])


def _get_new_buy_memos() -> str:
    """Get BUY memos generated in the last 24h."""
    repo_root = find_repo_root()

    # Check filing research state for today and yesterday
    buys = []
    for day_offset in [0, 1]:
        d = (date.today() - timedelta(days=day_offset)).isoformat()
        state_path = repo_root / "data" / f"filing_research_state_{d}.yaml"
        if not state_path.exists():
            continue
        try:
            state = yaml.safe_load(state_path.read_text()) or {}
            for key, f in state.get("filings", {}).items():
                if f.get("decision") != "research_done":
                    continue
                ticker = f.get("ticker", "")
                try:
                    from cli.memo_reader import read_memo_yaml
                    memo = read_memo_yaml(ticker)
                    if memo:
                        decision = (memo.get("decision") or "").upper().strip()
                        if decision in ("BUY", "SPECULATIVE_BUY", "SPECULATIVE BUY"):
                            thesis = memo.get("thesis_summary", "")[:100]
                            scores = memo.get("scores", {}) or {}
                            buys.append(f"  • {ticker} (T:{scores.get('tactical','?')} F:{scores.get('fundamental','?')}): {thesis}")
                except Exception:
                    pass
        except Exception:
            pass

    if not buys:
        return ""

    return "NEW BUY MEMOS\n" + "\n".join(buys[:10])


def _get_upcoming_events() -> str:
    """Get events in the next 5 days."""
    try:
        from cli.event_calendar import get_upcoming_events
        events = get_upcoming_events(5)
    except Exception:
        return ""

    if not events:
        return ""

    lines = []
    for e in events:
        try:
            days_until = (date.fromisoformat(e.date) - date.today()).days
            timing = "TODAY" if days_until == 0 else f"in {days_until}d"
        except ValueError:
            timing = "?"
        lines.append(f"  • {e.date} ({timing}) [{e.ticker}] {e.type}: {e.description}")

    return "UPCOMING EVENTS\n" + "\n".join(lines)


def _get_analyst_findings() -> str:
    """Get recent analyst findings from overnight."""
    state_path = find_repo_root() / "data" / "analyst_state.yaml"
    if not state_path.exists():
        return ""

    try:
        state = yaml.safe_load(state_path.read_text()) or {}
        reactions = state.get("reactions", [])
    except Exception:
        return ""

    # Get findings from last 24h
    cutoff = (datetime.now(ET) - timedelta(hours=24)).isoformat()
    recent = []
    for r in reactions:
        if not r.get("investigated"):
            continue
        ts = r.get("timestamp", "")
        if isinstance(ts, str) and ts >= cutoff:
            ticker = r.get("ticker", "")
            finding = r.get("finding", "")[:80]
            actionability = r.get("actionability", "")
            if actionability not in ("none", ""):
                recent.append(f"  • [{ticker}] {actionability}: {finding}")

    if not recent:
        return ""

    return "ANALYST FINDINGS (24h)\n" + "\n".join(recent[:10])


def _get_daemon_health() -> str:
    """Check if all daemons are running."""
    import subprocess

    try:
        result = subprocess.run(
            ["bash", "-c", "ps aux | grep '[p]raxis' | grep -v grep"],
            capture_output=True, text=True, timeout=5,
        )
        lines = result.stdout.strip().splitlines()
    except Exception:
        return "SYSTEM HEALTH\n  ⚠ Could not check daemon status"

    daemons = {
        "scanner": False,
        "filing-research": False,
        "analyst": False,
        "queue": False,
    }

    for line in lines:
        for name in daemons:
            if name in line:
                daemons[name] = True

    health_lines = []
    all_ok = True
    for name, running in daemons.items():
        status = "✓ running" if running else "✗ DOWN"
        if not running:
            all_ok = False
        health_lines.append(f"  {name:<20} {status}")

    header = "SYSTEM HEALTH — all OK" if all_ok else "SYSTEM HEALTH — ⚠ ISSUES"
    return header + "\n" + "\n".join(health_lines)


def _get_capacity_summary() -> str:
    """Get capacity usage summary."""
    try:
        from cli.telemetry import get_capacity_estimate
        cap = get_capacity_estimate()
        pct = cap.get("estimated_pct", 0)
        return (
            f"CAPACITY\n"
            f"  Window usage: {pct}% | "
            f"Remaining: {cap.get('remaining_budget_tokens', 0):,} tok"
        )
    except Exception:
        return ""


def send_morning_briefing() -> None:
    """Compile and send the morning briefing email."""
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    body = compile_briefing()

    today_label = datetime.now(ET).strftime("%b %d")
    subject = f"[PRAXIS BRIEFING] {today_label}"

    click.echo(body)

    if topic_arn:
        try:
            import boto3
            sns = boto3.client("sns", region_name="us-east-1")
            sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=body)
            click.echo(f"\nBriefing sent via email.")
        except Exception as e:
            logger.error("Briefing email failed: %s", e)
    else:
        click.echo("\n[NO SNS] Would send briefing email")
