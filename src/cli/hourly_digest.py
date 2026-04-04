"""Hourly system health + activity digest email.

Sent every hour during market hours (9 AM - 4 PM ET).
Serves as both an activity summary and a health ping.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from cli.config_utils import find_repo_root

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


def compile_hourly_digest() -> str:
    """Compile the hourly activity digest."""
    now = datetime.now(ET)
    hour_label = now.strftime("%I:%M %p ET")

    # Load today's filing research state
    repo_root = find_repo_root()
    today = now.strftime("%Y-%m-%d")
    state_path = repo_root / "data" / f"filing_research_state_{today}.yaml"

    filings_summary = ""
    detailed_lines = []
    if state_path.exists():
        try:
            state = yaml.safe_load(state_path.read_text()) or {}
            filings = state.get("filings", {})

            # Counts by decision
            total = len(filings)
            by_decision = {}
            by_classification = {"BUY": 0, "SELL": 0, "NEUTRAL": 0, "HOLD": 0}
            over_threshold = 0
            researched = 0
            research_done = 0
            research_running = 0
            research_queued = 0

            for key, f in filings.items():
                decision = f.get("decision", "")
                by_decision[decision] = by_decision.get(decision, 0) + 1

                classification = (f.get("classification") or "").upper()
                if classification in by_classification:
                    by_classification[classification] += 1

                mag = f.get("magnitude")
                if mag is not None and mag >= 0.5:
                    over_threshold += 1

                if decision == "research_done":
                    research_done += 1
                elif decision == "research_running":
                    research_running += 1
                elif decision == "research_queued":
                    research_queued += 1

                if decision.startswith("research_"):
                    researched += 1

                # Build detailed line
                ticker = f.get("ticker", "?")
                mag_str = f"{mag:.2f}" if mag is not None else "—"
                cls = classification[:4] if classification else "—"
                stage = decision.replace("skip_", "→ ").replace("research_", "⬤ ")
                detailed_lines.append(f"  {ticker:12s} {cls:5s} mag={mag_str:5s} {stage}")

            filings_summary = (
                f"PIPELINE SUMMARY\n"
                f"  Filings/PRs discovered: {total}\n"
                f"  Classifications: {by_classification['BUY']} BUY / {by_classification['SELL']} SELL / {by_classification['NEUTRAL']} NEUTRAL\n"
                f"  Over 0.5 magnitude: {over_threshold} → emailed as alerts\n"
                f"  Sent to research: {researched} (done={research_done}, running={research_running}, queued={research_queued})\n"
                f"  Skipped: {total - researched} (low mag, SELL/HOLD, not analyzed)\n"
            )
        except Exception:
            filings_summary = "PIPELINE SUMMARY\n  Could not load state.\n"
    else:
        filings_summary = "PIPELINE SUMMARY\n  No filing research state for today.\n"

    # Source breakdown from scanner log
    source_summary = _get_source_breakdown()

    # Capacity
    capacity_line = ""
    try:
        from cli.telemetry import get_capacity_estimate, get_usage_report
        cap = get_capacity_estimate()
        report = get_usage_report()
        capacity_line = (
            f"\nCAPACITY\n"
            f"  Window: {cap.get('estimated_pct', 0)}% used | "
            f"Remaining: {cap.get('remaining_budget_tokens', 0):,} tok\n"
            f"  Today: {report.get('total_invocations', 0)} calls, "
            f"{report.get('total_output_tokens', 0):,} output tok, "
            f"${report.get('total_cost_usd', 0):.2f} equiv\n"
            f"  Rate limits: {report.get('rate_limit_hits', 0)}\n"
        )
    except Exception:
        pass

    # Daemon health
    health = _check_health()

    # Detailed ticker list
    detail_section = ""
    if detailed_lines:
        # Sort: research_done first, then by magnitude desc
        detailed_lines.sort(key=lambda l: ("⬤" not in l, l))
        detail_section = f"\n{'─'*50}\nDETAILED VIEW ({len(detailed_lines)} items)\n{'─'*50}\n" + "\n".join(detailed_lines[:50])
        if len(detailed_lines) > 50:
            detail_section += f"\n  ... and {len(detailed_lines) - 50} more"

    body = (
        f"PRAXIS HOURLY DIGEST — {hour_label}\n"
        f"{'='*50}\n\n"
        f"{filings_summary}\n"
        f"{source_summary}\n"
        f"{capacity_line}\n"
        f"{health}\n"
        f"{detail_section}"
    )

    return body


def _get_source_breakdown() -> str:
    """Get source breakdown from today's scanner log."""
    repo_root = find_repo_root()
    log_path = repo_root / "logs" / "scanner.log"
    if not log_path.exists():
        return ""

    try:
        content = log_path.read_text()
        edgar_count = content.count("EDGAR:")
        gnw_count = content.count("GNW:")
        newsfile_count = content.count("Newsfile:")

        # Count alerts sent
        alerted = content.count("Alerted:")

        return (
            f"SOURCES (today's scan cycles)\n"
            f"  EDGAR polls: {edgar_count} | GNW polls: {gnw_count} | Newsfile polls: {newsfile_count}\n"
            f"  Alerts emailed: {alerted}\n"
        )
    except Exception:
        return ""


def _check_health() -> str:
    """Quick daemon health check."""
    import subprocess
    try:
        result = subprocess.run(
            ["bash", "-c", "ps aux | grep '[p]raxis' | grep -v mcp"],
            capture_output=True, text=True, timeout=5,
        )
        lines = result.stdout.strip().splitlines()
    except Exception:
        return "HEALTH\n  ⚠ Could not check"

    daemons = {"scanner": False, "filing-research": False, "analyst": False, "queue": False}
    for line in lines:
        for name in daemons:
            if name in line:
                daemons[name] = True

    all_ok = all(daemons.values())
    status_lines = []
    for name, running in daemons.items():
        status = "✓" if running else "✗ DOWN"
        status_lines.append(f"  {name}: {status}")

    header = "HEALTH — all OK" if all_ok else "HEALTH — ⚠ ISSUES"
    return header + "\n" + " | ".join(f"{n}: {'✓' if r else '✗'}" for n, r in daemons.items())


def send_hourly_digest() -> None:
    """Compile and send the hourly digest email."""
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    body = compile_hourly_digest()

    now = datetime.now(ET)
    subject = f"[PRAXIS HOURLY] {now.strftime('%I %p ET')} digest"

    if topic_arn:
        try:
            import boto3
            sns = boto3.client("sns", region_name="us-east-1")
            sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=body)
            logger.info("Hourly digest sent")
        except Exception as e:
            logger.error("Hourly digest failed: %s", e)
    else:
        logger.debug("No SNS_TOPIC_ARN, skipping hourly digest")
