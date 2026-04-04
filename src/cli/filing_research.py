"""Automated filing research daemon and status viewer.

Polls S3 for items analyzed by the local CLI scanner (analysis.json).
Filters by magnitude and classification, then orchestrates full opus
research sessions via the coordinator pipeline.

Note: Fully Lambda-independent. The scanner writes analysis.json to S3,
this daemon reads it. No dependency on Lambda pipeline stages.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from enum import Enum
from pathlib import Path
from zoneinfo import ZoneInfo

import click
import yaml
from pydantic import BaseModel, Field

from cli.config_utils import find_repo_root, get_config_dir, load_yaml
from cli.models import TickerRegistry
from cli.pipeline_status import (
    collect_pipeline_items,
    parse_day_window,
)
from cli.research_run import (
    build_run_prompt,
    fetch_tactical_context,
    launch_sessions,
    write_prompt_file,
)
from cli.s3 import download_file, get_s3_client, list_prefix
from cli.staging import stage_ticker, sync_research

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


def _parse_time(value: str) -> float:
    """Parse 'H:MM' or 'HH:MM' to fractional hours. Plain numbers pass through."""
    if ":" in value:
        h, m = value.split(":", 1)
        return int(h) + int(m) / 60.0
    return float(value)


def _format_time(fractional_hour: float) -> str:
    """Format fractional hours as 'H:MM'."""
    h = int(fractional_hour)
    m = int((fractional_hour - h) * 60)
    return f"{h}:{m:02d}"

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class FilingDecision(str, Enum):
    PENDING = "pending"
    SKIP_HOLD = "skip_hold"
    SKIP_SELL = "skip_sell"
    SKIP_LOW_MAG = "skip_low_mag"
    SKIP_SCREENED = "skip_screened"
    SKIP_NO_TICKER = "skip_no_ticker"
    SKIP_NOT_ANALYZED = "skip_not_analyzed"
    SKIP_DUPLICATE = "skip_duplicate"
    RESEARCH_QUEUED = "research_queued"
    RESEARCH_STAGING = "research_staging"
    RESEARCH_RUNNING = "research_running"
    RESEARCH_SYNCING = "research_syncing"
    RESEARCH_DONE = "research_done"
    RESEARCH_FAILED = "research_failed"


SKIP_DECISIONS = {
    FilingDecision.SKIP_HOLD,
    FilingDecision.SKIP_SELL,
    FilingDecision.SKIP_LOW_MAG,
    FilingDecision.SKIP_SCREENED,
    FilingDecision.SKIP_NO_TICKER,
    FilingDecision.SKIP_NOT_ANALYZED,
    FilingDecision.SKIP_DUPLICATE,
}

RESEARCH_DECISIONS = {
    FilingDecision.RESEARCH_QUEUED,
    FilingDecision.RESEARCH_STAGING,
    FilingDecision.RESEARCH_RUNNING,
    FilingDecision.RESEARCH_SYNCING,
    FilingDecision.RESEARCH_DONE,
    FilingDecision.RESEARCH_FAILED,
}


class TrackedFiling(BaseModel):
    key_prefix: str
    item_id: str
    source_type: str
    ticker: str
    cik: str
    form_type: str
    classification: str = ""
    magnitude: float | None = None
    summary: str = ""
    discovered_at: datetime
    decision: FilingDecision = FilingDecision.PENDING
    decision_reason: str = ""
    research_started_at: datetime | None = None
    research_finished_at: datetime | None = None
    research_session_id: str | None = None
    research_success: bool | None = None
    artifacts_found: list[str] = Field(default_factory=list)


class FilingResearchState(BaseModel):
    run_date: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_poll_at: datetime | None = None
    poll_count: int = 0
    daemon_pid: int | None = None
    filings: dict[str, TrackedFiling] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def _state_path(run_date: str) -> Path:
    repo_root = find_repo_root()
    return repo_root / "data" / f"filing_research_state_{run_date}.yaml"


def _load_state(run_date: str) -> FilingResearchState:
    path = _state_path(run_date)
    if path.exists():
        raw = yaml.safe_load(path.read_text()) or {}
        return FilingResearchState(**raw)
    return FilingResearchState(run_date=run_date)


def _save_state(state: FilingResearchState) -> None:
    path = _state_path(state.run_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(state.model_dump(mode="json"), default_flow_style=False, sort_keys=False))


# ---------------------------------------------------------------------------
# Evaluation logic
# ---------------------------------------------------------------------------


def _evaluate_filing(
    s3,
    item,
    magnitude_threshold: float,
    researched_tickers: set[str],
) -> TrackedFiling:
    """Evaluate a single pipeline item and decide whether to research it.

    Lambda-independent: reads analysis.json directly from S3 (written by CLI
    scanner). Does not rely on Lambda pipeline stages or Haiku prescreen.
    """
    now = datetime.now(ET)

    tracked = TrackedFiling(
        key_prefix=item.key_prefix,
        item_id=item.item_id,
        source_type=item.source_type,
        ticker=item.ticker or "",
        cik=item.cik or "",
        form_type=item.form_type or "",
        discovered_at=now,
    )

    # Must have a ticker
    ticker = (item.ticker or "").upper()
    if not ticker:
        tracked.decision = FilingDecision.SKIP_NO_TICKER
        tracked.decision_reason = "no ticker on filing"
        return tracked

    # Read analysis.json directly — written by CLI scanner or Lambda
    try:
        analysis_raw = download_file(s3, f"{item.key_prefix}/analysis.json")
        analysis = json.loads(analysis_raw)
    except Exception:
        tracked.decision = FilingDecision.SKIP_NOT_ANALYZED
        tracked.decision_reason = "no analysis.json found"
        return tracked

    tracked.classification = analysis.get("classification", "")
    tracked.magnitude = analysis.get("magnitude")
    tracked.summary = analysis.get("new_information", "") or analysis.get("explanation", "")

    classification = tracked.classification.upper()

    if classification == "SELL":
        tracked.decision = FilingDecision.SKIP_SELL
        tracked.decision_reason = "SELL classification"
        return tracked

    if classification == "HOLD":
        tracked.decision = FilingDecision.SKIP_HOLD
        tracked.decision_reason = "HOLD classification"
        return tracked

    if tracked.magnitude is not None and tracked.magnitude < magnitude_threshold:
        tracked.decision = FilingDecision.SKIP_LOW_MAG
        tracked.decision_reason = f"magnitude {tracked.magnitude:.2f} < {magnitude_threshold}"
        return tracked

    # Duplicate ticker check
    if ticker in researched_tickers:
        tracked.decision = FilingDecision.SKIP_DUPLICATE
        tracked.decision_reason = f"already researching/researched {ticker} today"
        return tracked

    tracked.decision = FilingDecision.RESEARCH_QUEUED
    return tracked


# ---------------------------------------------------------------------------
# Research job
# ---------------------------------------------------------------------------


def _run_research_job(
    ticker: str,
    key_prefix: str,
    cik: str,
) -> tuple[bool, list[str], str]:
    """Run the full stage -> research -> sync pipeline for a ticker.

    Returns (success, artifacts_found, session_id).
    This runs in a worker thread.
    """
    config_dir = get_config_dir()
    registry_cfg = TickerRegistry(**load_yaml(config_dir / "ticker_registry.yaml"))
    s3 = get_s3_client()

    # Resolve CIK if missing (common for press release alerts)
    if not cik:
        try:
            from cli.edgar import resolve_ticker
            info = resolve_ticker(ticker)
            if info:
                cik = info.cik
                logger.info("Resolved CIK for %s: %s", ticker, cik)
        except Exception:
            logger.debug("CIK resolution failed for %s", ticker, exc_info=True)

    # Fetch macro files
    macro_keys = list_prefix(s3, "data/context/macro/")
    macro_files = [k for k in macro_keys if k != "data/context/macro/"]

    # Stage
    workspace = stage_ticker(ticker, config_dir, registry_cfg, s3, macro_files, quiet=True, cik=cik, tactical=True)
    if workspace is None:
        # Staging failed (no CIK / no data) — create minimal workspace with tactical context only
        from cli.config_utils import find_repo_root

        logger.info("Staging failed for %s, creating minimal workspace for web-only research", ticker)
        repo_root = find_repo_root()
        workspace = repo_root / "workspace" / ticker
        workspace.mkdir(parents=True, exist_ok=True)

        # Detect exchange context from ticker format or source
        exchange_hint = ""
        is_newsfile = "newsfile" in key_prefix
        if ".V" in ticker.upper():
            exchange_hint = f"{ticker} trades on the TSX Venture Exchange (TSXV). This is a Canadian small-cap exchange. Use Canadian sources (SEDAR+, TMX Money, etc.) for filings and financials.\n"
        elif ".TO" in ticker.upper():
            exchange_hint = f"{ticker} trades on the Toronto Stock Exchange (TSX). This is a Canadian exchange. Use Canadian sources (SEDAR+, TMX Money, etc.) for filings and financials.\n"
        elif ".CO" in ticker.upper():
            exchange_hint = f"{ticker} trades on the Copenhagen Stock Exchange (Nasdaq Copenhagen). Use European sources for filings and financials.\n"
        elif ".L" in ticker.upper():
            exchange_hint = f"{ticker} trades on the London Stock Exchange. Use UK sources (Companies House, LSE) for filings and financials.\n"
        elif is_newsfile:
            exchange_hint = f"{ticker} was sourced from a Canadian newswire (Newsfile). This ticker likely trades on the TSX or TSXV. Use Canadian sources (SEDAR+, TMX Money, etc.) for filings and financials.\n"

        claude_md = (
            f"# Research Task: {ticker}\n\n"
            f"{exchange_hint}"
            f"No SEC filings or fundamental data were pre-ingested for this ticker.\n"
            f"Use web search (WebSearch, WebFetch) to research this company thoroughly.\n"
            f"Find recent news, financials, press releases, and any available filings.\n"
            f"Produce memo.md and memo.yaml per standard format.\n"
            f"Include scores.tactical and scores.fundamental in memo.yaml.\n"
        )
        (workspace / "CLAUDE.md").write_text(claude_md)

    # Build tactical prompt
    tactical_context = fetch_tactical_context(ticker)
    prompt = build_run_prompt(ticker, tactical=True, tactical_context=tactical_context)
    write_prompt_file(workspace, prompt)

    session_id = str(uuid.uuid4())
    sessions = [(ticker, workspace, prompt)]
    session_map = {ticker: session_id}

    results, session_map = launch_sessions(
        sessions,
        max_parallel=1,
        max_retries=2,
        session_map=session_map,
    )

    if not results:
        return False, [], session_id

    _, sid, success, output, found, missing = results[0]
    artifacts = sorted(found)

    # Sync results back to S3 (don't clean up workspace so user can read memos)
    if found:
        sync_research(ticker, s3, quiet=True, cleanup=False)

    return success, artifacts, sid


def _maybe_extract_watches(ticker: str) -> None:
    """Auto-extract thesis watches from a completed memo."""
    repo_root = find_repo_root()
    memo_path = repo_root / "workspace" / ticker / "memo.yaml"
    if not memo_path.exists():
        return
    try:
        memo = yaml.safe_load(memo_path.read_text()) or {}
        from cli.thesis_monitors import extract_watches_from_memo, load_watches, save_watches, notify_new_watches
        new_watches = extract_watches_from_memo(ticker, memo)
        if new_watches:
            existing = load_watches()
            # Remove old watches for this ticker
            existing = [w for w in existing if w.ticker.upper() != ticker.upper()]
            existing.extend(new_watches)
            save_watches(existing)
            notify_new_watches(ticker, new_watches)
            logger.info("Extracted %d thesis watches for %s", len(new_watches), ticker)
    except Exception as e:
        logger.debug("Watch extraction failed for %s: %s", ticker, e)


def _maybe_email_memo(ticker: str) -> None:
    """Email the user if a completed research memo has a BUY decision."""
    repo_root = find_repo_root()
    memo_path = repo_root / "workspace" / ticker / "memo.yaml"
    if not memo_path.exists():
        return

    try:
        memo = yaml.safe_load(memo_path.read_text()) or {}
    except Exception:
        return

    decision = (memo.get("decision") or "").upper().strip()
    if decision not in ("BUY", "SPECULATIVE_BUY", "SPECULATIVE BUY"):
        return

    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not topic_arn:
        return

    thesis = memo.get("thesis_summary", "")
    scores = memo.get("scores", {}) or {}
    tac = scores.get("tactical", "?")
    fun = scores.get("fundamental", "?")
    valuation = memo.get("valuation", {}) or {}
    fv = valuation.get("fair_value_estimate", "N/A")
    entry = valuation.get("entry_range", [None, None])
    tactical = memo.get("tactical", {}) or {}

    _exch_map = {".AX": "ASX", ".TO": "TSX", ".V": "TSXV", ".L": "LSE",
                 ".CO": "Copenhagen", ".SW": "SIX", ".HK": "HKEX"}
    exch = next((v for k, v in _exch_map.items() if ticker.upper().endswith(k)), "")
    exch_note = f" [{exch}]" if exch else ""

    # Market cap lookup
    mcap_str = ""
    try:
        import sys
        repo_root_str = str(repo_root)
        if repo_root_str not in sys.path:
            sys.path.insert(0, repo_root_str)
        from src.modules.events.eight_k_scanner.financials import lookup_market_cap
        mcap = lookup_market_cap(ticker)
        if mcap:
            mcap_str = f"${mcap / 1_000_000_000:.1f}B" if mcap >= 1_000_000_000 else f"${mcap / 1_000_000:.0f}M"
    except Exception:
        pass

    subject = f"[PRAXIS BUY] {ticker}{exch_note}: {thesis[:60]}"

    # Header block
    mcap_line = f" | Mcap: {mcap_str}" if mcap_str else ""
    exit_range = valuation.get("exit_range", [None, None])

    body = (
        f"{'='*50}\n"
        f"BUY MEMO — {ticker}{exch_note}{mcap_line}\n"
        f"{'='*50}\n\n"
        f"Decision: {decision}\n"
        f"Tactical: {tac}/10 | Fundamental: {fun}/10\n"
        f"Fair value: ${fv}\n"
    )
    if entry and entry[0] is not None:
        body += f"Entry: ${entry[0]} – ${entry[1]}\n"
    if exit_range and exit_range[0] is not None:
        body += f"Exit: ${exit_range[0]} – ${exit_range[1]}\n"

    body += f"\n{'─'*50}\nTHESIS\n{'─'*50}\n{thesis}\n"

    if tactical:
        body += f"\n{'─'*50}\nTACTICAL SETUP\n{'─'*50}\n"
        for key in ("setup", "entry_trigger", "risk_reward", "catalyst", "timeframe", "invalidation"):
            if tactical.get(key):
                body += f"  {key.replace('_', ' ').title()}: {tactical[key]}\n"

    key_assumptions = valuation.get("key_assumptions", [])
    invalidation_list = valuation.get("invalidation", [])
    if key_assumptions:
        body += f"\n{'─'*50}\nKEY ASSUMPTIONS\n{'─'*50}\n"
        for a in key_assumptions:
            body += f"  • {a}\n"
    if invalidation_list:
        body += f"\n{'─'*50}\nINVALIDATION\n{'─'*50}\n"
        for inv in invalidation_list:
            body += f"  ⚠ {inv}\n"

    # Include first 2000 chars of memo.md
    memo_md_path = repo_root / "workspace" / ticker / "memo.md"
    if memo_md_path.exists():
        try:
            full_memo = memo_md_path.read_text()
            excerpt = full_memo[:2000]
            body += f"\n{'─'*50}\nMEMO\n{'─'*50}\n{excerpt}"
            if len(full_memo) > 2000:
                body += "\n\n[... truncated — full memo in workspace]"
            body += "\n"
        except Exception:
            pass

    try:
        import boto3
        sns = boto3.client("sns", region_name="us-east-1")
        sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=body)
        logger.info("Emailed BUY memo for %s", ticker)
    except Exception as e:
        logger.error("Failed to email memo for %s: %s", ticker, e)


# ---------------------------------------------------------------------------
# Daemon
# ---------------------------------------------------------------------------


def run_daemon(
    *,
    date_str: str | None = None,
    start_hour: float = 6.0,
    end_hour: float = 22.0,
    poll_interval: int = 600,
    magnitude_threshold: float = 0.5,
    max_parallel: int = 4,
    dry_run: bool = False,
) -> None:
    """Main daemon loop: poll for filings, evaluate, launch research."""
    import os
    from cli.env_loader import load_env
    load_env()

    now_et = datetime.now(ET)
    run_date = date_str or now_et.strftime("%Y-%m-%d")

    state = _load_state(run_date)
    state.started_at = state.started_at or now_et
    state.daemon_pid = os.getpid()
    _save_state(state)

    # Load config
    config_dir = get_config_dir()

    click.echo(f"Filing research daemon started for {run_date}")
    click.echo(f"  Window: {_format_time(start_hour)} - {_format_time(end_hour)} ET")
    click.echo(f"  Poll interval: {poll_interval}s | Magnitude threshold: {magnitude_threshold}")
    click.echo(f"  Max parallel research: {max_parallel} | Dry run: {dry_run}")
    click.echo()

    s3 = get_s3_client()

    # Track which tickers are already being researched or done
    researched_tickers: set[str] = set()
    for filing in state.filings.values():
        if filing.decision in RESEARCH_DECISIONS:
            researched_tickers.add(filing.ticker.upper())

    # Thread pool for research jobs
    executor = ThreadPoolExecutor(max_workers=max_parallel) if not dry_run else None
    pending_futures: dict[str, Future] = {}  # ticker -> future

    try:
        while True:
            now_et = datetime.now(ET)
            current_hour = now_et.hour + now_et.minute / 60.0

            # No work on weekends
            if now_et.weekday() >= 5:
                time.sleep(600)
                continue

            # Check if we're past the window and have no pending research
            past_window = current_hour >= end_hour
            if past_window and not pending_futures:
                click.echo(f"\nWindow closed and all research complete. Shutting down.")
                break

            if past_window:
                click.echo(f"\nWindow closed. Draining {len(pending_futures)} in-progress research job(s)...")

            # Poll for new filings (only if still in window)
            if not past_window:
                state.poll_count += 1
                state.last_poll_at = now_et

                day_start_utc, day_end_utc, _ = parse_day_window(run_date)
                items = collect_pipeline_items(
                    s3,
                    day_start_utc=day_start_utc,
                    day_end_utc=day_end_utc,
                    source="all",
                    stuck_minutes=30,
                )

                new_count = 0
                queued_count = 0
                for item in items:
                    if item.key_prefix in state.filings:
                        # Re-evaluate items that were previously not analyzed
                        existing = state.filings[item.key_prefix]
                        if existing.decision == FilingDecision.SKIP_NOT_ANALYZED:
                            tracked = _evaluate_filing(
                                s3, item, magnitude_threshold, researched_tickers
                            )
                            state.filings[item.key_prefix] = tracked
                            if tracked.decision == FilingDecision.RESEARCH_QUEUED:
                                queued_count += 1
                        continue

                    tracked = _evaluate_filing(
                        s3, item, magnitude_threshold, researched_tickers
                    )
                    state.filings[item.key_prefix] = tracked
                    new_count += 1

                    if tracked.decision == FilingDecision.RESEARCH_QUEUED:
                        queued_count += 1

                if new_count > 0:
                    skip_count = sum(
                        1 for f in state.filings.values() if f.decision in SKIP_DECISIONS
                    )
                    click.echo(
                        f"[{now_et.strftime('%H:%M:%S')}] Poll #{state.poll_count}: "
                        f"{new_count} new item(s), {queued_count} queued for research, "
                        f"{skip_count} total skipped"
                    )

            # Check completed research futures
            completed_tickers = []
            for ticker, future in pending_futures.items():
                if future.done():
                    completed_tickers.append(ticker)
                    try:
                        success, artifacts, session_id = future.result()
                    except Exception as e:
                        success, artifacts, session_id = False, [], ""
                        logger.error(f"Research job for {ticker} raised: {e}")

                    # Update all filings for this ticker
                    for filing in state.filings.values():
                        if filing.ticker.upper() == ticker and filing.decision == FilingDecision.RESEARCH_RUNNING:
                            filing.research_finished_at = datetime.now(ET)
                            filing.research_session_id = session_id
                            filing.research_success = success
                            filing.artifacts_found = artifacts
                            filing.decision = (
                                FilingDecision.RESEARCH_DONE if success
                                else FilingDecision.RESEARCH_FAILED
                            )

                    status = "DONE" if success else "FAILED"
                    click.echo(
                        f"[{datetime.now(ET).strftime('%H:%M:%S')}] Research {status}: "
                        f"{ticker} ({len(artifacts)} artifacts)"
                    )

            for ticker in completed_tickers:
                del pending_futures[ticker]
                # Email promising memos (BUY decisions)
                _maybe_email_memo(ticker)
                # Auto-extract thesis watches from new memo
                # Thesis watch extraction disabled — sunset per user request
                # Code remains in thesis_monitors.py for manual use via `praxis watches refresh`

            # Auto-regenerate HTML report when research completes
            if completed_tickers:
                try:
                    from cli.filing_research_report import generate_and_write_report
                    generate_and_write_report(date_str=run_date, skip_charts=False, open_browser=False, quiet=True)
                except Exception as e:
                    logger.debug("Report regeneration failed: %s", e)

            # Submit new research jobs (only up to max_parallel, respecting capacity)
            if executor and not past_window:
                # Check capacity before submitting new work
                try:
                    from cli.telemetry import get_capacity_estimate
                    cap = get_capacity_estimate()
                    if cap.get("at_target", False):
                        if not pending_futures:  # Only log if we'd otherwise be idle
                            click.echo(f"[{datetime.now(ET).strftime('%H:%M:%S')}] At 80% capacity, waiting...")
                        _save_state(state)
                        time.sleep(60)
                        continue
                except Exception:
                    pass

                for filing in state.filings.values():
                    if len(pending_futures) >= max_parallel:
                        break
                    if filing.decision != FilingDecision.RESEARCH_QUEUED:
                        continue
                    ticker = filing.ticker.upper()
                    if ticker in pending_futures:
                        continue

                    if dry_run:
                        click.echo(
                            f"[DRY RUN] Would research {ticker} "
                            f"(classification={filing.classification}, "
                            f"magnitude={filing.magnitude})"
                        )
                        filing.decision = FilingDecision.SKIP_DUPLICATE
                        filing.decision_reason = "dry run"
                        continue

                    filing.decision = FilingDecision.RESEARCH_RUNNING
                    filing.research_started_at = datetime.now(ET)
                    researched_tickers.add(ticker)

                    click.echo(
                        f"[{datetime.now(ET).strftime('%H:%M:%S')}] Starting research: "
                        f"{ticker} (classification={filing.classification}, "
                        f"magnitude={filing.magnitude})"
                    )

                    future = executor.submit(
                        _run_research_job, ticker, filing.key_prefix, filing.cik
                    )
                    pending_futures[ticker] = future

            _save_state(state)

            # Sleep until next poll (shorter if draining)
            if past_window:
                time.sleep(10)
            else:
                time.sleep(poll_interval)

    except KeyboardInterrupt:
        click.echo("\nInterrupted. Saving state...")
    finally:
        if executor:
            executor.shutdown(wait=False)
        state.finished_at = datetime.now(ET)
        _save_state(state)
        click.echo(f"State saved to {_state_path(state.run_date)}")

    # Final summary
    _print_summary(state)


# ---------------------------------------------------------------------------
# Status display
# ---------------------------------------------------------------------------


def _print_summary(state: FilingResearchState) -> None:
    """Print a human-readable summary of filing research state."""
    click.echo(f"\nFiling Research Summary -- {state.run_date}")

    started = state.started_at.astimezone(ET).strftime("%H:%M:%S ET") if state.started_at else "—"
    finished = state.finished_at.astimezone(ET).strftime("%H:%M:%S ET") if state.finished_at else "RUNNING"
    last_poll = state.last_poll_at.astimezone(ET).strftime("%H:%M:%S ET") if state.last_poll_at else "—"

    click.echo(f"Started: {started} | Last poll: {last_poll} | Polls: {state.poll_count} | Status: {finished}")
    click.echo()

    # Counts
    total = len(state.filings)
    research_triggered = sum(1 for f in state.filings.values() if f.decision in RESEARCH_DECISIONS)
    research_done = sum(1 for f in state.filings.values() if f.decision == FilingDecision.RESEARCH_DONE)
    research_running = sum(1 for f in state.filings.values() if f.decision == FilingDecision.RESEARCH_RUNNING)
    research_failed = sum(1 for f in state.filings.values() if f.decision == FilingDecision.RESEARCH_FAILED)
    research_queued = sum(1 for f in state.filings.values() if f.decision == FilingDecision.RESEARCH_QUEUED)

    skip_counts: dict[str, int] = {}
    for f in state.filings.values():
        if f.decision in SKIP_DECISIONS:
            label = f.decision.value.replace("skip_", "")
            skip_counts[label] = skip_counts.get(label, 0) + 1

    click.echo(f"Filings discovered: {total}")
    click.echo(f"  Research triggered: {research_triggered}")
    if research_done:
        click.echo(f"    Done:    {research_done}")
    if research_running:
        click.echo(f"    Running: {research_running}")
    if research_queued:
        click.echo(f"    Queued:  {research_queued}")
    if research_failed:
        click.echo(f"    Failed:  {research_failed}")
    for reason, count in sorted(skip_counts.items()):
        click.echo(f"  Skipped ({reason}): {count}")

    # Research details table
    research_filings = [
        f for f in state.filings.values() if f.decision in RESEARCH_DECISIONS
    ]
    if research_filings:
        click.echo(
            f"\n{'Ticker':<8} {'Form':<8} {'Mag':>5} {'Status':<16} "
            f"{'Started':<10} {'Duration':<10} {'Artifacts'}"
        )
        click.echo("-" * 85)
        for f in sorted(research_filings, key=lambda x: x.discovered_at):
            mag_str = f"{f.magnitude:.2f}" if f.magnitude is not None else "—"
            status = f.decision.value.replace("research_", "").upper()

            if f.research_started_at:
                started_str = f.research_started_at.astimezone(ET).strftime("%H:%M ET")
            else:
                started_str = "—"

            if f.research_started_at and f.research_finished_at:
                delta = f.research_finished_at - f.research_started_at
                mins = int(delta.total_seconds() / 60)
                duration_str = f"{mins}m"
            elif f.research_started_at:
                delta = datetime.now(ET) - f.research_started_at
                mins = int(delta.total_seconds() / 60)
                duration_str = f"{mins}m+"
            else:
                duration_str = "—"

            artifacts_str = f"{len(f.artifacts_found)}" if f.artifacts_found else "—"

            click.echo(
                f"{f.ticker:<8} {f.form_type:<8} {mag_str:>5} {status:<16} "
                f"{started_str:<10} {duration_str:<10} {artifacts_str}"
            )

    # Skipped details (compact)
    skipped = [f for f in state.filings.values() if f.decision in SKIP_DECISIONS]
    if skipped:
        click.echo(f"\nSkipped ({len(skipped)}):")
        click.echo(f"  {'Ticker':<8} {'Form':<8} {'Class':<6} {'Mag':>5}  Reason")
        click.echo(f"  {'-'*60}")
        for f in sorted(skipped, key=lambda x: x.discovered_at)[:20]:
            mag_str = f"{f.magnitude:.2f}" if f.magnitude is not None else "—"
            cls = f.classification[:6] if f.classification else "—"
            click.echo(
                f"  {f.ticker or '—':<8} {f.form_type or '—':<8} {cls:<6} {mag_str:>5}  "
                f"{f.decision_reason}"
            )
        if len(skipped) > 20:
            click.echo(f"  ... and {len(skipped) - 20} more")


def show_status(date_str: str | None = None, as_json: bool = False) -> None:
    """Display the current filing research state."""
    now_et = datetime.now(ET)
    run_date = date_str or now_et.strftime("%Y-%m-%d")
    state = _load_state(run_date)

    if not state.filings and not state.started_at:
        click.echo(f"No filing research state found for {run_date}.")
        click.echo(f"Run 'praxis filing-research run' to start the daemon.")
        return

    if as_json:
        click.echo(json.dumps(state.model_dump(mode="json"), indent=2, default=str))
        return

    _print_summary(state)


# ---------------------------------------------------------------------------
# Click commands
# ---------------------------------------------------------------------------


@click.group("filing-research")
def filing_research():
    """Automated filing research daemon and status viewer."""
    pass


@filing_research.command("run")
@click.option("--date", "date_str", default=None, help="Run date YYYY-MM-DD (default: today ET)")
@click.option("--start-hour", "start_hour_str", default="6:00", show_default=True, help="Start time ET (H:MM or HH:MM)")
@click.option("--end-hour", "end_hour_str", default="22:00", show_default=True, help="End time ET (H:MM or HH:MM, daemon drains after this)")
@click.option("--poll-interval", type=int, default=600, show_default=True, help="Seconds between S3 polls")
@click.option("--magnitude-threshold", type=float, default=0.5, show_default=True, help="Minimum magnitude to trigger research")
@click.option("--max-parallel", type=int, default=4, show_default=True, help="Max concurrent research sessions")
@click.option("--dry-run", is_flag=True, help="Evaluate filings but don't launch research")
def filing_research_run(
    date_str: str | None,
    start_hour_str: str,
    end_hour_str: str,
    poll_interval: int,
    magnitude_threshold: float,
    max_parallel: int,
    dry_run: bool,
):
    """Start the filing research daemon.

    Polls S3 for new filing analyses, evaluates them against early-exit
    criteria, and launches full research sessions for promising filings.

    \b
    The daemon runs from start-hour to end-hour ET, then drains any
    in-progress research before exiting. Status can be checked from
    another terminal with: praxis filing-research status

    \b
    Examples:
      praxis filing-research run
      praxis filing-research run --dry-run
      praxis filing-research run --magnitude-threshold 0.5 --max-parallel 1
      praxis filing-research run --start-hour 8:00 --end-hour 11:00
    """
    run_daemon(
        date_str=date_str,
        start_hour=_parse_time(start_hour_str),
        end_hour=_parse_time(end_hour_str),
        poll_interval=poll_interval,
        magnitude_threshold=magnitude_threshold,
        max_parallel=max_parallel,
        dry_run=dry_run,
    )


@filing_research.command("status")
@click.option("--date", "date_str", default=None, help="Run date YYYY-MM-DD (default: today ET)")
@click.option("--json", "as_json", is_flag=True, help="Output raw JSON state")
def filing_research_status(date_str: str | None, as_json: bool):
    """Show the current filing research status.

    Reads the state file directly -- does not require the daemon to be running.

    \b
    Examples:
      praxis filing-research status
      praxis filing-research status --date 2026-03-22
      praxis filing-research status --json
    """
    show_status(date_str=date_str, as_json=as_json)


@filing_research.command("schedule")
def filing_research_schedule():
    """Install the launchd plist to auto-start daily at 9 AM ET."""
    import shutil

    plist_name = "com.praxis.filing-research.plist"
    repo_root = find_repo_root()
    source = repo_root / plist_name

    if not source.exists():
        click.echo(f"Plist not found at {source}")
        click.echo("Expected the plist to be in the repo root.")
        return

    dest_dir = Path.home() / "Library" / "LaunchAgents"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / plist_name

    shutil.copy2(source, dest)
    click.echo(f"Installed plist to {dest}")

    import subprocess
    result = subprocess.run(["launchctl", "load", str(dest)], capture_output=True, text=True)
    if result.returncode == 0:
        click.echo("Loaded into launchd. The daemon will start daily at 9:00 AM.")
    else:
        click.echo(f"launchctl load failed: {result.stderr}")
        click.echo(f"Try manually: launchctl load {dest}")


@filing_research.command("unschedule")
def filing_research_unschedule():
    """Remove the launchd plist."""
    plist_name = "com.praxis.filing-research.plist"
    dest = Path.home() / "Library" / "LaunchAgents" / plist_name

    if not dest.exists():
        click.echo("No plist found. Not currently scheduled.")
        return

    import subprocess
    subprocess.run(["launchctl", "unload", str(dest)], capture_output=True, text=True)
    dest.unlink()
    click.echo("Unloaded and removed launchd plist.")


@filing_research.command("report")
@click.option("--date", "date_str", default=None, help="Run date YYYY-MM-DD (default: today ET)")
@click.option("--output", "output_path", default=None, help="Output HTML path (default: data/filing_research_report_{date}.html)")
@click.option("--no-charts", is_flag=True, help="Skip price chart fetching")
def filing_research_report(date_str: str | None, output_path: str | None, no_charts: bool):
    """Generate an HTML dashboard of the day's filing research results.

    \b
    Cards are ranked by tactical score (primary) and fundamental score
    (tiebreaker) from memo.yaml. Fetches 30-day price charts per ticker.

    \b
    Examples:
      praxis filing-research report
      praxis filing-research report --date 2026-03-28
      praxis filing-research report --no-charts
    """
    from cli.filing_research_report import generate_and_write_report

    generate_and_write_report(
        date_str=date_str,
        output_path=output_path,
        skip_charts=no_charts,
    )
