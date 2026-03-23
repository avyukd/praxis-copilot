"""Automated filing research daemon and status viewer.

Polls S3 for new filing analyses, identifies research-worthy items via early-exit
filters, and orchestrates full research sessions automatically.
"""

from __future__ import annotations

import json
import logging
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
from cli.models import TickerRegistry, UniverseConfig
from cli.pipeline_status import (
    build_pipeline_trace,
    collect_pipeline_items,
    parse_day_window,
)
from cli.research_run import (
    build_run_prompt,
    fetch_tactical_context,
    launch_sessions,
    write_prompt_file,
)
from cli.s3 import get_s3_client, list_prefix
from cli.staging import stage_ticker, sync_research

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

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
    universe_tickers: set[str],
    magnitude_threshold: float,
    researched_tickers: set[str],
) -> TrackedFiling:
    """Evaluate a single pipeline item and decide whether to research it."""
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

    # Early exit: screened out by Haiku prescreen
    if item.stage == "screened_out":
        tracked.decision = FilingDecision.SKIP_SCREENED
        tracked.decision_reason = "Haiku prescreen rejected"
        return tracked

    # Stage gate: must be analyzed
    if item.stage not in ("analyzed", "alerted"):
        tracked.decision = FilingDecision.SKIP_NOT_ANALYZED
        tracked.decision_reason = f"stage={item.stage}"
        return tracked

    # Ticker must be in universe
    ticker = (item.ticker or "").upper()
    if not ticker or ticker not in universe_tickers:
        tracked.decision = FilingDecision.SKIP_NO_TICKER
        tracked.decision_reason = f"{ticker or '(empty)'} not in universe"
        return tracked

    # Fetch analysis details
    trace = build_pipeline_trace(s3, source_type=item.source_type, key_prefix=item.key_prefix)
    tracked.classification = trace.analysis_classification or ""
    tracked.magnitude = trace.analysis_magnitude
    tracked.summary = trace.analysis_summary or ""

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
    state: FilingResearchState,
    key_prefix: str,
) -> tuple[bool, list[str], str]:
    """Run the full stage -> research -> sync pipeline for a ticker.

    Returns (success, artifacts_found, session_id).
    This runs in a worker thread.
    """
    config_dir = get_config_dir()
    registry_cfg = TickerRegistry(**load_yaml(config_dir / "ticker_registry.yaml"))
    s3 = get_s3_client()

    # Fetch macro files
    macro_keys = list_prefix(s3, "data/context/macro/")
    macro_files = [k for k in macro_keys if k != "data/context/macro/"]

    # Stage
    workspace = stage_ticker(ticker, config_dir, registry_cfg, s3, macro_files, quiet=True)
    if workspace is None:
        return False, [], ""

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
        max_retries=1,
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


# ---------------------------------------------------------------------------
# Daemon
# ---------------------------------------------------------------------------


def run_daemon(
    *,
    date_str: str | None = None,
    start_hour: int = 9,
    end_hour: int = 10,
    poll_interval: int = 120,
    magnitude_threshold: float = 0.3,
    max_parallel: int = 2,
    dry_run: bool = False,
) -> None:
    """Main daemon loop: poll for filings, evaluate, launch research."""
    import os

    now_et = datetime.now(ET)
    run_date = date_str or now_et.strftime("%Y-%m-%d")

    state = _load_state(run_date)
    state.started_at = state.started_at or now_et
    state.daemon_pid = os.getpid()
    _save_state(state)

    # Load config
    config_dir = get_config_dir()
    universe_cfg = UniverseConfig(**load_yaml(config_dir / "universe.yaml"))
    universe_tickers = {t.upper() for t in universe_cfg.tickers}

    click.echo(f"Filing research daemon started for {run_date}")
    click.echo(f"  Window: {start_hour}:00 - {end_hour}:00 ET")
    click.echo(f"  Poll interval: {poll_interval}s | Magnitude threshold: {magnitude_threshold}")
    click.echo(f"  Max parallel research: {max_parallel} | Dry run: {dry_run}")
    click.echo(f"  Universe: {len(universe_tickers)} tickers")
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
                                s3, item, universe_tickers, magnitude_threshold, researched_tickers
                            )
                            state.filings[item.key_prefix] = tracked
                            if tracked.decision == FilingDecision.RESEARCH_QUEUED:
                                queued_count += 1
                        continue

                    tracked = _evaluate_filing(
                        s3, item, universe_tickers, magnitude_threshold, researched_tickers
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

            # Submit new research jobs
            if executor and not past_window:
                for filing in state.filings.values():
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
                        _run_research_job, ticker, state, filing.key_prefix
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
@click.option("--start-hour", type=int, default=9, show_default=True, help="Start hour ET")
@click.option("--end-hour", type=int, default=10, show_default=True, help="End hour ET (daemon polls until this, then drains)")
@click.option("--poll-interval", type=int, default=120, show_default=True, help="Seconds between S3 polls")
@click.option("--magnitude-threshold", type=float, default=0.3, show_default=True, help="Minimum magnitude to trigger research")
@click.option("--max-parallel", type=int, default=2, show_default=True, help="Max concurrent research sessions")
@click.option("--dry-run", is_flag=True, help="Evaluate filings but don't launch research")
def filing_research_run(
    date_str: str | None,
    start_hour: int,
    end_hour: int,
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
      praxis filing-research run --start-hour 8 --end-hour 11
    """
    run_daemon(
        date_str=date_str,
        start_hour=start_hour,
        end_hour=end_hour,
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
