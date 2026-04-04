"""Unified priority research queue — single executor for all research paths.

Replaces the separate filing research daemon and queue daemon with one
priority queue and one executor. All research goes through the same
CLAUDE.md coordinator pipeline.

Priority levels:
  P1: User override — "research this NOW"
  P2: Scanner findings (8-K/PR with mag >= 0.5, non-SELL/HOLD)
  P3: GitHub Issues research queue
  P4: Analyst escalations (sonnet found something worth deeper research)
  P5: Background work (weekly thesis review, pre-earnings)

Architecture:
  Two input pollers feed the queue:
    - S3 poller: reads scanner's analysis.json → creates P2 items
    - GitHub poller: reads research-queue issues → creates P3 items
  One executor pulls from queue by priority, runs coordinator pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta
from enum import IntEnum
from pathlib import Path
from zoneinfo import ZoneInfo

import click
import yaml
from pydantic import BaseModel, Field

from cli.config_utils import find_repo_root, get_config_dir, load_yaml
from cli.models import TickerRegistry
from cli.s3 import download_file, get_s3_client, list_prefix

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Priority(IntEnum):
    URGENT = 1     # User override
    SCANNER = 2    # Scanner findings
    QUEUE = 3      # GitHub Issues
    ANALYST = 4    # Analyst escalations
    BACKGROUND = 5 # Weekly review, pre-earnings


class ResearchItem(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    ticker: str
    priority: int = Priority.SCANNER
    source: str = ""  # scanner, github, analyst, user, background
    source_id: str = ""  # key_prefix for scanner, issue number for github
    description: str = ""
    classification: str = ""
    magnitude: float | None = None
    tactical_context: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(ET))
    status: str = "queued"  # queued, running, done, failed
    started_at: datetime | None = None
    finished_at: datetime | None = None
    session_id: str | None = None
    artifacts: list[str] = Field(default_factory=list)
    decision: str = ""  # from memo.yaml after completion
    error: str = ""


class ResearchQueueState(BaseModel):
    last_s3_poll_at: datetime | None = None
    last_github_poll_at: datetime | None = None
    s3_poll_count: int = 0
    github_poll_count: int = 0
    items: dict[str, ResearchItem] = Field(default_factory=dict)  # id -> item
    processed_keys: list[str] = Field(default_factory=list)  # dedup for scanner items


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def _state_path() -> Path:
    return find_repo_root() / "data" / "research_queue_state.yaml"


def _load_state() -> ResearchQueueState:
    path = _state_path()
    if path.exists():
        try:
            raw = yaml.safe_load(path.read_text()) or {}
            return ResearchQueueState(**raw)
        except Exception:
            return ResearchQueueState()
    return ResearchQueueState()


def _save_state(state: ResearchQueueState) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    data = state.model_dump(mode="json")
    # Keep last 500 items and 500 processed keys
    if len(data.get("items", {})) > 500:
        # Keep only recent items
        sorted_items = sorted(data["items"].items(), key=lambda x: x[1].get("created_at", ""), reverse=True)
        data["items"] = dict(sorted_items[:500])
    if len(data.get("processed_keys", [])) > 500:
        data["processed_keys"] = data["processed_keys"][-500:]
    path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


# ---------------------------------------------------------------------------
# Input: S3 scanner items
# ---------------------------------------------------------------------------


def _poll_scanner_items(
    s3,
    state: ResearchQueueState,
    magnitude_threshold: float = 0.5,
) -> list[ResearchItem]:
    """Poll S3 for new scanner-analyzed items. Returns new research items."""
    from cli.pipeline_status import collect_pipeline_items, parse_day_window

    now_et = datetime.now(ET)
    run_date = now_et.strftime("%Y-%m-%d")
    day_start_utc, day_end_utc, _ = parse_day_window(run_date)

    items = collect_pipeline_items(
        s3,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
        source="all",
        stuck_minutes=30,
    )

    new_items = []
    researched_tickers: set[str] = set()
    for existing in state.items.values():
        if existing.status in ("running", "done", "queued"):
            researched_tickers.add(existing.ticker.upper())

    for item in items:
        if item.key_prefix in state.processed_keys:
            continue

        ticker = (item.ticker or "").upper()
        if not ticker:
            state.processed_keys.append(item.key_prefix)
            continue

        # Skip if already researching this ticker
        if ticker in researched_tickers:
            state.processed_keys.append(item.key_prefix)
            continue

        # Read analysis.json
        try:
            analysis_raw = download_file(s3, f"{item.key_prefix}/analysis.json")
            analysis = json.loads(analysis_raw)
        except Exception:
            continue  # Not yet analyzed

        classification = (analysis.get("classification") or "").upper()
        magnitude = analysis.get("magnitude")

        # Filter
        if classification in ("SELL", "HOLD"):
            state.processed_keys.append(item.key_prefix)
            continue
        if magnitude is not None and magnitude < magnitude_threshold:
            state.processed_keys.append(item.key_prefix)
            continue

        new_items.append(ResearchItem(
            ticker=ticker,
            priority=Priority.SCANNER,
            source="scanner",
            source_id=item.key_prefix,
            description=analysis.get("new_information", "")[:200],
            classification=classification,
            magnitude=magnitude,
        ))
        state.processed_keys.append(item.key_prefix)
        researched_tickers.add(ticker)

    state.last_s3_poll_at = now_et
    state.s3_poll_count += 1
    return new_items


# ---------------------------------------------------------------------------
# Input: GitHub Issues
# ---------------------------------------------------------------------------


def _poll_github_items(state: ResearchQueueState) -> list[ResearchItem]:
    """Poll GitHub for new research-queue issues. Returns new research items."""
    from cli.queue_github import poll_issues, parse_issue

    try:
        raw_issues = poll_issues()
    except Exception as e:
        logger.error("GitHub poll failed: %s", e)
        return []

    new_items = []
    existing_issues = {
        i.source_id for i in state.items.values()
        if i.source == "github"
    }

    for raw in raw_issues:
        issue_num = str(raw["number"])
        if issue_num in existing_issues:
            continue

        task = parse_issue(raw)

        # Determine priority
        priority = Priority.QUEUE
        title_lower = (raw.get("title") or "").lower()
        if "[urgent]" in title_lower or "[p1]" in title_lower:
            priority = Priority.URGENT
        elif "[analyst]" in title_lower:
            priority = Priority.ANALYST
        elif "[weekly]" in title_lower or "[earnings]" in title_lower:
            priority = Priority.BACKGROUND

        ticker = task.tickers[0] if task.tickers else ""
        new_items.append(ResearchItem(
            ticker=ticker,
            priority=priority,
            source="github",
            source_id=issue_num,
            description=raw.get("title", ""),
        ))

    state.last_github_poll_at = datetime.now(ET)
    state.github_poll_count += 1
    return new_items


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


def _run_research(item: ResearchItem) -> tuple[bool, list[str], str, str]:
    """Run the full coordinator research pipeline for an item.

    Returns (success, artifacts, session_id, decision).
    """
    from cli.research_run import launch_sessions, _find_claude, build_run_prompt, fetch_tactical_context, write_prompt_file
    from cli.staging import stage_ticker, sync_research
    from cli.edgar import resolve_ticker

    config_dir = get_config_dir()
    registry_cfg = TickerRegistry(**load_yaml(config_dir / "ticker_registry.yaml"))
    s3 = get_s3_client()
    ticker = item.ticker

    # Resolve CIK if missing
    cik = ""
    entry = registry_cfg.tickers.get(ticker)
    if entry:
        cik = entry.cik
    if not cik:
        try:
            info = resolve_ticker(ticker)
            if info:
                cik = info.cik
        except Exception:
            pass

    # Fetch macro files
    macro_keys = list_prefix(s3, "data/context/macro/")
    macro_files = [k for k in macro_keys if k != "data/context/macro/"]

    # Stage workspace
    workspace = stage_ticker(ticker, config_dir, registry_cfg, s3, macro_files, quiet=True, cik=cik, tactical=True)
    if workspace is None:
        # Minimal workspace for web-only research
        from cli.config_utils import find_repo_root
        repo_root = find_repo_root()
        workspace = repo_root / "workspace" / ticker
        workspace.mkdir(parents=True, exist_ok=True)

        exchange_hint = ""
        _suffix_map = {".V": "TSXV", ".TO": "TSX", ".AX": "ASX", ".L": "LSE", ".CO": "Copenhagen", ".SW": "SIX"}
        for suffix, label in _suffix_map.items():
            if ticker.upper().endswith(suffix):
                exchange_hint = f"{ticker} trades on the {label}. Use local sources for filings.\n"
                break

        claude_md = (
            f"# Research Task: {ticker}\n\n"
            f"{exchange_hint}"
            f"No SEC filings or fundamental data were pre-ingested.\n"
            f"Use web search to research this company thoroughly.\n"
            f"Produce memo.md and memo.yaml per standard format.\n"
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
        sessions, max_parallel=1, max_retries=2, session_map=session_map,
    )

    if not results:
        return False, [], session_id, ""

    _, sid, success, output, found, missing = results[0]
    artifacts = sorted(found)

    # Sync to S3
    if found:
        sync_research(ticker, s3, quiet=True, cleanup=False)

    # Read decision from memo.yaml
    decision = ""
    memo_path = workspace / "memo.yaml"
    if memo_path.exists():
        try:
            memo = yaml.safe_load(memo_path.read_text()) or {}
            decision = memo.get("decision", "")
        except Exception:
            pass

    return success, artifacts, sid, decision


def _post_completion(item: ResearchItem) -> None:
    """Post-completion actions: email BUY memos, regen report, etc."""
    from cli.filing_research import _maybe_email_memo

    # Email BUY memos
    _maybe_email_memo(item.ticker)

    # Post result to GitHub issue if source is github
    if item.source == "github" and item.source_id:
        try:
            from cli.queue_github import post_result
            summary = f"Research completed. Decision: {item.decision}. Artifacts: {len(item.artifacts)}"
            post_result(int(item.source_id), summary, item.status == "done")
        except Exception:
            pass

    # Auto-regen HTML report
    try:
        from cli.filing_research_report import generate_and_write_report
        generate_and_write_report(skip_charts=False, open_browser=False, quiet=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Daemon
# ---------------------------------------------------------------------------


def run_research_queue(
    *,
    poll_interval: int = 600,
    magnitude_threshold: float = 0.5,
    max_parallel: int = 4,
    dry_run: bool = False,
) -> None:
    """Unified research queue daemon."""
    from cli.env_loader import load_env
    load_env()

    state = _load_state()
    s3 = get_s3_client()

    click.echo("Unified research queue started")
    click.echo(f"  Poll interval: {poll_interval}s | Max parallel: {max_parallel}")
    click.echo(f"  Magnitude threshold: {magnitude_threshold} | Dry run: {dry_run}")
    click.echo()

    executor = ThreadPoolExecutor(max_workers=max_parallel) if not dry_run else None
    pending: dict[str, Future] = {}  # item_id -> future

    try:
        while True:
            now_et = datetime.now(ET)

            # Weekend guard
            if now_et.weekday() >= 5:
                time.sleep(600)
                continue

            # Market hours only (6 AM - 10 PM ET)
            if now_et.hour < 6 or now_et.hour >= 22:
                time.sleep(300)
                continue

            # Capacity check
            try:
                from cli.telemetry import get_capacity_estimate
                cap = get_capacity_estimate()
                if cap.get("at_target", False) and not pending:
                    time.sleep(60)
                    continue
            except Exception:
                pass

            # Poll for new items
            scanner_items = _poll_scanner_items(s3, state, magnitude_threshold)
            github_items = _poll_github_items(state)

            for item in scanner_items + github_items:
                state.items[item.id] = item

            new_count = len(scanner_items) + len(github_items)
            if new_count > 0:
                queued = sum(1 for i in state.items.values() if i.status == "queued")
                click.echo(
                    f"[{now_et.strftime('%H:%M:%S')}] {new_count} new item(s), "
                    f"{queued} queued, {len(pending)} running"
                )

            # Check completed futures
            completed_ids = []
            for item_id, future in pending.items():
                if future.done():
                    completed_ids.append(item_id)
                    item = state.items[item_id]
                    try:
                        success, artifacts, session_id, decision = future.result()
                    except Exception as e:
                        success, artifacts, session_id, decision = False, [], "", ""
                        logger.error("Research job for %s raised: %s", item.ticker, e)

                    item.finished_at = datetime.now(ET)
                    item.session_id = session_id
                    item.artifacts = artifacts
                    item.decision = decision
                    item.status = "done" if success else "failed"

                    status = "DONE" if success else "FAILED"
                    click.echo(
                        f"[{datetime.now(ET).strftime('%H:%M:%S')}] {status}: "
                        f"P{item.priority} {item.ticker} ({len(artifacts)} artifacts, {decision})"
                    )

                    if success:
                        _post_completion(item)

            for item_id in completed_ids:
                del pending[item_id]

            # Submit new work — priority order
            if executor:
                queued_items = sorted(
                    [i for i in state.items.values() if i.status == "queued"],
                    key=lambda i: (i.priority, i.created_at),
                )

                for item in queued_items:
                    if len(pending) >= max_parallel:
                        break

                    # Re-check capacity
                    try:
                        cap = get_capacity_estimate()
                        if cap.get("at_target", False):
                            break
                    except Exception:
                        pass

                    if dry_run:
                        click.echo(
                            f"  [DRY RUN] P{item.priority} {item.ticker}: {item.description[:60]}"
                        )
                        item.status = "done"
                        continue

                    item.status = "running"
                    item.started_at = datetime.now(ET)
                    click.echo(
                        f"[{datetime.now(ET).strftime('%H:%M:%S')}] Starting P{item.priority}: "
                        f"{item.ticker} ({item.source})"
                    )

                    future = executor.submit(_run_research, item)
                    pending[item.id] = future

            _save_state(state)
            time.sleep(poll_interval)

    except KeyboardInterrupt:
        click.echo("\nInterrupted. Saving state...")
    finally:
        if executor:
            executor.shutdown(wait=False)
        _save_state(state)


# ---------------------------------------------------------------------------
# Status display
# ---------------------------------------------------------------------------


def show_status() -> None:
    """Display research queue status."""
    state = _load_state()
    if not state.items:
        click.echo("Research queue is empty.")
        return

    queued = [i for i in state.items.values() if i.status == "queued"]
    running = [i for i in state.items.values() if i.status == "running"]
    done = [i for i in state.items.values() if i.status == "done"]
    failed = [i for i in state.items.values() if i.status == "failed"]

    click.echo(f"Research Queue: {len(state.items)} total")
    click.echo(f"  Queued: {len(queued)} | Running: {len(running)} | Done: {len(done)} | Failed: {len(failed)}")
    click.echo(f"  S3 polls: {state.s3_poll_count} | GitHub polls: {state.github_poll_count}")

    # Show by priority
    for p in [1, 2, 3, 4, 5]:
        p_items = [i for i in state.items.values() if i.priority == p and i.status in ("queued", "running")]
        if p_items:
            click.echo(f"\n  P{p} ({'URGENT' if p == 1 else 'SCANNER' if p == 2 else 'QUEUE' if p == 3 else 'ANALYST' if p == 4 else 'BACKGROUND'}):")
            for i in p_items:
                click.echo(f"    {i.ticker:12s} {i.status:8s} {i.description[:50]}")

    # Recent completed
    recent_done = sorted(done, key=lambda i: i.finished_at or i.created_at, reverse=True)[:5]
    if recent_done:
        click.echo(f"\n  Recent completed:")
        for i in recent_done:
            click.echo(f"    {i.ticker:12s} {i.decision:12s} P{i.priority} {i.source}")


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


@click.group("research-queue")
def research_queue_cli():
    """Unified priority research queue."""
    pass


@research_queue_cli.command("run")
@click.option("--poll-interval", type=int, default=600, show_default=True, help="Seconds between polls")
@click.option("--magnitude-threshold", type=float, default=0.5, show_default=True, help="Min magnitude for scanner items")
@click.option("--max-parallel", type=int, default=4, show_default=True, help="Max concurrent research sessions")
@click.option("--dry-run", is_flag=True, help="Poll but don't run research")
def research_queue_run(poll_interval: int, magnitude_threshold: float, max_parallel: int, dry_run: bool):
    """Start the unified research queue daemon.

    \b
    Pulls from two sources: S3 scanner items (P2) and GitHub Issues (P3).
    Runs research in priority order through the full coordinator pipeline.
    """
    run_research_queue(
        poll_interval=poll_interval,
        magnitude_threshold=magnitude_threshold,
        max_parallel=max_parallel,
        dry_run=dry_run,
    )


@research_queue_cli.command("status")
def research_queue_status():
    """Show research queue status."""
    show_status()


@research_queue_cli.command("add")
@click.argument("ticker")
@click.option("--priority", "-p", type=int, default=1, help="Priority (1=urgent, 2=scanner, 3=queue)")
@click.option("--desc", "-d", default="", help="Description")
def research_queue_add(ticker: str, priority: int, desc: str):
    """Add an item directly to the research queue.

    \b
    P1 = urgent (researched next), P2 = normal, P3 = low priority.

    \b
    Examples:
      praxis research-queue add CLMT -p 1 -d "RVO impact — research NOW"
      praxis research-queue add NVDA
    """
    state = _load_state()
    item = ResearchItem(
        ticker=ticker.strip("/").upper(),
        priority=priority,
        source="user",
        description=desc or f"Manual research request for {ticker}",
    )
    state.items[item.id] = item
    _save_state(state)
    click.echo(f"Added: P{priority} {item.ticker} ({item.id})")
