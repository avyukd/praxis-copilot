"""Research queue daemon: polls GitHub Issues, runs Claude CLI research during idle periods."""

from __future__ import annotations

import logging
import os
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import click
import yaml

from cli.config_utils import find_repo_root
from cli.queue_capacity import check_capacity
from cli.queue_github import mark_in_progress, parse_issue, poll_issues, post_result
from cli.queue_models import QueueState, QueueTask, QueueTaskStatus
from cli.queue_workspace import setup_workspace, sync_queue_artifacts
from cli.research_run import _find_claude

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

REQUIRED_ARTIFACTS = {"summary.md", "summary.yaml"}


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


def _state_path() -> Path:
    return find_repo_root() / "data" / "queue_state.yaml"


def _load_state() -> QueueState:
    path = _state_path()
    if path.exists():
        raw = yaml.safe_load(path.read_text()) or {}
        return QueueState(**raw)
    return QueueState()


def _save_state(state: QueueState) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.dump(state.model_dump(mode="json"), default_flow_style=False, sort_keys=False)
    )


# ---------------------------------------------------------------------------
# Job execution
# ---------------------------------------------------------------------------


def _run_queue_job(task: QueueTask, workspace: Path) -> tuple[bool, str, list[str]]:
    """Run a Claude CLI session for a queue task.

    Returns (success, summary_text, artifacts_found).
    """
    claude_bin = _find_claude()

    # Read the CLAUDE.md as the prompt
    claude_md = workspace / "CLAUDE.md"
    prompt = f"Complete the research task described in CLAUDE.md. Read it first."

    session_id = str(uuid.uuid4())

    env = os.environ.copy()
    env.pop("ANTHROPIC_API_KEY", None)
    env.pop("CLAUDE_API_KEY", None)
    env.pop("CLAUDE_CODE_ENTRYPOINT", None)
    env.pop("CLAUDECODE", None)

    from cli.telemetry import track_claude_call

    cmd = [claude_bin, "-p", prompt, "--dangerously-skip-permissions", "--session-id", session_id]
    result_json = track_claude_call(
        cmd, cwd=workspace, env=env, timeout=2700,
        daemon="queue", task_id=str(task.issue_number), ticker=task.tickers[0] if task.tickers else "",
    )

    if result_json.get("timed_out"):
        return False, "Timed out after 45 minutes", []

    if result_json.get("is_error"):
        logger.error("Claude failed for #%d: %s", task.issue_number, result_json.get("result", "")[:500])

    # Check artifacts
    found = []
    for f in workspace.rglob("*"):
        if f.is_file():
            rel = str(f.relative_to(workspace))
            if not rel.startswith("data/") and not rel.startswith("macro/") and not rel.startswith("context/"):
                if f.name not in {"CLAUDE.md", ".mcp.json"}:
                    found.append(rel)

    missing_required = REQUIRED_ARTIFACTS - {f for f in found}
    success = result.returncode == 0 and not missing_required

    # Read summary if it exists
    summary_text = ""
    summary_path = workspace / "summary.md"
    if summary_path.exists():
        summary_text = summary_path.read_text()

    # Read structured summary for one-liner
    summary_yaml_path = workspace / "summary.yaml"
    if summary_yaml_path.exists():
        try:
            sy = yaml.safe_load(summary_yaml_path.read_text()) or {}
            one_line = sy.get("one_line", "")
            if one_line and not summary_text:
                summary_text = one_line
        except Exception:
            pass

    return success, summary_text, found


# ---------------------------------------------------------------------------
# Daemon
# ---------------------------------------------------------------------------


def run_daemon(
    *,
    poll_interval_idle: int = 300,
    poll_interval_active: int = 60,
    max_parallel: int = 2,
    dry_run: bool = False,
) -> None:
    """Main daemon loop: poll GitHub, check capacity, run research."""
    from cli.env_loader import load_env
    load_env()

    state = _load_state()

    click.echo("Research queue daemon started")
    click.echo(f"  Poll interval: {poll_interval_idle}s idle / {poll_interval_active}s active")
    click.echo(f"  Max parallel: {max_parallel} | Dry run: {dry_run}")
    click.echo()

    executor = ThreadPoolExecutor(max_workers=max_parallel) if not dry_run else None
    pending_futures: dict[int, Future] = {}  # issue_number -> future

    try:
        while True:
            now_et = datetime.now(ET)

            # Poll GitHub for new issues
            try:
                raw_issues = poll_issues()
            except Exception as e:
                logger.error("GitHub poll failed: %s", e)
                raw_issues = []

            new_count = 0
            for raw in raw_issues:
                issue_num = raw["number"]
                if issue_num in state.tasks:
                    continue
                task = parse_issue(raw)
                state.tasks[issue_num] = task
                new_count += 1

            state.last_poll_at = now_et

            if new_count > 0:
                queued = sum(1 for t in state.tasks.values() if t.status == QueueTaskStatus.QUEUED)
                click.echo(
                    f"[{now_et.strftime('%H:%M:%S')}] {new_count} new issue(s), "
                    f"{queued} queued"
                )

            # Check completed futures
            completed_issues = []
            for issue_num, future in pending_futures.items():
                if future.done():
                    completed_issues.append(issue_num)
                    task = state.tasks[issue_num]
                    try:
                        success, summary, artifacts = future.result()
                    except Exception as e:
                        success, summary, artifacts = False, str(e), []
                        logger.error("Queue job for #%d raised: %s", issue_num, e)

                    task.finished_at = datetime.now(ET)
                    task.artifacts = artifacts
                    task.summary = summary[:2000] if summary else ""

                    if success:
                        task.status = QueueTaskStatus.DONE
                        click.echo(
                            f"[{datetime.now(ET).strftime('%H:%M:%S')}] DONE: "
                            f"#{issue_num} {task.title} ({len(artifacts)} artifacts)"
                        )
                    else:
                        task.status = QueueTaskStatus.FAILED
                        task.error = summary[:500] if not success else ""
                        click.echo(
                            f"[{datetime.now(ET).strftime('%H:%M:%S')}] FAILED: "
                            f"#{issue_num} {task.title}"
                        )

                    # Post results to GitHub
                    try:
                        post_result(issue_num, task.summary or task.error or "No output", success)
                    except Exception as e:
                        logger.error("Failed to post result for #%d: %s", issue_num, e)

                    # Sync artifacts to S3
                    workspace = find_repo_root() / "workspace" / "queue" / str(issue_num)
                    if workspace.exists():
                        try:
                            uploaded = sync_queue_artifacts(task, workspace)
                            if uploaded:
                                click.echo(f"  Synced {uploaded} artifact(s) to S3")
                        except Exception as e:
                            logger.error("S3 sync failed for #%d: %s", issue_num, e)

                    # Send notification
                    try:
                        _send_notification(task)
                    except Exception as e:
                        logger.debug("Notification failed for #%d: %s", issue_num, e)

            for issue_num in completed_issues:
                del pending_futures[issue_num]

            # Submit new jobs if capacity available
            if executor:
                slots = check_capacity()
                available = max(0, slots - len(pending_futures))

                if available > 0:
                    queued_tasks = [
                        t for t in state.tasks.values()
                        if t.status == QueueTaskStatus.QUEUED
                    ]
                    queued_tasks.sort(key=lambda t: t.created_at)

                    for task in queued_tasks[:available]:
                        if dry_run:
                            click.echo(
                                f"[DRY RUN] Would process #{task.issue_number}: "
                                f"{task.title} (type={task.task_type.value}, "
                                f"tickers={task.tickers})"
                            )
                            task.status = QueueTaskStatus.DONE
                            continue

                        click.echo(
                            f"[{datetime.now(ET).strftime('%H:%M:%S')}] Starting: "
                            f"#{task.issue_number} {task.title} "
                            f"(type={task.task_type.value})"
                        )

                        task.status = QueueTaskStatus.RUNNING
                        task.started_at = datetime.now(ET)
                        task.session_id = str(uuid.uuid4())

                        # Mark in-progress on GitHub
                        try:
                            mark_in_progress(task.issue_number)
                        except Exception:
                            pass

                        # Setup workspace
                        try:
                            workspace = setup_workspace(task)
                        except Exception as e:
                            logger.error("Workspace setup failed for #%d: %s", task.issue_number, e)
                            task.status = QueueTaskStatus.FAILED
                            task.error = f"Workspace setup failed: {e}"
                            continue

                        future = executor.submit(_run_queue_job, task, workspace)
                        pending_futures[task.issue_number] = future

            _save_state(state)

            # Sleep — shorter if there are queued items
            has_queued = any(t.status == QueueTaskStatus.QUEUED for t in state.tasks.values())
            sleep_time = poll_interval_active if (has_queued or pending_futures) else poll_interval_idle
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        click.echo("\nInterrupted. Saving state...")
    finally:
        if executor:
            executor.shutdown(wait=False)
        _save_state(state)
        click.echo(f"State saved to {_state_path()}")

    _print_summary(state)


def _send_notification(task: QueueTask) -> None:
    """Send SNS notification for a completed queue task."""
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not topic_arn:
        return

    import boto3

    status = "DONE" if task.status == QueueTaskStatus.DONE else "FAILED"
    tickers = ", ".join(task.tickers) if task.tickers else "N/A"

    subject = f"Queue #{task.issue_number}: {task.title[:60]}"
    message = (
        f"Research queue task {status}.\n\n"
        f"Title: {task.title}\n"
        f"Type: {task.task_type.value}\n"
        f"Tickers: {tickers}\n\n"
        f"Summary:\n{task.summary[:1000] if task.summary else task.error or 'No output'}\n\n"
        f"GitHub: https://github.com/avyukd/praxis-copilot/issues/{task.issue_number}"
    )

    try:
        sns = boto3.client("sns")
        sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)
    except Exception as e:
        logger.debug("SNS publish failed: %s", e)


# ---------------------------------------------------------------------------
# Status display
# ---------------------------------------------------------------------------


def _print_summary(state: QueueState) -> None:
    """Print queue status summary."""
    if not state.tasks:
        click.echo("No queue tasks.")
        return

    queued = sum(1 for t in state.tasks.values() if t.status == QueueTaskStatus.QUEUED)
    running = sum(1 for t in state.tasks.values() if t.status == QueueTaskStatus.RUNNING)
    done = sum(1 for t in state.tasks.values() if t.status == QueueTaskStatus.DONE)
    failed = sum(1 for t in state.tasks.values() if t.status == QueueTaskStatus.FAILED)

    click.echo(f"\nQueue Status: {len(state.tasks)} total")
    click.echo(f"  Queued: {queued} | Running: {running} | Done: {done} | Failed: {failed}")

    if state.last_poll_at:
        click.echo(f"  Last poll: {state.last_poll_at.strftime('%H:%M:%S ET')}")

    # Show recent tasks
    recent = sorted(state.tasks.values(), key=lambda t: t.created_at, reverse=True)[:10]
    if recent:
        click.echo(f"\n{'#':<6} {'Status':<10} {'Type':<18} {'Title'}")
        click.echo("-" * 70)
        for t in recent:
            click.echo(
                f"#{t.issue_number:<5} {t.status.value:<10} {t.task_type.value:<18} "
                f"{t.title[:40]}"
            )


def show_status(as_json: bool = False) -> None:
    """Display current queue state."""
    import json as json_mod

    state = _load_state()

    if not state.tasks and not state.last_poll_at:
        click.echo("No queue state found. Run 'praxis queue run' to start the daemon.")
        return

    if as_json:
        click.echo(json_mod.dumps(state.model_dump(mode="json"), indent=2, default=str))
        return

    _print_summary(state)
