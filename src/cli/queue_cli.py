"""CLI commands for the research queue."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import click

from cli.config_utils import find_repo_root


@click.group("queue")
def queue():
    """Research queue: GitHub Issues -> Claude CLI -> artifacts."""
    pass


@queue.command("run")
@click.option("--poll-interval-idle", type=int, default=300, show_default=True, help="Seconds between polls when idle")
@click.option("--poll-interval-active", type=int, default=60, show_default=True, help="Seconds between polls when items queued")
@click.option("--max-parallel", type=int, default=2, show_default=True, help="Max concurrent research sessions")
@click.option("--dry-run", is_flag=True, help="Poll and classify but don't run research")
def queue_run(poll_interval_idle: int, poll_interval_active: int, max_parallel: int, dry_run: bool):
    """Start the research queue daemon.

    \b
    Polls GitHub Issues labeled 'research-queue' for new tasks,
    waits for idle Claude CLI capacity, then runs research sessions.
    Results are posted back to the issue and synced to S3.

    \b
    Examples:
      praxis queue run
      praxis queue run --dry-run
      praxis queue run --max-parallel 1
    """
    from cli.queue_daemon import run_daemon

    run_daemon(
        poll_interval_idle=poll_interval_idle,
        poll_interval_active=poll_interval_active,
        max_parallel=max_parallel,
        dry_run=dry_run,
    )


@queue.command("status")
@click.option("--json", "as_json", is_flag=True, help="Output raw JSON state")
def queue_status(as_json: bool):
    """Show current research queue status.

    \b
    Examples:
      praxis queue status
      praxis queue status --json
    """
    from cli.queue_daemon import show_status

    show_status(as_json=as_json)


@queue.command("add")
@click.argument("title")
@click.option("--body", "-b", default="", help="Issue body text")
@click.option("--image", "-i", multiple=True, help="Attach image file(s)")
@click.option("--link", "-l", multiple=True, help="Include URL(s) in body")
@click.option("--ticker", "-t", multiple=True, help="Tag ticker(s) in body")
def queue_add(title: str, body: str, image: tuple[str, ...], link: tuple[str, ...], ticker: tuple[str, ...]):
    """Create a research queue issue on GitHub.

    \b
    Examples:
      praxis queue add "Research NVDA earnings impact on semis"
      praxis queue add "Compare HCC vs ARCH" -t HCC -t ARCH
      praxis queue add "What's this 8-K about?" -i screenshot.png
      praxis queue add "Look into this thread" -l https://x.com/...
    """
    from cli.queue_github import QUEUE_LABEL, REPO

    # Build body
    parts = []
    if ticker:
        parts.append("Tickers: " + ", ".join(f"${t.upper()}" for t in ticker))
    if body:
        parts.append(body)
    if link:
        parts.append("\n".join(link))

    full_body = "\n\n".join(parts) if parts else ""

    cmd = [
        "gh", "issue", "create",
        "--repo", REPO,
        "--label", QUEUE_LABEL,
        "--title", title,
        "--body", full_body,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode == 0:
        click.echo(f"Created: {result.stdout.strip()}")
    else:
        click.echo(f"Failed: {result.stderr}")

    # Note: --image flag for gh issue create doesn't exist natively.
    # Images need to be uploaded via the GitHub UI or API.
    if image:
        click.echo(f"Note: {len(image)} image(s) specified. Upload them to the issue via GitHub UI.")
        click.echo(f"  Issue: {result.stdout.strip()}")


@queue.command("schedule")
def queue_schedule():
    """Install the launchd plist to run the queue daemon continuously."""
    plist_name = "com.praxis.research-queue.plist"
    repo_root = find_repo_root()
    source = repo_root / plist_name

    if not source.exists():
        click.echo(f"Plist not found at {source}")
        return

    dest_dir = Path.home() / "Library" / "LaunchAgents"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / plist_name

    shutil.copy2(source, dest)
    click.echo(f"Installed plist to {dest}")

    result = subprocess.run(["launchctl", "load", str(dest)], capture_output=True, text=True)
    if result.returncode == 0:
        click.echo("Loaded into launchd. Queue daemon is now running.")
    else:
        click.echo(f"launchctl load failed: {result.stderr}")
        click.echo(f"Try manually: launchctl load {dest}")


@queue.command("unschedule")
def queue_unschedule():
    """Remove the launchd plist."""
    plist_name = "com.praxis.research-queue.plist"
    dest = Path.home() / "Library" / "LaunchAgents" / plist_name

    if not dest.exists():
        click.echo("No plist found. Not currently scheduled.")
        return

    subprocess.run(["launchctl", "unload", str(dest)], capture_output=True, text=True)
    dest.unlink()
    click.echo("Unloaded and removed launchd plist.")
