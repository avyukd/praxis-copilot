"""Tactical context fetcher and research session launcher for `praxis research run`."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import yaml
from botocore.exceptions import ClientError

from cli.s3 import BUCKET, get_s3_client, list_prefix

logger = logging.getLogger(__name__)

MONITORS_PREFIX = "data/monitors"
FILINGS_PREFIX = "data/raw/filings"
PRESS_RELEASES_PREFIX = "data/raw/press_releases"


def fetch_tactical_context(ticker: str) -> str:
    """Pull latest monitor snapshots, filing analyses, and press release analyses for a ticker.

    Returns formatted context string. Warns if nothing is found.
    """
    s3 = get_s3_client()
    sections: list[str] = []

    # 1. Monitor snapshots for this ticker
    monitor_section = _fetch_monitor_snapshots(s3, ticker)
    if monitor_section:
        sections.append(monitor_section)

    # 2. Recent filing analyses
    filing_section = _fetch_filing_analyses(s3, ticker)
    if filing_section:
        sections.append(filing_section)

    # 3. Recent press release analyses
    pr_section = _fetch_press_release_analyses(s3, ticker)
    if pr_section:
        sections.append(pr_section)

    if not sections:
        return ""

    return "\n\n---\n\n".join(sections)


def _fetch_monitor_snapshots(s3, ticker: str) -> str | None:
    """Fetch latest snapshots from monitors that cover this ticker."""
    # List all monitor configs to find ones covering this ticker
    config_prefix = "config/monitors/"
    config_keys = list_prefix(s3, config_prefix)

    matching_monitor_ids: list[str] = []
    for key in config_keys:
        if not key.endswith(".yaml"):
            continue
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            config = yaml.safe_load(obj["Body"].read().decode())
            if not isinstance(config, dict):
                continue
            tickers = config.get("tickers", [])
            if isinstance(tickers, list) and ticker.upper() in [t.upper() for t in tickers]:
                monitor_id = config.get("id", key.split("/")[-1].replace(".yaml", ""))
                matching_monitor_ids.append(monitor_id)
        except Exception:
            continue

    if not matching_monitor_ids:
        return None

    # Fetch latest snapshot for each matching monitor
    snapshots: list[str] = []
    for monitor_id in matching_monitor_ids:
        key = f"{MONITORS_PREFIX}/{monitor_id}/latest.yaml"
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            snap = yaml.safe_load(obj["Body"].read().decode())
            if not isinstance(snap, dict):
                continue
            status = snap.get("status", "unknown")
            significance = snap.get("significance", "low")
            current_state = snap.get("current_state", "")
            delta = snap.get("delta_from_previous", "")
            date = snap.get("date", "")

            if current_state and current_state != "Seeded — awaiting first delta":
                entry = f"**{monitor_id}** (last: {date}, status: {status}, significance: {significance})"
                if delta and delta not in ("", "Initial observation"):
                    entry += f"\nDelta: {delta}"
                entry += f"\n{current_state[:2000]}"
                snapshots.append(entry)
        except ClientError:
            continue

    if not snapshots:
        return None

    return f"### Monitor Snapshots for {ticker}\n\n" + "\n\n".join(snapshots)


def _fetch_filing_analyses(s3, ticker: str) -> str | None:
    """Fetch recent filing analyses for a ticker."""
    # Load ticker registry to get CIK
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="config/ticker_registry.yaml")
        registry = yaml.safe_load(obj["Body"].read().decode()) or {}
    except Exception:
        return None

    tickers = registry.get("tickers", {})
    entry = tickers.get(ticker.upper(), {})
    cik = entry.get("cik", "") if isinstance(entry, dict) else ""
    if not cik:
        return None

    # List analysis files for this CIK (most recent first)
    prefix = f"{FILINGS_PREFIX}/{cik}/"
    keys = list_prefix(s3, prefix)
    analysis_keys = sorted(
        [k for k in keys if k.endswith("/analysis.json")],
        reverse=True,
    )[:5]  # Last 5 filings

    if not analysis_keys:
        return None

    analyses: list[str] = []
    for key in analysis_keys:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            data = json.loads(obj["Body"].read())
            form_type = data.get("form_type", "?")
            significance = data.get("significance", "unknown")
            summary = data.get("summary", data.get("analysis", ""))
            if summary:
                analyses.append(f"**{form_type}** (significance: {significance})\n{summary[:1500]}")
        except Exception:
            continue

    if not analyses:
        return None

    return f"### Recent Filing Analyses for {ticker}\n\n" + "\n\n".join(analyses)


def _fetch_press_release_analyses(s3, ticker: str) -> str | None:
    """Fetch recent press release analyses for a ticker."""
    # Press releases are stored under data/raw/press_releases/{source}/{ticker}/
    analyses: list[str] = []

    for source in ("gnw", "bw", "prn"):
        prefix = f"{PRESS_RELEASES_PREFIX}/{source}/{ticker.upper()}/"
        keys = list_prefix(s3, prefix)
        analysis_keys = sorted(
            [k for k in keys if k.endswith("/analysis.json")],
            reverse=True,
        )[:3]

        for key in analysis_keys:
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=key)
                data = json.loads(obj["Body"].read())
                significance = data.get("significance", "unknown")
                summary = data.get("summary", data.get("analysis", ""))
                headline = data.get("headline", data.get("title", ""))
                if summary:
                    entry = f"**{headline}** (significance: {significance})" if headline else f"(significance: {significance})"
                    entry += f"\n{summary[:1500]}"
                    analyses.append(entry)
            except Exception:
                continue

    if not analyses:
        return None

    return f"### Recent Press Release Analyses for {ticker}\n\n" + "\n\n".join(analyses)


def build_run_prompt(ticker: str, tactical: bool, tactical_context: str) -> str:
    """Build the initial prompt for a research session."""
    if not tactical:
        return (
            f"Analyze {ticker} stock using the process described in CLAUDE.md."
        )

    if not tactical_context:
        return (
            f"Analyze {ticker} stock using the process described in CLAUDE.md.\n\n"
            f"NOTE: --tactical was requested but no recent alerts, analyses, or monitor "
            f"snapshots were found for {ticker}. Proceed with standard analysis."
        )

    return (
        f"Analyze {ticker} stock using the process described in CLAUDE.md.\n\n"
        f"Additionally, the monitoring system has flagged the following recent context. "
        f"Incorporate this into your analysis and provide a tactical read on where "
        f"the stock should trade given these developments:\n\n"
        f"<tactical-context>\n{tactical_context}\n</tactical-context>"
    )


def write_prompt_file(workspace: Path, prompt: str) -> Path:
    """Write the initial prompt to a file in the workspace. Returns the path."""
    prompt_path = workspace / ".research-prompt.txt"
    prompt_path.write_text(prompt)
    return prompt_path


# ---------------------------------------------------------------------------
# Session launcher
# ---------------------------------------------------------------------------

def _find_claude() -> str:
    """Resolve the claude binary path."""
    found = shutil.which("claude")
    if found:
        return found
    local_bin = Path.home() / ".local" / "bin" / "claude"
    if local_bin.exists():
        return str(local_bin)
    raise FileNotFoundError("Could not find 'claude' binary on PATH or ~/.local/bin/claude")


def _run_session(
    claude_bin: str,
    ticker: str,
    prompt: str,
    session_id: str,
    workspace: Path,
) -> tuple[str, str, bool, str]:
    """Run a single Claude session. Returns (ticker, session_id, success, output)."""
    env = os.environ.copy()
    env.pop("CLAUDE_CODE_ENTRYPOINT", None)
    env.pop("CLAUDECODE", None)

    try:
        result = subprocess.run(
            [claude_bin, "-p", prompt, "--allowedTools", "*", "--session-id", session_id],
            cwd=workspace,
            capture_output=True,
            text=True,
            env=env,
        )
        output = result.stdout + result.stderr
        return (ticker, session_id, result.returncode == 0, output)
    except Exception as e:
        return (ticker, session_id, False, str(e))


def launch_sessions(
    sessions: list[tuple[str, Path, str]],
    max_parallel: int = 4,
    on_status: Any = None,
    session_map: dict[str, str] | None = None,
) -> tuple[list[tuple[str, str, bool, str]], dict[str, str]]:
    """Launch Claude sessions in parallel.

    Args:
        sessions: List of (ticker, workspace, prompt) tuples.
        max_parallel: Max concurrent sessions.
        on_status: Optional callback(ticker, session_id, success) called as each finishes.
        session_map: Optional pre-built {ticker: session_id} map. Generated if not provided.

    Returns:
        Tuple of (results list, session_map). Results are (ticker, session_id, success, output).
    """
    claude_bin = _find_claude()

    if session_map is None:
        session_map = {ticker: str(uuid.uuid4()) for ticker, _, _ in sessions}

    results: list[tuple[str, str, bool, str]] = []
    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        futures = {
            pool.submit(
                _run_session, claude_bin, ticker, prompt, session_map[ticker], workspace
            ): ticker
            for ticker, workspace, prompt in sessions
        }
        for future in as_completed(futures):
            ticker, sid, success, output = future.result()
            if on_status:
                on_status(ticker, sid, success)
            results.append((ticker, sid, success, output))

    return results, session_map
