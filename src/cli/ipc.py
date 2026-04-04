"""IPC system — Claude Code ↔ Claude Desktop communication.

Tasks flow: Code creates tasks → Desktop picks them up → Desktop writes results
Findings flow: Desktop writes to inbox → Code daemons pick them up

All communication via shared filesystem under data/ipc/.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
from pydantic import BaseModel, Field

from cli.config_utils import find_repo_root

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class IPCTask(BaseModel):
    id: str = ""
    created_at: str = ""
    created_by: str = ""
    status: str = "pending"  # pending, in_progress, done, failed
    priority: str = "medium"

    type: str = "browse"  # browse, screenshot, search, monitor, research
    ticker: str = ""
    description: str = ""

    url: str = ""
    search_query: str = ""
    sources: list[str] = Field(default_factory=list)

    output_format: str = "markdown"


class IPCResult(BaseModel):
    task_id: str = ""
    completed_at: str = ""
    status: str = "done"
    ticker: str = ""

    finding: str = ""
    actionability: str = "none"  # none, monitor, research_deeper, trade_idea
    urgency: str = "low"

    screenshots: list[str] = Field(default_factory=list)
    sources_checked: list[str] = Field(default_factory=list)


class IPCFinding(BaseModel):
    ticker: str = ""
    timestamp: str = ""
    source: str = ""  # who wrote it: desktop, user, analyst
    content: str = ""
    actionability: str = "none"
    urgency: str = "low"


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------


def _ipc_dir() -> Path:
    d = find_repo_root() / "data" / "ipc"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _tasks_dir() -> Path:
    d = _ipc_dir() / "tasks"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _results_dir() -> Path:
    d = _ipc_dir() / "results"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _inbox_dir() -> Path:
    d = _ipc_dir() / "inbox"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---------------------------------------------------------------------------
# Task management (Code → Desktop)
# ---------------------------------------------------------------------------


def _next_task_id() -> str:
    now = datetime.now(ET)
    date_str = now.strftime("%Y%m%d")
    existing = list(_tasks_dir().glob(f"task_{date_str}_*.yaml"))
    seq = len(existing) + 1
    return f"{date_str}_{seq:03d}"


def create_task(
    task_type: str,
    description: str,
    *,
    ticker: str = "",
    url: str = "",
    search_query: str = "",
    sources: list[str] | None = None,
    priority: str = "medium",
    created_by: str = "code",
) -> IPCTask:
    """Create a task for Claude Desktop to process."""
    task_id = _next_task_id()
    task = IPCTask(
        id=task_id,
        created_at=datetime.now(ET).isoformat(),
        created_by=created_by,
        priority=priority,
        type=task_type,
        ticker=ticker,
        description=description,
        url=url,
        search_query=search_query,
        sources=sources or [],
    )

    path = _tasks_dir() / f"task_{task_id}.yaml"
    path.write_text(yaml.dump(task.model_dump(), default_flow_style=False, sort_keys=False))
    return task


def get_pending_tasks() -> list[IPCTask]:
    """Get all pending tasks for Desktop to process."""
    tasks = []
    for path in sorted(_tasks_dir().glob("task_*.yaml")):
        try:
            raw = yaml.safe_load(path.read_text()) or {}
            task = IPCTask(**raw)
            if task.status == "pending":
                tasks.append(task)
        except Exception:
            continue
    return tasks


def claim_task(task_id: str) -> bool:
    """Mark a task as in_progress."""
    path = _tasks_dir() / f"task_{task_id}.yaml"
    if not path.exists():
        return False
    try:
        raw = yaml.safe_load(path.read_text()) or {}
        raw["status"] = "in_progress"
        path.write_text(yaml.dump(raw, default_flow_style=False, sort_keys=False))
        return True
    except Exception:
        return False


def submit_result(
    task_id: str,
    finding: str,
    *,
    ticker: str = "",
    actionability: str = "none",
    urgency: str = "low",
    screenshots: list[str] | None = None,
    sources_checked: list[str] | None = None,
) -> IPCResult:
    """Submit a result for a completed task."""
    result = IPCResult(
        task_id=task_id,
        completed_at=datetime.now(ET).isoformat(),
        ticker=ticker,
        finding=finding,
        actionability=actionability,
        urgency=urgency,
        screenshots=screenshots or [],
        sources_checked=sources_checked or [],
    )

    # Write result
    result_path = _results_dir() / f"result_{task_id}.yaml"
    result_path.write_text(yaml.dump(result.model_dump(), default_flow_style=False, sort_keys=False))

    # Update task status
    task_path = _tasks_dir() / f"task_{task_id}.yaml"
    if task_path.exists():
        try:
            raw = yaml.safe_load(task_path.read_text()) or {}
            raw["status"] = "done"
            task_path.write_text(yaml.dump(raw, default_flow_style=False, sort_keys=False))
        except Exception:
            pass

    return result


# ---------------------------------------------------------------------------
# Inbox (Desktop → Code, free-form findings)
# ---------------------------------------------------------------------------


def write_finding(
    content: str,
    *,
    ticker: str = "",
    actionability: str = "none",
    urgency: str = "low",
    source: str = "desktop",
) -> Path:
    """Write a free-form finding to the inbox."""
    now = datetime.now(ET)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    ticker_slug = f"_{ticker}" if ticker else ""

    finding = IPCFinding(
        ticker=ticker,
        timestamp=now.isoformat(),
        source=source,
        content=content,
        actionability=actionability,
        urgency=urgency,
    )

    filename = f"finding{ticker_slug}_{timestamp}.yaml"
    path = _inbox_dir() / filename
    path.write_text(yaml.dump(finding.model_dump(), default_flow_style=False, sort_keys=False))
    return path


def read_inbox(limit: int = 20) -> list[IPCFinding]:
    """Read recent findings from the inbox."""
    findings = []
    paths = sorted(_inbox_dir().glob("finding_*.yaml"), reverse=True)
    for path in paths[:limit]:
        try:
            raw = yaml.safe_load(path.read_text()) or {}
            findings.append(IPCFinding(**raw))
        except Exception:
            continue
    return findings


def get_unprocessed_findings() -> list[tuple[Path, IPCFinding]]:
    """Get inbox findings that haven't been processed by daemons yet."""
    results = []
    for path in sorted(_inbox_dir().glob("finding_*.yaml")):
        # Check if there's a .processed marker
        processed_marker = path.with_suffix(".processed")
        if processed_marker.exists():
            continue
        try:
            raw = yaml.safe_load(path.read_text()) or {}
            results.append((path, IPCFinding(**raw)))
        except Exception:
            continue
    return results


def mark_finding_processed(path: Path) -> None:
    """Mark an inbox finding as processed."""
    marker = path.with_suffix(".processed")
    marker.write_text(datetime.now(ET).isoformat())


# ---------------------------------------------------------------------------
# Sources config
# ---------------------------------------------------------------------------


def load_sources() -> dict:
    """Load browse sources configuration."""
    path = _ipc_dir() / "sources.yaml"
    if path.exists():
        try:
            return yaml.safe_load(path.read_text()) or {}
        except Exception:
            pass
    return {}


def get_source_url(source_name: str, ticker: str = "", query: str = "") -> str | None:
    """Get a browse URL for a source, with ticker/query substituted."""
    sources = load_sources().get("sources", {})
    source = sources.get(source_name)
    if not source:
        return None
    template = source.get("url_template", "")
    return template.replace("${ticker}", ticker).replace("${query}", query or ticker)
