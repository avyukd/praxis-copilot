"""IPCBridge — the main interface for Claude Code ↔ Desktop communication.

All methods are filesystem-based. No network, no database, no external deps.
Pass a base directory and the bridge manages tasks/, results/, and inbox/ under it.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from claude_ipc.models import IPCFinding, IPCResult, IPCTask

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


class IPCBridge:
    """Filesystem-based IPC between Claude Code and Claude Desktop."""

    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)
        self.tasks_dir = self.base_dir / "tasks"
        self.results_dir = self.base_dir / "results"
        self.inbox_dir = self.base_dir / "inbox"

        # Ensure directories exist
        for d in (self.tasks_dir, self.results_dir, self.inbox_dir):
            d.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Task management (Code → Desktop)
    # ------------------------------------------------------------------

    def _next_task_id(self) -> str:
        now = datetime.now(ET)
        date_str = now.strftime("%Y%m%d")
        existing = list(self.tasks_dir.glob(f"task_{date_str}_*.yaml"))
        seq = len(existing) + 1
        return f"{date_str}_{seq:03d}"

    def create_task(
        self,
        task_type: str,
        description: str,
        *,
        ticker: str = "",
        url: str = "",
        search_query: str = "",
        sources: list[str] | None = None,
        priority: str = "medium",
        created_by: str = "code",
        timeout_minutes: int = 30,
    ) -> IPCTask:
        """Create a task for Desktop to process."""
        task_id = self._next_task_id()
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
            timeout_minutes=timeout_minutes,
        )
        path = self.tasks_dir / f"task_{task_id}.yaml"
        path.write_text(yaml.dump(task.model_dump(), default_flow_style=False, sort_keys=False))
        return task

    def get_pending_tasks(self) -> list[IPCTask]:
        """Get all pending tasks."""
        tasks = []
        for path in sorted(self.tasks_dir.glob("task_*.yaml")):
            try:
                raw = yaml.safe_load(path.read_text()) or {}
                task = IPCTask(**raw)
                if task.status == "pending":
                    tasks.append(task)
            except Exception:
                continue
        return tasks

    def claim_task(self, task_id: str) -> bool:
        """Mark a task as in_progress."""
        path = self.tasks_dir / f"task_{task_id}.yaml"
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
        self,
        task_id: str,
        finding: str,
        *,
        ticker: str = "",
        actionability: str = "none",
        urgency: str = "low",
        screenshots: list[str] | None = None,
        sources_checked: list[str] | None = None,
        metadata: dict | None = None,
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
            metadata=metadata or {},
        )
        result_path = self.results_dir / f"result_{task_id}.yaml"
        result_path.write_text(yaml.dump(result.model_dump(), default_flow_style=False, sort_keys=False))

        # Update task status
        task_path = self.tasks_dir / f"task_{task_id}.yaml"
        if task_path.exists():
            try:
                raw = yaml.safe_load(task_path.read_text()) or {}
                raw["status"] = "done"
                task_path.write_text(yaml.dump(raw, default_flow_style=False, sort_keys=False))
            except Exception:
                pass
        return result

    def expire_stale_tasks(self, max_age_minutes: int = 30) -> int:
        """Mark pending tasks older than max_age as expired. Returns count."""
        now = datetime.now(ET)
        expired = 0
        for path in self.tasks_dir.glob("task_*.yaml"):
            try:
                raw = yaml.safe_load(path.read_text()) or {}
                if raw.get("status") != "pending":
                    continue
                created = datetime.fromisoformat(raw.get("created_at", ""))
                if (now - created).total_seconds() > max_age_minutes * 60:
                    raw["status"] = "expired"
                    path.write_text(yaml.dump(raw, default_flow_style=False, sort_keys=False))
                    expired += 1
            except Exception:
                continue
        return expired

    # ------------------------------------------------------------------
    # Inbox (Desktop → Code, free-form findings)
    # ------------------------------------------------------------------

    def write_finding(
        self,
        content: str,
        *,
        ticker: str = "",
        actionability: str = "none",
        urgency: str = "low",
        source: str = "desktop",
        metadata: dict | None = None,
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
            metadata=metadata or {},
        )

        filename = f"finding{ticker_slug}_{timestamp}.yaml"
        path = self.inbox_dir / filename
        path.write_text(yaml.dump(finding.model_dump(), default_flow_style=False, sort_keys=False))
        return path

    def read_inbox(self, limit: int = 20) -> list[IPCFinding]:
        """Read recent findings from the inbox."""
        findings = []
        paths = sorted(self.inbox_dir.glob("finding_*.yaml"), reverse=True)
        for path in paths[:limit]:
            try:
                raw = yaml.safe_load(path.read_text()) or {}
                findings.append(IPCFinding(**raw))
            except Exception:
                continue
        return findings

    def get_unprocessed_findings(self) -> list[tuple[Path, IPCFinding]]:
        """Get inbox findings that haven't been processed yet."""
        results = []
        for path in sorted(self.inbox_dir.glob("finding_*.yaml")):
            processed_marker = path.with_suffix(".processed")
            if processed_marker.exists():
                continue
            try:
                raw = yaml.safe_load(path.read_text()) or {}
                results.append((path, IPCFinding(**raw)))
            except Exception:
                continue
        return results

    def mark_finding_processed(self, path: Path) -> None:
        """Mark an inbox finding as processed."""
        marker = path.with_suffix(".processed")
        marker.write_text(datetime.now(ET).isoformat())

    # ------------------------------------------------------------------
    # Sources config
    # ------------------------------------------------------------------

    def load_sources(self) -> dict:
        """Load browse sources configuration."""
        path = self.base_dir / "sources.yaml"
        if path.exists():
            try:
                return yaml.safe_load(path.read_text()) or {}
            except Exception:
                pass
        return {}

    def get_source_url(self, source_name: str, ticker: str = "", query: str = "") -> str | None:
        """Get a browse URL for a source, with ticker/query substituted."""
        sources = self.load_sources().get("sources", {})
        source = sources.get(source_name)
        if not source:
            return None
        template = source.get("url_template", "")
        return template.replace("${ticker}", ticker).replace("${query}", query or ticker)
