"""Capacity detection and adaptive throttling for praxis daemons.

Provides capacity checks for the queue daemon, analyst agent, and other
background processes. Tracks rate limits and adjusts behavior to stay
under ~80% of Claude Max subscription capacity.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from cli.config_utils import find_repo_root

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


def _count_claude_processes() -> tuple[int, int]:
    """Count running claude processes.

    Returns (interactive_count, batch_count).
    Interactive = user-facing Claude Code sessions (attached to a terminal).
    Batch = daemon-spawned sessions (no terminal, running in background).
    """
    try:
        result = subprocess.run(
            ["bash", "-c", "ps -eo pid,tty,args | grep '[c]laude'"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return 0, 0

        interactive = 0
        batch = 0
        for line in result.stdout.strip().splitlines():
            if not line.strip():
                continue
            parts = line.split(None, 2)
            if len(parts) < 3:
                continue
            tty = parts[1]
            cmd = parts[2]
            # Skip non-claude processes
            if "claude" not in cmd.lower():
                continue
            # Skip tmux/shell wrappers
            if "tmux" in cmd:
                continue
            # Batch processes have no terminal (tty = "??")
            if tty == "??":
                batch += 1
            else:
                interactive += 1
        return interactive, batch
    except Exception:
        logger.debug("process check failed", exc_info=True)
        return 0, 0


def _in_filing_research_window() -> bool:
    """Check if we're in the filing research daemon's active window (8:30-10:00 ET)."""
    now = datetime.now(ET)
    hour = now.hour + now.minute / 60.0
    return 8.5 <= hour < 10.0


def check_capacity() -> int:
    """Return number of available queue slots (0-2).

    Used by the queue daemon and other background processors.
    """
    _, batch = _count_claude_processes()

    # During filing research window, limit to 1 queue slot
    if _in_filing_research_window():
        return 0 if batch >= 4 else 1

    # Outside window, allow 2 slots unless heavily loaded
    if batch >= 8:
        return 0
    return 2


def check_analyst_capacity() -> bool:
    """Check if the analyst agent should run.

    The analyst mostly does tiny haiku triages — it's lightweight.
    Only block it when we're truly saturated.
    """
    _, batch = _count_claude_processes()

    # Only block if heavily loaded (8+ batch processes)
    return batch < 8


# ---------------------------------------------------------------------------
# CapacityTracker — adaptive throttling for the analyst agent
# ---------------------------------------------------------------------------


class CapacityTracker:
    """Tracks rate limits and adapts batch size to stay under ~80% capacity.

    Persists state to disk so it survives daemon restarts.
    """

    def __init__(self, state_path: Path | None = None):
        self.state_path = state_path or (find_repo_root() / "data" / "capacity_state.yaml")
        self.rate_limit_hits: list[datetime] = []
        self.last_success: datetime | None = None
        self.current_backoff_seconds: int = 0
        self.base_batch_size: int = 5
        self._load()

    def _load(self) -> None:
        if self.state_path.exists():
            try:
                raw = yaml.safe_load(self.state_path.read_text()) or {}
                self.rate_limit_hits = [
                    datetime.fromisoformat(ts) for ts in raw.get("rate_limit_hits", [])
                    if isinstance(ts, str)
                ]
                self.current_backoff_seconds = raw.get("current_backoff_seconds", 0)
                self.base_batch_size = raw.get("base_batch_size", 5)
                ls = raw.get("last_success")
                if ls:
                    self.last_success = datetime.fromisoformat(ls) if isinstance(ls, str) else None
            except Exception:
                pass

    def save(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        # Only keep recent hits (last 2 hours)
        cutoff = datetime.now(ET) - timedelta(hours=2)
        self.rate_limit_hits = [h for h in self.rate_limit_hits if h > cutoff]
        # Reset backoff if no recent rate limits (clean slate for new day)
        if not self.rate_limit_hits:
            self.current_backoff_seconds = 0
            self.base_batch_size = max(self.base_batch_size, 5)
        data = {
            "rate_limit_hits": [h.isoformat() for h in self.rate_limit_hits],
            "current_backoff_seconds": self.current_backoff_seconds,
            "base_batch_size": self.base_batch_size,
            "last_success": self.last_success.isoformat() if self.last_success else None,
        }
        self.state_path.write_text(yaml.dump(data, default_flow_style=False))

    def should_run(self) -> bool:
        """Check if we should run a task right now.

        Uses telemetry capacity estimate + rate limit history + process count.
        """
        # Process-based check
        if not check_analyst_capacity():
            return False

        # Telemetry-based capacity check (80% target)
        try:
            from cli.telemetry import get_capacity_estimate
            cap = get_capacity_estimate()
            if cap.get("at_target", False):
                return False  # At or above 80% — stop
        except Exception:
            pass  # Telemetry not available, fall back to other checks

        # Rate limit backoff
        if self.current_backoff_seconds > 0:
            if self.last_success:
                elapsed = (datetime.now(ET) - self.last_success).total_seconds()
                if elapsed < self.current_backoff_seconds:
                    return False

        now = datetime.now(ET)
        recent_30m = sum(1 for h in self.rate_limit_hits if h > now - timedelta(minutes=30))
        recent_1h = sum(1 for h in self.rate_limit_hits if h > now - timedelta(hours=1))

        if recent_30m >= 3:
            return False
        if recent_1h >= 5:
            return False

        return True

    def on_rate_limit(self) -> None:
        """Record a rate limit hit and increase backoff."""
        self.rate_limit_hits.append(datetime.now(ET))
        # Exponential backoff: 60s, 120s, 240s, 480s, max 900s (15 min)
        if self.current_backoff_seconds == 0:
            self.current_backoff_seconds = 60
        else:
            self.current_backoff_seconds = min(self.current_backoff_seconds * 2, 900)
        # Reduce batch size
        self.base_batch_size = max(1, self.base_batch_size - 1)
        self.save()

    def on_success(self) -> None:
        """Record a successful task and gradually reduce backoff."""
        self.last_success = datetime.now(ET)
        # Gradually recover
        if self.current_backoff_seconds > 0:
            self.current_backoff_seconds = max(0, self.current_backoff_seconds - 15)
        # Gradually increase batch size (max 6)
        if self.current_backoff_seconds == 0 and self.base_batch_size < 6:
            self.base_batch_size = min(6, self.base_batch_size + 1)
        self.save()

    @property
    def recommended_batch_size(self) -> int:
        """How many tasks to run in the current batch."""
        now = datetime.now(ET)
        recent_1h = sum(1 for h in self.rate_limit_hits if h > now - timedelta(hours=1))
        if recent_1h >= 3:
            return 1
        if recent_1h >= 1:
            return max(1, self.base_batch_size - 2)
        return self.base_batch_size

    def is_rate_limited(self, stderr: str) -> bool:
        """Check if a Claude CLI error indicates a rate limit."""
        rate_limit_indicators = [
            "rate limit",
            "too many requests",
            "429",
            "capacity",
            "overloaded",
        ]
        lower = stderr.lower()
        return any(ind in lower for ind in rate_limit_indicators)
