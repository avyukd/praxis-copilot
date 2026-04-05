"""claude_ipc — Generic IPC framework for Claude Code ↔ Claude Desktop.

A lightweight, filesystem-based communication layer. No dependencies
beyond pydantic and pyyaml. Drop into any project.

Usage:
    from claude_ipc import IPCBridge

    bridge = IPCBridge("/path/to/data/ipc")
    bridge.create_task("browse", "Check Twitter for $CLMT", ticker="CLMT", url="...")
    tasks = bridge.get_pending_tasks()
    bridge.submit_result(task_id, "Found bullish sentiment...")
    bridge.write_finding("Unusual activity on MCC for FOTB", ticker="FOTB")
"""

from claude_ipc.bridge import IPCBridge
from claude_ipc.models import IPCTask, IPCResult, IPCFinding

__all__ = ["IPCBridge", "IPCTask", "IPCResult", "IPCFinding"]
