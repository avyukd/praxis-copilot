"""Praxis IPC — thin wrapper around the generic claude_ipc library.

Configures the IPCBridge with the praxis data directory.
All the generic IPC logic lives in lib/claude_ipc/.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add lib/ to path so claude_ipc is importable
_lib_dir = str(Path(__file__).resolve().parent.parent.parent / "lib")
if _lib_dir not in sys.path:
    sys.path.insert(0, _lib_dir)

from claude_ipc import IPCBridge, IPCFinding, IPCResult, IPCTask

from cli.config_utils import find_repo_root


def _get_bridge() -> IPCBridge:
    return IPCBridge(find_repo_root() / "data" / "ipc")


# Re-export convenience functions that use the praxis bridge
def create_task(task_type, description, **kwargs):
    return _get_bridge().create_task(task_type, description, **kwargs)

def get_pending_tasks():
    return _get_bridge().get_pending_tasks()

def claim_task(task_id):
    return _get_bridge().claim_task(task_id)

def submit_result(task_id, finding, **kwargs):
    return _get_bridge().submit_result(task_id, finding, **kwargs)

def write_finding(content, **kwargs):
    return _get_bridge().write_finding(content, **kwargs)

def read_inbox(limit=20):
    return _get_bridge().read_inbox(limit)

def get_unprocessed_findings():
    return _get_bridge().get_unprocessed_findings()

def mark_finding_processed(path):
    return _get_bridge().mark_finding_processed(path)

def load_sources():
    return _get_bridge().load_sources()

def get_source_url(source_name, ticker="", query=""):
    return _get_bridge().get_source_url(source_name, ticker, query)
