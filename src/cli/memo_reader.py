"""Memo reader — reads memos from workspace (local) or S3 (fallback).

Workspace is ephemeral. This module ensures all daemons can read memos
even if the workspace has been cleaned up, by falling back to S3.
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from cli.config_utils import find_repo_root

logger = logging.getLogger(__name__)


def read_memo_yaml(ticker: str) -> dict | None:
    """Read memo.yaml for a ticker. Tries workspace first, then S3."""
    # Try local workspace first (fast)
    repo_root = find_repo_root()
    local_path = repo_root / "workspace" / ticker / "memo.yaml"
    if local_path.exists():
        try:
            return yaml.safe_load(local_path.read_text()) or {}
        except Exception:
            pass

    # Fallback to S3
    try:
        from cli.s3 import download_file, get_s3_client
        s3 = get_s3_client()
        raw = download_file(s3, f"data/research/{ticker}/memo.yaml")
        return yaml.safe_load(raw) or {}
    except Exception:
        return None


def read_memo_md(ticker: str) -> str | None:
    """Read memo.md for a ticker. Tries workspace first, then S3."""
    repo_root = find_repo_root()
    local_path = repo_root / "workspace" / ticker / "memo.md"
    if local_path.exists():
        try:
            return local_path.read_text()
        except Exception:
            pass

    try:
        from cli.s3 import download_file, get_s3_client
        s3 = get_s3_client()
        raw = download_file(s3, f"data/research/{ticker}/memo.md")
        return raw.decode("utf-8") if isinstance(raw, bytes) else raw
    except Exception:
        return None


def list_tickers_with_memos() -> list[str]:
    """List all tickers that have memos (workspace + S3)."""
    repo_root = find_repo_root()
    workspace = repo_root / "workspace"
    tickers = set()

    # Local workspace
    if workspace.exists():
        for d in workspace.iterdir():
            if d.is_dir() and d.name not in ("queue", "analyst", "macro"):
                if (d / "memo.yaml").exists():
                    tickers.add(d.name)

    # S3 — check for research directories
    try:
        from cli.s3 import get_s3_client, list_prefix
        s3 = get_s3_client()
        keys = list_prefix(s3, "data/research/")
        for key in keys:
            parts = key.split("/")
            if len(parts) >= 4 and parts[3] == "memo.yaml":
                tickers.add(parts[2])
    except Exception:
        pass

    return sorted(tickers)
