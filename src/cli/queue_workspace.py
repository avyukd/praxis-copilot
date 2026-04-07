"""Workspace setup for research queue tasks."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

from cli.config_utils import find_repo_root, get_config_dir, load_yaml
from cli.models import TickerRegistry
from cli.queue_github import download_image
from cli.queue_models import QueueTask, QueueTaskType
from cli.queue_prompt import generate_queue_prompt
from cli.s3 import download_file, get_s3_client, list_prefix, upload_file

logger = logging.getLogger(__name__)

CODEX_BIN = "/opt/homebrew/bin/codex"


def _ensure_codex_mcp(workspace: Path) -> None:
    """Add codex-cli MCP server to the workspace .mcp.json."""
    mcp_path = workspace / ".mcp.json"
    if mcp_path.exists():
        try:
            config = json.loads(mcp_path.read_text())
        except Exception:
            config = {"mcpServers": {}}
    else:
        config = {"mcpServers": {}}

    config.setdefault("mcpServers", {})["codex-cli"] = {
        "command": CODEX_BIN,
        "args": ["--mcp"],
    }
    mcp_path.write_text(json.dumps(config, indent=2))


def setup_workspace(task: QueueTask) -> Path:
    """Create and populate a workspace for a queue task.

    Returns the workspace path.
    """
    repo_root = find_repo_root()
    workspace = repo_root / "workspace" / "queue" / str(task.issue_number)
    workspace.mkdir(parents=True, exist_ok=True)

    # Download context files (images)
    context_dir = workspace / "context"
    if task.image_urls:
        context_dir.mkdir(parents=True, exist_ok=True)
        for i, url in enumerate(task.image_urls):
            ext = _guess_ext(url)
            dest = context_dir / f"image_{i + 1}{ext}"
            if download_image(url, dest):
                logger.info("Downloaded image to %s", dest)

    # Write links file
    if task.link_urls:
        context_dir.mkdir(parents=True, exist_ok=True)
        links_path = context_dir / "links.txt"
        links_path.write_text("\n".join(task.link_urls))

    # Stage ticker data if applicable
    has_mcp = False
    data_manifest = ""
    if task.tickers and task.task_type in (
        QueueTaskType.TICKER_RESEARCH,
        QueueTaskType.COMPARATIVE,
    ):
        has_mcp, data_manifest = _stage_ticker_data(task, workspace)

    # Ensure codex-cli MCP is configured in workspace
    _ensure_codex_mcp(workspace)

    # Generate CLAUDE.md
    prompt = generate_queue_prompt(
        task=task,
        workspace=workspace,
        data_manifest=data_manifest,
        has_mcp=has_mcp,
    )
    (workspace / "CLAUDE.md").write_text(prompt)

    return workspace


def _stage_ticker_data(task: QueueTask, workspace: Path) -> tuple[bool, str]:
    """Pull ticker data from S3 for research. Returns (has_mcp, data_manifest)."""
    s3 = get_s3_client()
    config_dir = get_config_dir()
    registry_cfg = TickerRegistry(**load_yaml(config_dir / "ticker_registry.yaml"))

    has_mcp = False
    manifest_lines: list[str] = []

    for ticker in task.tickers:
        ticker_upper = ticker.upper()
        data_prefix = f"data/research/{ticker_upper}/data/"
        data_keys = list_prefix(s3, data_prefix)

        if not data_keys:
            logger.info("No ingested data for %s, skipping data pull", ticker_upper)
            continue

        # Pull data into workspace
        if len(task.tickers) == 1:
            data_dir = workspace / "data"
        else:
            data_dir = workspace / "data" / ticker_upper
        data_dir.mkdir(parents=True, exist_ok=True)

        for key in data_keys:
            relative = key[len(data_prefix):]
            if not relative:
                continue
            local_path = data_dir / relative
            local_path.parent.mkdir(parents=True, exist_ok=True)
            content = download_file(s3, key)
            local_path.write_bytes(content)

        # Build manifest for this ticker
        for path in sorted(data_dir.rglob("*")):
            if path.is_file() and path.name != "fundamentals.json":
                rel = path.relative_to(workspace)
                size = path.stat().st_size
                size_str = f"{size // 1024}KB" if size > 1024 else f"{size}B"
                manifest_lines.append(f"  - {rel} ({size_str})")

        # Configure MCP server for fundamentals if available
        fundamentals_path = data_dir / "fundamentals" / "fundamentals.json"
        if fundamentals_path.exists() and len(task.tickers) == 1:
            server_script = str(Path(__file__).parent / "fundamentals_server.py")
            server_env = {}
            eodhd_key = os.environ.get("EODHD_API_KEY", "")
            if eodhd_key:
                server_env["EODHD_API_KEY"] = eodhd_key
            mcp_server_def: dict = {
                "command": sys.executable,
                "args": [server_script, str(fundamentals_path)],
            }
            if server_env:
                mcp_server_def["env"] = server_env
            mcp_config = {"mcpServers": {"fundamentals": mcp_server_def}}
            (workspace / ".mcp.json").write_text(json.dumps(mcp_config, indent=2))
            has_mcp = True

    # Pull macro context
    macro_keys = list_prefix(s3, "data/context/macro/")
    macro_files = [k for k in macro_keys if k != "data/context/macro/"]
    if macro_files:
        macro_dir = workspace / "macro"
        macro_dir.mkdir(parents=True, exist_ok=True)
        for key in macro_files:
            relative = key[len("data/context/macro/"):]
            local_path = macro_dir / relative
            local_path.parent.mkdir(parents=True, exist_ok=True)
            content = download_file(s3, key)
            local_path.write_bytes(content)
        manifest_lines.append("- macro/ -- Current macro views and theses")

    return has_mcp, "\n".join(manifest_lines)


def sync_queue_artifacts(task: QueueTask, workspace: Path) -> int:
    """Sync queue task artifacts to S3. Returns number of files uploaded."""
    s3 = get_s3_client()
    s3_prefix = f"data/queue/{task.issue_number}"

    skip_prefixes = ("data/", "macro/", "context/")
    skip_names = {"CLAUDE.md", ".mcp.json"}

    found: list[Path] = []
    for path in workspace.rglob("*"):
        if path.is_file():
            rel = path.relative_to(workspace)
            rel_str = str(rel)
            if any(rel_str.startswith(p) for p in skip_prefixes):
                continue
            if rel.name in skip_names:
                continue
            found.append(rel)

    for rel in found:
        s3_key = f"{s3_prefix}/{rel}"
        upload_file(s3, workspace / rel, s3_key)

    return len(found)


def _guess_ext(url: str) -> str:
    """Guess file extension from URL."""
    lower = url.lower().split("?")[0]
    for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".pdf"):
        if lower.endswith(ext):
            return ext
    return ".png"
