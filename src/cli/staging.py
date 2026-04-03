"""Shared staging and sync helpers for research workflows."""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from pathlib import Path

import click
import yaml

from cli.config_utils import find_repo_root, get_config_dir, load_yaml
from cli.ingest import run_ingestion
from cli.models import TickerRegistry, TickerRegistryEntry, UniverseConfig
from cli.research_prompt import ResearchBudget, generate_research_prompt
from cli.s3 import BUCKET, download_file, get_s3_client, list_prefix, upload_file

logger = logging.getLogger(__name__)


def ingestion_options_for_registry_entry(entry: TickerRegistryEntry | None) -> dict[str, bool]:
    """Return ingestion kwargs based on whether the ticker supports EDGAR."""
    if not entry:
        return {}
    if entry.edgar_supported:
        return {}
    return {
        "skip_sec_filings": True,
        "skip_fundamentals": True,
    }


def stage_ticker(
    ticker: str,
    config_dir: Path,
    registry_cfg: TickerRegistry,
    s3,
    macro_files: list[str],
    *,
    quiet: bool = False,
    cik: str | None = None,
    tactical: bool = False,
) -> Path | None:
    """Stage a single ticker workspace. Returns workspace path or None on failure.

    If *quiet* is True, uses logging instead of click.echo (for daemon usage).
    If *cik* is provided, it is used as a fallback when the ticker is not in the registry.
    """

    def _echo(msg: str) -> None:
        if quiet:
            logger.info(msg)
        else:
            click.echo(msg)

    # Ensure data is ingested
    data_prefix = f"data/research/{ticker}/data/"
    data_keys = list_prefix(s3, data_prefix)
    if not data_keys:
        _echo(f"  No ingested data found. Running ingestion...")
        entry = registry_cfg.tickers.get(ticker)
        ingestion_cik = entry.cik if entry else cik
        if ingestion_cik:
            result = run_ingestion(
                ticker,
                ingestion_cik,
                s3,
                **ingestion_options_for_registry_entry(entry),
            )
            _echo(
                f"  Filings: {result.filings_count}, "
                f"Fundamentals: {result.fundamentals_source or 'N/A'}, "
                f"Transcripts: {result.transcripts_count}"
            )
            data_keys = list_prefix(s3, data_prefix)
        else:
            _echo(f"  No CIK found for {ticker}. Re-add with 'praxis universe add {ticker}'.")
            return None

    # Set up workspace
    repo_root = find_repo_root()
    workspace = repo_root / "workspace" / ticker
    workspace.mkdir(parents=True, exist_ok=True)

    # Pull ingested data into workspace/data/
    _echo(f"  Pulling ingested data...")
    data_dir = workspace / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    for key in data_keys:
        relative = key[len(data_prefix):]
        if not relative:
            continue
        local_path = data_dir / relative
        local_path.parent.mkdir(parents=True, exist_ok=True)
        content = download_file(s3, key)
        local_path.write_bytes(content)
    _echo(f"  {len(data_keys)} file(s) pulled")

    # Pull macro context if it exists
    if macro_files:
        macro_dir = workspace / "macro"
        macro_dir.mkdir(parents=True, exist_ok=True)
        for key in macro_files:
            relative = key[len("data/context/macro/"):]
            local_path = macro_dir / relative
            local_path.parent.mkdir(parents=True, exist_ok=True)
            content = download_file(s3, key)
            local_path.write_bytes(content)
        _echo(f"  {len(macro_files)} macro file(s) pulled")

    # Pull existing research artifacts (for re-analysis idempotency)
    research_prefix = f"data/research/{ticker}/"
    all_research = list_prefix(s3, research_prefix)
    artifact_keys = [k for k in all_research if not k[len(research_prefix):].startswith("data/")]
    if artifact_keys:
        for key in artifact_keys:
            relative = key[len(research_prefix):]
            local_path = workspace / relative
            local_path.parent.mkdir(parents=True, exist_ok=True)
            content = download_file(s3, key)
            local_path.write_bytes(content)
        _echo(f"  {len(artifact_keys)} existing artifact(s) pulled")

    # Build file manifest for the prompt
    data_manifest = _build_manifest(data_dir)

    # Load ticker info
    entry = registry_cfg.tickers.get(ticker)

    # Configure MCP server for fundamentals querying
    fundamentals_path = data_dir / "fundamentals" / "fundamentals.json"
    has_fundamentals_mcp = fundamentals_path.exists()
    if has_fundamentals_mcp:
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
        mcp_path = workspace / ".mcp.json"
        mcp_path.write_text(json.dumps(mcp_config, indent=2))
        _echo(f"  Configured fundamentals MCP server")

    # Generate CLAUDE.md with priority-scaled budgets
    priority = entry.research_priority if entry else 5
    budget = ResearchBudget.from_priority(priority)
    prompt = generate_research_prompt(
        ticker=ticker,
        company_name=entry.name if entry else ticker,
        data_manifest=data_manifest,
        has_macro=bool(macro_files),
        has_existing_artifacts=bool(artifact_keys),
        research_priority=priority,
        has_fundamentals_mcp=has_fundamentals_mcp,
        tactical=tactical,
    )
    claude_md_path = workspace / "CLAUDE.md"
    claude_md_path.write_text(prompt)
    _echo(f"  CLAUDE.md generated (depth: {budget.depth_label})")

    return workspace


def sync_research(ticker: str, s3=None, *, quiet: bool = False, cleanup: bool = True) -> int:
    """Sync local research artifacts for a ticker to S3.

    Returns the number of files uploaded. If *cleanup* is True, removes
    the workspace after a successful full sync.
    """

    def _echo(msg: str) -> None:
        if quiet:
            logger.info(msg)
        else:
            click.echo(msg)

    if s3 is None:
        s3 = get_s3_client()

    repo_root = find_repo_root()
    local_dir = repo_root / "workspace" / ticker

    if not local_dir.exists():
        _echo(f"No workspace found at {local_dir}")
        return 0

    # Only sync research artifacts -- skip ingested data, CLAUDE.md, and MCP config
    skip_prefixes = ("data/", "macro/")
    skip_names = {"CLAUDE.md", ".mcp.json"}
    found: list[Path] = []
    for path in local_dir.rglob("*"):
        if path.is_file():
            rel = path.relative_to(local_dir)
            rel_str = str(rel)
            if any(rel_str.startswith(p) for p in skip_prefixes):
                continue
            if rel.name in skip_names:
                continue
            found.append(rel)

    if not found:
        _echo(f"No artifacts found in {local_dir}/")
        return 0

    _echo(f"Uploading {len(found)} artifact(s) for {ticker}...")
    s3_prefix = f"data/research/{ticker}"
    uploaded = []
    for rel in found:
        s3_key = f"{s3_prefix}/{rel}"
        upload_file(s3, local_dir / rel, s3_key)
        uploaded.append(s3_key)

    _echo(f"Synced {len(uploaded)} file(s) for {ticker}")

    if cleanup and len(uploaded) == len(found):
        shutil.rmtree(local_dir)
        _echo(f"Cleaned up workspace at {local_dir}")

    return len(uploaded)


def _build_manifest(data_dir: Path) -> str:
    """Build a file listing for the research prompt.

    Excludes fundamentals.json (too large for context -- use MCP tools instead).
    """
    lines = []
    for path in sorted(data_dir.rglob("*")):
        if path.is_file():
            if path.name == "fundamentals.json":
                continue
            relative = path.relative_to(data_dir)
            size = path.stat().st_size
            if size > 1024:
                size_str = f"{size // 1024}KB"
            else:
                size_str = f"{size}B"
            lines.append(f"  - data/{relative} ({size_str})")
    return "\n".join(lines)
