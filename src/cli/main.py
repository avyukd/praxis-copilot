"""Praxis CLI — management tool for the Praxis Copilot system."""

import json
import shutil
from pathlib import Path

import click
import yaml
from dotenv import load_dotenv

load_dotenv()

from cli.config_utils import find_repo_root, get_config_dir, load_yaml, save_yaml
from cli.edgar import resolve_ticker
from cli.ingest import run_ingestion
from cli.macro import (
    list_macro_files,
    pull_macro_workspace,
    sync_macro_workspace,
)
from cli.models import TickerRegistry, TickerRegistryEntry, UniverseConfig
from cli.research_prompt import ResearchBudget, generate_research_prompt
from cli.s3 import (
    BUCKET,
    download_file,
    get_s3_client,
    key_exists,
    list_prefix,
    upload_directory,
)


@click.group()
def cli():
    """Praxis Copilot CLI."""
    pass


# ---------------------------------------------------------------------------
# praxis config sync
# ---------------------------------------------------------------------------

@cli.group()
def config():
    """Config management commands."""
    pass


@config.command("sync")
def config_sync():
    """Sync local config/ to S3."""
    config_dir = get_config_dir()
    if not any(config_dir.iterdir()):
        click.echo("No config files found in config/. Nothing to sync.")
        return

    click.echo(f"Syncing config/ to s3://{BUCKET}/config/ ...")
    s3 = get_s3_client()
    uploaded = upload_directory(s3, config_dir, "config")

    if uploaded:
        click.echo(f"Synced {len(uploaded)} file(s):")
        for key in uploaded:
            click.echo(f"  {key}")
    else:
        click.echo("No files to sync.")


# ---------------------------------------------------------------------------
# praxis universe
# ---------------------------------------------------------------------------

@cli.group()
def universe():
    """Universe management commands."""
    pass


@universe.command("add")
@click.argument("ticker")
@click.option("-p", "--priority", type=click.IntRange(0, 10), default=5,
              help="Research priority 0-10 (0=quick screen, 5=standard, 10=full deep dive)")
def universe_add(ticker: str, priority: int):
    """Add TICKER to the investment universe."""
    ticker = ticker.upper()
    config_dir = get_config_dir()

    # Load existing configs
    universe_path = config_dir / "universe.yaml"
    registry_path = config_dir / "ticker_registry.yaml"
    universe_cfg = UniverseConfig(**load_yaml(universe_path))
    registry_cfg = TickerRegistry(**load_yaml(registry_path))

    # Check if already in universe
    if ticker in universe_cfg.tickers:
        click.echo(f"{ticker} is already in the universe.")
        return

    # Resolve ticker via EDGAR
    click.echo(f"Resolving {ticker} via EDGAR...")
    info = resolve_ticker(ticker)
    if not info:
        click.echo(f"Could not resolve {ticker} via EDGAR. Please verify the ticker symbol.")
        return

    click.echo(f"  Found: {info.name} (CIK: {info.cik}, Exchange: {info.exchange})")
    budget = ResearchBudget.from_priority(priority)
    click.echo(f"  Research depth: {budget.depth_label}")

    # Update universe.yaml
    universe_cfg.tickers.append(ticker)
    save_yaml(universe_path, universe_cfg.model_dump())
    click.echo(f"Added {ticker} to universe.yaml")

    # Update ticker_registry.yaml
    existing = registry_cfg.tickers.get(ticker)
    if existing and existing.universe_status == "external":
        click.echo(f"  Promoting {ticker} from external to in-universe")
    registry_entry = TickerRegistryEntry(
        cik=info.cik,
        exchange=info.exchange,
        name=info.name,
        news_queries=[f'"{info.name}" OR "{ticker}"'],
        research_priority=priority,
    )
    registry_cfg.tickers[ticker] = registry_entry
    save_yaml(registry_path, registry_cfg.model_dump(exclude_none=True))
    click.echo(f"Added {ticker} to ticker_registry.yaml")

    # Sync config
    click.echo()
    s3 = get_s3_client()
    uploaded = upload_directory(s3, config_dir, "config")
    click.echo(f"Synced {len(uploaded)} config file(s) to S3.")

    # Run data ingestion locally
    click.echo()
    click.echo(f"Ingesting data for {ticker} (CIK: {info.cik})...")
    result = run_ingestion(ticker, info.cik, s3)
    click.echo(f"  Filings: {result.filings_count} section(s)")
    click.echo(f"  Fundamentals: {result.fundamentals_source or 'unavailable'}")
    click.echo(f"  Transcripts: {result.transcripts_count}")
    if result.warnings:
        click.echo(f"  Warnings:")
        for w in result.warnings:
            click.echo(f"    - {w}")
    click.echo(f"\n  Data stored at: s3://{BUCKET}/data/research/{ticker}/data/")

@universe.command("remove")
@click.argument("ticker")
def universe_remove(ticker: str):
    """Remove TICKER from the investment universe."""
    ticker = ticker.upper()
    config_dir = get_config_dir()

    universe_path = config_dir / "universe.yaml"
    registry_path = config_dir / "ticker_registry.yaml"
    universe_cfg = UniverseConfig(**load_yaml(universe_path))
    registry_cfg = TickerRegistry(**load_yaml(registry_path))

    if ticker not in universe_cfg.tickers:
        click.echo(f"{ticker} is not in the universe.")
        return

    # Remove from universe.yaml
    universe_cfg.tickers.remove(ticker)
    save_yaml(universe_path, universe_cfg.model_dump())
    click.echo(f"Removed {ticker} from universe.yaml")

    # Check if any monitors depend on this ticker
    monitors_dir = config_dir / "monitors"
    has_monitor_deps = False
    if monitors_dir.exists():
        for monitor_file in monitors_dir.glob("*.yaml"):
            monitor_data = load_yaml(monitor_file)
            listen = monitor_data.get("listen", [])
            for entry in listen:
                if isinstance(entry, str) and entry.startswith(f"{ticker}:"):
                    has_monitor_deps = True
                    break
            if has_monitor_deps:
                break

    # Update ticker registry
    if ticker in registry_cfg.tickers:
        registry_cfg.tickers[ticker].universe_status = "external"
        save_yaml(registry_path, registry_cfg.model_dump(exclude_none=True))
        if has_monitor_deps:
            click.echo(
                f"Marked {ticker} as external in ticker_registry.yaml "
                f"(monitors still depend on it)"
            )
        else:
            click.echo(f"Marked {ticker} as external in ticker_registry.yaml")

    # Sync config
    click.echo()
    s3 = get_s3_client()
    uploaded = upload_directory(s3, config_dir, "config")
    click.echo(f"Synced {len(uploaded)} config file(s) to S3.")


# ---------------------------------------------------------------------------
# praxis stage TICKER
# ---------------------------------------------------------------------------

@cli.command("stage")
@click.argument("ticker")
def stage(ticker: str):
    """Stage workspace for TICKER research.

    Pulls ingested data from S3, downloads macro context, generates
    a CLAUDE.md research prompt. Then: cd workspace/TICKER && claude
    """
    ticker = ticker.upper()
    config_dir = get_config_dir()

    # Check ticker is in universe
    universe_cfg = UniverseConfig(**load_yaml(config_dir / "universe.yaml"))
    if ticker not in universe_cfg.tickers:
        click.echo(f"{ticker} is not in the universe. Run 'praxis universe add {ticker}' first.")
        return

    s3 = get_s3_client()

    # Ensure data is ingested
    data_prefix = f"data/research/{ticker}/data/"
    data_keys = list_prefix(s3, data_prefix)
    if not data_keys:
        click.echo(f"No ingested data found. Running ingestion...")
        registry_cfg = TickerRegistry(**load_yaml(config_dir / "ticker_registry.yaml"))
        entry = registry_cfg.tickers.get(ticker)
        if entry:
            result = run_ingestion(ticker, entry.cik, s3)
            click.echo(f"  Filings: {result.filings_count}, Fundamentals: {result.fundamentals_source or 'N/A'}, Transcripts: {result.transcripts_count}")
            data_keys = list_prefix(s3, data_prefix)
        else:
            click.echo(f"  No CIK found for {ticker}. Re-add with 'praxis universe add {ticker}'.")
            return

    # Set up workspace
    repo_root = find_repo_root()
    workspace = repo_root / "workspace" / ticker
    workspace.mkdir(parents=True, exist_ok=True)

    # Pull ingested data into workspace/data/
    click.echo(f"Pulling ingested data to workspace...")
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
    click.echo(f"  {len(data_keys)} file(s) pulled to {data_dir}/")

    # Pull macro context if it exists
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
        click.echo(f"  {len(macro_files)} macro file(s) pulled to {workspace}/macro/")

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
        click.echo(f"  {len(artifact_keys)} existing artifact(s) pulled")

    # Build file manifest for the prompt
    data_manifest = _build_manifest(data_dir)

    # Load ticker info
    registry_cfg = TickerRegistry(**load_yaml(config_dir / "ticker_registry.yaml"))
    entry = registry_cfg.tickers.get(ticker)

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
    )
    claude_md_path = workspace / "CLAUDE.md"
    claude_md_path.write_text(prompt)
    click.echo(f"  Generated CLAUDE.md (depth: {budget.depth_label})")

    click.echo(f"\nWorkspace ready: {workspace}")
    click.echo(f"  cd {workspace} && claude")
    click.echo(f"  After analysis: praxis research sync {ticker}")


def _build_manifest(data_dir: Path) -> str:
    """Build a file listing for the research prompt."""
    lines = []
    for path in sorted(data_dir.rglob("*")):
        if path.is_file():
            relative = path.relative_to(data_dir)
            size = path.stat().st_size
            if size > 1024:
                size_str = f"{size // 1024}KB"
            else:
                size_str = f"{size}B"
            lines.append(f"  - data/{relative} ({size_str})")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# praxis status
# ---------------------------------------------------------------------------

@cli.command("status")
def status():
    """Show universe tickers with their status."""
    config_dir = get_config_dir()
    universe_cfg = UniverseConfig(**load_yaml(config_dir / "universe.yaml"))

    if not universe_cfg.tickers:
        click.echo("Universe is empty. Run 'praxis universe add TICKER' to add tickers.")
        return

    click.echo(f"Universe: {len(universe_cfg.tickers)} ticker(s)\n")

    try:
        s3 = get_s3_client()
        use_s3 = True
    except SystemExit:
        click.echo("(Could not connect to AWS — showing local status only)\n")
        use_s3 = False

    for ticker in sorted(universe_cfg.tickers):
        parts = [ticker]

        if use_s3:
            # Check for memo
            has_memo = key_exists(s3, f"data/research/{ticker}/memo.yaml")
            parts.append("memo:yes" if has_memo else "memo:no")

            # Check for ingested data
            data_keys = list_prefix(s3, f"data/research/{ticker}/data/")
            parts.append(f"data:{len(data_keys)} files" if data_keys else "data:none")

            # Count monitors
            monitor_keys = list_prefix(s3, f"config/monitors/")
            monitor_count = 0
            for mk in monitor_keys:
                try:
                    content = download_file(s3, mk)
                    mdata = yaml.safe_load(content)
                    listen = mdata.get("listen", [])
                    for entry in listen:
                        if isinstance(entry, str) and entry.startswith(f"{ticker}:"):
                            monitor_count += 1
                            break
                except Exception:
                    pass
            parts.append(f"monitors:{monitor_count}")

        click.echo("  ".join(parts))


# ---------------------------------------------------------------------------
# praxis events TICKER
# ---------------------------------------------------------------------------

@cli.command("events")
@click.argument("ticker")
@click.option("-n", "--limit", default=10, help="Number of recent events to show")
def events(ticker: str, limit: int):
    """List recent 8k-scanner results for TICKER."""
    ticker = ticker.upper()
    s3 = get_s3_client()

    # Load ticker registry to get CIK
    try:
        registry_content = download_file(s3, "config/ticker_registry.yaml")
        registry_cfg = TickerRegistry(**yaml.safe_load(registry_content))
        entry = registry_cfg.tickers.get(ticker)
        cik = entry.cik if entry else None
    except Exception:
        cik = None

    if not cik:
        click.echo(f"No CIK found for {ticker} in ticker registry.")
        return

    # List analysis files for this CIK
    prefix = f"data/raw/8k/{cik}/"
    all_keys = list_prefix(s3, prefix)
    analysis_keys = [k for k in all_keys if k.endswith("/analysis.json")]

    if not analysis_keys:
        click.echo(f"No 8-K analyses found for {ticker} (CIK: {cik})")
        return

    # Sort by key (contains accession, roughly chronological) and take most recent
    analysis_keys.sort(reverse=True)
    analysis_keys = analysis_keys[:limit]

    click.echo(f"Recent 8-K analyses for {ticker} (CIK: {cik}):\n")
    for key in analysis_keys:
        try:
            content = download_file(s3, key)
            data = json.loads(content)
            accession = key.split("/")[-2]
            classification = data.get("classification", "?")
            magnitude = data.get("magnitude", "?")
            summary = data.get("summary", data.get("headline", ""))
            if len(summary) > 100:
                summary = summary[:100] + "..."
            click.echo(f"  {accession}  {classification}  mag:{magnitude}  {summary}")
        except Exception:
            click.echo(f"  {key}  (could not parse)")


# ---------------------------------------------------------------------------
# praxis research
# ---------------------------------------------------------------------------

@cli.group()
def research():
    """Research artifact commands."""
    pass


@research.command("show")
@click.argument("ticker")
@click.argument("file", required=False)
def research_show(ticker: str, file: str | None):
    """Show research artifacts for TICKER.

    Without FILE, lists all artifacts. With FILE, streams content to stdout.

    Examples:
        praxis research show NVDA
        praxis research show NVDA memo.md | glow
        praxis research show NVDA memo.yaml | less
    """
    ticker = ticker.upper()
    s3 = get_s3_client()

    if file:
        s3_key = f"data/research/{ticker}/{file}"
        if not key_exists(s3, s3_key):
            click.echo(f"File not found: s3://{BUCKET}/{s3_key}", err=True)
            return
        content = download_file(s3, s3_key)
        click.get_text_stream("stdout").write(content.decode("utf-8"))
        return

    prefix = f"data/research/{ticker}/"
    keys = list_prefix(s3, prefix)
    if not keys:
        click.echo(f"No research artifacts found for {ticker}")
        return

    click.echo(f"Research artifacts for {ticker}:\n")
    for key in sorted(keys):
        relative = key[len(prefix):]
        if relative.startswith("data/"):
            continue
        click.echo(f"  {relative}")

    data_keys = [k for k in keys if k[len(prefix):].startswith("data/")]
    if data_keys:
        click.echo(f"\n  ({len(data_keys)} ingested data file(s) under data/)")

    click.echo(f"\nView: praxis research show {ticker} memo.md | glow")
    click.echo(f"Pull: praxis research pull {ticker}")


@research.command("pull")
@click.argument("ticker")
def research_pull(ticker: str):
    """Pull research artifacts from S3 into workspace/{TICKER}/ for re-analysis.

    Downloads memo, specialist reports, and draft_monitors — everything
    except raw ingested data. After editing, run praxis sync TICKER to push back.
    """
    ticker = ticker.upper()
    s3 = get_s3_client()
    prefix = f"data/research/{ticker}/"

    keys = list_prefix(s3, prefix)
    # Filter out ingested data — only pull research artifacts
    artifact_keys = [k for k in keys if not k[len(prefix):].startswith("data/")]

    if not artifact_keys:
        click.echo(f"No research artifacts found for {ticker} on S3.")
        return

    repo_root = find_repo_root()
    workspace = repo_root / "workspace" / ticker
    workspace.mkdir(parents=True, exist_ok=True)

    click.echo(f"Pulling research artifacts for {ticker} into {workspace}/\n")
    for key in sorted(artifact_keys):
        relative = key[len(prefix):]
        local_path = workspace / relative
        local_path.parent.mkdir(parents=True, exist_ok=True)

        content = download_file(s3, key)
        local_path.write_bytes(content)
        click.echo(f"  {relative}")

    click.echo(f"\nPulled {len(artifact_keys)} file(s) to {workspace}/")
    click.echo(f"After re-analysis, run: praxis research sync {ticker}")


@research.command("sync")
@click.argument("ticker")
def research_sync(ticker: str):
    """Sync local research artifacts for TICKER to S3 and clean up workspace."""
    ticker = ticker.upper()

    repo_root = find_repo_root()
    local_dir = repo_root / "workspace" / ticker
    if not local_dir.exists():
        click.echo(f"No workspace found at {local_dir}")
        click.echo(f"Run 'praxis stage {ticker}' first to set up the workspace.")
        return

    found = []
    for path in local_dir.rglob("*"):
        if path.is_file():
            found.append(path.relative_to(local_dir))

    if not found:
        click.echo(f"No artifacts found in {local_dir}/")
        return

    click.echo(f"Found artifacts in {local_dir}/:")
    for name in sorted(str(f) for f in found):
        click.echo(f"  {name}")

    s3_prefix = f"data/research/{ticker}"
    click.echo(f"\nUploading to s3://{BUCKET}/{s3_prefix}/ ...")

    s3 = get_s3_client()
    uploaded = upload_directory(s3, local_dir, s3_prefix)
    click.echo(f"Synced {len(uploaded)} file(s):")
    for key in uploaded:
        click.echo(f"  {key}")

    if len(uploaded) == len(found):
        shutil.rmtree(local_dir)
        click.echo(f"\nCleaned up workspace at {local_dir}")
    else:
        click.echo(
            f"\nWarning: uploaded {len(uploaded)}/{len(found)} files. "
            f"Workspace preserved at {local_dir}"
        )


# ---------------------------------------------------------------------------
# praxis macro
# ---------------------------------------------------------------------------

@cli.group()
def macro():
    """Macro workspace — freeform collection of macro views and notes."""
    pass


@macro.command("show")
@click.argument("file", required=False)
def macro_show(file: str | None):
    """Show macro workspace contents.

    Without FILE, lists all files. With FILE, streams content to stdout.

    Examples:
        praxis macro show
        praxis macro show rates-higher-longer.md | glow
    """
    s3 = get_s3_client()

    if file:
        s3_key = f"data/context/macro/{file}"
        if not key_exists(s3, s3_key):
            click.echo(f"File not found: s3://{BUCKET}/{s3_key}", err=True)
            return
        content = download_file(s3, s3_key)
        click.get_text_stream("stdout").write(content.decode("utf-8"))
        return

    files = list_macro_files(s3)
    if not files:
        click.echo("No macro files on S3. Run 'praxis macro pull' after adding files to workspace/macro/.")
        return

    click.echo(f"Macro workspace ({len(files)} file(s)):\n")
    for f in sorted(files):
        click.echo(f"  {f}")
    click.echo(f"\nView: praxis macro show <file> | glow")
    click.echo(f"Pull: praxis macro pull")


@macro.command("pull")
def macro_pull():
    """Download all macro files from S3 to workspace/macro/ for editing."""
    s3 = get_s3_client()

    repo_root = find_repo_root()
    local_dir = repo_root / "workspace" / "macro"
    local_dir.mkdir(parents=True, exist_ok=True)

    pulled = pull_macro_workspace(s3, local_dir)
    if not pulled:
        click.echo("No macro files found on S3.")
        click.echo("Add files to workspace/macro/ and run 'praxis macro sync'.")
        return

    click.echo(f"Pulled {len(pulled)} file(s) to {local_dir}/:\n")
    for f in sorted(pulled):
        click.echo(f"  {f}")
    click.echo(f"\nEdit or add files, then run: praxis macro sync")


@macro.command("sync")
def macro_sync():
    """Upload workspace/macro/ contents to S3."""
    repo_root = find_repo_root()
    local_dir = repo_root / "workspace" / "macro"
    if not local_dir.exists() or not any(local_dir.rglob("*")):
        click.echo(f"No files in {local_dir}/")
        click.echo("Add macro notes/views there, then run this again.")
        return

    s3 = get_s3_client()
    uploaded = sync_macro_workspace(s3, local_dir)
    click.echo(f"Synced {len(uploaded)} file(s):")
    for key in uploaded:
        click.echo(f"  {key}")


if __name__ == "__main__":
    cli()
