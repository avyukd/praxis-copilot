"""Praxis CLI — management tool for the Praxis Copilot system."""

from pathlib import Path

import click

from cli.config_utils import find_repo_root, get_config_dir, load_yaml, save_yaml
from cli.edgar import resolve_ticker
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
def universe_add(ticker: str):
    """Add TICKER to the investment universe."""
    ticker = ticker.upper()
    config_dir = get_config_dir()

    # Load existing configs
    universe_path = config_dir / "universe.yaml"
    registry_path = config_dir / "ticker_registry.yaml"
    universe_data = load_yaml(universe_path)
    registry_data = load_yaml(registry_path)

    # Check if already in universe
    tickers_list = universe_data.get("tickers", [])
    if ticker in tickers_list:
        click.echo(f"{ticker} is already in the universe.")
        return

    # Resolve ticker via EDGAR
    click.echo(f"Resolving {ticker} via EDGAR...")
    info = resolve_ticker(ticker)
    if not info:
        click.echo(f"Could not resolve {ticker} via EDGAR. Please verify the ticker symbol.")
        return

    click.echo(f"  Found: {info['name']} (CIK: {info['cik']}, Exchange: {info['exchange']})")

    # Update universe.yaml
    if "tickers" not in universe_data:
        universe_data["tickers"] = []
    universe_data["tickers"].append(ticker)
    save_yaml(universe_path, universe_data)
    click.echo(f"Added {ticker} to universe.yaml")

    # Update ticker_registry.yaml
    if "tickers" not in registry_data:
        registry_data["tickers"] = {}
    registry_entry = {
        "cik": info["cik"],
        "exchange": info["exchange"],
        "name": info["name"],
        "news_queries": [f'"{info["name"]}" OR "{ticker}"'],
    }
    # If it was previously external, remove that status
    existing = registry_data["tickers"].get(ticker, {})
    if existing.get("universe_status") == "external":
        click.echo(f"  Promoting {ticker} from external to in-universe")
    registry_data["tickers"][ticker] = registry_entry
    save_yaml(registry_path, registry_data)
    click.echo(f"Added {ticker} to ticker_registry.yaml")

    # Sync config
    click.echo()
    s3 = get_s3_client()
    uploaded = upload_directory(s3, config_dir, "config")
    click.echo(f"Synced {len(uploaded)} config file(s) to S3.")

    # Stub: invoke data ingestion Lambda
    click.echo()
    click.echo(f"[STUB] Would invoke data ingestion Lambda for {ticker} (CIK: {info['cik']})")
    click.echo(f"  This will pull SEC filings, fundamentals, and transcripts to:")
    click.echo(f"  s3://{BUCKET}/data/research/{ticker}/data/")


@universe.command("remove")
@click.argument("ticker")
def universe_remove(ticker: str):
    """Remove TICKER from the investment universe."""
    ticker = ticker.upper()
    config_dir = get_config_dir()

    universe_path = config_dir / "universe.yaml"
    registry_path = config_dir / "ticker_registry.yaml"
    universe_data = load_yaml(universe_path)
    registry_data = load_yaml(registry_path)

    tickers_list = universe_data.get("tickers", [])
    if ticker not in tickers_list:
        click.echo(f"{ticker} is not in the universe.")
        return

    # Remove from universe.yaml
    universe_data["tickers"].remove(ticker)
    save_yaml(universe_path, universe_data)
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
    if ticker in registry_data.get("tickers", {}):
        if has_monitor_deps:
            registry_data["tickers"][ticker]["universe_status"] = "external"
            save_yaml(registry_path, registry_data)
            click.echo(
                f"Marked {ticker} as external in ticker_registry.yaml "
                f"(monitors still depend on it)"
            )
        else:
            # No dependencies — could remove entirely, but keep for audit trail
            registry_data["tickers"][ticker]["universe_status"] = "external"
            save_yaml(registry_path, registry_data)
            click.echo(f"Marked {ticker} as external in ticker_registry.yaml")

    # Sync config
    click.echo()
    s3 = get_s3_client()
    uploaded = upload_directory(s3, config_dir, "config")
    click.echo(f"Synced {len(uploaded)} config file(s) to S3.")


# ---------------------------------------------------------------------------
# praxis analyze TICKER
# ---------------------------------------------------------------------------

@cli.command("analyze")
@click.argument("ticker")
def analyze(ticker: str):
    """Check status and start analysis for TICKER."""
    ticker = ticker.upper()
    config_dir = get_config_dir()

    # Check ticker is in universe
    universe_data = load_yaml(config_dir / "universe.yaml")
    tickers_list = universe_data.get("tickers", [])
    if ticker not in tickers_list:
        click.echo(f"{ticker} is not in the universe. Run 'praxis universe add {ticker}' first.")
        return

    # Check data ingestion status (stub — check S3 prefix)
    click.echo(f"Checking data ingestion status for {ticker}...")
    try:
        s3 = get_s3_client()
        data_prefix = f"data/research/{ticker}/data/"
        data_keys = list_prefix(s3, data_prefix)
        if data_keys:
            click.echo(f"  Data available: {len(data_keys)} file(s) under {data_prefix}")
        else:
            click.echo(f"  No ingested data found at s3://{BUCKET}/{data_prefix}")
            click.echo(f"  Data ingestion may still be in progress.")
            click.echo()
    except SystemExit:
        click.echo("  Could not check S3 (AWS credentials issue). Continuing anyway...")
        click.echo()

    # Set up workspace staging directory
    repo_root = find_repo_root()
    workspace = repo_root / "workspace" / ticker
    workspace.mkdir(parents=True, exist_ok=True)

    click.echo(f"\nWorkspace ready at: {workspace}")
    click.echo(f"To analyze {ticker}:")
    click.echo(f"  1. cd {workspace}")
    click.echo(f"  2. Start a Claude Code session with the research pipeline prompt")
    click.echo(f"  3. Artifacts will be produced in {workspace}/")
    click.echo(f"  4. After analysis, run: praxis sync {ticker}")


# ---------------------------------------------------------------------------
# praxis sync TICKER
# ---------------------------------------------------------------------------

@cli.command("sync")
@click.argument("ticker")
def sync(ticker: str):
    """Sync local research artifacts for TICKER to S3 and clean up workspace."""
    ticker = ticker.upper()

    # Look for workspace staging directory
    repo_root = find_repo_root()
    local_dir = repo_root / "workspace" / ticker
    if not local_dir.exists():
        click.echo(f"No workspace found at {local_dir}")
        click.echo(f"Run 'praxis analyze {ticker}' first to set up the workspace.")
        return

    # Collect all artifacts
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

    # Clean up workspace
    import shutil
    shutil.rmtree(local_dir)
    click.echo(f"\nCleaned up workspace at {local_dir}")


# ---------------------------------------------------------------------------
# praxis status
# ---------------------------------------------------------------------------

@cli.command("status")
def status():
    """Show universe tickers with their status."""
    config_dir = get_config_dir()
    universe_data = load_yaml(config_dir / "universe.yaml")
    tickers_list = universe_data.get("tickers", [])

    if not tickers_list:
        click.echo("Universe is empty. Run 'praxis universe add TICKER' to add tickers.")
        return

    click.echo(f"Universe: {len(tickers_list)} ticker(s)\n")

    try:
        s3 = get_s3_client()
        use_s3 = True
    except SystemExit:
        click.echo("(Could not connect to AWS — showing local status only)\n")
        use_s3 = False

    for ticker in sorted(tickers_list):
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
                    import yaml
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
        import yaml
        registry = yaml.safe_load(registry_content)
        ticker_info = registry.get("tickers", {}).get(ticker, {})
        cik = ticker_info.get("cik")
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
    import json
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
# praxis research TICKER [FILE]
# ---------------------------------------------------------------------------

@cli.command("research")
@click.argument("ticker")
@click.argument("file", required=False)
def research(ticker: str, file: str | None):
    """List or view research artifacts for TICKER.

    Without FILE: lists all artifacts in data/research/{TICKER}/.
    With FILE: streams the file content to stdout (pipe to glow or less).

    Examples:
        praxis research NVDA
        praxis research NVDA memo.md | glow
        praxis research NVDA memo.yaml | less
    """
    ticker = ticker.upper()
    s3 = get_s3_client()

    prefix = f"data/research/{ticker}/"

    if file:
        # Stream a specific file to stdout
        s3_key = f"{prefix}{file}"
        if not key_exists(s3, s3_key):
            click.echo(f"File not found: s3://{BUCKET}/{s3_key}", err=True)
            return
        content = download_file(s3, s3_key)
        click.get_text_stream("stdout").write(content.decode("utf-8"))
    else:
        # List all artifacts
        keys = list_prefix(s3, prefix)
        if not keys:
            click.echo(f"No research artifacts found for {ticker}")
            return

        click.echo(f"Research artifacts for {ticker}:\n")
        for key in sorted(keys):
            relative = key[len(prefix):]
            # Skip the data/ ingestion directory in the listing for clarity
            if relative.startswith("data/"):
                continue
            click.echo(f"  {relative}")

        # Show data ingestion summary separately
        data_keys = [k for k in keys if k[len(prefix):].startswith("data/")]
        if data_keys:
            click.echo(f"\n  ({len(data_keys)} ingested data file(s) under data/)")

        click.echo(f"\nView a file: praxis research {ticker} memo.md | glow")


if __name__ == "__main__":
    cli()
