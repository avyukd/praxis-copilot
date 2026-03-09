"""Praxis CLI — management tool for the Praxis Copilot system."""

import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import click
import yaml
from botocore.exceptions import ClientError, NoRegionError
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
from cli.monitors import monitor
from cli.pipeline_interactive import interactive_pipeline_day_view, trace_payload
from cli.pipeline_status import (
    build_pipeline_trace,
    collect_pipeline_items,
    find_prefixes_by_item_id,
    parse_day_window,
    summarize_pipeline_items,
)
from cli.research_prompt import ResearchBudget, generate_research_prompt
from cli.s3 import (
    BUCKET,
    download_file,
    get_s3_client,
    key_exists,
    list_prefix,
    upload_directory,
    upload_file,
)
from cli.watch import alert, market, watch


@click.group()
def cli():
    """Praxis Copilot CLI."""
    pass


cli.add_command(monitor)
cli.add_command(market)
cli.add_command(watch)
cli.add_command(alert)


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
@click.argument("tickers", nargs=-1, required=True)
@click.option("-p", "--priority", type=click.IntRange(0, 10), default=5,
              help="Research priority 0-10 (0=quick screen, 5=standard, 10=full deep dive)")
def universe_add(tickers: tuple[str, ...], priority: int):
    """Add TICKER(s) to the investment universe.

    \b
    Examples:
      praxis universe add NVDA
      praxis universe add CRDO APP MKL -p 7
    """
    config_dir = get_config_dir()
    universe_path = config_dir / "universe.yaml"
    registry_path = config_dir / "ticker_registry.yaml"
    universe_cfg = UniverseConfig(**load_yaml(universe_path))
    registry_cfg = TickerRegistry(**load_yaml(registry_path))

    s3 = get_s3_client()
    added = []

    for raw_ticker in tickers:
        ticker = raw_ticker.upper()
        click.echo(f"\n{'='*40}")
        click.echo(f"Processing {ticker}...")

        if ticker in universe_cfg.tickers:
            click.echo(f"  {ticker} is already in the universe, skipping.")
            continue

        # Resolve ticker via EDGAR
        click.echo(f"  Resolving via EDGAR...")
        info = resolve_ticker(ticker)
        if not info:
            click.echo(f"  Could not resolve {ticker} via EDGAR. Skipping.")
            continue

        click.echo(f"  Found: {info.name} (CIK: {info.cik}, Exchange: {info.exchange})")

        # Update configs
        universe_cfg.tickers.append(ticker)
        existing = registry_cfg.tickers.get(ticker)
        if existing and existing.universe_status == "external":
            click.echo(f"  Promoting {ticker} from external to in-universe")
        registry_cfg.tickers[ticker] = TickerRegistryEntry(
            cik=info.cik,
            exchange=info.exchange,
            name=info.name,
            news_queries=[f'"{info.name}" OR "{ticker}"'],
            research_priority=priority,
        )

        # Save after each ticker so progress isn't lost
        save_yaml(universe_path, universe_cfg.model_dump())
        save_yaml(registry_path, registry_cfg.model_dump(exclude_none=True))

        # Run data ingestion
        click.echo(f"  Ingesting data...")
        result = run_ingestion(ticker, info.cik, s3)
        click.echo(f"  Filings: {result.filings_count} | Fundamentals: {result.fundamentals_source or 'N/A'} | Transcripts: {result.transcripts_count}")
        if result.warnings:
            for w in result.warnings:
                click.echo(f"    ⚠ {w}")

        added.append(ticker)

    # Sync config once at the end
    if added:
        click.echo(f"\n{'='*40}")
        uploaded = upload_directory(s3, config_dir, "config")
        click.echo(f"Synced config to S3. Added {len(added)} ticker(s): {', '.join(added)}")

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
            # New schema: check tickers list
            if ticker in monitor_data.get("tickers", []):
                has_monitor_deps = True
                break
            # Legacy schema: check listen keys
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
@click.argument("tickers", nargs=-1, required=True)
def stage(tickers: tuple[str, ...]):
    """Stage workspace for TICKER(s) research.

    Pulls ingested data from S3, downloads macro context, generates
    a CLAUDE.md research prompt. Then: cd workspace/TICKER && claude

    \b
    Examples:
      praxis stage NVDA
      praxis stage CRDO APP MKL
    """
    config_dir = get_config_dir()
    universe_cfg = UniverseConfig(**load_yaml(config_dir / "universe.yaml"))
    registry_cfg = TickerRegistry(**load_yaml(config_dir / "ticker_registry.yaml"))
    s3 = get_s3_client()

    # Pre-fetch macro context once (shared across all tickers)
    macro_keys = list_prefix(s3, "data/context/macro/")
    macro_files = [k for k in macro_keys if k != "data/context/macro/"]

    staged = []
    for raw_ticker in tickers:
        ticker = raw_ticker.upper()
        click.echo(f"\n{'='*40}")
        click.echo(f"Staging {ticker}...")

        if ticker not in universe_cfg.tickers:
            click.echo(f"  {ticker} is not in the universe. Skipping.")
            continue

        workspace = _stage_ticker(ticker, config_dir, registry_cfg, s3, macro_files)
        if workspace:
            staged.append((ticker, workspace))

    if staged:
        click.echo(f"\n{'='*40}")
        click.echo(f"Staged {len(staged)} workspace(s):")
        for tk, ws in staged:
            click.echo(f"  cd {ws} && claude")


def _stage_ticker(ticker: str, config_dir: Path, registry_cfg, s3, macro_files: list[str]) -> Path | None:
    """Stage a single ticker workspace. Returns workspace path or None on failure."""
    # Ensure data is ingested
    data_prefix = f"data/research/{ticker}/data/"
    data_keys = list_prefix(s3, data_prefix)
    if not data_keys:
        click.echo(f"  No ingested data found. Running ingestion...")
        entry = registry_cfg.tickers.get(ticker)
        if entry:
            result = run_ingestion(ticker, entry.cik, s3)
            click.echo(f"  Filings: {result.filings_count}, Fundamentals: {result.fundamentals_source or 'N/A'}, Transcripts: {result.transcripts_count}")
            data_keys = list_prefix(s3, data_prefix)
        else:
            click.echo(f"  No CIK found for {ticker}. Re-add with 'praxis universe add {ticker}'.")
            return None

    # Set up workspace
    repo_root = find_repo_root()
    workspace = repo_root / "workspace" / ticker
    workspace.mkdir(parents=True, exist_ok=True)

    # Pull ingested data into workspace/data/
    click.echo(f"  Pulling ingested data...")
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
    click.echo(f"  {len(data_keys)} file(s) pulled")

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
        click.echo(f"  {len(macro_files)} macro file(s) pulled")

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
    entry = registry_cfg.tickers.get(ticker)

    # Configure MCP server for fundamentals querying
    fundamentals_path = data_dir / "fundamentals" / "fundamentals.json"
    has_fundamentals_mcp = fundamentals_path.exists()
    if has_fundamentals_mcp:
        server_script = str(Path(__file__).parent / "fundamentals_server.py")
        mcp_config = {
            "mcpServers": {
                "fundamentals": {
                    "command": sys.executable,
                    "args": [server_script, str(fundamentals_path)],
                }
            }
        }
        mcp_path = workspace / ".mcp.json"
        mcp_path.write_text(json.dumps(mcp_config, indent=2))
        click.echo(f"  Configured fundamentals MCP server")

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
    )
    claude_md_path = workspace / "CLAUDE.md"
    claude_md_path.write_text(prompt)
    click.echo(f"  CLAUDE.md generated (depth: {budget.depth_label})")

    return workspace


def _build_manifest(data_dir: Path) -> str:
    """Build a file listing for the research prompt.

    Excludes fundamentals.json (too large for context — use MCP tools instead).
    """
    lines = []
    for path in sorted(data_dir.rglob("*")):
        if path.is_file():
            # Skip raw fundamentals JSON — Claude uses MCP tools for that
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
                    # New schema: check tickers list
                    tickers_list = mdata.get("tickers", [])
                    if ticker in tickers_list:
                        monitor_count += 1
                        continue
                    # Legacy schema: check listen keys
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
# praxis pipeline
# ---------------------------------------------------------------------------

@cli.command("pipeline")
@click.argument("item_id", required=False)
@click.option("--date", "date_str", default=None, help="ET day in YYYY-MM-DD (default: today ET)")
@click.option(
    "--source",
    type=click.Choice(["all", "filings", "press_releases"]),
    default="all",
    show_default=True,
    help="Source scope",
)
@click.option(
    "--stuck-minutes",
    type=click.IntRange(1, 24 * 60),
    default=30,
    show_default=True,
    help="Lag threshold to mark stuck stages",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="For ITEM_ID mode, print full derived JSON outputs (extracted/screening/analysis)",
)
@click.option(
    "-i",
    "--interactive",
    is_flag=True,
    help="Interactive day browser to inspect filings/releases in a pager",
)
@click.option("--json", "as_json", is_flag=True, help="Emit JSON output")
@click.option("--logs", is_flag=True, help="Include CloudWatch logs (best effort)")
@click.option("--log-lines", type=click.IntRange(1, 200), default=20, show_default=True, help="Max lines per Lambda")
@click.option("--since-minutes", type=click.IntRange(1, 24 * 60), default=240, show_default=True, help="Log lookback window")
def pipeline(
    item_id: str | None,
    date_str: str | None,
    source: str,
    stuck_minutes: int,
    verbose: bool,
    interactive: bool,
    as_json: bool,
    logs: bool,
    log_lines: int,
    since_minutes: int,
):
    """Show day summary or deep trace for a single filing/release id.

    Stage definitions:
      arrived: index.json exists, waiting for extraction
      extracted: extracted.json exists, waiting for analysis/screening decision
      screened_out: screening.json exists and full analysis was skipped
      analyzed: analysis.json exists, alert not sent
      alerted: analysis.json exists and index.alert_sent_at is set
      stuck_extract: still arrived beyond --stuck-minutes
      stuck_analyze: still extracted beyond --stuck-minutes
    """
    s3 = get_s3_client()

    if interactive and (item_id or as_json):
        raise click.UsageError("--interactive is supported only for day view (without ITEM_ID and --json).")

    if item_id:
        matches = find_prefixes_by_item_id(s3, item_id=item_id, source=source)
        if not matches:
            click.echo(
                f"No item found for id '{item_id}' under source={source}. "
                "Try --source all if not already set."
            )
            return

        traces = [build_pipeline_trace(s3, source_type=src_type, key_prefix=prefix) for src_type, prefix in matches]
        traces_payload = [trace_payload(trace) for trace in traces]

        logs_payload = []
        if logs:
            logs_region = (
                s3.meta.region_name
                or os.environ.get("AWS_REGION")
                or os.environ.get("AWS_DEFAULT_REGION")
            )
            logs_payload = _fetch_pipeline_logs(
                item_id=item_id,
                key_prefixes=[trace.key_prefix for trace in traces],
                max_lines=log_lines,
                since_minutes=since_minutes,
                region_name=logs_region,
            )

        if as_json:
            click.echo(json.dumps({"item_id": item_id, "matches": traces_payload, "logs": logs_payload}, indent=2))
            return

        click.echo(f"Pipeline trace for id={item_id} (matches={len(traces)})")
        for trace in traces:
            click.echo("\n" + "=" * 60)
            click.echo(f"source_type={trace.source_type}  stage={trace.stage}")
            click.echo(f"key_prefix={trace.key_prefix}")
            click.echo(
                f"ticker={trace.ticker or '-'}  cik={trace.cik or '-'}  "
                f"form={trace.form_type or '-'}  source={trace.source or '-'}"
            )
            click.echo(
                f"arrived_at={trace.arrived_at or '-'}  extracted_at={trace.extracted_at or '-'}  "
                f"analyzed_at={trace.analyzed_at or '-'}  screening_at={trace.screening_at or '-'}"
            )
            click.echo(f"alert_sent_at={trace.alert_sent_at or '-'}")
            if trace.analysis_classification or trace.analysis_magnitude is not None or trace.analysis_summary:
                click.echo(
                    f"analysis: class={trace.analysis_classification or '-'}  "
                    f"mag={trace.analysis_magnitude if trace.analysis_magnitude is not None else '-'}"
                )
                if trace.analysis_summary:
                    click.echo(f"analysis_summary: {trace.analysis_summary[:240]}")
            if trace.extracted_total_chars is not None or trace.extracted_items:
                click.echo(
                    f"extracted: total_chars={trace.extracted_total_chars if trace.extracted_total_chars is not None else '-'}  "
                    f"items={','.join(trace.extracted_items) if trace.extracted_items else '-'}"
                )
            click.echo("files:")
            for name in trace.files:
                click.echo(f"  {name}")
            if verbose:
                _print_trace_file_contents(s3, trace.key_prefix, trace.files)

        if logs:
            if logs_payload:
                click.echo("\n" + "=" * 60)
                click.echo("CloudWatch logs:")
                for row in logs_payload:
                    click.echo(f"\n[{row['function']}]")
                    for line in row["lines"]:
                        click.echo(f"  {line}")
            else:
                click.echo("\nNo matching logs found (or access unavailable).")
        return

    day_start_utc, day_end_utc, target_day = parse_day_window(date_str)
    items = collect_pipeline_items(
        s3,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
        source=source,
        stuck_minutes=stuck_minutes,
    )
    summary = summarize_pipeline_items(items)

    if interactive:
        interactive_pipeline_day_view(s3_client=s3, items=items, target_day=target_day, source=source)
        return

    if as_json:
        payload = {
            "date": target_day.isoformat(),
            "source": source,
            "stuck_minutes": stuck_minutes,
            "summary": summary,
            "items": [
                {
                    "source_type": item.source_type,
                    "ticker": item.ticker,
                    "cik": item.cik,
                    "form_type": item.form_type,
                    "source": item.source,
                    "item_id": item.item_id,
                    "stage": item.stage,
                    "arrived_at": item.arrived_at.isoformat(),
                    "age_minutes": item.age_minutes,
                    "key_prefix": item.key_prefix,
                    "alert_sent_at": item.alert_sent_at,
                }
                for item in items
            ],
        }
        click.echo(json.dumps(payload, indent=2))
        return

    click.echo(f"Pipeline day view ({target_day.isoformat()} ET, source={source})")
    click.echo(f"Total arrived: {summary['total']}")

    stage_counts = summary["stage_counts"]
    if stage_counts:
        click.echo("\nBy stage:")
        for stage in sorted(stage_counts):
            click.echo(f"  {stage}: {stage_counts[stage]}")

    source_counts = summary["source_counts"]
    if source_counts:
        click.echo("\nBy source:")
        for source_name in sorted(source_counts):
            click.echo(f"  {source_name}: {source_counts[source_name]}")

    form_counts = summary["form_counts"]
    if form_counts:
        click.echo("\nBy form:")
        for form in sorted(form_counts):
            click.echo(f"  {form}: {form_counts[form]}")

    click.echo(f"\nAlerts sent: {summary['alerts_sent']}")

    if verbose and items:
        click.echo("\nItems:")
        et = ZoneInfo("America/New_York")
        for item in items:
            ticker = item.ticker or "-"
            form = item.form_type or "-"
            source_label = item.source or "-"
            arrived_et = item.arrived_at.astimezone(et).strftime("%Y-%m-%d %H:%M:%S %z")
            click.echo(
                f"  {item.stage:13} {item.source_type:14} {ticker:8} {form:8} "
                f"{item.item_id} age={item.age_minutes}m source={source_label} arrived={arrived_et}"
            )


def _fetch_pipeline_logs(
    item_id: str,
    key_prefixes: list[str],
    max_lines: int,
    since_minutes: int,
    region_name: str | None = None,
) -> list[dict]:
    """Best-effort CloudWatch lookup across event pipeline lambdas."""
    functions = [
        "sec-filings-poller",
        "press-releases-poller",
        "filings-extractor",
        "filing-analyzer",
        "filing-alerts",
    ]
    patterns = [item_id] + key_prefixes
    start_ms = int((datetime.now().timestamp() - since_minutes * 60) * 1000)
    try:
        logs_client = boto3.client("logs", region_name=region_name) if region_name else boto3.client("logs")
    except NoRegionError:
        return []
    results = []

    for fn in functions:
        group = f"/aws/lambda/{fn}"
        lines: list[str] = []
        for pattern in patterns:
            if len(lines) >= max_lines:
                break
            try:
                resp = logs_client.filter_log_events(
                    logGroupName=group,
                    startTime=start_ms,
                    filterPattern=f'"{pattern}"',
                    limit=max_lines,
                )
            except ClientError:
                continue
            except Exception:
                continue
            for ev in resp.get("events", []):
                msg = (ev.get("message") or "").strip()
                if msg:
                    lines.append(msg)
                if len(lines) >= max_lines:
                    break

        deduped = []
        seen = set()
        for line in lines:
            if line in seen:
                continue
            seen.add(line)
            deduped.append(line)

        if deduped:
            results.append({"function": fn, "lines": deduped[:max_lines]})

    return results


def _print_trace_file_contents(s3_client, key_prefix: str, files: list[str]) -> None:
    """Print full contents for derived pipeline JSON artifacts only."""
    desired = ["extracted.json", "screening.json", "analysis.json"]
    printed = 0
    for name in desired:
        key = f"{key_prefix}/{name}"
        try:
            raw = download_file(s3_client, key)
        except Exception:
            continue

        printed += 1
        text = raw.decode("utf-8", errors="replace")
        click.echo("\n" + "-" * 60)
        click.echo(f"{name}:")
        click.echo(text)

    if printed == 0:
        click.echo("\n(no derived JSON artifacts found)")


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
@click.argument("tickers", nargs=-1)
def research_sync(tickers: tuple[str, ...]):
    """Sync local research artifacts for TICKER(s) to S3 and clean up workspace.

    If no tickers are given, syncs all workspaces that exist locally.
    """
    repo_root = find_repo_root()
    s3 = get_s3_client()

    if not tickers:
        workspace_root = repo_root / "workspace"
        tickers = tuple(
            d.name
            for d in sorted(workspace_root.iterdir())
            if d.is_dir() and d.name != "macro"
        )
        if not tickers:
            click.echo("No workspaces found to sync.")
            return
        click.echo(f"Syncing all {len(tickers)} workspace(s): {', '.join(tickers)}\n")

    for ticker in tickers:
        ticker = ticker.upper()
        local_dir = repo_root / "workspace" / ticker

        if not local_dir.exists():
            click.echo(f"No workspace found at {local_dir}")
            click.echo(f"Run 'praxis stage {ticker}' first to set up the workspace.")
            continue

        # Only sync research artifacts — skip ingested data, CLAUDE.md, and MCP config
        skip_prefixes = ("data/", "macro/")
        skip_names = {"CLAUDE.md", ".mcp.json"}
        found = []
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
            click.echo(f"No artifacts found in {local_dir}/")
            continue

        click.echo(f"Found artifacts in {local_dir}/:")
        for name in sorted(str(f) for f in found):
            click.echo(f"  {name}")

        s3_prefix = f"data/research/{ticker}"
        click.echo(f"\nUploading to s3://{BUCKET}/{s3_prefix}/ ...")

        uploaded = []
        for rel in found:
            s3_key = f"{s3_prefix}/{rel}"
            upload_file(s3, local_dir / rel, s3_key)
            uploaded.append(s3_key)
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
# praxis migrate
# ---------------------------------------------------------------------------

@cli.command("migrate")
@click.argument("source_dir", type=click.Path(exists=True, file_okay=False))
@click.option("--dry-run", is_flag=True, help="Show what would be uploaded without uploading")
@click.option("--ticker", help="Migrate a single ticker instead of all")
def migrate(source_dir: str, dry_run: bool, ticker: str | None):
    """Bulk-import existing research artifacts to S3.

    Walks SOURCE_DIR for subdirectories containing memo.md and uploads
    all files in each as research artifacts for that ticker.

    \b
    Example:
      praxis migrate ~/dev/models/claude/
      praxis migrate ~/dev/models/claude/ --ticker CRDO
      praxis migrate ~/dev/models/claude/ --dry-run
    """
    source = Path(source_dir)
    s3 = None if dry_run else get_s3_client()

    # Find ticker directories (must contain memo.md)
    ticker_dirs = []
    for child in sorted(source.iterdir()):
        if not child.is_dir():
            continue
        if ticker and child.name.upper() != ticker.upper():
            continue
        if (child / "memo.md").exists():
            ticker_dirs.append(child)

    if not ticker_dirs:
        click.echo("No ticker directories with memo.md found.")
        return

    click.echo(f"Found {len(ticker_dirs)} ticker(s) to migrate\n")

    total_uploaded = 0
    for tdir in ticker_dirs:
        tk = tdir.name.upper()
        files = [f for f in tdir.rglob("*") if f.is_file()]
        # Skip hidden files and non-research artifacts
        files = [f for f in files if not f.name.startswith(".")]

        if dry_run:
            click.echo(f"  {tk}: {len(files)} file(s)")
            for f in files:
                click.echo(f"    {f.relative_to(tdir)}")
            continue

        s3_prefix = f"data/research/{tk}"
        for f in files:
            relative = str(f.relative_to(tdir))
            s3_key = f"{s3_prefix}/{relative}"
            s3.put_object(Bucket=BUCKET, Key=s3_key, Body=f.read_bytes())

        total_uploaded += len(files)
        click.echo(f"  {tk}: {len(files)} file(s) → s3://{BUCKET}/{s3_prefix}/")

    if dry_run:
        click.echo(f"\nDry run — nothing uploaded. Remove --dry-run to migrate.")
    else:
        click.echo(f"\nMigrated {total_uploaded} file(s) across {len(ticker_dirs)} ticker(s)")
        click.echo("View with: praxis research show <TICKER>")
        click.echo("To ingest data + add to universe: praxis universe add <TICKER>")


# ---------------------------------------------------------------------------
# praxis supplement
# ---------------------------------------------------------------------------

@cli.group()
def supplement():
    """Add supplementary materials to research sessions."""
    pass


@supplement.command("add")
@click.argument("ticker")
@click.option("-f", "--file", "file_path", type=click.Path(exists=True),
              help="Path to a local file to upload as supplement")
@click.option("-u", "--url", help="URL to scrape and save as supplement")
@click.option("-n", "--name", help="Override filename (e.g. 'competitor-analysis.md')")
def supplement_add(ticker: str, file_path: str | None, url: str | None, name: str | None):
    """Add a supplement to TICKER's research data.

    Three modes:

    \b
      praxis supplement add NVDA -f notes.md          # upload local file
      praxis supplement add NVDA -u https://example.com/article  # scrape URL
      echo "my notes" | praxis supplement add NVDA -n notes.md   # pipe stdin

    Supplements land in S3 under data/research/{ticker}/data/supplements/
    and are pulled into the workspace by `praxis stage`.
    """
    ticker = ticker.upper()
    s3 = get_s3_client()

    if file_path and url:
        click.echo("Specify either --file or --url, not both.", err=True)
        return

    if file_path:
        # Mode 1: upload local file
        path = Path(file_path)
        filename = name or path.name
        content = path.read_bytes()
        click.echo(f"Uploading {path.name} as supplement for {ticker}...")

    elif url:
        # Mode 2: scrape URL
        import requests
        from bs4 import BeautifulSoup

        click.echo(f"Fetching {url}...")
        try:
            resp = requests.get(url, timeout=30, headers={
                "User-Agent": "Mozilla/5.0 (compatible; PraxisCopilot/0.1)"
            })
            resp.raise_for_status()
        except requests.RequestException as e:
            click.echo(f"Failed to fetch URL: {e}", err=True)
            return

        soup = BeautifulSoup(resp.text, "lxml")

        # Remove noise
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

        # Extract title
        title = soup.title.string.strip() if soup.title and soup.title.string else ""

        # Get text
        body = soup.find("article") or soup.find("main") or soup.body or soup
        text = body.get_text(separator="\n")
        # Clean whitespace
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        text = "\n".join(lines)
        text = re.sub(r"\n{3,}", "\n\n", text)

        # Build markdown
        date_str = datetime.now().strftime("%Y-%m-%d")
        md = f"# {title}\n\n> Source: {url}\n> Scraped: {date_str}\n\n{text}"
        content = md.encode("utf-8")

        if name:
            filename = name
        else:
            # Derive filename from URL
            slug = re.sub(r"[^a-z0-9]+", "-", url.split("//")[-1].lower()).strip("-")[:60]
            filename = f"{slug}.md"

        click.echo(f"Scraped {len(lines)} lines from {url}")

    elif not sys.stdin.isatty():
        # Mode 3: pipe from stdin
        if not name:
            click.echo("When piping stdin, --name is required (e.g. -n notes.md)", err=True)
            return
        filename = name
        content = sys.stdin.buffer.read()
        click.echo(f"Reading from stdin...")

    else:
        click.echo("Provide --file, --url, or pipe content to stdin.", err=True)
        return

    # Ensure .md extension for text content
    if not any(filename.endswith(ext) for ext in [".md", ".txt", ".yaml", ".json", ".csv"]):
        filename += ".md"

    s3_key = f"data/research/{ticker}/data/supplements/{filename}"
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=content)
    click.echo(f"  Uploaded: s3://{BUCKET}/{s3_key}")

    # Also write to local workspace if it exists
    repo_root = find_repo_root()
    local_path = repo_root / "workspace" / ticker / "data" / "supplements" / filename
    if (repo_root / "workspace" / ticker).exists():
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(content)
        click.echo(f"  Also written to: {local_path}")


@supplement.command("list")
@click.argument("ticker")
def supplement_list(ticker: str):
    """List supplements for TICKER."""
    ticker = ticker.upper()
    s3 = get_s3_client()

    prefix = f"data/research/{ticker}/data/supplements/"
    keys = list_prefix(s3, prefix)
    supplements = [k[len(prefix):] for k in keys if k != prefix]

    if not supplements:
        click.echo(f"No supplements for {ticker}.")
        click.echo(f"Add one: praxis supplement add {ticker} -f file.md")
        return

    click.echo(f"Supplements for {ticker} ({len(supplements)}):\n")
    for name in sorted(supplements):
        click.echo(f"  {name}")


@supplement.command("remove")
@click.argument("ticker")
@click.argument("filename")
def supplement_remove(ticker: str, filename: str):
    """Remove a supplement from TICKER."""
    ticker = ticker.upper()
    s3 = get_s3_client()

    s3_key = f"data/research/{ticker}/data/supplements/{filename}"
    if not key_exists(s3, s3_key):
        click.echo(f"Supplement not found: {filename}", err=True)
        return

    s3.delete_object(Bucket=BUCKET, Key=s3_key)
    click.echo(f"Removed: {filename}")

    # Clean local copy too
    repo_root = find_repo_root()
    local_path = repo_root / "workspace" / ticker / "data" / "supplements" / filename
    if local_path.exists():
        local_path.unlink()
        click.echo(f"  Also removed local copy")


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
