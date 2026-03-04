"""CLI commands for managing monitors."""
from __future__ import annotations

import os
import re
import subprocess
import tempfile
from pathlib import Path

import click
import yaml

from cli.config_utils import find_repo_root, get_config_dir, load_yaml, save_yaml
from cli.s3 import BUCKET, download_file, get_s3_client, list_prefix


MONITOR_TYPES = ("filing", "scraper", "search")


def _monitors_dir() -> Path:
    d = get_config_dir() / "monitors"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _load_local_monitors() -> list[dict]:
    """Load all monitor YAML files from config/monitors/."""
    monitors = []
    d = _monitors_dir()
    for f in sorted(d.glob("*.yaml")):
        data = load_yaml(f)
        if data:
            monitors.append(data)
    return monitors


def _slugify(text: str) -> str:
    """Convert text to a slug suitable for monitor IDs."""
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")


def _resolve_type(draft: dict) -> str:
    """Resolve the monitor type from a draft.

    If the draft already specifies a valid type (filing/scraper/search), use it.
    For legacy types (agent), infer from description.
    """
    draft_type = draft.get("type", "").lower().strip()

    # Draft already uses our schema types — trust it
    if draft_type in MONITOR_TYPES:
        return draft_type

    # Legacy "agent" type needs inference from description
    if draft_type == "agent":
        desc = (draft.get("description", "") + " " + draft.get("name", "")).lower()
        # If it references filings/10-K/10-Q/proxy, it's filing
        if any(kw in desc for kw in ["10-k", "10-q", "filing", "proxy", "earnings call"]):
            return "filing"
        # Otherwise agent maps to search (external signals)
        return "search"

    # Unknown type — guess from description
    desc = (draft.get("description", "") + " " + draft.get("name", "")).lower()
    if any(kw in desc for kw in ["10-k", "10-q", "filing", "quarterly filings"]):
        return "filing"
    if any(kw in desc for kw in ["url", "website", "scrape", "monthly revenue"]):
        return "scraper"
    return "search"


def _infer_filing_types(draft: dict) -> list[str]:
    """Infer filing types from draft description."""
    desc = draft.get("description", "").lower()
    types = []
    if "10-k" in desc or "annual" in desc:
        types.append("10-K")
    if "10-q" in desc or "quarterly" in desc:
        types.append("10-Q")
    if "8-k" in desc:
        types.append("8-K")
    if "earnings" in desc and not types:
        types = ["10-K", "10-Q"]
    if "proxy" in desc:
        types.append("DEF 14A")
    return types or ["10-K", "10-Q"]


def _draft_to_monitor(draft: dict, ticker: str) -> dict:
    """Convert a research draft into a monitor config dict.

    Pre-fills as much as possible from the draft content.
    """
    name = draft.get("name", draft.get("id", "unknown"))
    monitor_id = f"{ticker.lower()}-{_slugify(name)}"
    mtype = _resolve_type(draft)

    monitor: dict = {
        "id": monitor_id,
        "type": mtype,
        "tickers": [ticker],
        "description": draft.get("description", name),
        "extract": draft.get("extract", draft.get("description", "")),
        "threshold": draft.get("threshold", ""),
    }

    if mtype == "filing":
        monitor["filing_types"] = _infer_filing_types(draft)
    elif mtype == "search":
        # Auto-generate search queries from description
        monitor["queries"] = _generate_search_queries(draft, ticker)
        monitor["search_backend"] = "brave"
        monitor["frequency"] = "daily"

    return monitor


def _generate_search_queries(draft: dict, ticker: str) -> list[str]:
    """Generate search queries from draft description."""
    desc = draft.get("description", "")
    name = draft.get("name", "")
    # Use the name as primary query, add ticker context
    queries = []
    if name:
        queries.append(f'"{name.lower()}"')
    # Pull quoted terms from description if any
    quoted = re.findall(r'"([^"]+)"', desc)
    for q in quoted:
        if q not in queries:
            queries.append(f'"{q}"')
    return queries or [name]


@click.group()
def monitor():
    """Monitor management commands."""
    pass


@monitor.command("list")
def monitor_list():
    """List committed monitors from local config/monitors/."""
    monitors = _load_local_monitors()
    if not monitors:
        click.echo("No monitors configured. Run 'praxis monitor drafts' to review drafts.")
        return

    click.echo(f"Monitors ({len(monitors)}):\n")
    for m in monitors:
        tickers = ", ".join(m.get("tickers", []))
        mtype = m.get("type", "?")
        mid = m.get("id", "?")
        click.echo(f"  {mid}")
        click.echo(f"    type: {mtype}  tickers: {tickers}")
        click.echo(f"    {m.get('description', '')}")
        if mtype == "filing":
            click.echo(f"    filing_types: {', '.join(m.get('filing_types', []))}")
        elif mtype == "search":
            click.echo(f"    queries: {m.get('queries', [])}")
            click.echo(f"    backend: {m.get('search_backend', 'brave')}  frequency: {m.get('frequency', 'daily')}")
        elif mtype == "scraper":
            click.echo(f"    script: {m.get('scraper_script', '?')}  url: {m.get('source_url', '?')}")
        if m.get("threshold"):
            click.echo(f"    threshold: {m['threshold']}")
        click.echo()


@monitor.command("drafts")
@click.argument("ticker", required=False)
def monitor_drafts(ticker: str | None):
    """Show draft monitors from S3 research sessions.

    Without TICKER, lists all tickers with drafts. With TICKER, shows full
    detail for each draft including inferred type, filing types, and threshold.
    """
    s3 = get_s3_client()

    if ticker is None:
        prefix = "data/research/"
        keys = list_prefix(s3, prefix)
        draft_keys = [k for k in keys if k.endswith("/draft_monitors.yaml")]
        if not draft_keys:
            click.echo("No draft monitors found in S3.")
            return

        click.echo(f"Tickers with draft monitors ({len(draft_keys)}):\n")
        for key in sorted(draft_keys):
            tk = key.split("/")[2]
            try:
                content = download_file(s3, key)
                drafts = yaml.safe_load(content) or {}
                if isinstance(drafts, dict):
                    drafts = drafts.get("monitors", [drafts])
                click.echo(f"  {tk}  ({len(drafts)} drafts)")
            except Exception:
                click.echo(f"  {tk}")
        click.echo(f"\nView details: praxis monitor drafts <TICKER>")
        click.echo(f"Approve:      praxis monitor approve <TICKER>")
        return

    ticker = ticker.upper()
    key = f"data/research/{ticker}/draft_monitors.yaml"
    try:
        content = download_file(s3, key)
    except Exception:
        click.echo(f"No draft monitors found for {ticker}.")
        return

    drafts = yaml.safe_load(content)
    if not drafts:
        click.echo(f"Empty draft_monitors.yaml for {ticker}.")
        return

    if isinstance(drafts, dict):
        drafts = drafts.get("monitors", [drafts])

    click.echo(f"Draft monitors for {ticker} ({len(drafts)}):\n")
    for i, draft in enumerate(drafts):
        name = draft.get("name", draft.get("id", "?"))
        draft_type = draft.get("type", "?")
        resolved = _resolve_type(draft)
        desc = draft.get("description", "")
        threshold = draft.get("threshold", "")

        click.echo(f"  [{i}] {name}")
        if draft_type != resolved:
            click.echo(f"      type: {draft_type} -> {resolved}")
        else:
            click.echo(f"      type: {resolved}")
        if resolved == "filing":
            ft = _infer_filing_types(draft)
            click.echo(f"      filing_types: {', '.join(ft)}")
        elif resolved == "search":
            queries = _generate_search_queries(draft, ticker)
            click.echo(f"      queries: {queries}")
            click.echo(f"      frequency: daily")
        elif resolved == "scraper":
            url = draft.get("source_url", "")
            if url:
                click.echo(f"      source_url: {url}")
        click.echo(f"      description: {desc}")
        if threshold:
            click.echo(f"      threshold: {threshold}")
        click.echo()

    click.echo(f"Edit drafts: praxis monitor drafts-edit {ticker}")
    click.echo(f"Approve:     praxis monitor approve {ticker}")
    click.echo(f"Approve one: praxis monitor approve {ticker} -n 0")


@monitor.command("drafts-edit")
@click.argument("ticker")
def monitor_drafts_edit(ticker: str):
    """Edit draft monitors for TICKER in $EDITOR, then push back to S3.

    Downloads draft_monitors.yaml, opens in your editor, uploads on save.
    """
    ticker = ticker.upper()
    s3 = get_s3_client()

    s3_key = f"data/research/{ticker}/draft_monitors.yaml"
    try:
        content = download_file(s3, s3_key)
    except Exception:
        click.echo(f"No draft monitors found for {ticker}.")
        return

    # Write to temp file and open in editor
    editor = os.environ.get("EDITOR", "vi")
    with tempfile.NamedTemporaryFile(
        suffix=f"-{ticker}-drafts.yaml", mode="wb", delete=False
    ) as f:
        f.write(content)
        tmp_path = f.name

    click.echo(f"Opening {ticker} drafts in {editor}...")
    subprocess.run([editor, tmp_path])

    # Read back and validate
    try:
        with open(tmp_path) as f:
            edited = f.read()
        data = yaml.safe_load(edited)
        if not data:
            click.echo("Empty file — not uploading.")
            return
    except yaml.YAMLError as e:
        click.echo(f"Invalid YAML: {e}", err=True)
        click.echo(f"Your edits are saved at {tmp_path}")
        return

    # Count monitors for confirmation
    monitors = data.get("monitors", [data] if isinstance(data, dict) and "name" in data else [])
    if isinstance(data, list):
        monitors = data

    # Upload back to S3
    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=edited.encode(),
        ContentType="application/x-yaml",
    )
    Path(tmp_path).unlink(missing_ok=True)
    click.echo(f"Updated {ticker} drafts on S3 ({len(monitors)} monitors).")
    click.echo(f"Review: praxis monitor drafts {ticker}")


@monitor.command("approve")
@click.argument("ticker")
@click.option("-n", "--number", type=int, multiple=True,
              help="Approve specific draft(s) by index number. Can repeat: -n 0 -n 2")
@click.option("--all", "approve_all", is_flag=True, help="Approve all drafts without prompting")
@click.option("--edit", is_flag=True, help="Open each monitor in $EDITOR before saving")
def monitor_approve(ticker: str, number: tuple[int, ...], approve_all: bool, edit: bool):
    """Approve draft monitors for TICKER.

    Pre-fills type, filing_types, queries etc. from draft content.
    Use --edit to review/amend each monitor YAML in your editor before saving.

    \b
    Examples:
      praxis monitor approve AGM              # interactive, one by one
      praxis monitor approve AGM --all        # approve all with inferred defaults
      praxis monitor approve AGM -n 0 -n 2    # approve drafts #0 and #2
      praxis monitor approve AGM --all --edit # approve all, edit each in $EDITOR
    """
    ticker = ticker.upper()
    s3 = get_s3_client()

    key = f"data/research/{ticker}/draft_monitors.yaml"
    try:
        content = download_file(s3, key)
    except Exception:
        click.echo(f"No draft monitors found for {ticker}.")
        return

    drafts = yaml.safe_load(content)
    if not drafts:
        click.echo(f"Empty draft_monitors.yaml for {ticker}.")
        return

    if isinstance(drafts, dict):
        drafts = drafts.get("monitors", [drafts])

    # Filter to specific indices if requested
    if number:
        selected = []
        for n in number:
            if 0 <= n < len(drafts):
                selected.append(drafts[n])
            else:
                click.echo(f"Draft index {n} out of range (0-{len(drafts)-1})")
        drafts = selected

    monitors_dir = _monitors_dir()
    approved = 0

    for draft in drafts:
        name = draft.get("name", draft.get("id", "unknown"))
        monitor_data = _draft_to_monitor(draft, ticker)

        if not approve_all:
            click.echo(f"\n{'='*50}")
            click.echo(f"  {name}")
            click.echo(f"  type: {monitor_data['type']}")
            click.echo(f"  description: {monitor_data['description']}")
            if monitor_data.get("threshold"):
                click.echo(f"  threshold: {monitor_data['threshold']}")
            if monitor_data.get("filing_types"):
                click.echo(f"  filing_types: {monitor_data['filing_types']}")
            if monitor_data.get("queries"):
                click.echo(f"  queries: {monitor_data['queries']}")

            if not click.confirm("\nApprove?"):
                continue

            # Let user override type if the inference was wrong
            if click.confirm("Change type?", default=False):
                monitor_data["type"] = click.prompt(
                    "Type", type=click.Choice(MONITOR_TYPES),
                    default=monitor_data["type"],
                )
                # Re-fill type-specific fields
                if monitor_data["type"] == "filing" and not monitor_data.get("filing_types"):
                    ft = click.prompt("Filing types (comma-separated)", default="10-K,10-Q")
                    monitor_data["filing_types"] = [t.strip() for t in ft.split(",")]
                elif monitor_data["type"] == "search" and not monitor_data.get("queries"):
                    queries_str = click.prompt("Search queries (semicolon-separated)")
                    monitor_data["queries"] = [q.strip() for q in queries_str.split(";")]
                    monitor_data["search_backend"] = "brave"
                elif monitor_data["type"] == "scraper":
                    monitor_data["source_url"] = click.prompt("Source URL")
                    monitor_data["scraper_script"] = click.prompt("Script name", default=monitor_data["id"])

        if edit:
            monitor_data = _edit_in_editor(monitor_data)
            if monitor_data is None:
                click.echo("  Skipped (empty or invalid YAML from editor)")
                continue

        out_path = monitors_dir / f"{monitor_data['id']}.yaml"
        save_yaml(out_path, monitor_data)
        click.echo(f"  Wrote {out_path.name}")
        approved += 1

    if approved:
        click.echo(f"\nApproved {approved} monitor(s). Run 'praxis config sync' to push to S3.")
    else:
        click.echo("\nNo monitors approved.")


@monitor.command("edit")
@click.argument("monitor_id")
def monitor_edit(monitor_id: str):
    """Open a committed monitor in $EDITOR for editing."""
    monitors_dir = _monitors_dir()
    path = monitors_dir / f"{monitor_id}.yaml"
    if not path.exists():
        click.echo(f"Monitor not found: {monitor_id}")
        click.echo(f"Available: {', '.join(p.stem for p in monitors_dir.glob('*.yaml'))}")
        return

    editor = os.environ.get("EDITOR", "vi")
    subprocess.run([editor, str(path)])

    # Validate after edit
    try:
        data = load_yaml(path)
        if not data:
            click.echo("Warning: file is empty after edit")
        elif "id" not in data or "type" not in data:
            click.echo("Warning: missing required fields (id, type)")
        else:
            click.echo(f"Updated {monitor_id}. Run 'praxis config sync' to push to S3.")
    except Exception as e:
        click.echo(f"Warning: YAML parse error: {e}")


@monitor.command("show")
@click.argument("monitor_id")
def monitor_show(monitor_id: str):
    """Show full YAML for a committed monitor."""
    monitors_dir = _monitors_dir()
    path = monitors_dir / f"{monitor_id}.yaml"
    if not path.exists():
        click.echo(f"Monitor not found: {monitor_id}")
        return

    click.echo(path.read_text())


@monitor.command("add")
@click.option("--id", "monitor_id", prompt="Monitor ID", help="Unique monitor identifier")
@click.option("--type", "mtype", prompt="Type", type=click.Choice(MONITOR_TYPES), help="Monitor type")
@click.option("--tickers", prompt="Tickers (comma-separated)", help="Comma-separated tickers")
@click.option("--description", prompt="Description", help="Monitor description")
def monitor_add(monitor_id: str, mtype: str, tickers: str, description: str):
    """Manually create a new monitor."""
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]

    extract = click.prompt("Extract prompt")
    threshold = click.prompt("Threshold", default="")

    monitor_data: dict = {
        "id": monitor_id,
        "type": mtype,
        "tickers": ticker_list,
        "description": description,
        "extract": extract,
        "threshold": threshold,
    }

    if mtype == "filing":
        ft = click.prompt("Filing types (comma-separated)", default="10-K,10-Q")
        monitor_data["filing_types"] = [t.strip() for t in ft.split(",") if t.strip()]
    elif mtype == "scraper":
        monitor_data["source_url"] = click.prompt("Source URL")
        monitor_data["scraper_script"] = click.prompt("Scraper script name", default=monitor_id)
    elif mtype == "search":
        queries_str = click.prompt("Search queries (semicolon-separated)")
        monitor_data["queries"] = [q.strip() for q in queries_str.split(";") if q.strip()]
        monitor_data["search_backend"] = click.prompt(
            "Search backend",
            type=click.Choice(["brave", "tavily", "serpapi"]),
            default="brave",
        )

    monitors_dir = _monitors_dir()
    out_path = monitors_dir / f"{monitor_id}.yaml"
    save_yaml(out_path, monitor_data)
    click.echo(f"Wrote {out_path}")
    click.echo("Run 'praxis config sync' to push to S3.")


@monitor.command("remove")
@click.argument("monitor_id")
def monitor_remove(monitor_id: str):
    """Remove a monitor from config/monitors/."""
    monitors_dir = _monitors_dir()
    path = monitors_dir / f"{monitor_id}.yaml"
    if not path.exists():
        click.echo(f"Monitor not found: {monitor_id}")
        return

    path.unlink()
    click.echo(f"Removed {monitor_id}. Run 'praxis config sync' to update S3.")


@monitor.group("scraper")
def monitor_scraper():
    """Scraper management subcommands."""
    pass


@monitor_scraper.command("init")
@click.argument("monitor_id")
def scraper_init(monitor_id: str):
    """Scaffold a new scraper script for a scraper monitor."""
    repo_root = find_repo_root()
    scrapers_dir = repo_root / "config" / "scrapers"
    scrapers_dir.mkdir(parents=True, exist_ok=True)

    path = scrapers_dir / f"{monitor_id}.py"
    if path.exists():
        click.echo(f"Scraper already exists: {path}")
        return

    template = f'''"""Scraper for {monitor_id} monitor.

This script is executed by the monitor evaluator to collect data.
It must define a `scrape() -> str` function that returns the scraped text.
"""
import requests


def scrape() -> str:
    """Fetch and return the data to monitor.

    Returns plain text that will be compared with previous runs
    and analyzed by Sonnet if changes are detected.
    """
    # TODO: Implement scraping logic
    # Example:
    # resp = requests.get("https://example.com/data", timeout=30)
    # resp.raise_for_status()
    # return resp.text
    raise NotImplementedError("Implement scrape() for {monitor_id}")
'''
    path.write_text(template)
    click.echo(f"Created scraper template: {path}")
    click.echo(f"Edit the scrape() function, then test with: praxis monitor scraper test {monitor_id}")


@monitor_scraper.command("test")
@click.argument("monitor_id")
def scraper_test(monitor_id: str):
    """Run a scraper script locally and show output."""
    repo_root = find_repo_root()
    path = repo_root / "config" / "scrapers" / f"{monitor_id}.py"
    if not path.exists():
        click.echo(f"Scraper not found: {path}")
        click.echo(f"Create one: praxis monitor scraper init {monitor_id}")
        return

    code = path.read_text()
    namespace: dict = {}
    try:
        exec(code, namespace)  # noqa: S102
    except Exception as e:
        click.echo(f"Failed to load scraper: {e}", err=True)
        return

    scrape_fn = namespace.get("scrape")
    if not callable(scrape_fn):
        click.echo("Scraper has no scrape() function", err=True)
        return

    click.echo(f"Running scraper {monitor_id}...\n")
    try:
        result = scrape_fn()
        click.echo(result)
        click.echo(f"\n--- {len(result)} chars ---")
    except Exception as e:
        click.echo(f"Scraper failed: {e}", err=True)


def _edit_in_editor(data: dict) -> dict | None:
    """Open a dict as YAML in $EDITOR, return parsed result."""
    editor = os.environ.get("EDITOR", "vi")
    yaml_str = yaml.dump(data, default_flow_style=False, sort_keys=False)

    with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
        f.write(yaml_str)
        tmp_path = f.name

    try:
        subprocess.run([editor, tmp_path])
        with open(tmp_path) as f:
            result = yaml.safe_load(f.read())
        if result and isinstance(result, dict):
            return result
        return None
    except Exception:
        return None
    finally:
        Path(tmp_path).unlink(missing_ok=True)
