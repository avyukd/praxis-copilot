"""CLI commands for managing monitors."""
from __future__ import annotations

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


@click.group()
def monitor():
    """Monitor management commands."""
    pass


@monitor.command("list")
def monitor_list():
    """List committed monitors from local config/monitors/."""
    monitors = _load_local_monitors()
    if not monitors:
        click.echo("No monitors configured. Run 'praxis monitor drafts <TICKER>' to review drafts.")
        return

    click.echo(f"Monitors ({len(monitors)}):\n")
    for m in monitors:
        tickers = ", ".join(m.get("tickers", []))
        mtype = m.get("type", "?")
        click.echo(f"  {m['id']}  [{mtype}]  tickers={tickers}")
        click.echo(f"    {m.get('description', '')}")


@monitor.command("drafts")
@click.argument("ticker", required=False)
def monitor_drafts(ticker: str | None):
    """Show draft monitors from S3 research sessions.

    Without TICKER, lists all tickers with drafts. With TICKER, shows draft details.
    """
    s3 = get_s3_client()

    if ticker is None:
        # List all tickers with draft_monitors.yaml
        prefix = "data/research/"
        keys = list_prefix(s3, prefix)
        draft_keys = [k for k in keys if k.endswith("/draft_monitors.yaml")]
        if not draft_keys:
            click.echo("No draft monitors found in S3.")
            return

        click.echo(f"Tickers with draft monitors ({len(draft_keys)}):\n")
        for key in sorted(draft_keys):
            tk = key.split("/")[2]
            click.echo(f"  {tk}")
        click.echo(f"\nView: praxis monitor drafts <TICKER>")
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
        click.echo(f"  [{i}] {draft.get('id', draft.get('name', '?'))}")
        click.echo(f"      {draft.get('description', '')}")
        if draft.get("extract"):
            extract_preview = draft["extract"][:100].replace("\n", " ")
            click.echo(f"      extract: {extract_preview}...")

    click.echo(f"\nApprove: praxis monitor approve {ticker}")


@monitor.command("approve")
@click.argument("ticker")
def monitor_approve(ticker: str):
    """Interactively approve draft monitors for TICKER.

    Shows each draft, prompts for type and type-specific fields,
    writes approved monitors to config/monitors/{id}.yaml.
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

    monitors_dir = _monitors_dir()
    approved = 0

    for draft in drafts:
        draft_id = draft.get("id", draft.get("name", "unknown"))
        description = draft.get("description", "")
        extract = draft.get("extract", "")

        click.echo(f"\n{'='*50}")
        click.echo(f"Draft: {draft_id}")
        click.echo(f"Description: {description}")
        if extract:
            click.echo(f"Extract prompt:\n  {extract[:200]}")

        if not click.confirm("\nApprove this monitor?"):
            continue

        # Pick type
        mtype = click.prompt(
            "Type",
            type=click.Choice(MONITOR_TYPES),
            default="filing",
        )

        # Normalize ID
        monitor_id = click.prompt("Monitor ID", default=_slugify(draft_id))

        # Tickers
        tickers_str = click.prompt("Tickers (comma-separated)", default=ticker)
        tickers = [t.strip().upper() for t in tickers_str.split(",") if t.strip()]

        # Extract prompt (editable)
        if extract:
            if click.confirm(f"Keep existing extract prompt?", default=True):
                final_extract = extract
            else:
                final_extract = click.prompt("Extract prompt")
        else:
            final_extract = click.prompt("Extract prompt")

        # Threshold
        threshold = click.prompt(
            "Threshold (when to alert)",
            default=draft.get("threshold", ""),
        )

        # Build monitor config
        monitor_data: dict = {
            "id": monitor_id,
            "type": mtype,
            "tickers": tickers,
            "description": description,
            "extract": final_extract,
            "threshold": threshold,
        }

        # Type-specific fields
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

        # Write to config/monitors/
        out_path = monitors_dir / f"{monitor_id}.yaml"
        save_yaml(out_path, monitor_data)
        click.echo(f"  Wrote {out_path}")
        approved += 1

    click.echo(f"\nApproved {approved} monitor(s). Run 'praxis config sync' to push to S3.")


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


def _slugify(text: str) -> str:
    """Convert text to a slug suitable for monitor IDs."""
    import re
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    return slug.strip("-")
