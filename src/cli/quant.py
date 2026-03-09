"""CLI commands for the quant signal workspace."""
from __future__ import annotations

import shutil
from pathlib import Path

import click
import yaml

from cli.config_utils import find_repo_root, get_config_dir, save_yaml
from cli.quant_backtest import format_report, load_signal, run_backtest
from cli.quant_data import fetch_ohlcv
from cli.quant_prompt import generate_quant_prompt
from cli.s3 import BUCKET, get_s3_client, list_prefix, upload_directory, download_file


def _get_backtest_source() -> str:
    """Read the backtest module source for copying into workspace."""
    return (Path(__file__).parent / "quant_backtest.py").read_text()


@click.group()
def quant():
    """Quant signal exploration workspace."""
    pass


@quant.command("init")
@click.argument("tickers", nargs=-1, required=True)
def quant_init(tickers: tuple[str, ...]):
    """Initialize quant workspace for TICKER(s).

    Fetches OHLCV data (daily + intraday), sets up backtest harness, and
    generates a CLAUDE.md prompt for signal exploration.
    """
    repo_root = find_repo_root()

    for raw_ticker in tickers:
        ticker = raw_ticker.strip().upper()
        click.echo(f"\n{'='*50}")
        click.echo(f"Initializing quant workspace for {ticker}")
        click.echo(f"{'='*50}")

        workspace = repo_root / "workspace" / f"quant-{ticker}"
        data_dir = workspace / "data"
        lib_dir = workspace / "lib"
        signals_dir = workspace / "signals"
        results_dir = workspace / "results"

        for d in [data_dir, lib_dir, signals_dir, results_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Fetch data for ticker + SPY benchmark
        all_data_summary: dict[str, int] = {}

        click.echo(f"\n  Fetching {ticker} data...")
        ticker_summary = fetch_ohlcv(ticker, data_dir)
        all_data_summary.update(ticker_summary)

        click.echo(f"  Fetching SPY benchmark data...")
        spy_summary = fetch_ohlcv("SPY", data_dir)
        all_data_summary.update(spy_summary)

        # Copy backtest harness
        backtest_src = _get_backtest_source()
        (lib_dir / "backtest.py").write_text(backtest_src)
        # Also write __init__.py so lib is importable
        (lib_dir / "__init__.py").write_text("")
        click.echo(f"  Copied backtest harness to lib/backtest.py")

        # Generate CLAUDE.md
        prompt = generate_quant_prompt(ticker, all_data_summary)
        (workspace / "CLAUDE.md").write_text(prompt)
        click.echo(f"  Generated CLAUDE.md")

        # Summary
        click.echo(f"\n  Data summary:")
        for fname, rows in sorted(all_data_summary.items()):
            click.echo(f"    {fname}: {rows:,} rows")

        click.echo(f"\n  Workspace ready: {workspace}")
        click.echo(f"  cd {workspace} && claude")


@quant.command("sync")
@click.argument("tickers", nargs=-1)
def quant_sync(tickers: tuple[str, ...]):
    """Sync signals/ and results/ to S3, then clean up workspace.

    If no tickers given, auto-discovers all workspace/quant-* dirs.
    """
    repo_root = find_repo_root()
    workspace_root = repo_root / "workspace"

    # Discover workspaces
    if tickers:
        dirs = [workspace_root / f"quant-{t.strip().upper()}" for t in tickers]
    else:
        dirs = sorted(workspace_root.glob("quant-*")) if workspace_root.exists() else []

    if not dirs:
        click.echo("No quant workspaces found.")
        return

    s3 = get_s3_client()

    for ws_dir in dirs:
        if not ws_dir.exists():
            click.echo(f"  Workspace not found: {ws_dir}")
            continue

        ticker = ws_dir.name.replace("quant-", "")
        click.echo(f"\nSyncing {ticker}...")

        uploaded = 0
        for subdir in ["signals", "results"]:
            local = ws_dir / subdir
            if local.exists() and any(local.iterdir()):
                s3_prefix = f"data/quant/{ticker}/{subdir}"
                keys = upload_directory(s3, local, s3_prefix)
                uploaded += len(keys)
                click.echo(f"  {subdir}/: {len(keys)} file(s) uploaded")

        if uploaded > 0:
            click.echo(f"  Cleaning up workspace...")
            shutil.rmtree(ws_dir)
            click.echo(f"  Done. {uploaded} file(s) synced to s3://{BUCKET}/data/quant/{ticker}/")
        else:
            click.echo(f"  No signals or results to sync.")


@quant.command("promote")
@click.argument("signal_id")
@click.option("--ticker", "-t", help="Ticker (auto-detected from config if omitted)")
def quant_promote(signal_id: str, ticker: str | None):
    """Promote a validated signal to a live scraper monitor.

    Finds the signal .py + .yaml in local workspace or S3, generates a
    monitor config and self-contained scraper script.
    """
    repo_root = find_repo_root()
    config_dir = get_config_dir()

    # Find signal source: local workspace first, then S3
    signal_py = None
    signal_yaml = None

    # Search local workspaces
    for ws_dir in sorted((repo_root / "workspace").glob("quant-*")) if (repo_root / "workspace").exists() else []:
        signals_dir = ws_dir / "signals"
        if (signals_dir / f"{signal_id}.py").exists():
            signal_py = (signals_dir / f"{signal_id}.py").read_text()
            if (signals_dir / f"{signal_id}.yaml").exists():
                signal_yaml = yaml.safe_load((signals_dir / f"{signal_id}.yaml").read_text())
            if not ticker:
                ticker = ws_dir.name.replace("quant-", "")
            break

    # Fallback to S3
    if signal_py is None:
        s3 = get_s3_client()
        # Search all quant directories
        quant_keys = list_prefix(s3, "data/quant/")
        for key in quant_keys:
            if key.endswith(f"signals/{signal_id}.py"):
                signal_py = download_file(s3, key).decode()
                yaml_key = key.replace(".py", ".yaml")
                try:
                    signal_yaml = yaml.safe_load(download_file(s3, yaml_key).decode())
                except Exception:
                    pass
                if not ticker:
                    # Extract ticker from key: data/quant/{TICKER}/signals/...
                    parts = key.split("/")
                    ticker = parts[2] if len(parts) > 2 else None
                break

    if signal_py is None:
        raise click.ClickException(f"Signal '{signal_id}' not found in local workspaces or S3.")
    if not ticker:
        raise click.ClickException("Could not determine ticker. Use --ticker flag.")

    config_yaml = signal_yaml or {}
    description = config_yaml.get("description", f"Quant signal: {signal_id}")

    # Generate monitor config
    monitor_id = f"quant-{signal_id}"
    monitor_config = {
        "id": monitor_id,
        "type": "scraper",
        "tickers": [ticker],
        "description": description,
        "scraper_script": f"quant-{signal_id}",
        "threshold": "Alert on signal state change (fires / stops firing)",
    }

    monitors_dir = config_dir / "monitors"
    monitors_dir.mkdir(parents=True, exist_ok=True)
    monitor_path = monitors_dir / f"{monitor_id}.yaml"
    save_yaml(monitor_path, monitor_config)
    click.echo(f"  Monitor config: {monitor_path}")

    # Generate self-contained scraper script
    scraper_script = _generate_scraper_script(signal_id, signal_py, ticker, config_yaml)
    scrapers_dir = config_dir / "scrapers"
    scrapers_dir.mkdir(parents=True, exist_ok=True)
    scraper_path = scrapers_dir / f"quant-{signal_id}.py"
    scraper_path.write_text(scraper_script)
    click.echo(f"  Scraper script: {scraper_path}")

    click.echo(f"\n  Signal '{signal_id}' promoted to live monitor.")
    click.echo(f"  Run `praxis config sync` to deploy.")


def _generate_scraper_script(
    signal_id: str,
    signal_source: str,
    ticker: str,
    config: dict,
) -> str:
    """Generate a self-contained scraper script that embeds the signal function."""
    bar_size = config.get("bar_size", "1d")
    lookback = config.get("lookback", 50)
    # yfinance period mapping
    period = "1y" if bar_size == "1d" else "7d"
    interval = "1d" if bar_size == "1d" else "1m"

    return f'''"""Auto-generated quant scraper for signal: {signal_id}

Ticker: {ticker} | Bar size: {bar_size}
Self-contained: fetches latest bars via yfinance, runs signal, returns result.
"""
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime


# === Embedded signal function ===
{signal_source}
# === End signal function ===


def scrape():
    """Run the signal and return result string.

    Returns:
        SIGNAL_FIRED|{{TICKER}}|{{date}}|{{price}}|{{volume}}
        or NO_SIGNAL|{{TICKER}}|{{date}}
    """
    ticker = "{ticker}"
    try:
        t = yf.Ticker(ticker)
        df = t.history(period="{period}", interval="{interval}")
        if df.empty:
            return f"ERROR|{{ticker}}|no data"

        df = df.rename(columns={{
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        }})
        df = df[["open", "high", "low", "close", "volume"]]

        signals = signal(df)
        if not isinstance(signals, pd.Series):
            signals = pd.Series(signals, index=df.index)

        latest_date = df.index[-1].strftime("%Y-%m-%d")
        latest_price = df["close"].iloc[-1]
        latest_volume = int(df["volume"].iloc[-1])

        # Check if signal fires on the most recent bar
        if signals.iloc[-1]:
            return f"SIGNAL_FIRED|{{ticker}}|{{latest_date}}|{{latest_price:.2f}}|{{latest_volume}}"
        else:
            return f"NO_SIGNAL|{{ticker}}|{{latest_date}}"
    except Exception as e:
        return f"ERROR|{{ticker}}|{{e}}"


if __name__ == "__main__":
    print(scrape())
'''
