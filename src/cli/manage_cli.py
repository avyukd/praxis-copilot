"""Local CLI for the manage (price alert) pipeline.

Replaces the Lambda-based manage pipeline with a local daemon that uses
yfinance as the price source. Generates the same alerts.yaml files in S3
that the analyst daemon consumes.
"""
from __future__ import annotations

import logging
import time as _time
from datetime import datetime
from zoneinfo import ZoneInfo

import click

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


@click.group("manage")
def manage():
    """Price alert pipeline — intraday threshold checks."""
    pass


@manage.command("run")
def manage_run():
    """Run a single intraday price check pass."""
    from modules.manage.handler import _handle_intraday

    result = _handle_intraday()
    click.echo(f"Checked {result.get('tickers_checked', 0)} tickers, "
               f"{result.get('alerts_generated', 0)} alerts generated")
    if result.get("errors"):
        for err in result["errors"]:
            click.echo(f"  ERROR: {err}")


@manage.command("daemon")
@click.option("--poll-interval", type=int, default=900, show_default=True,
              help="Seconds between checks (default 15 min)")
@click.option("--start-hour", type=int, default=9, show_default=True,
              help="Start hour ET")
@click.option("--end-hour", type=int, default=16, show_default=True,
              help="End hour ET")
def manage_daemon(poll_interval: int, start_hour: int, end_hour: int):
    """Run the price alert pipeline as a continuous daemon.

    Replaces the Lambda-based pipeline. Uses yfinance for price data
    when EODHD is unavailable.
    """
    from cli.env_loader import load_env
    load_env()

    from modules.manage.handler import _handle_intraday

    click.echo("Manage daemon started")
    click.echo(f"  Window: {start_hour}:00 - {end_hour}:00 ET")
    click.echo(f"  Poll interval: {poll_interval}s")

    try:
        while True:
            now_et = datetime.now(ET)

            # No work on weekends
            if now_et.weekday() >= 5:
                _time.sleep(600)
                continue

            # Outside market hours
            if now_et.hour < start_hour or now_et.hour >= end_hour:
                _time.sleep(60)
                continue

            try:
                result = _handle_intraday()
                checked = result.get("tickers_checked", 0)
                alerts = result.get("alerts_generated", 0)
                errors = result.get("errors", [])
                click.echo(
                    f"[{now_et.strftime('%H:%M:%S')}] "
                    f"Checked {checked} tickers, {alerts} alerts"
                    f"{f', {len(errors)} errors' if errors else ''}"
                )
            except Exception as e:
                logger.error("Intraday check failed: %s", e, exc_info=True)

            _time.sleep(poll_interval)

    except KeyboardInterrupt:
        click.echo("\nManage daemon stopped.")
