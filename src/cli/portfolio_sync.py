"""Portfolio sync — pull positions from IBKR and Fidelity.

Scaffolding for broker integration. Updates config/portfolio.yaml
with real positions from brokerage accounts.

TODO: user will provide API keys/credentials later.
"""

from __future__ import annotations

import logging
from pathlib import Path

import click
import yaml

from cli.config_utils import get_config_dir, load_yaml, save_yaml
from cli.models import PortfolioConfig, PortfolioPosition

logger = logging.getLogger(__name__)


def sync_ibkr(
    *,
    host: str = "127.0.0.1",
    port: int = 7497,
    client_id: int = 1,
) -> list[PortfolioPosition]:
    """Pull positions from Interactive Brokers TWS/Gateway.

    Requires: ib_insync package + TWS/Gateway running locally.
    Returns list of PortfolioPosition.

    TODO: implement when user provides IBKR credentials.
    """
    try:
        from ib_insync import IB
    except ImportError:
        click.echo("ib_insync not installed. Run: pip install ib_insync")
        return []

    click.echo(f"Connecting to IBKR at {host}:{port}...")
    ib = IB()
    try:
        ib.connect(host, port, clientId=client_id)
        positions = []
        for pos in ib.positions():
            contract = pos.contract
            ticker = contract.symbol
            # Add exchange suffix for non-US
            if contract.exchange and contract.exchange not in ("SMART", "NYSE", "NASDAQ", "ARCA"):
                exchange_map = {
                    "TSE": ".TO", "VENTURE": ".V", "ASX": ".AX",
                    "LSE": ".L", "SFB": ".CO", "SWX": ".SW",
                }
                suffix = exchange_map.get(contract.exchange, "")
                if suffix:
                    ticker = f"{ticker}{suffix}"

            positions.append(PortfolioPosition(
                ticker=ticker,
                shares=int(pos.position),
                avg_cost=float(pos.avgCost),
            ))
        click.echo(f"  Found {len(positions)} positions")
        return positions
    except Exception as e:
        click.echo(f"IBKR connection failed: {e}")
        return []
    finally:
        ib.disconnect()


def sync_fidelity(csv_path: str) -> list[PortfolioPosition]:
    """Parse Fidelity CSV export of positions.

    Fidelity doesn't have a real-time API for retail accounts.
    Export positions from Fidelity.com → Positions → Download.

    TODO: implement CSV parsing when user provides a sample export.
    """
    path = Path(csv_path)
    if not path.exists():
        click.echo(f"File not found: {csv_path}")
        return []

    click.echo(f"Parsing Fidelity CSV: {csv_path}")
    # TODO: parse CSV format
    # Typical Fidelity columns: Account, Symbol, Description, Quantity, Last Price, Current Value
    click.echo("  Fidelity CSV parsing not yet implemented. Provide a sample export.")
    return []


def merge_positions(
    existing: PortfolioConfig,
    new_positions: list[PortfolioPosition],
    source: str = "broker",
) -> PortfolioConfig:
    """Merge broker positions into existing portfolio config.

    Adds new positions, updates existing ones, but preserves
    manually-added positions that aren't in the broker data.
    """
    existing_tickers = {p.ticker.upper(): i for i, p in enumerate(existing.positions)}

    for pos in new_positions:
        t = pos.ticker.upper()
        if t in existing_tickers:
            # Update existing
            idx = existing_tickers[t]
            existing.positions[idx].shares = pos.shares
            existing.positions[idx].avg_cost = pos.avg_cost
        else:
            # Add new
            existing.positions.append(pos)

    return existing


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.group("portfolio")
def portfolio_cli():
    """Portfolio management and broker sync."""
    pass


@portfolio_cli.command("sync-ibkr")
@click.option("--host", default="127.0.0.1", help="TWS/Gateway host")
@click.option("--port", type=int, default=7497, help="TWS/Gateway port")
@click.option("--dry-run", is_flag=True, help="Show positions without saving")
def portfolio_sync_ibkr(host: str, port: int, dry_run: bool):
    """Sync portfolio from Interactive Brokers.

    \b
    Requires TWS or IB Gateway running locally.
    """
    positions = sync_ibkr(host=host, port=port)
    if not positions:
        return

    for p in positions:
        click.echo(f"  {p.ticker:12s} {p.shares:>8} shares @ ${p.avg_cost:.2f}")

    if dry_run:
        click.echo(f"\n[DRY RUN] Would update {len(positions)} positions")
        return

    config_dir = get_config_dir()
    portfolio_path = config_dir / "portfolio.yaml"
    existing = PortfolioConfig(**load_yaml(portfolio_path)) if portfolio_path.exists() else PortfolioConfig()
    updated = merge_positions(existing, positions, source="ibkr")
    save_yaml(portfolio_path, updated.model_dump())
    click.echo(f"\nPortfolio updated with {len(positions)} IBKR positions")


@portfolio_cli.command("sync-fidelity")
@click.argument("csv_path")
@click.option("--dry-run", is_flag=True, help="Show positions without saving")
def portfolio_sync_fidelity(csv_path: str, dry_run: bool):
    """Sync portfolio from Fidelity CSV export.

    \b
    Export from Fidelity.com → Positions → Download CSV.
    """
    positions = sync_fidelity(csv_path)
    if not positions:
        return

    if dry_run:
        click.echo(f"\n[DRY RUN] Would update {len(positions)} positions")
        return

    config_dir = get_config_dir()
    portfolio_path = config_dir / "portfolio.yaml"
    existing = PortfolioConfig(**load_yaml(portfolio_path)) if portfolio_path.exists() else PortfolioConfig()
    updated = merge_positions(existing, positions, source="fidelity")
    save_yaml(portfolio_path, updated.model_dump())
    click.echo(f"\nPortfolio updated with {len(positions)} Fidelity positions")


@portfolio_cli.command("show")
def portfolio_show():
    """Show current portfolio."""
    config_dir = get_config_dir()
    portfolio_path = config_dir / "portfolio.yaml"
    if not portfolio_path.exists():
        click.echo("No portfolio.yaml found.")
        return

    portfolio = PortfolioConfig(**load_yaml(portfolio_path))
    click.echo(f"Positions ({len(portfolio.positions)}):")
    for p in portfolio.positions:
        shares_str = f"{p.shares} shares" if p.shares else ""
        cost_str = f"@ ${p.avg_cost:.2f}" if p.avg_cost else ""
        click.echo(f"  {p.ticker:12s} {shares_str} {cost_str}")

    if portfolio.watchlist:
        click.echo(f"\nWatchlist ({len(portfolio.watchlist)}):")
        for t in portfolio.watchlist:
            click.echo(f"  {t}")
