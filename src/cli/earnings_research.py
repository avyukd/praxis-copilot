"""Pre-earnings auto-research — queues research before earnings dates.

Checks the event calendar for upcoming earnings within N days.
For each, auto-creates a GitHub Issue research task focused on:
- Consensus estimates and whisper numbers
- Recent guidance and management commentary
- Sector comps that already reported
- Key metrics to watch
- Position sizing / risk management for the event

Runs daily or can be triggered manually.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import date, timedelta
from pathlib import Path

import click
import yaml

from cli.config_utils import find_repo_root, get_config_dir, load_yaml
from cli.event_calendar import get_upcoming_events, load_calendar, add_event
from cli.models import PortfolioConfig

logger = logging.getLogger(__name__)


def _load_portfolio() -> PortfolioConfig:
    config_dir = get_config_dir()
    path = config_dir / "portfolio.yaml"
    if path.exists():
        return PortfolioConfig(**load_yaml(path))
    return PortfolioConfig()


def find_upcoming_earnings(days_ahead: int = 5) -> list[dict]:
    """Find earnings events within the next N days.

    Returns list of dicts with ticker, date, description, and portfolio status.
    """
    events = get_upcoming_events(days_ahead)
    portfolio = _load_portfolio()
    portfolio_tickers = {p.ticker.upper() for p in portfolio.positions}
    watchlist_tickers = {t.upper() for t in portfolio.watchlist}

    earnings = []
    for e in events:
        if e.type != "earnings":
            continue
        ticker = e.ticker.upper()
        is_portfolio = ticker in portfolio_tickers
        is_watchlist = ticker in watchlist_tickers
        days_until = (date.fromisoformat(e.date) - date.today()).days

        earnings.append({
            "ticker": ticker,
            "date": e.date,
            "days_until": days_until,
            "description": e.description,
            "is_portfolio": is_portfolio,
            "is_watchlist": is_watchlist,
            "priority": 0 if is_portfolio else 1 if is_watchlist else 2,
        })

    # Sort: portfolio first, then watchlist, then by date
    earnings.sort(key=lambda e: (e["priority"], e["days_until"]))
    return earnings


def queue_earnings_research(
    earnings: list[dict],
    *,
    already_queued: set[str] | None = None,
    dry_run: bool = False,
) -> list[str]:
    """Create GitHub Issues for pre-earnings research.

    Returns list of tickers that were queued.
    """
    if already_queued is None:
        already_queued = set()

    queued = []

    for e in earnings:
        ticker = e["ticker"]
        if ticker in already_queued:
            continue

        # Check if there's already a recent memo
        memo_path = find_repo_root() / "workspace" / ticker / "memo.yaml"
        memo_fresh = False
        if memo_path.exists():
            try:
                memo = yaml.safe_load(memo_path.read_text()) or {}
                memo_date = memo.get("date", "")
                if memo_date:
                    days_since = (date.today() - date.fromisoformat(memo_date)).days
                    if days_since <= 7:
                        memo_fresh = True
            except Exception:
                pass

        status = "PORTFOLIO" if e["is_portfolio"] else "WATCHLIST" if e["is_watchlist"] else "UNIVERSE"
        days_label = "TOMORROW" if e["days_until"] == 1 else f"in {e['days_until']} days"

        body = (
            f"Tickers: ${ticker}\n\n"
            f"**Pre-Earnings Research** — {ticker} reports {days_label} ({e['date']})\n"
            f"Status: {status}\n\n"
            f"{'Existing memo is recent (within 7 days). Focus on updating with earnings-specific analysis.' if memo_fresh else 'No recent memo. Run full research pipeline.'}\n\n"
            f"## Research Focus\n\n"
            f"1. **Consensus estimates** — What is the Street expecting for revenue, EPS, and key metrics?\n"
            f"2. **Whisper numbers** — Is there a buy-side consensus that differs from sell-side?\n"
            f"3. **Recent guidance** — What did management guide to last quarter? Any pre-announcements?\n"
            f"4. **Sector comps** — Have peers already reported? What did their results signal for {ticker}?\n"
            f"5. **Key metrics to watch** — What specific line items or KPIs will drive the reaction?\n"
            f"6. **Options implied move** — What is the options market pricing for the earnings move?\n"
            f"7. **Risk management** — Should we trim before, hold through, or add post-earnings?\n\n"
            f"Produce memo.md and memo.yaml with earnings-specific tactical setup."
        )

        if dry_run:
            click.echo(f"  [DRY RUN] Would queue: {ticker} ({status}, reports {days_label})")
            queued.append(ticker)
            continue

        try:
            result = subprocess.run(
                [
                    "gh", "issue", "create",
                    "--repo", "avyukd/praxis-copilot",
                    "--title", f"[earnings] {ticker} reports {e['date']} — pre-earnings research",
                    "--body", body,
                    "--label", "research-queue",
                ],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0:
                click.echo(f"  Queued: {ticker} ({status}, reports {days_label}) — {result.stdout.strip()}")
                queued.append(ticker)
            else:
                logger.error("Failed to queue %s: %s", ticker, result.stderr)
        except Exception as e_err:
            logger.error("Failed to queue %s: %s", ticker, e_err)

    return queued


def discover_earnings_dates(tickers: list[str] | None = None) -> int:
    """Discover earnings dates for portfolio/watchlist tickers and add to calendar.

    Uses yfinance to look up next earnings dates.
    Returns number of events added.
    """
    if tickers is None:
        portfolio = _load_portfolio()
        tickers = [p.ticker for p in portfolio.positions] + portfolio.watchlist

    added = 0
    for ticker in tickers:
        try:
            import yfinance as yf
            t = yf.Ticker(ticker)
            cal = t.calendar
            if cal is not None and not cal.empty:
                # yfinance calendar has 'Earnings Date' column
                if hasattr(cal, 'iloc'):
                    for idx, row in cal.iterrows():
                        if 'Earnings Date' in str(idx):
                            earnings_date = str(row.iloc[0])[:10]
                            add_event(
                                ticker=ticker.upper(),
                                event_date=earnings_date,
                                event_type="earnings",
                                description=f"{ticker.upper()} earnings report",
                                source="yfinance_discovery",
                            )
                            added += 1
                            break
            # Also try the earnings_dates property
            earnings_dates = t.earnings_dates
            if earnings_dates is not None and not earnings_dates.empty:
                next_date = earnings_dates.index[0]
                earnings_date = next_date.strftime("%Y-%m-%d")
                # Only add if in the future
                if earnings_date >= date.today().isoformat():
                    add_event(
                        ticker=ticker.upper(),
                        event_date=earnings_date,
                        event_type="earnings",
                        description=f"{ticker.upper()} earnings report",
                        source="yfinance_discovery",
                    )
                    added += 1
        except Exception:
            continue

    return added


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.group("earnings")
def earnings_cli():
    """Pre-earnings research automation."""
    pass


@earnings_cli.command("scan")
@click.option("--days", type=int, default=5, show_default=True, help="Days ahead to look")
@click.option("--dry-run", is_flag=True, help="Show what would be queued without creating issues")
def earnings_scan(days: int, dry_run: bool):
    """Check for upcoming earnings and queue pre-earnings research.

    \b
    Looks at the event calendar for earnings within N days.
    Creates GitHub Issues for the research queue daemon.

    \b
    Examples:
      praxis earnings scan
      praxis earnings scan --days 3 --dry-run
    """
    upcoming = find_upcoming_earnings(days)
    if not upcoming:
        click.echo(f"No earnings events in the next {days} days.")
        click.echo("Add earnings dates with: praxis calendar add TICKER YYYY-MM-DD --type earnings")
        click.echo("Or discover them: praxis earnings discover")
        return

    click.echo(f"Upcoming earnings ({len(upcoming)}):")
    queued = queue_earnings_research(upcoming, dry_run=dry_run)
    click.echo(f"\n{len(queued)} research task(s) {'would be ' if dry_run else ''}queued.")


@earnings_cli.command("discover")
@click.option("--tickers", "-t", default=None, help="Comma-separated tickers (default: portfolio + watchlist)")
def earnings_discover(tickers: str | None):
    """Discover and save earnings dates from yfinance.

    \b
    Looks up next earnings dates for portfolio/watchlist tickers
    and adds them to the event calendar.

    \b
    Examples:
      praxis earnings discover
      praxis earnings discover -t AAPL,NVDA,MSFT
    """
    ticker_list = tickers.split(",") if tickers else None
    click.echo("Discovering earnings dates...")
    added = discover_earnings_dates(ticker_list)
    click.echo(f"Added {added} earnings date(s) to calendar.")
    if added > 0:
        click.echo("Run 'praxis calendar list' to see them.")
