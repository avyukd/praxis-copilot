"""Praxis MCP server — exposes praxis tools to Claude Code and Claude Desktop.

Provides access to telemetry, briefings, portfolio, watchdog, audit,
filing research status, and analyst findings via MCP protocol.

Usage:
  Claude Code: configure in .mcp.json
  Claude Desktop: configure in claude_desktop_config.json
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

# Ensure praxis modules are importable
src_dir = str(Path(__file__).resolve().parent.parent)
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

mcp = FastMCP("praxis")


def _run_praxis(args: list[str]) -> str:
    """Run a praxis CLI command and return output."""
    praxis_bin = str(Path(__file__).resolve().parent.parent.parent / ".venv" / "bin" / "praxis")
    try:
        result = subprocess.run(
            [praxis_bin] + args,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(Path(__file__).resolve().parent.parent.parent),
        )
        return result.stdout + result.stderr
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def usage(date: str | None = None) -> str:
    """Show Claude CLI usage telemetry — token counts, cost, capacity, per-daemon breakdown.

    Args:
        date: Optional date in YYYY-MM-DD format. Defaults to today.
    """
    args = ["usage"]
    if date:
        args.extend(["--date", date])
    return _run_praxis(args)


@mcp.tool()
async def usage_json(date: str | None = None) -> str:
    """Get usage telemetry as JSON for programmatic analysis.

    Args:
        date: Optional date in YYYY-MM-DD format. Defaults to today.
    """
    args = ["usage", "--json"]
    if date:
        args.extend(["--date", date])
    return _run_praxis(args)


@mcp.tool()
async def briefing() -> str:
    """Get the morning briefing — entry/exit triggers, new BUY memos, upcoming events, analyst findings, daemon health."""
    from cli.morning_briefing import compile_briefing
    return compile_briefing()


@mcp.tool()
async def health() -> str:
    """Check if all praxis daemons are running (scanner, filing-research, analyst, queue)."""
    return _run_praxis(["health"])


@mcp.tool()
async def filing_research_status(date: str | None = None) -> str:
    """Show current filing research daemon status — filings discovered, researched, skipped.

    Args:
        date: Optional date in YYYY-MM-DD format. Defaults to today.
    """
    args = ["filing-research", "status"]
    if date:
        args.extend(["--date", date])
    return _run_praxis(args)


@mcp.tool()
async def analyst_status() -> str:
    """Show reactive analyst agent status — recent investigations, findings, alert reactions."""
    return _run_praxis(["analyst", "status"])


@mcp.tool()
async def portfolio() -> str:
    """Show current portfolio positions and watchlist."""
    return _run_praxis(["portfolio", "show"])


@mcp.tool()
async def watchdog_list() -> str:
    """Show all entry/exit target levels being monitored from research memos."""
    return _run_praxis(["watchdog", "list"])


@mcp.tool()
async def watchdog_check() -> str:
    """Check all entry/exit targets against current prices right now."""
    return _run_praxis(["watchdog", "check"])


@mcp.tool()
async def watches_list(ticker: str | None = None, watch_type: str | None = None) -> str:
    """Show thesis-driven monitoring watches extracted from research memos.

    Args:
        ticker: Optional ticker to filter by.
        watch_type: Optional type filter (price_level, news_watch, event_watch, context).
    """
    args = ["watches", "list"]
    if ticker:
        args.extend(["--ticker", ticker])
    if watch_type:
        args.extend(["--type", watch_type])
    return _run_praxis(args)


@mcp.tool()
async def exposure_map() -> str:
    """Show factor exposure map — which tickers are exposed to oil, rates, gold, biotech, etc."""
    return _run_praxis(["watches", "exposure"])


@mcp.tool()
async def audit(ticker: str | None = None) -> str:
    """View coordinator decisions for research sessions — early exits, checkpoints, outcomes.

    Args:
        ticker: Optional ticker to see specific coordinator log.
    """
    args = ["audit"]
    if ticker:
        args.append(ticker)
    return _run_praxis(args)


@mcp.tool()
async def calendar_list(days: int = 30) -> str:
    """Show upcoming events from the event calendar (earnings, FDA dates, catalysts).

    Args:
        days: Number of days ahead to show. Default 30.
    """
    return _run_praxis(["calendar", "list", "--days", str(days)])


@mcp.tool()
async def calendar_add(ticker: str, date: str, event_type: str = "other", description: str = "") -> str:
    """Add an event to the calendar.

    Args:
        ticker: Ticker symbol (e.g., AXSM).
        date: Event date in YYYY-MM-DD format.
        event_type: Type: earnings, clinical_data, fda_decision, investor_day, conference, regulatory, other.
        description: What the event is.
    """
    args = ["calendar", "add", ticker, date, "--type", event_type]
    if description:
        args.extend(["--desc", description])
    return _run_praxis(args)


@mcp.tool()
async def queue_add(title: str, body: str = "", tickers: str = "", link: str = "") -> str:
    """Create a research queue task via GitHub Issues.

    Args:
        title: Research task title.
        body: Task description/context.
        tickers: Comma-separated tickers to tag (e.g., "HCC,LBRT").
        link: URL to include for context.
    """
    args = ["queue", "add", title]
    if body:
        args.extend(["--body", body])
    for t in tickers.split(","):
        t = t.strip()
        if t:
            args.extend(["--ticker", t])
    if link:
        args.extend(["--link", link])
    return _run_praxis(args)


@mcp.tool()
async def queue_status() -> str:
    """Show research queue status — pending, running, and completed tasks."""
    return _run_praxis(["queue", "status"])


@mcp.tool()
async def capacity() -> str:
    """Get current capacity estimate — window usage %, remaining tokens, rate limit status."""
    from cli.telemetry import get_capacity_estimate
    cap = get_capacity_estimate()
    return (
        f"Window capacity: {cap['estimated_pct']}%\n"
        f"Budget: {cap['window_budget_tokens']:,} tokens\n"
        f"Used: {cap['current_window_used_tokens']:,} tokens (${cap['current_window_used_cost']:.2f})\n"
        f"Remaining: {cap['remaining_budget_tokens']:,} tokens (${cap['remaining_budget_cost']:.2f})\n"
        f"At 80% target: {'YES — daemons paused' if cap['at_target'] else 'No — headroom available'}\n"
        f"Calibrated: {'Yes' if cap['calibrated'] else 'No (using default 300K estimate)'}"
    )


@mcp.tool()
async def read_memo(ticker: str) -> str:
    """Read the investment memo for a ticker.

    Args:
        ticker: Ticker symbol (e.g., HCC, CLMT).
    """
    from cli.config_utils import find_repo_root
    memo_path = find_repo_root() / "workspace" / ticker.upper() / "memo.md"
    if memo_path.exists():
        return memo_path.read_text()
    return f"No memo found for {ticker}. Check workspace/{ticker.upper()}/."


@mcp.tool()
async def read_memo_yaml(ticker: str) -> str:
    """Read the structured memo data (decision, scores, valuation) for a ticker.

    Args:
        ticker: Ticker symbol.
    """
    from cli.config_utils import find_repo_root
    import yaml
    memo_path = find_repo_root() / "workspace" / ticker.upper() / "memo.yaml"
    if memo_path.exists():
        return memo_path.read_text()
    return f"No memo.yaml found for {ticker}."


if __name__ == "__main__":
    mcp.run(transport="stdio")
