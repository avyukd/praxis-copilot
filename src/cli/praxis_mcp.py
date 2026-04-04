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


# ---------------------------------------------------------------------------
# IPC Tools — Claude Code ↔ Claude Desktop communication
# ---------------------------------------------------------------------------


@mcp.tool()
async def ipc_check_tasks() -> str:
    """Check for pending IPC tasks that need to be processed.

    Desktop should call this periodically to pick up work from Code.
    Tasks include: browse URLs, take screenshots, search social media.
    """
    from cli.ipc import get_pending_tasks
    tasks = get_pending_tasks()
    if not tasks:
        return "No pending tasks."

    lines = [f"{len(tasks)} pending task(s):\n"]
    for t in tasks:
        lines.append(
            f"  ID: {t.id}\n"
            f"  Type: {t.type} | Priority: {t.priority}\n"
            f"  Ticker: {t.ticker or 'N/A'}\n"
            f"  Description: {t.description}\n"
            f"  URL: {t.url or 'N/A'}\n"
            f"  ---"
        )
    return "\n".join(lines)


@mcp.tool()
async def ipc_claim_task(task_id: str) -> str:
    """Mark a task as in-progress. Call this before starting work on a task.

    Args:
        task_id: The task ID from ipc_check_tasks.
    """
    from cli.ipc import claim_task
    if claim_task(task_id):
        return f"Task {task_id} claimed. Start processing."
    return f"Task {task_id} not found."


@mcp.tool()
async def ipc_submit_result(
    task_id: str,
    finding: str,
    ticker: str = "",
    actionability: str = "none",
    urgency: str = "low",
) -> str:
    """Submit a result for a completed task. Call when done processing.

    Args:
        task_id: The task ID being completed.
        finding: What you found — markdown text.
        ticker: Ticker symbol if relevant.
        actionability: none, monitor, research_deeper, or trade_idea.
        urgency: low, medium, or high.
    """
    from cli.ipc import submit_result
    result = submit_result(
        task_id, finding,
        ticker=ticker, actionability=actionability, urgency=urgency,
    )
    return f"Result submitted for task {task_id}. Actionability: {actionability}, Urgency: {urgency}"


@mcp.tool()
async def ipc_write_finding(
    content: str,
    ticker: str = "",
    actionability: str = "none",
    urgency: str = "low",
) -> str:
    """Write a free-form finding to the inbox. Use this for proactive discoveries.

    Call this when you browse social media, forums, or news and find something
    interesting — even without a specific task. The analyst agent will pick it up.

    Args:
        content: What you found — markdown text with sources.
        ticker: Ticker symbol if relevant (e.g., "CLMT").
        actionability: none, monitor, research_deeper, or trade_idea.
        urgency: low, medium, or high.
    """
    from cli.ipc import write_finding
    path = write_finding(content, ticker=ticker, actionability=actionability, urgency=urgency)
    return f"Finding saved to inbox. Analyst will pick it up."


@mcp.tool()
async def ipc_list_findings(limit: int = 10) -> str:
    """List recent findings from the inbox.

    Args:
        limit: Max findings to show.
    """
    from cli.ipc import read_inbox
    findings = read_inbox(limit)
    if not findings:
        return "No findings in inbox."

    lines = [f"{len(findings)} recent finding(s):\n"]
    for f in findings:
        ticker_str = f"[{f.ticker}] " if f.ticker else ""
        lines.append(f"  {ticker_str}{f.content[:100]}...")
        lines.append(f"  Actionability: {f.actionability} | Urgency: {f.urgency} | Source: {f.source}")
        lines.append(f"  ---")
    return "\n".join(lines)


@mcp.tool()
async def ipc_create_browse_task(
    url: str,
    description: str,
    ticker: str = "",
    priority: str = "medium",
) -> str:
    """Create a task for Desktop to browse a URL and report findings.

    Args:
        url: The URL to browse.
        description: What to look for.
        ticker: Ticker symbol if relevant.
        priority: low, medium, or high.
    """
    from cli.ipc import create_task
    task = create_task(
        "browse", description,
        ticker=ticker, url=url, priority=priority, created_by="mcp",
    )
    return f"Browse task created: {task.id}. Desktop will pick it up."


@mcp.tool()
async def ipc_create_search_task(
    query: str,
    description: str = "",
    ticker: str = "",
    sources: str = "twitter,stocktwits",
) -> str:
    """Create a task for Desktop to search social media / forums.

    Args:
        query: Search query (e.g., "$CLMT RVO").
        description: What you're looking for.
        ticker: Ticker symbol if relevant.
        sources: Comma-separated sources to check (twitter, stocktwits, microcapclub, etc).
    """
    from cli.ipc import create_task, get_source_url
    source_list = [s.strip() for s in sources.split(",")]

    task = create_task(
        "search",
        description or f"Search for: {query}",
        ticker=ticker,
        search_query=query,
        sources=source_list,
        created_by="mcp",
    )
    return f"Search task created: {task.id}. Sources: {', '.join(source_list)}"


@mcp.tool()
async def ipc_browse_sources() -> str:
    """List configured browse sources (Twitter, StockTwits, forums, etc)."""
    from cli.ipc import load_sources
    sources = load_sources().get("sources", {})
    if not sources:
        return "No sources configured."

    lines = [f"{len(sources)} sources:\n"]
    for name, info in sources.items():
        proactive = " [PROACTIVE]" if info.get("proactive") else ""
        lines.append(f"  {name}: {info.get('description', '')}{proactive}")
        lines.append(f"    Cadence: {info.get('cadence', 'manual')} | URL: {info.get('url_template', '')[:60]}")
    return "\n".join(lines)


if __name__ == "__main__":
    mcp.run(transport="stdio")
