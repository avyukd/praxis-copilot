"""MCP server for querying fundamentals data during Claude Code research sessions.

Loads a fundamentals.json file and exposes tools for targeted queries,
so Claude never needs to read the full 700KB+ file into context.

Usage (stdio transport, configured automatically by `praxis stage`):
    python -m cli.fundamentals_server /path/to/fundamentals.json
"""

import json
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("praxis-fundamentals")

# Loaded at startup from CLI arg
_data: dict = {}
_raw: dict = {}


def _load(path: str) -> None:
    global _data, _raw
    with open(path) as f:
        _data = json.load(f)
    _raw = _data.get("raw", _data)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def company_overview() -> dict:
    """Get company overview: highlights, valuation, share stats, dividends, analyst ratings.

    This is the compact summary — call this first to orient yourself.
    """
    sections = ["General", "Highlights", "Valuation", "SharesStats",
                "SplitsDividends", "AnalystRatings", "Technicals"]
    result = {}
    for s in sections:
        if s in _raw:
            result[s] = _raw[s]
    return result


@mcp.tool()
def list_financial_metrics(statement: str = "income") -> list[str]:
    """List available metric names for a financial statement.

    Args:
        statement: One of 'income', 'balance', 'cashflow'
    """
    key_map = {
        "income": "Income_Statement",
        "balance": "Balance_Sheet",
        "cashflow": "Cash_Flow",
    }
    stmt_key = key_map.get(statement)
    if not stmt_key:
        return [f"Unknown statement '{statement}'. Use: income, balance, cashflow"]

    financials = _raw.get("Financials", {}).get(stmt_key, {})
    yearly = financials.get("yearly", {})
    if not yearly:
        return []
    sample = list(yearly.values())[0]
    return sorted(sample.keys())


@mcp.tool()
def get_financial_data(
    statement: str,
    metrics: list[str],
    period_type: str = "yearly",
    count: int = 5,
) -> list[dict]:
    """Get specific metrics from a financial statement for recent periods.

    Args:
        statement: One of 'income', 'balance', 'cashflow'
        metrics: List of metric names (use list_financial_metrics to discover them)
        period_type: 'yearly' or 'quarterly'
        count: Number of most recent periods to return (max 20)
    """
    key_map = {
        "income": "Income_Statement",
        "balance": "Balance_Sheet",
        "cashflow": "Cash_Flow",
    }
    stmt_key = key_map.get(statement)
    if not stmt_key:
        return [{"error": f"Unknown statement '{statement}'. Use: income, balance, cashflow"}]

    financials = _raw.get("Financials", {}).get(stmt_key, {})
    periods = financials.get(period_type, {})
    if not periods:
        return [{"error": f"No {period_type} data available"}]

    count = min(count, 20)
    sorted_dates = sorted(periods.keys(), reverse=True)[:count]

    results = []
    for date in sorted_dates:
        row = {"date": date}
        for m in metrics:
            row[m] = periods[date].get(m)
        results.append(row)

    return results


@mcp.tool()
def get_full_statement(
    statement: str,
    period_type: str = "yearly",
    count: int = 3,
) -> list[dict]:
    """Get ALL metrics from a financial statement for recent periods.

    Use this when you need the complete picture for a few periods.
    Prefer get_financial_data with specific metrics when possible.

    Args:
        statement: One of 'income', 'balance', 'cashflow'
        period_type: 'yearly' or 'quarterly'
        count: Number of periods (max 5 to avoid context bloat)
    """
    key_map = {
        "income": "Income_Statement",
        "balance": "Balance_Sheet",
        "cashflow": "Cash_Flow",
    }
    stmt_key = key_map.get(statement)
    if not stmt_key:
        return [{"error": f"Unknown statement '{statement}'"}]

    financials = _raw.get("Financials", {}).get(stmt_key, {})
    periods = financials.get(period_type, {})
    if not periods:
        return []

    count = min(count, 5)
    sorted_dates = sorted(periods.keys(), reverse=True)[:count]
    return [{"date": d, **periods[d]} for d in sorted_dates]


@mcp.tool()
def get_earnings(count: int = 8) -> dict:
    """Get recent earnings history and estimates.

    Args:
        count: Number of recent earnings periods to return
    """
    earnings = _raw.get("Earnings", {})
    result = {}

    history = earnings.get("History", {})
    if history:
        sorted_dates = sorted(history.keys(), reverse=True)[:count]
        result["history"] = {d: history[d] for d in sorted_dates}

    trend = earnings.get("Trend", {})
    if trend:
        sorted_dates = sorted(trend.keys(), reverse=True)[:count]
        result["trend"] = {d: trend[d] for d in sorted_dates}

    annual = earnings.get("Annual", {})
    if annual:
        sorted_dates = sorted(annual.keys(), reverse=True)[:min(count, 5)]
        result["annual"] = {d: annual[d] for d in sorted_dates}

    return result


@mcp.tool()
def get_holders() -> dict:
    """Get institutional and insider holder information."""
    result = {}
    if "Holders" in _raw:
        result["holders"] = _raw["Holders"]
    if "InsiderTransactions" in _raw:
        # Limit to recent transactions
        txns = _raw["InsiderTransactions"]
        if isinstance(txns, dict):
            result["insider_transactions"] = dict(list(txns.items())[:20])
        elif isinstance(txns, list):
            result["insider_transactions"] = txns[:20]
    return result


@mcp.tool()
def search_fundamentals(keyword: str) -> dict:
    """Search across all fundamentals data for a keyword.

    Useful when you're not sure which statement or section contains
    the data point you need.

    Args:
        keyword: Case-insensitive keyword to search for in field names
    """
    keyword_lower = keyword.lower()
    matches = {}

    def _search(obj, path=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                current_path = f"{path}.{k}" if path else k
                if keyword_lower in k.lower():
                    if isinstance(v, (str, int, float, type(None))):
                        matches[current_path] = v
                    elif isinstance(v, dict) and len(v) < 5:
                        matches[current_path] = v
                    else:
                        matches[current_path] = f"<{type(v).__name__} with {len(v)} items>"
                elif isinstance(v, dict) and len(path.split(".")) < 3:
                    _search(v, current_path)

    _search(_raw)
    return matches


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m cli.fundamentals_server <fundamentals.json>", file=sys.stderr)
        sys.exit(1)

    path = sys.argv[1]
    if not Path(path).exists():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    _load(path)
    mcp.run()
