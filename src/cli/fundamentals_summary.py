"""Generate a compact markdown summary of fundamentals data.

Produces a ~5-10KB readable overview from 700KB+ raw JSON,
giving Claude orientation before it drills into specifics via MCP tools.
"""

import json


def generate_summary(fundamentals_json: dict) -> str:
    """Generate markdown summary from fundamentals data."""
    raw = fundamentals_json.get("raw", fundamentals_json)
    source = fundamentals_json.get("source", "unknown")
    sections = []

    # Header
    general = raw.get("General", {})
    name = general.get("Name", "Unknown")
    ticker = general.get("Code", "")
    exchange = general.get("Exchange", "")
    sector = general.get("Sector", "")
    industry = general.get("Industry", "")
    description = general.get("Description", "")

    sections.append(f"# {name} ({ticker}) — Fundamentals Summary")
    sections.append(f"*Source: {source} | {exchange} | {sector} > {industry}*\n")

    if description:
        # Truncate long descriptions
        if len(description) > 500:
            description = description[:500] + "..."
        sections.append(f"> {description}\n")

    # Highlights
    highlights = raw.get("Highlights", {})
    if highlights:
        sections.append("## Key Metrics")
        rows = [
            ("Market Cap", _fmt_large(highlights.get("MarketCapitalization"))),
            ("P/E (TTM)", _fmt_num(highlights.get("PERatio"))),
            ("PEG Ratio", _fmt_num(highlights.get("PEGRatio"))),
            ("EPS (TTM)", _fmt_num(highlights.get("EarningsShare"))),
            ("EPS Est (CY)", _fmt_num(highlights.get("EPSEstimateCurrentYear"))),
            ("EPS Est (NY)", _fmt_num(highlights.get("EPSEstimateNextYear"))),
            ("Profit Margin", _fmt_pct(highlights.get("ProfitMargin"))),
            ("Operating Margin", _fmt_pct(highlights.get("OperatingMarginTTM"))),
            ("ROE", _fmt_pct(highlights.get("ReturnOnEquityTTM"))),
            ("ROA", _fmt_pct(highlights.get("ReturnOnAssetsTTM"))),
            ("EBITDA", _fmt_large(highlights.get("EBITDA"))),
            ("Book Value", _fmt_num(highlights.get("BookValue"))),
            ("Dividend Yield", _fmt_pct(highlights.get("DividendYield"))),
            ("Wall St Target", _fmt_num(highlights.get("WallStreetTargetPrice"))),
        ]
        sections.append(_make_table(["Metric", "Value"], rows))

    # Valuation
    valuation = raw.get("Valuation", {})
    if valuation:
        sections.append("## Valuation")
        rows = [
            ("Trailing P/E", _fmt_num(valuation.get("TrailingPE"))),
            ("Forward P/E", _fmt_num(valuation.get("ForwardPE"))),
            ("P/S (TTM)", _fmt_num(valuation.get("PriceSalesTTM"))),
            ("P/B (MRQ)", _fmt_num(valuation.get("PriceBookMRQ"))),
            ("EV", _fmt_large(valuation.get("EnterpriseValue"))),
            ("EV/Revenue", _fmt_num(valuation.get("EnterpriseValueRevenue"))),
            ("EV/EBITDA", _fmt_num(valuation.get("EnterpriseValueEbitda"))),
        ]
        sections.append(_make_table(["Metric", "Value"], rows))

    # Income Statement — last 5 years
    _add_statement_table(
        sections, raw, "Income_Statement", "yearly", 5,
        "## Income Statement (Annual)",
        ["totalRevenue", "costOfRevenue", "grossProfit", "operatingIncome", "netIncome",
         "ebitda", "researchDevelopment", "sellingGeneralAdministrative"],
    )

    # Income Statement — last 6 quarters
    _add_statement_table(
        sections, raw, "Income_Statement", "quarterly", 6,
        "## Income Statement (Quarterly)",
        ["totalRevenue", "grossProfit", "operatingIncome", "netIncome", "ebitda"],
    )

    # Balance Sheet — last 5 years
    _add_statement_table(
        sections, raw, "Balance_Sheet", "yearly", 5,
        "## Balance Sheet (Annual)",
        ["totalAssets", "totalCurrentAssets", "cashAndShortTermInvestments",
         "totalLiab", "totalCurrentLiabilities", "longTermDebt",
         "shortLongTermDebt", "totalStockholderEquity"],
    )

    # Cash Flow — last 5 years
    _add_statement_table(
        sections, raw, "Cash_Flow", "yearly", 5,
        "## Cash Flow (Annual)",
        ["totalCashFromOperatingActivities", "capitalExpenditures",
         "freeCashFlow", "totalCashFromFinancingActivities",
         "dividendsPaid", "salePurchaseOfStock"],
    )

    # Shares outstanding trend
    outstanding = raw.get("outstandingShares", {})
    if isinstance(outstanding, dict):
        annual = outstanding.get("annual", {})
        if annual:
            sections.append("## Shares Outstanding")
            # annual may be a dict keyed by index strings or a list
            entries = list(annual.values()) if isinstance(annual, dict) else annual
            rows = []
            for entry in entries[:5]:
                if isinstance(entry, dict):
                    date = entry.get("date", "?")
                    shares = entry.get("shares")
                    rows.append((date, _fmt_large(shares)))
            if rows:
                sections.append(_make_table(["Date", "Shares"], rows))

    # Footer
    sections.append("\n---")
    sections.append("*For detailed drill-down, use the `fundamentals` MCP tools "
                    "(get_financial_data, get_earnings, search_fundamentals, etc.)*")

    return "\n\n".join(sections)


def _add_statement_table(
    sections: list, raw: dict, stmt_key: str, period_type: str,
    count: int, header: str, metrics: list[str],
) -> None:
    """Add a financial statement table to sections."""
    financials = raw.get("Financials", {}).get(stmt_key, {})
    periods = financials.get(period_type, {})
    if not periods:
        return

    sorted_dates = sorted(periods.keys(), reverse=True)[:count]
    # Use short date labels
    labels = [d[:10] for d in sorted_dates]

    sections.append(header)
    header_row = ["Metric"] + labels
    rows = []
    for metric in metrics:
        row_vals = [_clean_metric_name(metric)]
        for d in sorted_dates:
            val = periods[d].get(metric)
            row_vals.append(_fmt_large(val))
        rows.append(tuple(row_vals))

    sections.append(_make_table(header_row, rows))


def _make_table(headers: list[str], rows: list[tuple]) -> str:
    """Build a markdown table."""
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(v) for v in row) + " |")
    return "\n".join(lines)


def _fmt_num(val) -> str:
    if val is None:
        return "—"
    try:
        val = float(val)
        if abs(val) >= 100:
            return f"{val:,.0f}"
        return f"{val:,.2f}"
    except (ValueError, TypeError):
        return str(val)


def _fmt_large(val) -> str:
    if val is None:
        return "—"
    try:
        val = float(val)
    except (ValueError, TypeError):
        return str(val)
    abs_val = abs(val)
    sign = "-" if val < 0 else ""
    if abs_val >= 1e12:
        return f"{sign}${abs_val / 1e12:,.1f}T"
    if abs_val >= 1e9:
        return f"{sign}${abs_val / 1e9:,.1f}B"
    if abs_val >= 1e6:
        return f"{sign}${abs_val / 1e6:,.0f}M"
    if abs_val >= 1e3:
        return f"{sign}${abs_val / 1e3:,.0f}K"
    return f"{sign}${val:,.0f}"


def _fmt_pct(val) -> str:
    if val is None:
        return "—"
    try:
        val = float(val)
        return f"{val * 100:.1f}%" if abs(val) < 1 else f"{val:.1f}%"
    except (ValueError, TypeError):
        return str(val)


def _clean_metric_name(name: str) -> str:
    """Convert camelCase to readable form."""
    import re
    # Insert spaces before capitals
    spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    return spaced.replace("_", " ").title()
