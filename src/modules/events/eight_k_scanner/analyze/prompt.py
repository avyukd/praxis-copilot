"""Build LLM prompt for 8-K filing analysis."""
from __future__ import annotations

import logging

from src.modules.events.eight_k_scanner.models import ExtractedFiling, FinancialSnapshot

logger = logging.getLogger(__name__)

MAX_ITEM_CHARS = 20_000
MAX_EXHIBIT_CHARS = 25_000
MAX_CONTEXT_CHARS = 5_000

SYSTEM_PROMPT = """You are a senior equity analyst specializing in small-cap and micro-cap stocks.
You are analyzing an 8-K filing from the SEC.

Your job:
1. Identify what NEW information is being disclosed.
2. Assess how MATERIAL this information is to the company's cash flows, risk profile, or capital structure. Quantify magnitude where possible (e.g. "represents ~15% of annual revenue").
3. Classify the filing as BUY, SELL, or NEUTRAL.
4. Assign a magnitude from 0.0 (trivial) to 1.0 (transformative).

Classification guidelines:
- BUY: Filing discloses information likely to increase the stock price (positive earnings surprise, accretive acquisition, new major contract, debt refinancing at better terms, FDA approval, etc.)
- SELL: Filing discloses information likely to decrease the stock price (earnings miss, impairment, restructuring, auditor change, delisting notice, cybersecurity incident, covenant violation, etc.)
- NEUTRAL: Filing is routine, administrative, or the information has ambiguous impact.

Magnitude guidelines:
- 0.0-0.2: Minor/routine (officer change with no strategic implications, bylaw amendment)
- 0.2-0.5: Moderate (earnings roughly in-line, small acquisition, new credit facility)
- 0.5-0.8: Significant (earnings beat/miss, major contract win/loss, material impairment)
- 0.8-1.0: Transformative (change of control, bankruptcy, FDA approval for lead drug, delisting)

Respond with valid JSON matching the requested schema. Be concise but specific."""


def _format_dollars(value) -> str:
    if value is None:
        return "N/A"
    v = abs(value)
    sign = "-" if value < 0 else ""
    if v >= 1e9:
        return f"{sign}${v/1e9:.2f}B"
    if v >= 1e6:
        return f"{sign}${v/1e6:.1f}M"
    if v >= 1e3:
        return f"{sign}${v/1e3:.0f}K"
    return f"{sign}${v:.0f}"


def _truncate(text: str, max_chars: int, label: str) -> str:
    if len(text) <= max_chars:
        return text
    logger.warning(f"Truncating {label} from {len(text)} to {max_chars} chars")
    return text[:max_chars] + "\n\n[TRUNCATED]"


def build_messages(extracted: ExtractedFiling, financial_snapshot: FinancialSnapshot, ticker: str) -> list[dict]:
    """Build the messages list for the LLM call."""
    user_parts = []

    company = extracted.ticker or ticker
    accession = extracted.accession_number
    user_parts.append(f"## 8-K Filing: {company} ({accession})\n")

    if extracted.items:
        item_text = ""
        for item_num, text in extracted.items.items():
            item_text += f"### Item {item_num}\n{text}\n\n"
        user_parts.append(_truncate(item_text.strip(), MAX_ITEM_CHARS, f"items for {accession}"))

    if extracted.exhibits:
        exhibit_text = ""
        for ex in extracted.exhibits:
            exhibit_text += f"### Exhibit: {ex.filename} (type: {ex.type})\n{ex.text}\n\n"
        user_parts.append(_truncate(exhibit_text.strip(), MAX_EXHIBIT_CHARS, f"exhibits for {accession}"))

    user_parts.append("## Financial Snapshot")
    if financial_snapshot.market_cap is not None:
        user_parts.append(f"- Market Cap: {_format_dollars(financial_snapshot.market_cap)}")
        user_parts.append(f"- Revenue (TTM): {_format_dollars(financial_snapshot.revenue_ttm)}")
        user_parts.append(f"- Net Income (TTM): {_format_dollars(financial_snapshot.net_income_ttm)}")
        user_parts.append(f"- Cash: {_format_dollars(financial_snapshot.cash)}")
        user_parts.append(f"- Total Debt: {_format_dollars(financial_snapshot.total_debt)}")
        user_parts.append(f"- Source: {financial_snapshot.source}")
    else:
        user_parts.append("Financial data unavailable.")
    user_parts.append("")

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": "\n".join(user_parts)},
    ]
