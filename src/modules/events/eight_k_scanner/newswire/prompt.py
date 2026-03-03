"""LLM prompt builder for press release analysis."""
from __future__ import annotations

import logging

from src.modules.events.eight_k_scanner.models import FinancialSnapshot

logger = logging.getLogger(__name__)

MAX_RELEASE_CHARS = 40_000
MAX_CONTEXT_CHARS = 5_000

SYSTEM_PROMPT = """You are a senior equity analyst specializing in small-cap and micro-cap stocks.
You are analyzing a press release from a public company.

Your job:
1. Identify what NEW information is being disclosed.
2. Assess how MATERIAL this information is to the company's cash flows, risk profile, or capital structure. Quantify magnitude where possible (e.g. "represents ~15% of annual revenue").
3. Classify the press release as BUY, SELL, or NEUTRAL.
4. Assign a magnitude from 0.0 (trivial) to 1.0 (transformative).

Classification guidelines:
- BUY: Discloses information likely to increase the stock price (positive earnings surprise, accretive acquisition, new major contract, significant drill results, FDA approval, etc.)
- SELL: Discloses information likely to decrease the stock price (earnings miss, impairment, restructuring, going concern, failed trial, etc.)
- NEUTRAL: Routine, administrative, or ambiguous impact.

Magnitude guidelines:
- 0.0-0.2: Minor/routine (officer change, routine operational update)
- 0.2-0.5: Moderate (earnings roughly in-line, small acquisition, new credit facility)
- 0.5-0.8: Significant (earnings beat/miss, major contract win/loss, significant resource discovery)
- 0.8-1.0: Transformative (change of control, bankruptcy, major FDA approval, transformative deal)

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


def build_pr_messages(release_text: str, financial_snapshot: FinancialSnapshot, ticker: str) -> list[dict]:
    """Build LLM messages for press release analysis."""
    user_parts = []

    user_parts.append(f"## Press Release: {ticker}\n")
    user_parts.append(_truncate(release_text, MAX_RELEASE_CHARS, f"release text for {ticker}"))
    user_parts.append("")

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
