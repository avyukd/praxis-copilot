"""Generate CLAUDE.md prompts for research queue tasks."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from cli.queue_models import QueueTask, QueueTaskType


def generate_queue_prompt(
    task: QueueTask,
    workspace: Path,
    data_manifest: str = "",
    has_mcp: bool = False,
) -> str:
    """Generate a CLAUDE.md prompt for a queue research task."""
    today = date.today().isoformat()

    # Context files section
    context_dir = workspace / "context"
    context_section = ""
    if context_dir.exists():
        files = sorted(context_dir.rglob("*"))
        files = [f for f in files if f.is_file()]
        if files:
            items = []
            for f in files:
                rel = f.relative_to(workspace)
                size = f.stat().st_size
                size_str = f"{size // 1024}KB" if size > 1024 else f"{size}B"
                items.append(f"  - {rel} ({size_str})")
            context_section = f"""
## Context Files

The following files were attached to this task. Read them to inform your analysis.

{chr(10).join(items)}
"""

    # Links section
    links_file = context_dir / "links.txt" if context_dir.exists() else None
    links_section = ""
    if links_file and links_file.exists():
        links_section = """
## External Links

Links from `context/links.txt` should be fetched using WebFetch for analysis.
Read the file first to see what URLs are available.
"""

    # MCP section
    mcp_section = ""
    if has_mcp:
        mcp_section = """
## MCP Tools (Fundamentals + Price)

An MCP server is configured for querying financial data.
**Do NOT read `fundamentals.json` directly** -- use `data/fundamentals/summary.md` + MCP tools.

| Tool | Use For |
|------|---------|
| `company_overview()` | Highlights, valuation, share stats |
| `get_financial_data(statement, metrics, period_type, count)` | Pull specific metrics |
| `get_full_statement(statement, period_type, count)` | Full statement (max 5 periods) |
| `get_earnings(count)` | Earnings history + estimates |
| `get_price(ticker)` | **Current/delayed price, change %, volume** |
| `search_fundamentals(keyword)` | Find fields by keyword |

**Always use `get_price(ticker)` for current prices** -- do NOT web search for stock prices.
"""

    # Data section
    data_section = ""
    if data_manifest:
        data_section = f"""
## Available Data

{data_manifest}
"""

    # Task-type-specific guidance
    type_guidance = _type_guidance(task)

    tickers_str = ", ".join(task.tickers) if task.tickers else "N/A"

    return f"""# Research Task #{task.issue_number}: {task.title}

Generated: {today}
Tickers: {tickers_str}
Task type: {task.task_type.value}

---

## Your Task

{task.body}

---
{context_section}{links_section}{mcp_section}{data_section}
## Praxis System Context

You are running inside the praxis-copilot investment research system. Key facts:
- Existing research for any ticker can be found in `data/` if it was staged
- Use MCP tools for financial data if configured (see above)
- Web search and web fetch are available for external research
- Images in `context/` can be read directly (you are multimodal)
- Be thorough but concise. Lead with findings, not setup.

{type_guidance}

---

## Required Output

You MUST produce these files in the workspace root:

1. **summary.md** -- Your analysis and findings in markdown. Thorough but concise.

2. **summary.yaml** -- Structured output:

```yaml
task_id: {task.issue_number}
date: "{today}"
task_type: "{task.task_type.value}"
tickers: [{tickers_str}]
one_line: "<one sentence summary of your findings>"
key_findings:
  - "<finding 1>"
  - "<finding 2>"
  - "<finding 3>"
actionability: "<none|monitor|research_deeper|trade_idea>"
scores:
  tactical: <1-10, how compelling the near-term trading opportunity is>
  fundamental: <1-10, how compelling the fundamental case is>
follow_up:
  - "<suggested next step if any>"
```

Any additional analysis files are welcome (comparison tables, detailed notes, etc).

---

## Rules

- Lead with findings. No preambles or methodology explanations.
- Never fabricate numbers. If data is unavailable, say so.
- If the task is unclear, do your best interpretation and note assumptions.
- Tables over prose for comparable data.
- Be honest about conviction level. "Not enough information" is valid.

**Scoring rubric:**
- **tactical** (1-10): 8-10 = rare asymmetric setup, 5-7 = decent but uncertain, 1-4 = no edge
- **fundamental** (1-10): 8-10 = clear mispricing, 5-7 = interesting but holes, 1-4 = no edge
"""


def _type_guidance(task: QueueTask) -> str:
    """Return task-type-specific instructions."""
    if task.task_type == QueueTaskType.TICKER_RESEARCH:
        return """## Research Approach

This is a ticker-specific research task. Focus on:
1. Read all available data files first
2. Use MCP tools for financial drill-down if available
3. Assess the fundamental case and any near-term catalysts
4. Provide a clear Buy/Sell/Neutral/Too Hard assessment
5. If tactical context exists, evaluate the trading setup separately"""

    if task.task_type == QueueTaskType.COMPARATIVE:
        return """## Research Approach

This is a comparative analysis. Focus on:
1. Build a structured comparison framework
2. Use tables for side-by-side metrics (valuation, growth, margins, etc.)
3. Identify the key differentiating factors
4. Assess relative value -- which is the better risk/reward?
5. Be explicit about which you'd prefer and why"""

    if task.task_type == QueueTaskType.THEMATIC:
        return """## Research Approach

This is a thematic/macro research task. Focus on:
1. Map out the key dynamics and second-order effects
2. Identify which tickers/sectors are most exposed
3. Assess timing -- is this actionable now or a developing theme?
4. Provide concrete investment implications, not just analysis"""

    if task.task_type == QueueTaskType.DOCUMENT_REVIEW:
        return """## Research Approach

This task includes documents/images to review. Focus on:
1. Read all context files carefully (images, screenshots, documents)
2. Extract the key information and its implications
3. Connect it to any relevant tickers or market context
4. Assess materiality -- does this change the investment thesis?"""

    return """## Research Approach

This is a freeform research task. Use your judgment on the best approach.
Focus on actionable insights and clear conclusions."""
