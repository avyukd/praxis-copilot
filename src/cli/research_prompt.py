"""Generate CLAUDE.md research prompt for a ticker workspace."""

from datetime import date
from dataclasses import dataclass


@dataclass
class ResearchBudget:
    """Resource budget derived from research_priority (0-10)."""

    specialist_words: int
    memo_words: int
    web_lookups: str  # str because "unlimited" is a valid value
    agent_policy: str  # how to decide which agents to run
    depth_label: str  # human-readable label for the prompt

    @staticmethod
    def from_priority(priority: int) -> "ResearchBudget":
        p = max(0, min(10, priority))

        if p <= 2:
            return ResearchBudget(
                specialist_words=500,
                memo_words=1_000,
                web_lookups="3",
                agent_policy="minimal",
                depth_label=f"Quick Screen (priority {p}/10)",
            )
        if p <= 4:
            return ResearchBudget(
                specialist_words=1_000,
                memo_words=1_500,
                web_lookups="7",
                agent_policy="conservative",
                depth_label=f"Standard Scan (priority {p}/10)",
            )
        if p <= 6:
            return ResearchBudget(
                specialist_words=1_500,
                memo_words=2_500,
                web_lookups="10",
                agent_policy="standard",
                depth_label=f"Standard Research (priority {p}/10)",
            )
        if p <= 8:
            return ResearchBudget(
                specialist_words=2_500,
                memo_words=4_000,
                web_lookups="20",
                agent_policy="thorough",
                depth_label=f"Deep Research (priority {p}/10)",
            )
        return ResearchBudget(
            specialist_words=4_000,
            memo_words=6_000,
            web_lookups="unlimited",
            agent_policy="maximum",
            depth_label=f"Full Deep Dive (priority {p}/10)",
        )


def _agent_policy_text(budget: ResearchBudget) -> str:
    """Generate agent selection instructions based on policy."""
    if budget.agent_policy == "minimal":
        return """Which agents to run:
- **Required**: rigorous-financial-analyst, business-moat-analyst
- **Skip all optional agents** — this is a quick screen, not a deep dive
- Keep analysis focused on the 1-2 things that matter most"""

    if budget.agent_policy == "conservative":
        return """Which agents to run:
- **Required**: rigorous-financial-analyst, business-moat-analyst
- **Only if obviously relevant**: industry-structure-cycle-analyst, capital-allocation-analyst
- **Skip**: geopolitical-risk-analyst unless it's the primary risk
- Default to fewer, higher-quality outputs"""

    if budget.agent_policy == "standard":
        return """Which agents to run:
- **Always**: rigorous-financial-analyst, business-moat-analyst
- **If relevant**: industry-structure-cycle-analyst, capital-allocation-analyst
- **If material**: geopolitical-risk-analyst
- When in doubt, skip optional agents. Two high-quality outputs beat four mediocre ones."""

    if budget.agent_policy == "thorough":
        return """Which agents to run:
- **Always**: rigorous-financial-analyst, business-moat-analyst
- **Run if any relevance**: industry-structure-cycle-analyst, capital-allocation-analyst
- **Run if any exposure**: geopolitical-risk-analyst
- Err on the side of running more agents — thoroughness over efficiency at this priority"""

    # maximum
    return """Which agents to run:
- **Run ALL agents**: rigorous-financial-analyst, business-moat-analyst,
  industry-structure-cycle-analyst, capital-allocation-analyst, geopolitical-risk-analyst
- No agent is optional at this priority level — run everything
- Take as much space and depth as needed. Thoroughness is the priority."""


def generate_research_prompt(
    ticker: str,
    company_name: str,
    data_manifest: str,
    has_macro: bool,
    has_existing_artifacts: bool,
    research_priority: int = 5,
    has_fundamentals_mcp: bool = False,
    tactical: bool = False,
) -> str:
    """Generate a CLAUDE.md research pipeline prompt for the workspace."""
    today = date.today().isoformat()
    budget = ResearchBudget.from_priority(research_priority)

    web_constraint = (
        f"You have a budget of {budget.web_lookups} web lookups (WebSearch + WebFetch combined)."
        if budget.web_lookups != "unlimited"
        else "You have unlimited web lookups. Use as many as needed for thorough research."
    )

    macro_section = ""
    if has_macro:
        macro_section = f"""
### Macro Analyst (Run With Specialists)

6. **macro-analyst** — Reads all files in `macro/` directory. Analyzes how current macro environment
   (rates, inflation, trade policy, liquidity, cycle position) specifically affects this company.
   Focus on company-specific macro exposure, not generic macro commentary.
   - Input: `macro/` directory + company data
   - Output: `macro-analyst.md`
   - Word limit: {budget.specialist_words} words
   - This is NOT a macro overview — it's "what does macro mean for THIS stock"
"""

    idempotency_note = ""
    if has_existing_artifacts:
        idempotency_note = """
**Note:** Existing analysis files were pulled from a prior run. By default, do NOT overwrite them.
Only generate missing outputs. If the user explicitly requests a rerun, replace files entirely.
"""

    agent_selection = _agent_policy_text(budget)
    if has_macro:
        agent_selection += "\n- **macro-analyst**: run if macro/ directory exists"
    if tactical:
        agent_selection += "\n- **tactical-analyst**: ALWAYS run when tactical mode is active"

    tactical_agent_section = ""
    if tactical:
        tactical_agent_section = f"""
### Tactical Analyst (Run With Specialists)

7. **tactical-analyst** — Reads the `<tactical-context>` provided in the initial prompt (recent
   filing analyses, monitor alerts, press release analyses). Synthesizes the event-driven signals
   into a tactical trading assessment.
   - Input: `<tactical-context>` from session prompt + price data (use `get_price(ticker)`)
   - Output: `tactical-analyst.md`
   - Word limit: {budget.specialist_words:,} words
   - **CRITICAL: Focus on the DELTA** — what is genuinely NEW information vs what was already
     known or previously disclosed. Many 8-Ks and press releases rehash known info. Your job is
     to identify the 20% that's new and assess whether it changes anything. Ask:
     - What did we know before this filing/PR? What changed?
     - Is this new data confirming a known trend, or is it a genuine surprise?
     - What is the market likely NOT pricing in from this disclosure?
   - Focus on: what changed, expected price reaction, entry/exit timing, risk/reward
   - NOT a fundamental opinion — that's for the decision-maker. This is pure tactical read.
"""

    tactical_memo_yaml = ""
    if tactical:
        tactical_memo_yaml = """tactical:
    setup: "<1-2 sentence description of the tactical setup>"
    entry_trigger: "<specific condition that triggers entry — e.g., 'gap down to $X on volume', 'confirmation above $X'>"
    timeframe: "<expected holding period — e.g., '1-3 days', '1-2 weeks'>"
    risk_reward: "<concise risk/reward — e.g., 'risk $2 to make $5, 2.5:1'>"
    catalyst: "<what near-term event drives the trade>"
    invalidation: "<specific price or condition that kills the trade>"
  """

    tactical_memo_md_instruction = ""
    if tactical:
        tactical_memo_md_instruction = """
- The memo MUST include a **## Tactical Setup** section after the main thesis. This section should:
  - **Lead with the delta**: what is genuinely NEW in this filing/PR vs what was already known
  - Describe the near-term event/catalyst and expected market reaction
  - Specify a concrete entry point, position size guidance, and stop-loss
  - Distinguish the tactical trade from the fundamental view (they can disagree)
  - Be actionable — a trader should be able to act on this section alone
  - If the filing/PR is mostly rehashing known info, say so explicitly and score tactically low"""

    mcp_section = ""
    if has_fundamentals_mcp:
        mcp_section = """
## MCP Tools (Fundamentals + Price)

An MCP server is configured for querying financial data and price quotes.
**Do NOT read `fundamentals.json` directly** — it's 700KB+ and will waste context. Instead:

1. Read `data/fundamentals/summary.md` for orientation (key metrics, valuation, recent financials)
2. Use MCP tools to drill into specifics:

| Tool | Use For |
|------|---------|
| `company_overview()` | Highlights, valuation, share stats, dividends |
| `list_financial_metrics(statement)` | Discover available field names for a statement |
| `get_financial_data(statement, metrics, period_type, count)` | Pull specific metrics for N periods |
| `get_full_statement(statement, period_type, count)` | Full statement for a few periods (max 5) |
| `get_earnings(count)` | Recent earnings history + estimates |
| `get_holders()` | Institutional/insider holdings |
| `search_fundamentals(keyword)` | Find fields by keyword |
| `get_price(ticker)` | **Current/delayed price, change %, volume** |

**statement** values: `"income"`, `"balance"`, `"cashflow"`
**period_type** values: `"yearly"`, `"quarterly"`

### Price Data

**Always use `get_price(ticker)` for current prices** — do NOT web search for stock prices.
This returns delayed quotes (15-20 min during market hours) with price, previous close,
change %, and volume. Use this for valuation context and any price-related analysis.

Example workflow:
```
1. get_price("NVDA") → current price $142.50, +2.3% today
2. Read summary.md → see revenue is $216B, margins expanding
3. get_financial_data("income", ["totalRevenue", "grossProfit", "operatingIncome"], "quarterly", 8)
   → get quarterly trend
4. get_financial_data("balance", ["longTermDebt", "cashAndShortTermInvestments"], "yearly", 5)
   → check leverage trajectory
```

---
"""

    return f"""# Research Pipeline — {ticker} ({company_name})

Generated: {today}
Research depth: **{budget.depth_label}**

---

## Your Task

You are running an investment research pipeline for **{ticker} ({company_name})**. All source
data has been pre-ingested and is available locally in `data/`. Read these files to inform your
analysis — do NOT rely solely on web searches. The ingested data is your primary source of truth.
{idempotency_note}
---
{mcp_section}
## Resource Budget

| Resource | Limit |
|----------|-------|
| Specialist agent word limit | {budget.specialist_words:,} words |
| Decision memo word limit | {budget.memo_words:,} words |
| Web lookups (per agent) | {budget.web_lookups} |

---

## Available Data

{data_manifest}
{"- macro/ — Current macro views and theses (shared context)" if has_macro else ""}

---

## Agents

### Pre-Processing (Run First, If Supplements Exist)

0. **supplement-reader-analyst** — If a `supplements/` directory exists with additional research
   materials (PDFs, articles, notes), read and summarize them first to provide context for
   specialist agents.
   - Output: `supplement-reader-analyst.md`

### Specialist Agents (Run in Parallel)

These agents analyze independent dimensions and should run concurrently:

1. **rigorous-financial-analyst** — Earnings quality, cash flow analysis, balance sheet health,
   normalized earnings, valuation. Must read `data/fundamentals/summary.md` and use fundamentals MCP
   tools for detailed drill-down. Also read `data/filings/` (especially MD&A, financial statement
   notes on revenue, segments, debt, income tax).
   - Output: `rigorous-financial-analyst.md`
   - Word limit: {budget.specialist_words:,} words
   - Primary data: `data/fundamentals/summary.md` + MCP tools, `data/filings/10-K/*/item7_mda.txt`,
     `data/filings/10-Q/*/item2_mda.txt`, note_* files

2. **business-moat-analyst** — Competitive durability, switching costs, pricing power, network
   effects. Must read `data/filings/` (Item 1 Business, Risk Factors).
   - Output: `business-moat-analyst.md`
   - Word limit: {budget.specialist_words:,} words
   - Primary data: `data/filings/10-K/*/item1_business.txt`, `data/filings/10-K/*/item1a_risk_factors.txt`

3. **industry-structure-cycle-analyst** — Industry economics, cycle position, structural trends.
   - Output: `industry-structure-cycle-analyst.md`
   - Word limit: {budget.specialist_words:,} words

4. **capital-allocation-analyst** — Management incentives, M&A discipline, SBC/dilution, buyback
   policy.
   - Output: `capital-allocation-analyst.md`
   - Word limit: {budget.specialist_words:,} words
   - Primary data: `data/filings/10-K/*/item5_equity.txt`, `data/filings/10-K/*/item11_exec_comp.txt`,
     `data/filings/10-K/*/note_stock_comp.txt`, `data/filings/10-K/*/note_equity.txt`

5. **geopolitical-risk-analyst** — Sovereign risk, sanctions, regulatory/policy exposure.
   - Output: `geopolitical-risk-analyst.md`
   - Word limit: {budget.specialist_words:,} words
{macro_section}{tactical_agent_section}
### Decision Agent (Run After All Specialists Complete)

**investment-decision-maker** — Synthesizes all specialist reports + raw data into a final
investment decision.

Outputs:
- `memo.md` — Narrative investment memo ({budget.memo_words:,} word limit). Must make a Buy/Sell/Neutral decision.
  Emphasize variant perception. "Too hard" is a valid conclusion.{tactical_memo_md_instruction}
- `memo.yaml` — Structured memo data for system consumption:

```yaml
ticker: {ticker}
date: {today}
thesis_summary: "<one-sentence thesis>"
decision: "<Buy|Sell|Neutral|Too Hard>"
valuation:
  fair_value_estimate: <number>
  entry_range: [<low>, <high>]
  exit_range: [<low>, <high>]
  key_assumptions:
    - "<assumption 1>"
    - "<assumption 2>"
  invalidation:
    - "<what would prove us wrong 1>"
    - "<what would prove us wrong 2>"
scores:
  tactical: <1-10 integer, how compelling the tactical opportunity is — 10 = rare asymmetric setup, 1 = no tactical edge>
  fundamental: <1-10 integer, how compelling the fundamental case is — 10 = clear mispricing with strong conviction, 1 = no edge>
dependencies:
  data_vintage: "{today}"
{tactical_memo_yaml}```

  **Scoring rubric for `scores`:**
  - **tactical** (1-10): Rate the near-term trading opportunity. 8-10 = rare asymmetric setup with
    clear catalyst and favorable risk/reward. 5-7 = decent setup but timing or risk uncertain.
    1-4 = no tactical edge or wrong timing. If no tactical context was provided, score 1.
  - **fundamental** (1-10): Rate the fundamental case. 8-10 = clear mispricing with strong variant
    perception and high conviction. 5-7 = interesting but thesis has meaningful holes. 1-4 = fairly
    valued or negative. "Too Hard" decisions should score 1-3.
  - Be honest and discriminating. Most ideas should score 3-6. Reserve 8+ for genuinely compelling
    setups. The scores are used to rank opportunities for the human trader's attention.

- `draft_monitors.yaml` — Proposed monitoring signals for ongoing tracking. These are suggestions
  for the human to review and approve via `praxis monitor approve {ticker}`.

  Three monitor types are supported:

  **filing** — triggers when a subscribed SEC filing lands (10-K, 10-Q, 8-K, etc.)
  **search** — daily cron, runs search queries and analyzes results
  **scraper** — daily cron, runs a custom scraper script with delta detection

```yaml
monitors:
  - name: "<descriptive name>"
    type: "<filing|search|scraper>"
    description: "<what to monitor and why — be specific about the data source>"
    threshold: "<what triggers an alert — quantitative when possible>"
    # For filing type: specify which filings trigger this monitor
    filing_types: ["10-K", "10-Q"]  # optional, for type=filing
    # For search type: specify search queries
    queries: ['"exact phrase"', 'keyword search']  # optional, for type=search
    # For scraper type: specify the URL to scrape
    source_url: "https://..."  # optional, for type=scraper
```

  **Cost hierarchy (prefer cheaper types):**
  - `filing` — **cheapest**. Reactive: only runs when a filing actually lands. One Sonnet
    call on already-extracted text. No daily cost. Default for anything answerable from
    SEC filings.
  - `scraper` — **cheap**. Runs a script on cron, compares output to previous run.
    Sonnet only called when data actually changes (delta detection). Good for structured
    data sources with known update schedules (TSMC monthly revenue, USDA data, Fed
    surveys, government portals).
  - `search` — **most expensive**. Daily search API call + Sonnet analysis every run,
    even when nothing has changed. Use sparingly for signals that can't be captured by
    filing or scraper.

  **Guidelines:**
  - **Default to `filing`**. Most investment-relevant data eventually shows up in SEC
    filings. Financial metrics, segment data, risk factors, management changes,
    delinquency rates, compensation, debt covenants — all filing monitors.
  - Use `scraper` for specific external data sources with structured, regularly updated
    pages (monthly revenue disclosures, government statistical releases, central bank
    surveys). These are cheap because Sonnet only runs on changes.
  - Use `search` only for diffuse, unpredictable signals that have no single data source:
    legislative/regulatory activity, trade policy shifts, competitor announcements where
    you don't know which site will break the news.
  - A good monitor set is ~60% filing, ~25% scraper, ~15% search.
  - Thresholds should be quantitative when possible ("spread below 2%", "growth < 5% YoY").
  - Description should say WHERE the data comes from, not just what to track.

---

## Execution Flow

You are the **COORDINATOR**. You manage the research pipeline, decide what to run,
and can early-exit if something is clearly uninvestable. You also serve as the
investment decision maker at the end.

**All decisions must be logged to `coordinator_log.md`** — this file is auditable by the
human. Write one line per decision: timestamp, what you decided, and why.

### Step 1 — Read Data & Quick Screen

Read the ingested data to understand what's available:
- `data/fundamentals/summary.md` — key financial metrics overview
- Latest `data/filings/10-K/*/item7_mda.txt` — management's own narrative
- Latest `data/filings/10-K/*/item1_business.txt` — business description
- Any `data/transcripts/` files if they exist

**IMPORTANT:** Do NOT read `data/fundamentals/fundamentals.json` directly — it's 700KB+ raw JSON.
Use `summary.md` + the fundamentals MCP tools instead.

**Quick screen** — After reading the data, ask yourself: is this **100% clearly uninvestable?**

This is a VERY high bar. Early exit ONLY if ALL of these are true:
- No real business (shell company, blank check, no operations)
- No data to analyze (no filings, no financials, no meaningful information)
- No conceivable investment thesis exists

If you early exit, write `coordinator_log.md` explaining why, then produce a minimal
`memo.md` and `memo.yaml` with decision "Too Hard" and move to Step 5.

**Most companies should NOT be early-exited.** Small revenue, penny stock price, high
burn rate — these are NOT reasons to exit. Many great investments start as "uninvestable"
looking companies. When in doubt, continue.

### Step 2 — Run Financial Analyst First

Run **rigorous-financial-analyst** first (not in parallel with others). This agent is most
likely to surface fundamental dealbreakers.

Include in its prompt:
- "Your output MUST be under {budget.specialist_words:,} words."
- "Read the local data files first. {web_constraint}"
- "Lead with findings, not setup. No preambles. Tables over prose for comparable data."
- "At the end of your analysis, add a line: INVESTABILITY: [CONTINUE|STOP] with a one-sentence reason."

### Step 3 — Coordinator Checkpoint

Read `rigorous-financial-analyst.md`. Check the INVESTABILITY line.

If STOP and the reason is a clear fundamental dealbreaker (going concern with <1 month
cash, proven fraud, delisted with no path back), you MAY skip remaining agents.
Log your decision to `coordinator_log.md`.

**Again, this is a high bar.** Negative earnings, dilution risk, competitive pressure —
these are NOT reasons to stop. Most companies should CONTINUE.

If CONTINUE (or if in doubt): proceed to Step 4.

### Step 4 — Run Remaining Specialists

Launch the remaining applicable agents. These CAN run in parallel since the coordinator
already approved continuation.

{agent_selection}

Include in each agent's prompt:
- "Your output MUST be under {budget.specialist_words:,} words."
- "Read the local data files first. {web_constraint}"
- "Lead with findings, not setup. No preambles. Tables over prose for comparable data."

### Step 5 — Coordinator Synthesizes (Investment Decision)

Once all specialist files exist (or after early exit), you ARE the investment decision maker.
Read all specialist reports and produce memo.md, memo.yaml, and draft_monitors.yaml.

Include: "Your memo MUST be under {budget.memo_words:,} words."

Log your final decision to `coordinator_log.md`.

### Step 6 — Summary

Print a brief summary: what was produced, any early exits, and any gaps.

---

## Global Rules

### Source Priority
1. **Local ingested data** (data/ directory) — primary source, already vetted
2. **SEC filings** via web — for anything not in local data
3. **One financial data site** (e.g., stockanalysis.com) — for current price, estimates
4. **Earnings transcripts** — if available locally or via web
5. Other public sources with attribution and skepticism

### Disallowed Sources
- Motley Fool, AI-generated blogs, content farms, unattributed SEO finance blogs

### No Invented Data
- Never fabricate numbers. If data is unavailable, say so and explain impact on conclusions.

### Traceability
- Key figures must be traceable to a source. Assumptions labeled as assumptions.

### Scope Discipline
- Valuation only in: rigorous-financial-analyst, investment-decision-maker
- Moat analysis only in: business-moat-analyst
- Final recommendations only in: investment-decision-maker

### Decision Hygiene
- Do not force conviction. Passing is acceptable. "Too hard" is valid.
- A clean Neutral is better than a weak Buy.

### Output Efficiency
- Lead with findings, not setup
- No company overview (the decision-maker knows the company)
- No methodology explanations
- No preambles or "in conclusion" sections
- Tables over prose for comparable data
- If a sentence can be deleted without losing insight, delete it

---

## Second-Order Thinking

Every agent must answer:

1. **What are the 1-3 key factors that actually drive this stock?**
   Strip away noise. Find the load-bearing variables.

2. **What is our differentiated view that others are missing?**
   Consensus is priced in. We need a variant perception to have an edge.

For specialist agents: find the non-obvious insight a typical analyst would miss. Do the work
others won't — read the footnotes, trace the cash, question the narrative.

For the decision maker: the variant perception table is the core of the memo. If you cannot
fill it with conviction, you do not have a trade.

First-order: "Margins are expanding" → Bullish
Second-order: "Margins are expanding because of favorable mix" → Is mix sustainable or one-time?

**The edge is in the second layer.**

---

## Event Calendar

If during your research you discover upcoming events (earnings dates, FDA decisions,
clinical data readouts, investor days, conferences, contract deadlines, etc.), record
them by appending to `../../config/events.yaml` in this format:

```yaml
  - ticker: {ticker}
    date: "YYYY-MM-DD"
    type: "earnings|clinical_data|fda_decision|investor_day|conference|regulatory|other"
    description: "<what the event is>"
    source: "filing_research"
    added_at: "{today}"
```

This helps the analyst agent anticipate catalysts when monitoring price moves.

---

## After Analysis

When done, the human will run `praxis research sync {ticker}` to push artifacts to S3.
"""
