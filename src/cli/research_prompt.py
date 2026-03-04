"""Generate CLAUDE.md research prompt for a ticker workspace."""

from datetime import date


def generate_research_prompt(
    ticker: str,
    company_name: str,
    data_manifest: str,
    has_macro: bool,
    has_existing_artifacts: bool,
) -> str:
    """Generate a CLAUDE.md research pipeline prompt for the workspace."""
    today = date.today().isoformat()

    macro_section = ""
    if has_macro:
        macro_section = """
### Macro Analyst (Run With Specialists)

6. **macro-analyst** — Reads all files in `macro/` directory. Analyzes how current macro environment
   (rates, inflation, trade policy, liquidity, cycle position) specifically affects this company.
   Focus on company-specific macro exposure, not generic macro commentary.
   - Input: `macro/` directory + company data
   - Output: `macro-analyst.md`
   - Word limit: 1,500 words
   - This is NOT a macro overview — it's "what does macro mean for THIS stock"
"""

    idempotency_note = ""
    if has_existing_artifacts:
        idempotency_note = """
**Note:** Existing analysis files were pulled from a prior run. By default, do NOT overwrite them.
Only generate missing outputs. If the user explicitly requests a rerun, replace files entirely.
"""

    return f"""# Research Pipeline — {ticker} ({company_name})

Generated: {today}

---

## Your Task

You are running an investment research pipeline for **{ticker} ({company_name})**. All source
data has been pre-ingested and is available locally in `data/`. Read these files to inform your
analysis — do NOT rely solely on web searches. The ingested data is your primary source of truth.
{idempotency_note}
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
   normalized earnings, valuation. Must read `data/fundamentals/` and `data/filings/` (especially
   MD&A, financial statement notes on revenue, segments, debt, income tax).
   - Output: `rigorous-financial-analyst.md`
   - Word limit: 1,500 words
   - Primary data: `data/fundamentals/fundamentals.json`, `data/filings/10-K/*/item7_mda.txt`,
     `data/filings/10-Q/*/item2_mda.txt`, note_* files

2. **business-moat-analyst** — Competitive durability, switching costs, pricing power, network
   effects. Must read `data/filings/` (Item 1 Business, Risk Factors).
   - Output: `business-moat-analyst.md`
   - Word limit: 1,500 words
   - Primary data: `data/filings/10-K/*/item1_business.txt`, `data/filings/10-K/*/item1a_risk_factors.txt`

3. **industry-structure-cycle-analyst** — Industry economics, cycle position, structural trends.
   Run ONLY when the industry is meaningfully cyclical (commodities, semis, shipping, homebuilders)
   or when cycle position is a key driver. Skip for stable/secular-growth industries.
   - Output: `industry-structure-cycle-analyst.md`
   - Word limit: 1,500 words

4. **capital-allocation-analyst** — Management incentives, M&A discipline, SBC/dilution, buyback
   policy. Run ONLY when capital allocation is a genuine differentiator or risk: serial acquirers,
   SBC >10% of revenue, contested governance, or buyback/dividend policy as a core thesis driver.
   - Output: `capital-allocation-analyst.md`
   - Word limit: 1,500 words
   - Primary data: `data/filings/10-K/*/item5_equity.txt`, `data/filings/10-K/*/item11_exec_comp.txt`,
     `data/filings/10-K/*/note_stock_comp.txt`, `data/filings/10-K/*/note_equity.txt`

5. **geopolitical-risk-analyst** — Sovereign risk, sanctions, regulatory/policy exposure. Run ONLY
   if material (China/Russia/EM exposure, defense, energy, semiconductors).
   - Output: `geopolitical-risk-analyst.md`
   - Word limit: 1,500 words
{macro_section}
### Decision Agent (Run After All Specialists Complete)

**investment-decision-maker** — Synthesizes all specialist reports + raw data into a final
investment decision.

Outputs:
- `memo.md` — Narrative investment memo (2,500 word limit). Must make a Buy/Sell/Neutral decision.
  Emphasize variant perception. "Too hard" is a valid conclusion.
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
dependencies:
  data_vintage: "{today}"
```

- `draft_monitors.yaml` — Proposed monitoring signals for ongoing tracking. These are suggestions
  for the human to review, NOT auto-applied config:

```yaml
monitors:
  - name: "<descriptive name>"
    type: "<scraper|agent>"
    description: "<what to monitor and why>"
    threshold: "<what triggers an alert>"
```

---

## Execution Flow

### Step 1 — Read Data First

Before running any agent, read the ingested data to understand what's available. At minimum:
- `data/fundamentals/fundamentals.json` — key financial metrics
- Latest `data/filings/10-K/*/item7_mda.txt` — management's own narrative
- Latest `data/filings/10-K/*/item1_business.txt` — business description
- Any `data/transcripts/` files if they exist

### Step 2 — Run Specialist Agents (In Parallel)

Launch applicable agents concurrently. When launching each agent, include these constraints
verbatim in the prompt:
- "Your output MUST be under 1,500 words."
- "Read the local data files first. You have a budget of 10 web lookups for supplemental info only."
- "Lead with findings, not setup. No preambles. Tables over prose for comparable data."

Which agents to run:
- **Always**: rigorous-financial-analyst, business-moat-analyst
- **If relevant**: industry-structure-cycle-analyst, capital-allocation-analyst
- **If material**: geopolitical-risk-analyst
{"- **Always** (if macro/ exists): macro-analyst" if has_macro else ""}
- When in doubt, skip optional agents. Two high-quality outputs beat four mediocre ones.

### Step 3 — Run Investment Decision Maker

Once all specialist files exist, run investment-decision-maker. It reads all specialist reports
and produces memo.md, memo.yaml, and draft_monitors.yaml.

### Step 4 — Summary

After all agents complete, print a brief summary of what was produced and any gaps.

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

## After Analysis

When done, the human will run `praxis research sync {ticker}` to push artifacts to S3.
"""
