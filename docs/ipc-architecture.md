# Praxis IPC Architecture — Claude Code ↔ Claude Desktop

## Vision

Claude Desktop becomes the "eyes and hands" of the praxis system — it can browse the web,
interact with native apps, take screenshots, read PDFs, scroll social media. Claude Code
remains the "brain and infrastructure" — managing daemons, processing data, running research.

They communicate through a shared filesystem via the Praxis MCP server.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  CLAUDE DESKTOP                      │
│  (computer use, browser, screenshots, social media)  │
│                                                      │
│  Scheduled triggers:                                 │
│    • Every 30 min: check data/ipc/tasks/ for work    │
│    • Morning: browse key sources, write findings     │
│    • Continuous: respond to task requests             │
│                                                      │
│  MCP Tools (read/write):                            │
│    • ipc_check_tasks() — poll for new tasks          │
│    • ipc_submit_result() — write findings back       │
│    • ipc_submit_screenshot() — save screenshot       │
│    • All existing praxis tools (usage, briefing...)   │
└──────────────────────┬──────────────────────────────┘
                       │ shared filesystem
                       │ data/ipc/
┌──────────────────────┴──────────────────────────────┐
│                   CLAUDE CODE                        │
│  (daemons, research pipeline, analysis)              │
│                                                      │
│  Can create IPC tasks:                               │
│    • Analyst agent: "browse X for $CLMT sentiment"   │
│    • Research session: "screenshot this chart"        │
│    • Filing daemon: "check this company's website"   │
│    • User: "praxis desktop browse URL"               │
│                                                      │
│  Reads IPC results:                                  │
│    • Analyst incorporates Desktop findings            │
│    • Research sessions use screenshots as context     │
│    • Findings flow into email digests                 │
└─────────────────────────────────────────────────────┘
```

## IPC Directory Structure

```
data/ipc/
├── tasks/                    # Pending tasks for Desktop
│   ├── task_20260403_001.yaml
│   └── task_20260403_002.yaml
├── results/                  # Completed results from Desktop
│   ├── result_20260403_001.yaml
│   └── result_20260403_001_screenshot.png
├── inbox/                    # Free-form findings (Desktop → Code)
│   ├── finding_CLMT_20260403.md
│   └── finding_macro_20260403.md
└── sources.yaml              # Configured browse sources
```

## Task Schema

```yaml
# data/ipc/tasks/task_20260403_001.yaml
id: "20260403_001"
created_at: "2026-04-03T08:30:00-04:00"
created_by: "analyst_agent"  # or "user", "filing_research", etc.
status: "pending"  # pending, in_progress, done, failed
priority: "high"  # low, medium, high

type: "browse"  # browse, screenshot, search, monitor, research
ticker: "CLMT"
description: "Check Twitter/X for sentiment on CLMT after RVO announcement"

# Type-specific fields
url: "https://x.com/search?q=%24CLMT"  # for browse/screenshot
search_query: "$CLMT RVO RIN"  # for search
sources: ["twitter", "stocktwits"]  # for monitor

# Expected output
output_format: "markdown"  # markdown, screenshot, yaml
output_path: "data/ipc/results/result_20260403_001.yaml"
```

## Result Schema

```yaml
# data/ipc/results/result_20260403_001.yaml
task_id: "20260403_001"
completed_at: "2026-04-03T08:35:00-04:00"
status: "done"
ticker: "CLMT"

finding: |
  Twitter sentiment on CLMT post-RVO:
  - @Zerosumgame33: "D4 RINs should push to $1.80+ on the final rule"
  - Multiple accounts noting Montana Renewables capacity expansion
  - General bullish tone, 15 tweets in last 4 hours
  - Key concern: China soybean oil tariff impact on feedstock costs

actionability: "monitor"  # none, monitor, research_deeper, trade_idea
urgency: "medium"

screenshots: ["result_20260403_001_screenshot.png"]
sources_checked: ["twitter", "stocktwits"]
```

## Browse Sources Configuration

```yaml
# data/ipc/sources.yaml
sources:
  twitter:
    name: "Twitter/X"
    url_template: "https://x.com/search?q=${query}&f=live"
    description: "Financial Twitter — real-time sentiment, breaking news"
    cadence: "2h"  # How often to check proactively

  stocktwits:
    name: "StockTwits"
    url_template: "https://stocktwits.com/symbol/${ticker}"
    description: "Retail sentiment, message volume trends"
    cadence: "4h"

  microcapclub:
    name: "MicroCapClub"
    url_template: "https://microcapclub.com/search/?q=${ticker}"
    description: "Micro-cap focused community, deep research"
    cadence: "daily"

  ceo_ca:
    name: "CEO.CA"
    url_template: "https://ceo.ca/${ticker}"
    description: "Canadian small-cap forums, mining/resources focus"
    cadence: "daily"

  valueinvestorsclub:
    name: "Value Investors Club"
    url_template: "https://valueinvestorsclub.com"
    description: "Concentrated value ideas, high quality writeups"
    cadence: "daily"

  substack:
    name: "Substack"
    url_template: "https://substack.com/search/${query}"
    description: "Independent research, newsletters"
    cadence: "daily"

  edgar:
    name: "SEC EDGAR Full-Text Search"
    url_template: "https://efts.sec.gov/LATEST/search-index?q=${query}&dateRange=custom&startdt=${start_date}&enddt=${end_date}"
    description: "Direct SEC filing search"
    cadence: "4h"

  sedar:
    name: "SEDAR+ (Canadian filings)"
    url_template: "https://www.sedarplus.ca/landingpage/"
    description: "Canadian securities filings"
    cadence: "daily"

  # Proactive idea generation sources (no ticker needed)
  finviz_screener:
    name: "Finviz Screener"
    url_template: "https://finviz.com/screener.ashx?v=111&f=cap_smallunder,ta_perf_1wup"
    description: "Small-cap momentum screener"
    cadence: "daily"
    proactive: true

  unusual_whales:
    name: "Unusual Whales"
    url_template: "https://unusualwhales.com/flow"
    description: "Options flow visualization"
    cadence: "2h"
    proactive: true
```

## MCP Tools for IPC

New tools added to praxis_mcp.py:

### Task Management (for Desktop to poll)
- `ipc_check_tasks()` — return pending tasks for Desktop to process
- `ipc_claim_task(task_id)` — mark task as in_progress
- `ipc_submit_result(task_id, finding, actionability, urgency)` — submit result
- `ipc_submit_screenshot(task_id, description)` — save screenshot with context

### Task Creation (for Code to request work)
- `ipc_create_task(type, ticker, description, ...)` — create a task for Desktop
- `ipc_browse(url, prompt)` — shortcut: create a browse task
- `ipc_search(query, sources)` — shortcut: create a search task

### Inbox (for Desktop to write free-form findings)
- `ipc_write_finding(ticker, content, actionability)` — write to inbox
- `ipc_list_findings()` — list recent inbox findings

## Integration Points

### Analyst Agent → Desktop
When the analyst investigates a price alert and needs web context:
1. Creates IPC task: "browse Twitter for $CLMT sentiment"
2. Desktop picks up task, browses, writes finding
3. Analyst reads finding on next poll, incorporates into analysis

### Research Sessions → Desktop
When a research session needs visual data:
1. Creates IPC task: "screenshot TradingView chart for AXSM"
2. Desktop opens TradingView, screenshots, writes to results
3. Research session reads screenshot as context

### Proactive Browsing (Desktop autonomous)
Desktop runs scheduled triggers:
1. Every 2h: check Twitter for portfolio tickers
2. Daily: browse MicroCapClub, VIC, Substack for new ideas
3. Daily: check Finviz screener for momentum names
4. Write findings to inbox → analyst incorporates into digest

### User-Initiated
User can type in Claude Code:
- `praxis desktop browse https://x.com/search?q=%24CLMT` → creates task
- `praxis desktop search "CLMT RVO" --sources twitter,stocktwits` → creates task

Or ask Claude Desktop directly:
- "Check what people are saying about CLMT on Twitter"
- Desktop uses praxis MCP to write finding → Code picks it up

## Claude Desktop Setup Instructions

After the MCP server is configured, the user needs to set up scheduled triggers
in Claude Desktop:

1. Create a "Praxis Monitor" scheduled trigger (every 30 min):
   ```
   Check for pending IPC tasks using the praxis MCP ipc_check_tasks tool.
   For each task, process it according to its type (browse URLs, take
   screenshots, search for information). Submit results back via
   ipc_submit_result. Use computer use tools for browsing and screenshots.
   ```

2. Create a "Praxis Social Scanner" scheduled trigger (every 2h):
   ```
   Browse Twitter/X for the portfolio tickers listed in the praxis
   portfolio tool. Look for sentiment, breaking news, unusual activity.
   Write any interesting findings via ipc_write_finding. Focus on what's
   NEW and actionable — skip routine commentary.
   ```

3. Create a "Praxis Idea Generator" daily trigger (7 AM):
   ```
   Browse MicroCapClub, Value Investors Club, and financial Substack
   for new investment ideas. Check Finviz screener for small-cap momentum.
   Write any interesting findings to inbox via ipc_write_finding.
   ```
