# Claude Desktop Setup for Praxis IPC

## 1. Restart Claude Desktop

Quit Claude Desktop (Cmd+Q) and reopen it. The MCP config was already added to
`~/Library/Application Support/Claude/claude_desktop_config.json`.

After restart, verify the praxis MCP is connected — you should see 27 praxis tools
available when you ask Claude Desktop "what praxis tools do you have?"

## 2. Create Scheduled Triggers

In Claude Desktop, set up these scheduled triggers (via the sidebar):

### Trigger 1: "Praxis Task Runner" (every 30 min)

```
Check for pending IPC tasks using the ipc_check_tasks tool. For each pending task:
1. Call ipc_claim_task with the task ID
2. Process the task based on its type:
   - For "browse" tasks: use computer use tools to open the URL in a browser,
     read the content, and summarize findings
   - For "search" tasks: browse each source URL, search for the query,
     and compile findings
   - For "screenshot" tasks: open the URL and take a screenshot
3. Call ipc_submit_result with your findings

Be concise. Focus on what's NEW and actionable. Skip routine commentary.
```

### Trigger 2: "Praxis Social Scanner" (every 2 hours, market hours only)

```
Using the portfolio tool, get the list of portfolio positions. For each of the
top 5 positions by importance, browse Twitter/X by searching for the ticker
with a $ prefix (e.g., $CLMT, $FOTB).

For each ticker:
1. Open https://x.com/search?q=%24{TICKER}&f=live
2. Read the recent tweets (last few hours)
3. If anything material or interesting is found, call ipc_write_finding with
   the content, ticker, and appropriate actionability/urgency

Skip tickers with no meaningful social media activity. Focus on:
- Breaking news or developments
- Unusual sentiment shifts
- Notable accounts commenting (institutional, analysts)
- Rumors or catalysts being discussed

Keep it brief. Only write findings for genuinely interesting signals.
```

### Trigger 3: "Praxis Idea Generator" (daily, 7 AM)

```
Browse the following investment research sources for new ideas:

1. MicroCapClub (https://microcapclub.com) — check recent posts
2. Value Investors Club (https://valueinvestorsclub.com) — check new ideas
3. Finviz screener (https://finviz.com/screener.ashx?v=111&f=cap_smallunder,ta_perf_1wup)
   — scan for interesting small-cap momentum names

For each interesting idea found, call ipc_write_finding with:
- The ticker and thesis summary
- actionability: "research_deeper"
- urgency: "low"

Also check the calendar_list tool for any events happening today/tomorrow
and flag tickers that might need attention.
```

## 3. Test It

After setting up triggers, test the IPC:

1. In Claude Code terminal: `praxis desktop browse "https://x.com/search?q=%24CLMT" -t CLMT -d "Check sentiment"`
2. Wait for Desktop to pick up the task (or manually ask Desktop: "check for praxis tasks")
3. Verify: `praxis desktop inbox` should show the finding

## 4. Available Tools

The full list of praxis MCP tools available in Claude Desktop:

**Research & Analysis:**
- `read_memo(ticker)` — read investment memo
- `read_memo_yaml(ticker)` — read structured memo data
- `audit(ticker)` — view coordinator decisions

**Monitoring:**
- `briefing()` — morning briefing
- `health()` — daemon status
- `capacity()` — current capacity %
- `usage()` / `usage_json()` — telemetry

**Portfolio & Watches:**
- `portfolio()` — positions and watchlist
- `watchdog_list()` / `watchdog_check()` — entry/exit targets
- `watches_list()` — thesis monitors
- `exposure_map()` — factor exposure

**IPC (Claude Code ↔ Desktop):**
- `ipc_check_tasks()` — poll for pending tasks
- `ipc_claim_task(id)` — claim a task
- `ipc_submit_result(id, finding)` — submit result
- `ipc_write_finding(content)` — write proactive finding
- `ipc_list_findings()` — list recent findings
- `ipc_create_browse_task(url)` — request browsing
- `ipc_create_search_task(query)` — request search
- `ipc_browse_sources()` — list configured sources

**Events & Queue:**
- `calendar_list()` / `calendar_add()` — event calendar
- `queue_add(title)` — create research task
- `queue_status()` — queue status
- `filing_research_status()` — filing daemon status
- `analyst_status()` — analyst findings
