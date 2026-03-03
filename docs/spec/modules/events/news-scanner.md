# News Scanner Module

Hourly news monitoring for all universe tickers. Catches thesis-relevant events that don't come through SEC filings or earnings transcripts — product launches, regulatory actions, competitor moves, M&A, executive changes, etc.

---

## Two-Layer Architecture

### Layer 1: SERP Sweep (Deterministic, Cheap)

- **Trigger:** EventBridge cron, hourly during market hours
- **Action:** For each ticker in universe, run a news search query via SERP API
- **Query:** Simple string search — `"NVIDIA" OR "NVDA"`, `"Apple" OR "AAPL"`, etc. Query templates stored in ticker registry.
- **Dedup:** Hash each result set per ticker. Compare against previous sweep's hash. If unchanged, skip agent triage — no new content, no LLM cost.
- **Output:** Raw search results (headlines, URLs, snippets, dates) stored to S3
- **Cost:** Near-zero. API call per ticker per hour, no LLM unless content changed.

```yaml
# Example raw output: data/news/{date}/raw/{ticker}/{hour}.json
ticker: NVDA
date: 2025-04-17
hour: 14
content_hash: "b7e4f1..."
previous_hash: "b7e4f1..."  # same → no triage needed
changed: false
results:
  - headline: "NVIDIA Unveils Blackwell Ultra AI Chip at GTC"
    url: "https://..."
    snippet: "NVIDIA announced its next-generation Blackwell Ultra..."
    source: "Reuters"
    published: "2025-04-17T09:30:00Z"
```

### Layer 2: Agent Triage (Sonnet, One Call, Only On Change)

- **Trigger:** After SERP sweep completes, IF any ticker's content hash changed
- **Action:** Single Sonnet API call receives only the NEW/CHANGED headlines across affected tickers, plus thesis summaries from `memo.yaml`
- **Purpose:** Filter noise, flag what's material, connect dots across tickers
- **Output:** Triaged digest stored to S3

The agent sees all changed tickers at once — "NVDA announces new chip" + "TSMC expands capacity" in the same hour is a compound signal that per-ticker processing would miss.

### Agent Prompt Context

- New/changed SERP results only (not the full result set, just deltas)
- Thesis summaries from `memo.yaml` for affected tickers
- Active monitor descriptions (so the agent can flag when news relates to a monitored datapoint)

### Agent Output

```yaml
# data/news/{date}/digest/{hour}.yaml
date: 2025-04-17
hour: 14
material:
  - ticker: NVDA
    headline: "NVIDIA Unveils Blackwell Ultra AI Chip at GTC"
    url: "https://..."
    relevance: |
      New product cycle directly impacts revenue growth trajectory.
      Thesis assumes DC revenue growth >30% YoY — new chip supports
      this if adoption follows H100/H200 pattern. Check pricing and
      availability timeline.
    related_monitors: [nvda-dc-revenue-mix]
    significance: high

nothing_material: [AAPL, MSFT]  # changed tickers with no material news
```

---

## Integration with Event Dispatch

When the triage agent produces a digest with material items:

1. **Event dispatch** triggers on `digest/{hour}.yaml` S3 PUT
2. Dispatch matches `{ticker}:news` against monitor `listen` fields
3. Matching monitor collectors fire, may update snapshots

The news digest also feeds into:
- Monitor evaluator (pre-market pass sees overnight/morning news)
- Manage EOD assessment (full day's news context for price action interpretation)

---

## SERP API

Options (in preference order):

| API | Notes |
| --- | --- |
| SerpAPI | Google News results, structured JSON, ~$50/mo for 5k searches |
| NewsAPI | Dedicated news API, good coverage, free tier available |
| Google Custom Search | Official, $5/1000 queries |
| Bing News Search | Azure marketplace, competitive pricing |

Selection TBD. Requirements: structured JSON output, news-specific results, reasonable rate limits for hourly universe-wide sweep.

### Query Templates

Stored in ticker registry to allow customization:

```yaml
# In config/ticker_registry.yaml
tickers:
  NVDA:
    cik: "0001045810"
    news_queries:
      - '"NVIDIA" OR "NVDA"'
      - '"Jensen Huang"'  # CEO, often newsworthy
```

Default: `"{company_name}" OR "{ticker}"`. Human can add supplementary queries (CEO names, product names, subsidiary names) to improve recall.

---

## S3 Layout

```
s3://praxis-copilot/
├── data/news/{date}/
│   ├── raw/{ticker}/{hour}.json     (hourly SERP results per ticker)
│   └── digest/{hour}.yaml           (agent triage output, only when changes detected)
```

---

## Compute Model

| Component | Compute | Cost | Frequency |
| --- | --- | --- | --- |
| SERP sweep | Lambda | Near-zero (API calls) | Hourly, per ticker |
| Agent triage | Lambda + Sonnet API | Low (one call, only on change) | Hourly, only when content hash changes |

Most hours: SERP runs, hashes match, no Sonnet call. Cost is dominated by SERP API fees (fixed, small). LLM cost only on actual news flow.

---

## Config

```yaml
# config/news.yaml
enabled: true
serp_api: serpapi  # or newsapi, google_custom_search
results_per_ticker: 10  # top N results to fetch
lookback_hours: 24  # how far back to search
sweep_interval_minutes: 60  # hourly default
market_hours_only: true  # skip overnight sweeps
```

---

## Open Questions

- Which SERP API to use (cost/coverage tradeoff)
- Whether to add non-universe tickers to the sweep (e.g., external tickers from monitors)
- Full-text article fetching vs headline-only (headlines sufficient for triage, full text for deeper reads)
- Weekend/holiday handling (skip or catch up Monday?)
