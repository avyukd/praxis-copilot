# Event Dispatch

Routes incoming data from event-stage pipelines to downstream consumers: monitors, manage stage, and future modules.

---

## Problem

The 8k-scanner stores filings at `data/raw/8k/{cik}/{accession}/`. The news scanner stores digests at `data/news/{date}/digest/{hour}.yaml`. Monitors subscribe to data via `listen: [TSM:transcripts, TSM:filings]`. Something needs to connect these — when data arrives, which monitors should fire?

---

## Dispatch Lambda

Generalized routing layer. Triggers on S3 PUTs from ANY event-stage module:

### Trigger Sources

| Source | S3 Trigger Path | Data Type |
| --- | --- | --- |
| 8k-scanner | `data/raw/8k/{cik}/{accession}/analysis.json` | `filings` |
| CA-PR scanner | `data/raw/ca-pr/{ticker}/{release_id}/analysis.json` | `press_releases` |
| US-PR scanner | `data/raw/us-pr/{ticker}/{release_id}/analysis.json` | `press_releases` |
| News scanner | `data/news/{date}/digest/{hour}.yaml` | `news` |
| Transcript ingestion | TBD | `transcripts` |

### Action

1. Read the artifact — extract ticker(s), data type, metadata
2. For 8k-scanner sources: resolve CIK → ticker using the ticker registry
3. For news scanner: iterate over `material` items in digest, extract tickers
4. Read the monitor registry from S3 (`config/monitors/`)
5. Match against all monitors' `listen` fields: does any monitor listen to `{ticker}:{data_type}`?
6. For each matching monitor, invoke the monitor's collector (Lambda async invoke or SQS message), passing the source artifact location
7. Emit a structured event record to `data/events/{date}/{event_id}.json` for audit

**Does NOT** apply judgment. Pure routing — deterministic, cheap.

---

## Ticker Registry

Maintains bidirectional mapping between CIK (SEC identifier) and ticker symbol. Needed because:

- EDGAR uses CIK as the primary key
- Everything else in the system uses tickers
- The 8k-scanner S3 layout is `data/raw/8k/{cik}/...`
- Monitors, research, market data, and manage all use `{ticker}/...`

### Location

`config/ticker_registry.yaml` in repo, deployed to S3 via `praxis config sync`:

```yaml
# CIK → ticker mapping
# Maintained automatically by universe CLI, manually editable
tickers:
  NVDA:
    cik: "0001045810"
    exchange: NASDAQ
    name: "NVIDIA Corporation"
    news_queries:
      - '"NVIDIA" OR "NVDA"'
      - '"Jensen Huang"'
  TSM:
    cik: "0001046179"
    exchange: NYSE
    name: "Taiwan Semiconductor Manufacturing"
    universe_status: external  # not in universe, tracked for monitors only
    news_queries:
      - '"TSMC" OR "Taiwan Semiconductor"'
```

### Universe Status

Tickers can be:

- **In universe** (default): full pipeline — 8k-scanner ingests filings, news scanner sweeps headlines, research session produces memo, manage stage tracks price.
- **External** (`universe_status: external`): added because a monitor depends on it. Limited pipeline:
  - 8k-scanner WILL ingest filings (added to poller's ticker list)
  - News scanner WILL include in SERP sweep
  - No research session, no memo, no manage stage tracking
  - Only purpose: feed event-triggered monitors

This solves the external ticker gap — monitors on TSM will actually fire because TSM's filings get ingested, just without the full research/manage treatment.

### Populated By

- `praxis universe add TICKER` — CLI resolves CIK via EDGAR API, adds entry
- `praxis config sync` — after manual edits for external tickers
- When committing `draft_monitors.yaml` that references external tickers, human adds them to registry

### Consumed By

- Event dispatch (CIK → ticker resolution)
- 8k-scanner poller (universe + external ticker filtering)
- News scanner (SERP sweep list)
- Research session data ingestion
- Manage stage (universe tickers only)

---

## Event Record

Each dispatched event produces an audit record:

```yaml
# data/events/2025-04-17/evt-001.json
event_id: evt-001
timestamp: 2025-04-17T14:30:00Z
source: 8k-scanner
ticker: TSM
cik: "0001046179"
data_type: filings
s3_path: data/raw/8k/0001046179/0001234567/analysis.json
monitors_triggered:
  - tsmc-cowos-capacity
  - tsmc-capex-trend
```

This creates a queryable trail: "what events fired what monitors on what date."

---

## Data Types

The `listen` field in monitors uses these data type identifiers:

| Data Type | Source | Event Stage Module | Status |
| --- | --- | --- | --- |
| `filings` | SEC 8-K, 10-K, 10-Q | 8k-scanner | Built |
| `press_releases` | CA-PR, US-PR | 8k-scanner | Built |
| `news` | Hourly SERP sweep + agent triage | news-scanner | Specced, see [news-scanner.md](news-scanner.md) |
| `transcripts` | Earnings call transcripts | TBD | Not built — monitors using this will not trigger until pipeline exists |
| `fundamentals` | Financial data APIs | Research ingestion | Available but not event-driven |

Monitors can reference data types that don't have event pipelines yet. The dispatch layer simply has no events to route — the monitor sits dormant. No error, no silent failure. The system can surface: "monitor X listens to TSM:transcripts but no transcript pipeline exists."
