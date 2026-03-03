# Research Session Module

Implements the **analyze** stage of the pipeline for human-initiated deep research. Unlike event-stage analysis (reactive, lightweight LLM screening on Lambda), this is proactive, heavy research using Claude Code sessions on local compute.

---

## Trigger

Human runs `praxis universe add TICKER` or `praxis analyze TICKER` via CLI. The CLI is the sole entry point — there is no automatic trigger from config file changes.

- `praxis universe add TICKER` — adds ticker to universe config + ticker registry, triggers data ingestion, queues for analysis
- `praxis analyze TICKER` — runs analysis on a ticker already in universe (re-analysis, or initial analysis if data is already ingested)

---

## Data Plane (Deterministic Ingestion)

Triggered by CLI. Pulls:

- **SEC filings** — via EDGAR (reuse 8k-scanner's `edgar/` modules). CIK resolved via ticker registry (see [event-dispatch.md](../events/event-dispatch.md)).
- **Fundamentals** — via FMP/EODHD APIs (reuse `financials.py` patterns)
- **Earnings call transcripts** — via API (provider TBD)

Store all to S3 under `research/{ticker}/data/`. Track ingestion status per ticker so the CLI can report "data ready for analysis" or "ingestion in progress."

---

## Control Plane (Reasoning — Claude Code)

This stage runs locally via Claude Code, NOT on Lambda/API. The reasoning is too heavy and interactive for serverless.

### Flow

1. CLI confirms data ingestion complete
2. Human initiates Claude Code session (optionally via agentq queue)
3. Session follows `~/dev/models/claude/run.md` multi-agent pipeline:
   - **Specialist agents:** financial, moat, industry-cycle, capital-allocation, geopolitical (conditional)
   - **macro-analyst** (new) — pulls shared macro context, analyzes company-specific macro exposure
   - **investment-decision-maker** — synthesizes specialist reports into final memo. Receives current monitor registry as input to produce draft monitors (new or amendments).

### Artifacts Produced

All artifacts are produced locally, then synced to S3 after human review.

| Artifact | Description |
| --- | --- |
| `{ticker}/memo.md` | Narrative investment memo for human consumption |
| `{ticker}/memo.yaml` | Structured memo data for system consumption (valuation anchors, assumptions, invalidation criteria) — see [manage spec](../manage/portfolio-manager.md) |
| `{ticker}/{agent_name}.md` | Specialist reports (one per agent) |
| `{ticker}/draft_monitors.yaml` | Proposed monitoring signals — DATA, not config. Contains `new` and `amend` entries. |

### memo.yaml

Structured data that the manage stage reads programmatically:

```yaml
ticker: NVDA
date: 2025-04-17
thesis_summary: "AI compute demand sustains pricing power while supply remains constrained"
valuation:
  fair_value_estimate: 28
  entry_range: [22, 25]
  exit_range: [30, 35]
  key_assumptions:
    - "CoWoS capacity supports 2025 supply"
    - "DC revenue growth >30% YoY"
  invalidation:
    - "CoWoS capacity cut or delayed"
    - "DC revenue growth <15% YoY"
dependencies:
  macro_view_hash: "a3f8c2..."
  data_vintage: "2025-04-17"
  monitor_snapshots:
    tsmc-cowos-capacity: "2025-04-15"
    hyperscaler-capex: "2025-04-10"
```

The `dependencies` section is how the system tracks staleness. When a monitor snapshot is newer than what's recorded here, the memo is stale. This replaces the previously proposed `.versions.json` — all dependency tracking lives directly in `memo.yaml`, co-located with the data it describes.

`draft_monitors.yaml` contains proposed signals and thresholds. Human reviews, edits, and commits to the config layer when ready. It is never auto-applied.

---

## Staleness

Every `memo.yaml` records the versions of its inputs in the `dependencies` section:

- `macro_view_hash` — hash of `context/macro/view.md` at time of analysis
- `data_vintage` — timestamp of ingested data used
- `monitor_snapshots` — map of monitor_id → snapshot date the memo was written against

When any of these inputs change (macro view updated, new monitor snapshot with significant delta), the system can mechanically determine staleness by comparing `memo.yaml` dependencies against current state. This is a monitoring signal surfaced to the human, not an auto-update trigger.

---

## Sync Model (Laptop → S3)

- Claude Code sessions run locally, produce artifacts on disk
- After human review/approval, `praxis sync TICKER` pushes artifacts to S3 under `research/{ticker}/`
- S3 is the durable store; local is the workspace
- Sync is explicit, CLI-driven

---

## S3 Layout

```
s3://praxis-copilot/data/
├── research/{ticker}/
│   ├── data/                    (ingested raw data)
│   │   ├── filings/
│   │   ├── fundamentals/
│   │   └── transcripts/
│   ├── memo.md                  (narrative memo, synced after review)
│   ├── memo.yaml                (structured memo, synced after review)
│   ├── financial.md             (specialist report)
│   ├── moat.md
│   ├── industry-cycle.md
│   ├── capital-allocation.md
│   ├── geopolitical.md          (conditional)
│   ├── macro-analyst.md
│   └── draft_monitors.yaml
```

---

## CLI

```
praxis universe add TICKER    # add to universe + ticker registry, trigger data ingestion
praxis analyze TICKER         # initiate Claude Code research session
praxis sync TICKER            # sync local artifacts to S3
```

---

## Open Questions

- Exact earnings transcript API provider
- agentq integration details (queuing analysis sessions with custom prompts)
- CLI implementation details (Python click, shell script, etc.)
