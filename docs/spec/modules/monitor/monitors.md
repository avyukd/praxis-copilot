# Monitor Module

Implements the **monitor** stage of the pipeline. Monitors are persistent, declarative signals that track datapoints relevant to investment theses. They run continuously, produce daily snapshot artifacts, detect meaningful changes, and surface alerts to the human.

---

## Three Layers

The system maintains a clean separation:

| Layer | Lives in | Description |
| --- | --- | --- |
| **Config** | Repo (`config/monitors/`) | Monitor definitions (DSL). Human-reviewed, version-controlled via git. |
| **Data** | S3 (`monitors/{monitor_id}/`) | Daily snapshot artifacts produced by collectors. Versioned by date. |
| **Control** | Lambda (API) + Claude Code (escalation) | Collector and evaluator logic. Sonnet API for daily runs, Claude Code for human-initiated deep dives. |

---

## Monitor DSL

Monitors are defined in `config/monitors/` as yaml files. The DSL includes structured fields for routing and scheduling, plus natural language `extract` prompts that instruct collector agents on what to look for.

```yaml
- id: tsmc-cowos-capacity
  relevant_to: [NVDA, AMD]
  description: "TSMC CoWoS advanced packaging capacity expansion"
  trigger: event
  listen: [TSM:transcripts, TSM:filings]
  universe_status: external  # TSM not in universe
  collector: agent
  extract: |
    CoWoS capacity plans, expansion timeline, and capex allocated to
    advanced packaging. Any revisions to previously stated capacity
    targets (e.g., "2x by end 2025"). Note Kumamoto and other new
    facility timelines. Customer allocation commentary if disclosed —
    specifically how capacity is split between NVIDIA (H100/H200/B100),
    AMD (MI300), and other customers. Management tone on demand
    duration and whether supply constraints are easing or tightening.
```

### DSL Fields

| Field | Required | Description |
| --- | --- | --- |
| `id` | yes | Unique identifier. Kebab-case, descriptive. |
| `relevant_to` | yes | Tickers whose theses depend on this monitor. Drives alert routing and staleness propagation. |
| `description` | yes | Short human-readable summary. |
| `trigger` | yes | `event`, `scheduled`, or `periodic`. See Trigger Types below. |
| `listen` | for event/scheduled | Data sources to subscribe to. Format: `TICKER:data_type` where data_type is `transcripts`, `filings`, `news`, `fundamentals`. |
| `extract` | yes | Natural language prompt for the collector. Should be detailed — input tokens are cheap, vague instructions produce bad snapshots. |
| `collector` | no | `scraper` or `agent`. Default: `agent`. |
| `fallback` | no | Fallback collector if primary fails. Typically `agent` when `collector: scraper`. |
| `frequency` | for periodic | `daily`, `weekly`, `quarterly`. |
| `schedule_from` | for scheduled | Event that determines when to run (e.g., `earnings_date`). |
| `universe_status` | no | `external` if `listen` includes tickers not in universe. System warns that data ingestion won't be automatic. |

### Trigger Types

**`event`** — fires when relevant data arrives through the event stage. A transcript lands for a ticker in `listen` → collector runs. Cheapest, most natural for monitors tied to company communications.

**`scheduled`** — fires on a known future date derived from an event. Example: event stage catches "NVDA earnings Jan 28" → schedules monitor run for that date → when transcript lands, collector extracts the datapoint. Event-triggered with a known future date.

**`periodic`** — fires on a cadence regardless of data arrival. For ambient monitoring with no specific trigger (regulatory changes, news scanning). Most expensive for agent collectors — use selectively.

---

## Collectors

Collectors produce snapshot artifacts. Two tiers:

### Scraper (deterministic, cheap)

Structured extraction from APIs or known document locations. Runs daily/on-trigger for near-zero cost. Suitable for quantitative datapoints with stable sources (revenue figures, capex guidance, financial metrics).

### Agent (Sonnet API, moderate cost)

LLM call that reads source data and extracts the datapoint per the `extract` prompt. Used for qualitative monitors or as fallback when scrapers break. Runs via Sonnet API — lightweight, automated, no human involvement.

### Post-MVP: Scraper Generation

When a monitor with `collector: scraper` is created and no scraper implementation exists, the system flags it for development. A human-initiated agent session attempts to build a deterministic scraper (find the source URL, write extraction logic). If it succeeds, that scraper runs daily at near-zero cost. If it fails or the source changes, the monitor silently falls back to agent mode. Aspirational optimization, not blocking.

---

## Snapshot Artifacts

Every collector run produces a snapshot artifact stored in S3:

```
s3://praxis-copilot/data/
├── monitors/{monitor_id}/
│   ├── 2025-01-15.yaml
│   ├── 2025-01-16.yaml
│   ├── ...
│   └── latest.yaml          (symlink/copy of most recent)
```

### Snapshot Format

```yaml
monitor_id: tsmc-cowos-capacity
date: 2025-04-17
source: "TSM Q1 2025 earnings transcript"
status: updated  # or "unchanged"
current_state: |
  CoWoS capacity target revised to 2.5x by end 2025 (previously 2x).
  Additional $2B capex allocated. New facility in Kumamoto contributing
  H2 2025. Management noted "demand continues to exceed supply through
  2026." No specific customer allocation breakdown disclosed, but noted
  "different product mix" suggesting H200 packaging taking larger share.
delta_from_previous: |
  Capacity target: 2x → 2.5x
  Capex: +$2B incremental
  New: Kumamoto facility timeline disclosed
  Tone shift: more bullish on demand duration
significance: high
```

- `status: unchanged` — collector ran but no new relevant data. Evaluator skips.
- `status: updated` — new data extracted. Evaluator assesses.
- `significance` — collector's initial assessment (`low`, `medium`, `high`). Evaluator uses for alerting threshold.
- `current_state` — full current picture, not just the delta. This is what the evaluator and human read.
- `delta_from_previous` — what changed since last `updated` snapshot. Structured enough for mechanical parsing, detailed enough for human reading.

---

## Evaluator

Runs after collectors complete. Also a Sonnet API call — looks at the day's snapshots across all monitors and produces a daily digest.

### Evaluator Logic

1. Filter to snapshots with `status: updated`
2. For each, assess: is this delta material to the theses of tickers in `relevant_to`?
3. Cross-reference deltas — multiple monitors changing in the same direction may compound significance
4. **Staleness check**: for each significant delta, check affected tickers' `memo.yaml` dependencies — is the memo now stale? Include staleness warnings in digest.
5. Produce daily digest with alert recommendations + staleness report

### Alerting

If significance meets threshold → publish to SNS. Notification includes:

- Which monitor(s) triggered
- The delta summary
- Which tickers are affected (`relevant_to`)
- Link/reference to full snapshot

Human receives email/SMS, decides whether to act.

---

## Escalation to Claude Code (Re-Analysis)

When a human receives a monitor alert and decides to act:

1. Human initiates a Claude Code session targeting affected ticker(s)
2. Session receives context:
   - The monitor delta(s) that triggered the alert
   - Current memo and its input hashes (what it was written against)
   - Which inputs have changed since the memo was written (staleness report)
   - Current macro view
   - Current monitor registry
3. Session is **scoped** — not a full re-run of all specialist agents. Typically the relevant specialist + investment-decision-maker to revise the memo.
4. Session may produce:
   - Updated `memo.md` with new version hashes
   - Updated specialist reports
   - Amended `draft_monitors.yaml` (new monitors or amendments to existing)
5. Human reviews, syncs artifacts to S3, commits monitor changes to repo

This is the same machinery as the initial research session, just with narrower scope and richer context.

---

## Staleness Propagation

When a monitor produces a significant delta:

1. System checks `relevant_to` → identifies affected tickers
2. For each ticker, reads `memo.yaml` → checks `dependencies.monitor_snapshots.{monitor_id}` → was the memo written against an older snapshot?
3. If yes, marks the memo as stale: "NVDA memo written against tsmc-cowos-capacity snapshot 2025-04-15, current significant delta on 2025-04-17"
4. Staleness is a **monitoring signal**, not an auto-update. Human triages what to re-analyze.

Cascade: updating an NVDA memo may produce amended monitors, which may affect other tickers. Each step is human-reviewed — no automatic propagation chains.

See [research-session.md](../analyze/research-session.md) for `memo.yaml` schema and dependency tracking.

---

## Draft Monitors: Creation and Amendment

Research sessions produce `draft_monitors.yaml` which, when committed by the human, modifies `config/monitors/`. The investment-decision-maker agent receives the current monitor registry as input context and can output two kinds of entries:

### New Monitors

```yaml
new:
  - id: nvda-dc-revenue-mix
    relevant_to: [NVDA]
    trigger: event
    listen: [NVDA:transcripts, NVDA:filings]
    extract: |
      Data center revenue, % of total revenue, QoQ and YoY growth.
      Customer concentration — any single hyperscaler >30% of DC revenue.
      Inference vs training revenue split if disclosed. Geographic mix
      of DC revenue for export control exposure assessment.
```

### Amendments to Existing Monitors

```yaml
amend:
  - id: tsmc-cowos-capacity
    add_relevant_to: [NVDA]
    append_extract: |
      Also note any commentary on allocation between NVDA (H100/H200/B100)
      and other customers. Lead time commentary specific to NVIDIA different
      from general CoWoS lead times.
```

When a second research session (e.g., AMD) identifies the same underlying datapoint, the agent sees the existing monitor and outputs an amendment rather than a duplicate:

```yaml
amend:
  - id: tsmc-cowos-capacity
    add_relevant_to: [AMD]
    append_extract: |
      MI300 packaging demand relative to H100/H200 for capacity share
      implications. Any AMD-specific lead time or allocation commentary.
```

The `extract` prompt grows richer over time as more research sessions contribute context. The monitor becomes more useful, not more numerous.

If the agent is unsure whether a match exists, it can propose a new monitor and flag: "may overlap with existing `tsmc-cowos-capacity` — human should reconcile."

---

## Compute Model

| Component | Compute | Cost | Frequency |
| --- | --- | --- | --- |
| Scraper collector | Lambda | Near-zero | On trigger / periodic |
| Agent collector | Lambda + Sonnet API | Low-moderate | On trigger / periodic |
| Evaluator | Lambda + Sonnet API | Low | Daily |
| Re-analysis (escalation) | Local Claude Code | High | Human-initiated, as needed |

Daily automated cost is dominated by agent collectors on periodic monitors. Event-triggered monitors are cheap — they only fire when data arrives. Scraper optimization (post-MVP) further reduces cost for monitors with stable structured sources.

---

## S3 Layout

```
s3://praxis-copilot/data/
├── monitors/
│   ├── {monitor_id}/
│   │   ├── 2025-01-15.yaml      (daily snapshot)
│   │   ├── 2025-01-16.yaml
│   │   └── latest.yaml
│   └── _evaluator/
│       ├── 2025-01-15.yaml      (daily digest)
│       └── latest.yaml
```

## Repo Layout

```
config/
├── monitors/
│   ├── tsmc-cowos-capacity.yaml
│   ├── hyperscaler-capex.yaml
│   ├── nvda-dc-revenue-mix.yaml
│   └── ...
├── universe.yaml
└── ...
```

---

## Open Questions

- How periodic agent monitors handle rate limiting / cost caps (daily Sonnet budget across all monitors)
- Scraper generation workflow details (post-MVP)
- Evaluator cross-monitor correlation logic — how sophisticated should compound significance detection be?
- Monitor retirement / archival policy (when a position is closed, what happens to its monitors?)
- ~~Event stage → monitor routing~~ — resolved, see [event-dispatch.md](../events/event-dispatch.md)
