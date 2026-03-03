# Architecture

## Data Plane (Deterministic)

Responsible for:

- Ingesting raw data
- Normalizing it
- Emitting events
- Producing structured artifacts

The Data Plane does NOT make portfolio decisions, autonomously update memos, trigger heavy LLM reasoning, or apply subjective interpretation. It produces facts, diffs, and signals.

## Control Plane (Policy + Reasoning)

Responsible for:

- Ranking events
- Managing alerting
- Enforcing LLM budget
- Triggering deep dives
- Producing action queues
- Proposing (never auto-committing) state updates

The Control Plane consumes structured outputs from the Data Plane. It does NOT re-parse filings, compute indicators, or store raw truth.

## AWS Stack

| Service      | Role                                                    |
| ------------ | ------------------------------------------------------- |
| EventBridge  | Cron scheduling for pollers and periodic jobs           |
| Lambda       | Stateless compute for each pipeline step                |
| S3           | Single bucket for config + data                         |
| SNS          | Alert delivery (email/SMS)                              |

## S3 Bucket Layout

Single bucket `s3://praxis-copilot/` with two top-level prefixes:

```
s3://praxis-copilot/
├── config/                          (deployed from repo via CLI)
│   ├── monitors/
│   ├── universe.yaml
│   ├── ticker_registry.yaml
│   ├── manage.yaml
│   ├── news.yaml
│   └── 8k_scanner.yaml
├── data/
│   ├── raw/8k/{cik}/{accession}/    (8k-scanner)
│   ├── raw/ca-pr/{ticker}/{release_id}/
│   ├── raw/us-pr/{ticker}/{release_id}/
│   ├── research/{ticker}/           (research artifacts + ingested data)
│   ├── monitors/{monitor_id}/       (daily snapshots)
│   ├── market/{ticker}/             (OHLCV, technicals, assessments)
│   ├── news/{date}/                 (SERP results + digest)
│   ├── events/{date}/               (dispatch audit records)
│   ├── context/macro/               (macro workspace)
│   └── state/                       (poller cursors, ingestion status)
```

Config is the source of truth in the repo. S3 `config/` is a deployment target — pushed via `praxis config sync`. Lambdas read config from S3 at runtime.

## Config Deployment Model

```
repo config/ → git commit → praxis config sync → S3 config/
                                                    ↓
                                              Lambdas read at runtime
```

- Repo is source of truth. S3 is a read-only deployment target.
- `praxis config sync` pushes all config files to S3.
- If S3 diverges from repo, repo wins — re-run sync.
- Lambdas read config from S3 on each invocation (config is small, no caching needed).

## CLI → Cloud Bridge

The CLI handles both config management and imperative actions:

| Command | Local effect | Cloud effect |
| --- | --- | --- |
| `praxis config sync` | None | Pushes repo `config/` to S3 `config/` |
| `praxis universe add TICKER` | Updates universe.yaml + ticker_registry.yaml, commits | Syncs config to S3, invokes ingestion Lambda |
| `praxis analyze TICKER` | Starts local Claude Code session | Reads data from S3 |
| `praxis sync TICKER` | None | Pushes local research artifacts to S3 `data/research/{ticker}/` |

CLI calls AWS APIs directly (Lambda invoke, S3 put) — no CI/CD pipeline needed.

## Module Pipeline

Each module plugs into the **event → analyze → monitor → manage** pipeline:

1. **Event** — A scheduled or triggered Lambda ingests external data, stores raw artifacts to S3
2. **Analyze** — Two modes depending on context:
   - *Event-stage analysis:* S3 PUT trigger fires extraction/analysis Lambdas (lightweight, reactive)
   - *Research sessions:* Human-initiated deep analysis via Claude Code on local compute (heavy, proactive). See [modules/analyze/research-session.md](modules/analyze/research-session.md)
3. **Monitor** — Collectors track datapoints, evaluator assesses deltas, surfaces alerts
4. **Manage** — Price/volume monitoring with thesis-aware assessment

Modules are independent and share only the S3 bucket and SNS topic.

## Compute Model

Not all stages run on AWS. The analyze stage's research sessions run locally via Claude Code — the reasoning is too heavy and interactive for Lambda. Event-stage processing remains serverless.

| Stage | Compute | Notes |
| --- | --- | --- |
| Event (8k-scanner) | Lambda | Cron-triggered, 1-min during market hours |
| Event (news scanner) | Lambda | Hourly SERP sweep + agent triage on change |
| Event (dispatch) | Lambda | S3 trigger, routes events to monitors |
| Analyze (event) | Lambda | Lightweight LLM screening |
| Analyze (research) | Local (Claude Code) | Human-initiated, multi-agent |
| Monitor (collectors) | Lambda + Sonnet API | Event-triggered or periodic |
| Monitor (evaluator) | Lambda + Sonnet API | Pre-market, cross-monitor assessment |
| Manage (intraday) | Lambda | Deterministic threshold checks, no LLM |
| Manage (EOD assessment) | Lambda + Sonnet API | After close, contextual assessment |
| Manage (delivery) | SNS | Alert delivery to human |

## Daily Cadence

No orchestration framework — simple EventBridge crons. Ordering managed by schedule offsets.

**Hourly (market hours):**
- News SERP sweep → hash-based dedup → agent triage only on new content

**Pre-market (~7am ET):**
- Periodic monitor collectors
- Monitor evaluator (reads overnight collector outputs)

**Market hours (continuous):**
- 8k-scanner polling (1-min intervals)
- Event-triggered monitor collectors (fire on data arrival via dispatch)
- Intraday manage: price/volume threshold checks (deterministic, no LLM). Raw signal alerts via SNS.

**After close (~5pm ET):**
- Market data ingestion (full day OHLCV)
- Technicals computation
- EOD manage assessment: Sonnet call with full context (day's breaches + news digest + monitor snapshots)
- Aggregate EOD notification via SNS

## Event Dispatch

Generalized routing layer. Triggers on S3 PUTs from ANY event-stage module — not just 8k-scanner. Routes events to subscribed monitors based on `listen` fields. See [modules/events/event-dispatch.md](modules/events/event-dispatch.md).

Dispatch sources:
- 8k-scanner: `analysis.json` PUT → dispatch resolves CIK→ticker, matches monitors
- News scanner: `digest.yaml` PUT → dispatch matches `{ticker}:news` monitors
- Future modules: same pattern — produce artifact, dispatch routes

## Artifact Versioning & Staleness

Every `memo.yaml` records hashes/timestamps of its inputs (macro view version, data vintage, monitor snapshot dates). When inputs change, the system can mechanically determine staleness by comparing recorded dependencies against current state. Staleness is a monitoring signal surfaced to the human, not an auto-update trigger.

The **staleness checker** runs as part of the monitor evaluator's pre-market pass. After assessing monitor deltas, it checks: do any significant deltas affect tickers whose `memo.yaml` records older snapshots? If yes, includes staleness warnings in the evaluator digest and SNS notifications.

See [modules/analyze/research-session.md](modules/analyze/research-session.md) and [modules/monitor/monitors.md](modules/monitor/monitors.md).

## Config / Data / Control Separation

| Layer | Location | Contents |
| --- | --- | --- |
| **Config** | Repo (`config/`) → deployed to S3 (`config/`) | Monitor definitions, universe, thresholds, policy. Human-reviewed, git-versioned. |
| **Data** | S3 (`data/`) | Raw filings, snapshots, memos, specialist reports. System-produced artifacts. |
| **Control** | Lambda + local | Collector/evaluator logic (Lambda), deep analysis (local Claude Code). |

Config is never auto-modified. Data is produced by the system. Control reads config and produces data.
