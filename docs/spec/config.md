# Configuration

Configuration is intentionally minimal and human-managed. Config lives in the repo under `config/`, is version-controlled via git, and deployed to S3 via `praxis config sync`. Lambdas read config from S3 at runtime.

## Deployment

```
repo config/ → git commit → praxis config sync → s3://praxis-copilot/config/
```

Repo is source of truth. S3 is a deployment target. If they diverge, re-run sync.

## Config Files

| File | Purpose |
| --- | --- |
| `config/universe.yaml` | Universe tickers, market cap thresholds, watchlist |
| `config/ticker_registry.yaml` | CIK↔ticker mapping, exchange, news queries, universe status |
| `config/monitors/` | Monitor definitions (DSL yaml files) |
| `config/8k_scanner.yaml` | 8k-scanner scheduling, thresholds, LLM config |
| `config/manage.yaml` | Price/volume thresholds, per-ticker overrides |
| `config/news.yaml` | News scanner API selection, sweep config |

## Universe & Filtering

- Market cap thresholds (US: $2B, CA: $500M)
- Item filters per strategy direction (long/short/both)
- Watchlist tickers (bypass all filters)

## Thresholds & Policy

- Magnitude threshold for alerting (default: 0.5)
- Price move thresholds (per-ticker overrides in manage.yaml)
- Volume anomaly thresholds
- Escalation triggers (auditor resignation, restatement, going concern, covenant breach, major financing, executive departure, guidance withdrawal)
- Alert aggregation window / quiet hours

## LLM

- Model selection
- Token budget per call

## Alerting

- SNS topic ARN
- Delivery channels (email, SMS)

## Universe & Analyze Pipeline

Adding a ticker to the universe is done via CLI: `praxis universe add TICKER`. The CLI adds the ticker to `config/universe.yaml` and `config/ticker_registry.yaml` (resolving CIK via EDGAR API), syncs config to S3, and invokes data ingestion. See [modules/analyze/research-session.md](modules/analyze/research-session.md).

### Ticker Registry

`config/ticker_registry.yaml` maintains bidirectional CIK↔ticker mapping, news query templates, and universe status (in-universe vs external). Populated by the CLI, consumed by event dispatch, 8k-scanner, news scanner, and research ingestion. See [modules/events/event-dispatch.md](modules/events/event-dispatch.md).

External tickers (`universe_status: external`) are tracked for monitor dependencies — they get filing ingestion and news sweeps but no research session or manage stage tracking.

### draft_monitors.yaml Lifecycle

Research sessions produce `draft_monitors.yaml` — proposed monitoring signals and thresholds. This is **data**, not config. The lifecycle:

1. Claude Code session generates `draft_monitors.yaml` as part of research artifacts
2. Human reviews and edits the draft
3. Human commits approved monitors into `config/monitors/` in repo
4. Human runs `praxis config sync` to deploy

Draft monitors are never auto-applied.

### Monitor Registry

Committed monitors live in `config/monitors/` as individual yaml files. This is the source of truth for what the system actively monitors. Git history provides full audit trail. See [modules/monitor/monitors.md](modules/monitor/monitors.md).
