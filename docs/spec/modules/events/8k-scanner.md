# 8K / PR Scanner Module

First MVP module. Implements the **event → analyze** stages of the pipeline for SEC 8-K filings and press releases.

---

## Data Plane (Deterministic)

### Poller Lambda

- **Trigger:** EventBridge cron `cron(* 11-14 ? * MON-FRI *)` (6am–10am ET, weekdays), 1-min interval
- **Action:** Poll EDGAR EFTS for new 8-K filings
- **Filters:** Reads `config/ticker_registry.yaml` from S3. Includes all tickers (universe + external). Market cap threshold from `config/8k_scanner.yaml`.
- **Output:** Fetch raw docs, store to S3 at `data/raw/8k/{cik}/{accession}/index.json` + primary document
- Same pattern for CA-PR and US-PR pipelines (separate pollers)

### Extractor Lambda

- **Trigger:** S3 PUT on `index.json`
- **Action:** HTML parse, item split
- **Output:** Write `extracted.json` back to S3 alongside source docs

---

## Control Plane (Reasoning + Policy)

### Analyzer Lambda

- **Trigger:** S3 PUT on `extracted.json`
- **Action:**
  1. Financial snapshot enrichment
  2. LLM two-stage screening: classify relevance → full analysis if actionable
- **Output:** Write `analysis.json` back to S3
- **Downstream:** `analysis.json` PUT triggers [event dispatch](event-dispatch.md), which routes to subscribed monitors

### Alert Decision

- If magnitude ≥ threshold → publish to SNS topic
- Human receives email/SMS, optionally acts

---

## S3 Layout

```
s3://praxis-copilot/
├── data/
│   ├── raw/8k/{cik}/{accession}/
│   │   ├── index.json
│   │   ├── primary.htm
│   │   ├── extracted.json    (Data Plane output)
│   │   └── analysis.json     (Control Plane output)
│   ├── raw/ca-pr/{ticker}/{release_id}/
│   ├── raw/us-pr/{ticker}/{release_id}/
│   └── state/
│       ├── poller_last_seen.json
│       ├── ca_poller_last_seen.json
│       └── us_gnw_poller_last_seen.json
```

---

## Config

```yaml
# config/8k_scanner.yaml
scheduling_window: "cron(* 11-14 ? * MON-FRI *)"
market_cap_us: 2000000000
market_cap_ca: 500000000
item_filters: both  # long / short / both
magnitude_threshold: 0.5
llm_model: ""  # configurable
token_budget: 0  # configurable
sns_topic_arn: ""  # configurable
```

Config lives in repo, deployed to S3 via `praxis config sync`. Universe/ticker filtering uses `config/ticker_registry.yaml`.

---

## Migration from ~/dev/8k-scanner

- Split `analyzer_handler.py` into two Lambdas: **Extractor** (deterministic HTML parse + item split) and **Analyzer** (LLM screening + enrichment)
- Keep all existing modules: `edgar/`, `extract/`, `analyze/`, `storage/`, `alerts`, `financials`, `universe`
- Adopt into praxis-copilot repo under `src/modules/events/8k_scanner/`
