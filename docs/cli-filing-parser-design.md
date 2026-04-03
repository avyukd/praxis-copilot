# CLI-Based Filing/PR Parser — Design Document

## Problem

The current pipeline uses Lambda + Sonnet API for filing/PR analysis, costing ~$0.016 per filing. With 50-100 filings per day, this is $25-50/month in API costs. The user has a Claude Max subscription that provides unlimited CLI usage.

## Solution

Replace the Sonnet API analysis stage with Claude CLI (`claude -p`) running locally. The rest of the pipeline (polling, extraction, S3 storage, alerting) remains on AWS Lambda.

## Architecture

```
CURRENT:
  Lambda Poller → S3 → Lambda Extractor → S3 → Lambda Analyzer (Sonnet API) → S3 → Lambda Alert → SNS

PROPOSED (hybrid):
  Lambda Poller → S3 → Lambda Extractor → S3 → Local CLI Analyzer (Claude Max) → S3 → Lambda Alert → SNS
                                                      ↑
                                         runs on user's always-on laptop
                                         polls S3 for extracted.json without analysis.json
```

## What Changes

| Component | Current | Proposed |
|-----------|---------|----------|
| Polling | Lambda (EventBridge cron) | Lambda (unchanged) |
| Extraction | Lambda (S3 trigger) | Lambda (unchanged) |
| Analysis | Lambda + Sonnet API ($) | Local CLI + Max subscription (free) |
| Alerting | Lambda (S3 trigger) | Lambda (unchanged, triggered by analysis.json upload) |
| Storage | S3 | S3 (unchanged) |

## Implementation: `src/cli/local_scanner.py`

### `praxis scanner scan`
- Polls S3 for items with `extracted.json` but no `analysis.json`
- Runs Claude CLI analysis locally (same prompt as Lambda analyzer)
- Uploads `analysis.json` to S3 (same format — Lambda alert trigger works unchanged)
- Optional prescreen stage (replaces Haiku)

### `praxis scanner analyze <s3-path>`
- Analyze a specific filing manually

### Integration with Filing Research Daemon
The scanner can run as a pre-stage before the filing research daemon:
1. Scanner runs at 8:00 AM ET — analyzes any unprocessed filings from overnight
2. Filing research daemon runs at 8:25 AM — picks up the now-analyzed filings

Or: scanner runs continuously as a launchd daemon, processing filings as they arrive.

## Migration Path

1. **Phase 1 (current)**: Lambda does everything. CLI scanner available for ad-hoc use.
2. **Phase 2**: Disable Lambda analyzer. CLI scanner runs on schedule, uploads analysis.json to S3. Lambda alerts still trigger on analysis.json upload.
3. **Phase 3**: Move polling/extraction to local CLI too (full local pipeline).

## Cost Savings

- Eliminates Sonnet API costs (~$25-50/month)
- Uses Max subscription that's already paid for
- No additional infrastructure needed (laptop is always on)

## Risks

- Laptop must be on and connected for analysis to run
- Claude Max may have rate limits on concurrent sessions
- Network latency for S3 reads/writes (mitigated by batch processing)

## Key Design Decisions

1. **Same S3 paths and analysis.json format** — Lambda alerts don't need changes
2. **`analyzer: "cli"` field** — distinguishes CLI-analyzed from API-analyzed
3. **Prescreen is optional** — Claude Max doesn't charge per-token, so prescreen saves time but not money
4. **Parallel analysis** — ThreadPoolExecutor, default 4 concurrent
