# Praxis Copilot

## Git workflow
- Always create a new feature branch and open a PR. Never commit directly to master/main or pile onto an existing feature branch.
- When commenting on GitHub PRs, always prefix with [claude] to differentiate from human comments.

## Environment
- User's `python` is aliased to `python3.13`. The project uses Python 3.13 via `.venv/`.
- Use `.venv/bin/praxis` or `.venv/bin/python` when running project commands.

## Code style
- Prefer Pydantic models and strong typing wherever possible. Avoid passing raw dicts between functions.
- pydantic is already a project dependency.
- Always import at the module top level. No inline/dynamic imports inside functions.

## Monitoring pipeline context
The system runs an automated monitoring pipeline that may produce artifacts you encounter during research:

- **Filing monitors** — trigger on new SEC filings (8-K, 10-K, 10-Q). Reactive, event-driven via S3.
- **Search monitors** — run Tavily queries at 8:30am/12pm/3pm ET checkpoints. Uses delta detection (new URLs only), Haiku pre-filter, then Sonnet analysis. Only alerts on real changes.
- **Scraper monitors** — run custom scripts on cadence with delta detection.

Monitor snapshots are stored in S3 under `data/monitors/{monitor_id}/latest.yaml`. Filing analyses under `data/raw/filings/`. When you see analysis artifacts from these systems (e.g. in `--tactical` context), they are machine-generated summaries — treat them as inputs to your own analysis, not conclusions.

## Price data
Use the `get_price(ticker)` MCP tool for current/delayed stock prices. Do NOT web search for prices.
