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

## Claude CLI subprocess safety — CRITICAL
When spawning Claude CLI as a subprocess (e.g. `claude -p`), you **MUST** strip all API key environment variables (`ANTHROPIC_API_KEY`, `CLAUDE_API_KEY`) from the child process environment. This forces the CLI to use the Max subscription instead of API billing. Failure to do this has previously caused hundreds of dollars in unexpected API charges.

- In Python: `env.pop("ANTHROPIC_API_KEY", None)` and `env.pop("CLAUDE_API_KEY", None)` before `subprocess.run(..., env=env)`.
- In shell scripts: `unset ANTHROPIC_API_KEY CLAUDE_API_KEY` at the top of the script.
- **Never** pass `ANTHROPIC_API_KEY` to a Claude CLI subprocess. If the CLI can't authenticate via Max, it should fail loudly — that's the correct behavior.
- This does NOT apply to LiteLLM calls in the monitoring pipeline (`src/modules/common/llm.py`, filing analyzers), which intentionally use the API for Lambda-deployed workloads.

## Price data
Use the `get_price(ticker)` MCP tool for current/delayed stock prices. Do NOT web search for prices.
