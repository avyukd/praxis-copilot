# Tonight's TODO — 2026-03-31

## 1. Reliability test run
Confirm zero failures on the filing research daemon with all fixes applied (`--dangerously-skip-permissions`, CIK resolution, minimal workspace fallback, max_retries=2). Run a test cycle and verify every item that should succeed does.

## 2. Queue capacity tuning
Fix `check_capacity()` so queue items actually process during idle periods. Current logic blocks too aggressively — interactive session detection counts all claude processes as interactive.

## 3. Cache OHLC for auto-regen report
Daemon auto-regenerates the report with `skip_charts=True` which means no price charts on auto-refresh. Cache OHLC data to a local file so auto-regen includes charts without re-fetching every time.

## 4. Morning briefing summary
Add a synthesis paragraph at the top of the HTML report — top 3-5 opportunities in a quick scannable block. Should be readable in 30 seconds before market open.

## 5. Scanner filters
Add conservative filters to avoid wasting CLI capacity on noise:
- Filter out ADTV < $1,000 (basically zero liquidity)
- Filter out market cap > $1B (we're focused on small/micro-cap)
- No other filters — stay permissive for now

## 6. Proactive Analyst Agent — MAIN BUILD
Full plan: `.claude/plans/optimized-napping-lark.md`

Build the production analyst agent with:
- Portfolio awareness (34 positions, 23 watchlist)
- Tiered task scheduling (portfolio every batch, watchlist daily, rest rotates)
- CapacityTracker (rate limit detection, adaptive throttling, 80% cap)
- Email notifications (urgent immediate, daily digest at 9 PM ET)
- Improved prompts (news_queries, thesis context, monitor snapshots)
- S3 integration (monitors, filing analyses)
- Auto-create GitHub issues for research_deeper findings
- CLI commands (run, status, digest, history, portfolio, schedule)
- launchd plist

Implementation order:
1. Models + portfolio config
2. Portfolio loading + tier classification
3. Task generation with tiered logic
4. Prompt improvements
5. S3 integration
6. Notification system (urgent + digest)
7. Daemon loop with CapacityTracker
8. CLI commands + main.py wiring
9. Launchd plist
10. Test and verify
