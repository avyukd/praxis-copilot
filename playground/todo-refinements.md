# Refinements TODO

## Reliability / Gaps Found

1. **Filing research should re-check "not_analyzed" items periodically**
   - Currently: items marked `skip_not_analyzed` are permanent — daemon never re-checks
   - Fix: every 3rd poll, re-evaluate `skip_not_analyzed` items to see if `analysis.json` appeared since
   - Why: scanner backfill creates analysis.json after the daemon already skipped the item

2. **CapacityTracker stale state across days**
   - Fixed the backoff reset, but should also reset `base_batch_size` at start of each trading day
   - Consider: separate capacity windows by date so yesterday's usage doesn't affect today

3. **Scanner should prioritize backfill when returning from throttle**
   - Currently: does one backfill pass per cycle, might get re-throttled
   - Fix: when coming back from throttle, run backfill FIRST before new polling
   - Why: missed items are higher priority than new discoveries

4. **SNS_TOPIC_ARN not in background processes**
   - Background processes (analyst, queue) need SNS env manually set
   - Fix: read from `.env` or a config file instead of relying on env vars in launchd plists
   - Consider: a shared env loader that all daemons use at startup

5. **Coordinator log missing for pre-coordinator memos**
   - 71 existing memos don't have `coordinator_log.md`
   - Not urgent, but `praxis audit` shows nothing for them

## Capacity Management

6. **Calibrate the 300K default budget**
   - We've never hit an actual Claude rate limit yet, so the budget is still a guess
   - Intentionally probe the limit once (maybe Saturday when nobody cares) to calibrate
   - Once calibrated, all capacity estimates become accurate

7. **Per-daemon capacity budgets**
   - Currently all daemons share one global capacity number
   - Could allocate: scanner 40%, filing research 40%, analyst 10%, queue 10%
   - Prevents one daemon from starving the others

8. **Smarter scanner scheduling based on market hours**
   - 5 AM PT: nothing happening, don't poll aggressively
   - 6:30-9:30 AM ET (pre-market + open): poll every 5 min
   - 9:30 AM - 4 PM ET: poll every 10 min
   - 4-8 PM ET: after-hours, poll every 15 min
   - Market-aware cadence saves capacity during quiet periods

## Email / Notifications

9. **Consolidate email subjects for easier filtering**
   - Currently: [PRAXIS BUY], [PRAXIS ENTRY], [PRAXIS EXIT], [PRAXIS THESIS], [PRAXIS URGENT], [PRAXIS DIGEST], [PRAXIS BRIEFING], [PRAXIS DESKTOP]
   - Good variety but maybe add priority indicators: ★ for high urgency
   - Gmail filters should be documented

10. **Morning briefing should include "missed overnight" section**
    - If scanner was throttled and backfilled, briefing should mention what was recovered
    - "3 items were delayed due to capacity throttling and have been recovered"

11. **Digest should include IPC Desktop findings**
    - Desktop findings go to inbox but aren't in the daily digest yet
    - The analyst adds them to digest_findings but only for urgent ones

## Research Quality

12. **Track coordinator early exit rate**
    - How often does the coordinator early-exit vs full research?
    - If exit rate is >50%, the quick screen is too aggressive
    - If <5%, it's not saving any tokens
    - Add to telemetry: `praxis usage` shows coordinator stats

13. **Memo freshness indicator**
    - Some memos are weeks old — thesis may be stale
    - Entry watchdog alerts on stale memos could include a warning
    - "⚠ Memo is 21 days old — consider re-research"

14. **Research session timeout handling**
    - If a research session times out (45 min), it leaves a partial workspace
    - Should clean up and mark as failed more clearly

## IPC / Desktop Integration

15. **Desktop task timeout**
    - If Desktop doesn't pick up a task in 30 min, mark it expired
    - Prevents stale tasks from piling up

16. **Desktop finding dedup**
    - If Desktop writes the same finding twice (e.g., same tweet), dedup
    - Hash-based dedup on content[:100] + ticker

17. **Desktop task templates**
    - Pre-built task templates for common operations:
      - "scan Twitter for $TICKER" → fills URL, description automatically
      - "check SEDAR for TICKER.V" → knows to use SEDAR URL
    - Available via `praxis desktop scan TICKER`

## Infrastructure

18. **Workspace cleanup**
    - 71+ workspaces eating disk
    - `praxis workspace clean --older-than 7d` — remove stale workspaces already synced to S3
    - Keep portfolio position workspaces regardless of age

19. **Log rotation**
    - logs/ directory growing unbounded
    - Add logrotate or truncate logs >10MB

20. **Git ignore data files properly**
    - `data/` is gitignored but some state files should be tracked
    - Consider: track config files, ignore runtime state
