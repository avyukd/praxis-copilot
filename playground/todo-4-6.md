# TODO 2026-04-06

- [x] Fix NEUTRAL alerts with mag >= 0.5 being emailed — scanner alert filter now skips SELL/HOLD/NEUTRAL
- [x] Add `praxis scanner show <TICKER>` command — pulls and prints analysis.json from S3, shows prescreen info for NEGATIVE items
- [x] BUG: scan_unanalyzed() (backfill path) now sends alerts matching run_full_pipeline behavior
- [x] Add discovered_at timestamps to skipped filings section in HTML report
- [x] Show source type in skipped filings section (Canadian PR, US PR, 8-K, etc.)
- [x] Show prescreen NEGATIVE filings distinctly — _evaluate_filing now checks screening.json and sets SKIP_SCREENED with reason "prescreened NEGATIVE by Haiku". CSS highlights pending vs screened rows.
- [x] Robust to computer restarts — added ThrottleInterval + ExitTimeOut to all 4 plists, created scripts/daemon-health.sh to re-enable disabled daemons
- [x] Fix yfinance multi-level columns breaking price charts
- [x] Fix yfinance thread-safety — serialized parallel downloads that returned corrupted data
- [x] Daemon report regen now logs errors properly (logger.error + exc_info) and falls back to skip_charts=True if price fetch fails
- [ ] INVESTIGATE: filing-research daemon sync_research() logged "Synced N file(s)" for last week's runs but artifacts weren't on S3. Today's ONMD synced fine via daemon. Monitor new completions.
