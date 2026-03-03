# Portfolio Manager Module

Implements the **manage** stage of the pipeline. Consumes price/volume data, technicals, and the full context of memos and monitor snapshots to assess whether market action is consistent with investment theses. Surfaces actionable alerts via SNS.

This is the final stage — it doesn't produce deep research, it connects dots that already exist in the system's artifacts.

---

## Two Passes: Intraday + EOD

### Intraday — Deterministic, No LLM

Runs continuously during market hours. Pure threshold checks against live/delayed price data. When a threshold is breached, fires a raw signal alert via SNS immediately. No Sonnet call, no context, just "NVDA down 6% at 11:30am, 2.5x avg volume."

Fast, cheap, gives the human a heads-up in real time.

Future-proofed: the intraday pass can optionally invoke Sonnet for contextual assessment if enabled. Default off for cost control.

### EOD — Contextual Assessment via Sonnet

Runs once after market close. Receives the full day's data: all intraday breaches, closing prices, news digest, recent monitor snapshots. Makes one Sonnet call per ticker that had breaches (or one batch call for all). Produces the "what does this mean for my thesis?" assessment.

---

## Data Plane (Deterministic)

### Market Data Ingestion

**Intraday:**
- Price/volume polling via API during market hours (frequency configurable — every 5 min, 15 min, etc.)
- Enough for threshold checks, not full OHLCV

**After close:**
- Full day OHLCV for all universe tickers via API (FMP/EODHD)
- Basic technicals computed deterministically:
  - RSI (14-day)
  - Moving averages (50d, 200d)
  - Volume vs 20-day average
- Options flow — if available, structured put/call ratios, unusual activity (API source TBD)
- Store to S3 under `data/market/{ticker}/`

### Threshold Checks (Deterministic, No LLM)

Run intraday on each price poll AND after close on final data:

| Signal | Condition |
| --- | --- |
| Price move | Move from previous close > X% (configurable per ticker) |
| Volume spike | Volume > Y× 20-day average |
| RSI overbought | RSI > 70 (EOD only, needs closing data) |
| RSI oversold | RSI < 30 (EOD only) |
| Price vs thesis target | Price crosses above exit range or below entry range from memo |
| Trend cross | 50d MA crosses 200d MA (EOD only) |

Intraday checks: price move, volume spike, price vs thesis target.
EOD checks: all of the above including RSI and trend crosses.

---

## Control Plane (Sonnet API — EOD Only by Default)

After close, for each ticker that had intraday breaches or EOD threshold breaches:

### Agent Input Context

- **Day's activity**: all breaches that occurred (intraday + EOD), with timestamps
- **Current memo**: thesis summary, valuation anchors from `memo.yaml`
- **Recent monitor snapshots**: any significant deltas in the last 7 days
- **Today's news digest**: material items from news scanner
- **Technicals**: RSI, MA positioning, trend
- **Current price vs thesis anchors**: where price sits relative to the memo's valuation framework

### Agent Assessment

The agent classifies the situation:

| Classification | Meaning | Example |
| --- | --- | --- |
| **Justified, thesis-consistent** | Move explained by known information, thesis intact | Earnings beat, stock up — expected |
| **Justified, thesis-threatening** | Move explained by known information, but thesis may be impaired | Monitor flagged capacity revision, stock down — thesis risk |
| **Unjustified, potential opportunity** | Significant move with no corresponding information change | Stock down 8% on no news, monitors stable — dislocation? |
| **Unjustified, unknown risk** | Significant move that may reflect information we don't have | Stock down 8% on huge volume, no news — investigate |
| **Thesis target reached** | Price has moved to or beyond the memo's valuation target | Bought at $24, memo says fair value $28, now at $30 — revisit |
| **Entry opportunity** | Price has moved into or below the memo's attractive entry range | Memo says attractive below $25, now at $23 — flag |

### Output

SNS notification with:

- Ticker and what triggered the alert
- Classification
- Brief reasoning (2-3 sentences connecting price action to thesis/monitors)
- Suggested action: "revisit position sizing", "investigate", "no action needed", "potential entry"

---

## Valuation Anchors

The manage stage reads `memo.yaml` (not `memo.md`) for structured valuation context. This file is produced by the research session alongside the narrative memo. See [research-session.md](../analyze/research-session.md) for the full schema.

The manage stage consumes:

- `valuation.fair_value_estimate` — for price-vs-target checks
- `valuation.entry_range` / `valuation.exit_range` — for opportunity/exit alerts
- `valuation.key_assumptions` — for contextualizing moves
- `valuation.invalidation` — for thesis-threatening assessments

These are opinions, not targets — the human ultimately decides. But they let the system say "you thought $25 was cheap, it's now $30, what's changed?"

---

## Daily Run Cadence

**Intraday (market hours):**
1. Price/volume poll (every N minutes)
2. Threshold checks (deterministic)
3. On breach → immediate raw signal alert via SNS (no LLM)

**After close (~5pm ET):**
1. Full OHLCV ingestion
2. Technicals computation (RSI, MAs)
3. EOD threshold checks (all signals including RSI, trend crosses)
4. Sonnet assessment for all tickers with breaches (intraday + EOD)
5. EOD notification via SNS with contextual assessment

---

## Escalation

When a human receives a manage alert and wants to act:

- **Thesis intact, sizing question** → human decides directly (no system involvement)
- **Thesis may be impaired** → human kicks off a scoped re-analysis session (same as monitor escalation path, see [monitor spec](../monitor/monitors.md))
- **Unknown risk** → human investigates manually, may update monitors or memo

---

## Compute Model

| Component | Compute | Cost | Frequency |
| --- | --- | --- | --- |
| Intraday price poll | Lambda | Near-zero (API calls) | Every N minutes, market hours |
| Intraday threshold checks | Lambda | Near-zero | On each poll |
| Intraday alerts | SNS | Near-zero | Only on breaches |
| EOD data ingestion | Lambda | Near-zero | Daily |
| EOD technicals | Lambda | Near-zero | Daily |
| EOD assessment | Lambda + Sonnet API | Low per call | Only for tickers with breaches |

Quiet days: near-zero cost (just API polling). Active days: one Sonnet call per breaching ticker at EOD.

---

## S3 Layout

```
s3://praxis-copilot/
├── data/market/{ticker}/
│   ├── ohlcv/
│   │   ├── 2025-01-15.json
│   │   └── ...
│   ├── technicals/
│   │   ├── 2025-01-15.json     (RSI, MAs, volume ratio)
│   │   └── ...
│   └── assessments/
│       ├── 2025-01-15.yaml     (EOD contextual assessment, if triggered)
│       └── ...
```

## Config

```yaml
# config/manage.yaml
defaults:
  price_move_threshold_pct: 5
  volume_spike_multiplier: 3
  rsi_overbought: 70
  rsi_oversold: 30
  intraday_poll_interval_minutes: 15
  intraday_sonnet_enabled: false  # future: enable Sonnet for intraday assessments

overrides:
  NVDA:
    price_move_threshold_pct: 7  # more volatile, higher threshold
```

---

## Open Questions

- Options flow data provider and integration
- Whether to add factor data (momentum, value, quality scores) as additional context for assessments
- How to handle earnings days (known high-volatility, suppress noise vs heightened monitoring)
- Whether EOD assessments should feed back into monitor snapshots
- Intraday price data source (FMP delayed quotes vs real-time provider)
