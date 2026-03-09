"""Generate CLAUDE.md prompt for quant signal workspaces."""
from __future__ import annotations

from datetime import date


def generate_quant_prompt(
    ticker: str,
    data_summary: dict[str, int],
) -> str:
    """Generate CLAUDE.md for a quant workspace."""
    today = date.today().isoformat()

    # Build data manifest
    manifest_lines = []
    for fname, rows in sorted(data_summary.items()):
        manifest_lines.append(f"- `data/{fname}` — {rows:,} rows")
    data_manifest = "\n".join(manifest_lines) if manifest_lines else "- (no data files)"

    return f"""# Quant Signal Workspace — {ticker}

Generated: {today}

---

## Your Role

You are a quantitative analyst exploring trading signals for **{ticker}**, a small/micro-cap
stock where technical patterns are more likely to persist due to lower algorithmic coverage.

Your job: discover, backtest, and validate signals that identify high-probability entry points.

---

## Available Data

{data_manifest}

**CSV schema:** `date,open,high,low,close,volume` (date is the index column)

All prices are adjusted. Volume is share count.

---

## Signal Interface Contract

Every signal MUST follow this exact interface:

```python
import pandas as pd
import numpy as np

def signal(df: pd.DataFrame) -> pd.Series:
    \"\"\"
    Args:
        df: OHLCV DataFrame with columns [open, high, low, close, volume]
            and a DatetimeIndex.
    Returns:
        Boolean Series (same index as df). True = signal fires on that bar.
    \"\"\"
    # Compute indicators from raw OHLCV — no pre-computed features
    # Return boolean Series
```

**Rules:**
- Input is raw OHLCV. Compute everything internally (moving averages, ratios, etc.)
- Only import pandas and numpy — nothing else
- Return a boolean Series with the same index as the input
- The function must be identical in backtest and live execution
- No lookahead bias — only use data available at or before each bar

---

## Signal Config (YAML)

Each signal `.py` file has a matching `.yaml`:

```yaml
bar_size: "1d"          # Which data file to use: 1d or 1m
lookback: 50            # Bars to skip at start (for indicator warmup)
hold_periods: [1, 5, 10, 20]  # Forward return windows to test
ticker: "{ticker}"
description: "Brief description of what this signal detects"
```

---

## Backtest Harness

Use the provided harness:

```python
import sys
sys.path.insert(0, ".")
from lib.backtest import load_signal, run_backtest, format_report
from pathlib import Path

# Option 1: Load from signals/ directory
signal_fn, config = load_signal(Path("signals"), "my_signal_id")
result = run_backtest(signal_fn, config, Path("data"), ticker="{ticker}")
print(format_report(result))

# Option 2: Test inline signal
from lib.backtest import SignalConfig
def my_signal(df):
    vol_ma = df["volume"].rolling(20).mean()
    return df["volume"] > vol_ma * 3

config = SignalConfig(signal_id="test", bar_size="1d", lookback=20, ticker="{ticker}")
result = run_backtest(my_signal, config, Path("data"), ticker="{ticker}")
print(format_report(result))
```

---

## Strategy Philosophy

**What works in small/micro-caps:**
- **Volume anomalies** — Unusual volume often precedes price moves. Institutional accumulation
  shows up as sustained above-average volume without proportional price movement.
- **Accumulation/distribution** — Money flow analysis. Are large players building positions?
- **Liquidity events** — Volume dry-ups followed by expansion. Breakouts from consolidation.
- **Relative volume** — Volume relative to recent history matters more than absolute levels.
- **Price-volume divergence** — Price making new lows on declining volume (selling exhaustion).

**What to be skeptical of:**
- Classical chart patterns (head & shoulders, cup & handle) — too subjective, low reproducibility
- Single-indicator signals (RSI oversold alone) — too simple, heavily arbitraged
- Mean reversion on small caps — these stocks trend; mean reversion works better on large caps

---

## Workflow

1. **Explore the data** — Load CSVs, compute basic stats. Key questions:
   - What's the overall trend? (trending stocks make any long-biased signal look good)
   - What's the volume distribution? (median vs mean volume, spike frequency)
   - Any regime changes? (different behavior in different periods)

2. **Establish a baseline** — Before ANY signal, compute a naive baseline:
   ```python
   # "Buy random day" baseline: what are unconditional forward returns?
   fwd_1 = df['close'].pct_change(1).shift(-1).dropna()
   fwd_5 = df['close'].pct_change(5).shift(-5).dropna()
   fwd_10 = df['close'].pct_change(10).shift(-10).dropna()
   fwd_20 = df['close'].pct_change(20).shift(-20).dropna()
   print(f"Baseline 1d: {{fwd_1.mean():.3%}} | 5d: {{fwd_5.mean():.3%}} | "
         f"10d: {{fwd_10.mean():.3%}} | 20d: {{fwd_20.mean():.3%}}")
   ```
   **A signal must beat this baseline to have value.** If the stock is up 100% in the sample,
   random entries average ~0.4%/day. Your signal needs to beat that, not just beat zero.

3. **Hypothesize** — Form a specific, falsifiable hypothesis BEFORE looking at results.

4. **Write signal** — Create `signals/{{id}}.py` and `signals/{{id}}.yaml`

5. **Backtest** — Run the backtest harness. Compare to baseline. Check beta/alpha.
   **Alpha matters more than raw returns** — if beta explains most of the return,
   the signal is just "be long a trending stock."

6. **Write report** — Save `results/{{id}}_report.md` with backtest output + your analysis.
   Include the baseline comparison and beta-adjusted assessment.

7. **Iterate** — Refine or try new hypotheses. Don't over-optimize a weak signal.

---

## Statistical Standards

| Metric | Minimum for Promotion | Notes |
|--------|----------------------|-------|
| Signal fires | >= 20 (prefer 30+) | Fewer = unreliable statistics |
| t-statistic | >= 2.0 | Below 2.0 is noise |
| Hit rate | > 50% for long signals | Must beat random |
| OOS validation | Required for promotion | 70/30 chronological split |
| Sharpe ratio | > 0.5 annualized | Below is not worth trading |

**Confidence interval on hit rate:** With N signals, 95% CI is approximately:
`hit_rate +/- 1.96 * sqrt(hit_rate * (1 - hit_rate) / N)`

If the lower bound of your CI includes 50%, your signal may not be real.

---

## Anti-Patterns (Avoid These)

1. **Overfitting** — More than 3 tunable parameters on fewer than 50 signal fires is suspicious.
   A good signal is simple and robust to parameter changes.

2. **Data snooping** — Testing 20 signals and reporting the best one. Each signal should come
   from a hypothesis, not from mining.

3. **Ignoring transaction costs** — For intraday signals, assume 0.1% round-trip cost. For
   daily, assume 0.2% round-trip (wider spreads on small caps).

4. **Cherry-picking hold periods** — If a signal only works at exactly 7 days but not 5 or 10,
   it's likely noise.

5. **Lookahead bias** — Using future data in signal computation. The signal function only
   receives data up to the current bar.

6. **Trend confound** — In a strongly trending stock, ANY long-biased dip-buying signal looks
   amazing. Always compare to the unconditional baseline. If your signal's mean return is only
   marginally better than "buy random day," the signal isn't adding value — you're just long.

7. **Survivorship bias** — Not relevant here (single-stock analysis), but be aware if comparing
   across tickers.

---

## After Analysis

When done exploring, the human will run:
- `praxis quant sync {ticker}` to upload signals/ and results/ to S3
- `praxis quant promote {{signal_id}}` to create a live monitor from a validated signal
"""
