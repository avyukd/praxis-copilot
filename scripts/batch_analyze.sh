#!/usr/bin/env bash
# Batch-stage workspaces for all universe tickers and launch Claude analysis.
#
# Usage:
#   ./scripts/batch_analyze.sh              # all tickers in universe.yaml
#   ./scripts/batch_analyze.sh NVDA TSM ZM  # specific tickers only
#   DRY_RUN=1 ./scripts/batch_analyze.sh    # print what would run, don't execute

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PRAXIS="$REPO_ROOT/.venv/bin/praxis"
UNIVERSE="$REPO_ROOT/config/universe.yaml"
WORKSPACE_DIR="$REPO_ROOT/workspace"

PROMPT="analyze {TICKER} with the provided new information, and output any missing artifacts. Do NOT repeat unnecessary analysis with agents whose information is already there"
LOGFILE="/tmp/ticker_analysis.log"

: > "$LOGFILE"  # truncate log at start
echo "==> Logging to $LOGFILE"

# ---------------------------------------------------------------------------
# Resolve ticker list
# ---------------------------------------------------------------------------
if [[ $# -gt 0 ]]; then
    TICKERS=("$@")
else
    # Parse tickers from universe.yaml (skip the "tickers:" header line)
    mapfile -t TICKERS < <(grep -E '^\s*-\s+' "$UNIVERSE" | sed 's/^\s*-\s*//')
fi

echo "==> ${#TICKERS[@]} tickers to process"

# ---------------------------------------------------------------------------
# Stage & analyze each ticker
# ---------------------------------------------------------------------------
for ticker in "${TICKERS[@]}"; do
    echo ""
    echo "--- [$ticker] staging workspace ---"

    if [[ -n "${DRY_RUN:-}" ]]; then
        echo "[dry-run] $PRAXIS stage $ticker"
        echo "[dry-run] claude -p '${PROMPT//\{TICKER\}/$ticker}' --allowedTools '*' in $WORKSPACE_DIR/$ticker"
        continue
    fi

    # Stage (creates workspace + pulls data)
    if ! $PRAXIS stage "$ticker"; then
        echo "WARNING: staging failed for $ticker, skipping"
        continue
    fi

    ticker_prompt="${PROMPT//\{TICKER\}/$ticker}"

    echo "--- [$ticker] launching claude ---"
    (cd "$WORKSPACE_DIR/$ticker" && claude -p "$ticker_prompt" --allowedTools '*') 2>&1 | tee -a "$LOGFILE" || {
        echo "WARNING: claude exited non-zero for $ticker" | tee -a "$LOGFILE"
    }

    echo "--- [$ticker] done ---" | tee -a "$LOGFILE"
done

echo ""
echo "==> batch complete"
