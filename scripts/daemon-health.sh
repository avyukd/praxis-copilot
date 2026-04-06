#!/bin/bash
# Health check script for praxis daemons.
# Re-enables and bootstraps any daemons that launchd has disabled.
# Run manually or via cron after reboot.

set -euo pipefail

GUI_DOMAIN="gui/$(id -u)"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"

DAEMONS=(
    "com.praxis.scanner"
    "com.praxis.filing-research"
    "com.praxis.analyst"
    "com.praxis.research-queue"
)

for daemon in "${DAEMONS[@]}"; do
    plist="$LAUNCH_AGENTS/$daemon.plist"

    if [ ! -f "$plist" ]; then
        echo "WARN: $daemon — plist not found at $plist"
        continue
    fi

    # Check if disabled
    disabled=$(launchctl print-disabled "$GUI_DOMAIN" 2>/dev/null | grep "\"$daemon\"" | grep -c "disabled" || true)
    if [ "$disabled" -gt 0 ]; then
        echo "FIXING: $daemon is disabled — re-enabling..."
        launchctl enable "$GUI_DOMAIN/$daemon"
    fi

    # Check if loaded
    loaded=$(launchctl list 2>/dev/null | grep -c "$daemon" || true)
    if [ "$loaded" -eq 0 ]; then
        echo "FIXING: $daemon not loaded — bootstrapping..."
        launchctl bootstrap "$GUI_DOMAIN" "$plist" 2>/dev/null || true
    fi

    # Check status
    info=$(launchctl list 2>/dev/null | grep "$daemon" || echo "NOT RUNNING")
    pid=$(echo "$info" | awk '{print $1}')
    exit_code=$(echo "$info" | awk '{print $2}')

    if [ "$pid" = "-" ] || [ "$pid" = "NOT" ]; then
        echo "DOWN: $daemon (exit=$exit_code)"
    else
        echo "  OK: $daemon (pid=$pid)"
    fi
done
