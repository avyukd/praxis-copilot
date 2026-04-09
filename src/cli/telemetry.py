"""Claude CLI usage telemetry — tracks every subprocess invocation.

Records token usage, cost, timing, model, and rate limit events.
Provides capacity estimation via heartbeat probes and calibrated
window budgets. Integrates with CapacityTracker for adaptive throttling.

Key concepts:
- **Window**: Claude Max uses ~4 hour usage windows with token budgets
- **Calibration**: first rate limit hit tells us the exact window budget
- **Heartbeat**: periodic tiny calls to probe response latency as a
  proxy for how close we are to capacity
- **Budget**: estimated remaining tokens in current window, used by
  daemons to decide whether to start new work
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from cli.config_utils import find_repo_root

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

TELEMETRY_DIR = "data/telemetry"
CALIBRATION_FILE = "data/telemetry/calibration.yaml"


# ---------------------------------------------------------------------------
# Core: run Claude CLI with JSON output and record telemetry
# ---------------------------------------------------------------------------


def track_claude_call(
    cmd: list[str],
    *,
    cwd: Path | str | None = None,
    env: dict | None = None,
    timeout: int = 600,
    daemon: str = "unknown",
    task_id: str = "",
    ticker: str = "",
) -> dict:
    """Run a Claude CLI command with --output-format json and record telemetry.

    Returns the parsed JSON result dict. The text output is in result["result"].
    """
    if "--output-format" not in cmd:
        cmd = list(cmd) + ["--output-format", "json"]

    start_time = time.time()
    start_dt = datetime.now(ET)

    try:
        proc = subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True, env=env, timeout=timeout,
        )
        elapsed = time.time() - start_time

        stdout = proc.stdout.strip()
        try:
            result = json.loads(stdout)
        except json.JSONDecodeError:
            result = {
                "type": "result", "subtype": "error", "is_error": True,
                "result": stdout[:500], "duration_ms": int(elapsed * 1000), "usage": {},
            }

        stderr = proc.stderr or ""
        rate_limited = _is_rate_limit(stderr, proc.returncode)
        if rate_limited:
            result["rate_limited"] = True

        _record_invocation(
            result=result, daemon=daemon, task_id=task_id, ticker=ticker,
            start_time=start_dt, elapsed_seconds=elapsed,
            rate_limited=rate_limited, returncode=proc.returncode,
            stderr_snippet=stderr[:200] if proc.returncode != 0 else "",
        )

        # Update capacity calibration on rate limit
        if rate_limited:
            _on_rate_limit_calibration(start_dt)

        return result

    except subprocess.TimeoutExpired:
        elapsed = time.time() - start_time
        result = {
            "type": "result", "subtype": "error", "is_error": True,
            "result": f"Timed out after {timeout}s",
            "duration_ms": int(elapsed * 1000), "usage": {}, "timed_out": True,
        }
        _record_invocation(
            result=result, daemon=daemon, task_id=task_id, ticker=ticker,
            start_time=start_dt, elapsed_seconds=elapsed,
            rate_limited=False, returncode=-1, stderr_snippet="timeout",
        )
        return result


def _is_rate_limit(stderr: str, returncode: int) -> bool:
    if returncode == 0:
        return False
    indicators = ["rate limit", "429", "too many requests", "capacity", "overloaded", "throttl"]
    lower = stderr.lower()
    return any(ind in lower for ind in indicators)


# ---------------------------------------------------------------------------
# Heartbeat probe
# ---------------------------------------------------------------------------


def run_heartbeat(env: dict | None = None) -> dict:
    """Run a tiny haiku call to probe capacity. Returns timing + status.

    Use this periodically to detect approaching capacity limits.
    Response latency > 5s suggests we're getting throttled.
    """
    claude_bin = shutil.which("claude") or str(Path.home() / ".local" / "bin" / "claude")
    if env is None:
        env = os.environ.copy()
        env.pop("ANTHROPIC_API_KEY", None)
        env.pop("CLAUDE_API_KEY", None)
        env.pop("CLAUDE_CODE_ENTRYPOINT", None)
        env.pop("CLAUDECODE", None)

    result = track_claude_call(
        [claude_bin, "-p", "Reply with just OK", "--dangerously-skip-permissions", "--model", "haiku"],
        env=env, timeout=15,
        daemon="heartbeat", task_id="probe",
    )

    latency_ms = result.get("duration_api_ms", result.get("duration_ms", 0))
    rate_limited = result.get("rate_limited", False)

    probe = {
        "timestamp": datetime.now(ET).isoformat(),
        "latency_ms": latency_ms,
        "rate_limited": rate_limited,
        "healthy": not rate_limited and latency_ms < 10000,
        "throttled": latency_ms > 5000 and not rate_limited,
    }

    # Record probe
    try:
        probe_path = _telemetry_dir() / "heartbeats.jsonl"
        with open(probe_path, "a") as f:
            f.write(json.dumps(probe, default=str) + "\n")
    except Exception:
        pass

    return probe


# ---------------------------------------------------------------------------
# Capacity calibration
# ---------------------------------------------------------------------------


def _calibration_path() -> Path:
    return find_repo_root() / CALIBRATION_FILE


def _load_calibration() -> dict:
    path = _calibration_path()
    if path.exists():
        try:
            return yaml.safe_load(path.read_text()) or {}
        except Exception:
            return {}
    return {}


def _save_calibration(cal: dict) -> None:
    path = _calibration_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(cal, default_flow_style=False))


def _on_rate_limit_calibration(when: datetime) -> None:
    """Called when a rate limit is hit. Calibrates window budget.

    The total output tokens consumed in the current window up to this point
    is approximately the window's budget (100% capacity).
    """
    cal = _load_calibration()

    # Find current window's total tokens
    records = _load_daily_records()
    window_start = when - timedelta(hours=4)
    window_records = [
        r for r in records
        if r.get("timestamp", "") >= window_start.isoformat()
        and r.get("timestamp", "") <= when.isoformat()
    ]

    window_output_tokens = sum(r.get("output_tokens", 0) for r in window_records)
    window_cost = sum(r.get("total_cost_usd", 0) for r in window_records)

    # Update calibration with exponential moving average
    prev_budget = cal.get("estimated_window_budget_tokens", 0)
    if prev_budget > 0:
        # EMA with alpha=0.3 — new data weighted 30%
        cal["estimated_window_budget_tokens"] = int(0.7 * prev_budget + 0.3 * window_output_tokens)
    else:
        cal["estimated_window_budget_tokens"] = window_output_tokens

    prev_cost = cal.get("estimated_window_budget_cost", 0)
    if prev_cost > 0:
        cal["estimated_window_budget_cost"] = round(0.7 * prev_cost + 0.3 * window_cost, 4)
    else:
        cal["estimated_window_budget_cost"] = round(window_cost, 4)

    cal["last_rate_limit"] = when.isoformat()
    cal["rate_limit_count"] = cal.get("rate_limit_count", 0) + 1
    cal["calibration_history"] = cal.get("calibration_history", [])
    cal["calibration_history"].append({
        "timestamp": when.isoformat(),
        "window_output_tokens": window_output_tokens,
        "window_cost": round(window_cost, 4),
    })
    # Keep last 20 calibrations
    cal["calibration_history"] = cal["calibration_history"][-20:]

    _save_calibration(cal)
    logger.info(
        "Capacity calibrated: window budget ~%d output tokens (~$%.2f)",
        cal["estimated_window_budget_tokens"],
        cal["estimated_window_budget_cost"],
    )


def get_capacity_estimate() -> dict:
    """Get current capacity estimate for the active window.

    Returns:
        window_budget_tokens: estimated total output tokens per 4h window
        window_budget_cost: estimated total cost per window
        current_window_used_tokens: output tokens used in current window
        current_window_used_cost: cost used in current window
        estimated_pct: estimated % of window budget used
        remaining_budget_tokens: estimated remaining output tokens
        remaining_budget_cost: estimated remaining cost
        calibrated: whether we have rate-limit calibration data
    """
    cal = _load_calibration()
    records = _load_daily_records()

    # Default budget estimates (conservative, pre-calibration)
    window_budget_tokens = cal.get("estimated_window_budget_tokens", 300_000)
    window_budget_cost = cal.get("estimated_window_budget_cost", 30.0)
    calibrated = "last_rate_limit" in cal

    # Adaptive budget expansion: if no rate limits in last 8h, bump by 10%
    last_rl = cal.get("last_rate_limit")
    if last_rl:
        try:
            last_rl_dt = datetime.fromisoformat(last_rl)
            hours_since = (datetime.now(ET) - last_rl_dt).total_seconds() / 3600
            if hours_since > 8:
                # No rate limits in 8h — we can afford more
                max_budget = 600_000  # Cap at 600K (2x default)
                new_budget = min(int(window_budget_tokens * 1.1), max_budget)
                if new_budget > window_budget_tokens:
                    cal["estimated_window_budget_tokens"] = new_budget
                    cal["estimated_window_budget_cost"] = round(window_budget_cost * 1.1, 2)
                    window_budget_tokens = new_budget
                    window_budget_cost = cal["estimated_window_budget_cost"]
                    _save_calibration(cal)
        except Exception:
            pass
    elif not calibrated:
        # Never hit a rate limit — gradually expand from default
        expansion_count = cal.get("expansion_count", 0)
        if expansion_count < 6:  # Max 6 expansions (300K → ~530K)
            max_budget = 600_000
            new_budget = min(int(window_budget_tokens * 1.1), max_budget)
            cal["estimated_window_budget_tokens"] = new_budget
            cal["expansion_count"] = expansion_count + 1
            cal["last_expansion"] = datetime.now(ET).isoformat()
            window_budget_tokens = new_budget
            _save_calibration(cal)

    # Current window usage
    now = datetime.now(ET)
    window_start = now - timedelta(hours=4)
    window_records = [
        r for r in records
        if r.get("timestamp", "") >= window_start.isoformat()
    ]

    used_tokens = sum(r.get("output_tokens", 0) for r in window_records)
    used_cost = sum(r.get("total_cost_usd", 0) for r in window_records)

    # Estimate percentage
    if window_budget_tokens > 0:
        pct = min(100, int(used_tokens / window_budget_tokens * 100))
    else:
        pct = 0

    # Check heartbeat latency for real-time signal
    heartbeat_signal = _get_latest_heartbeat_signal()

    # Adjust estimate based on heartbeat
    if heartbeat_signal.get("rate_limited"):
        pct = 100
    elif heartbeat_signal.get("throttled"):
        pct = max(pct, 85)

    remaining_tokens = max(0, window_budget_tokens - used_tokens)
    remaining_cost = max(0, window_budget_cost - used_cost)

    return {
        "window_budget_tokens": window_budget_tokens,
        "window_budget_cost": round(window_budget_cost, 2),
        "current_window_used_tokens": used_tokens,
        "current_window_used_cost": round(used_cost, 4),
        "estimated_pct": pct,
        "remaining_budget_tokens": remaining_tokens,
        "remaining_budget_cost": round(remaining_cost, 2),
        "calibrated": calibrated,
        "target_pct": 80,  # Our 80% cap
        "at_target": pct >= 80,
        "heartbeat_latency_ms": heartbeat_signal.get("latency_ms", 0),
        "heartbeat_healthy": heartbeat_signal.get("healthy", True),
    }


def _get_latest_heartbeat_signal() -> dict:
    """Get the most recent heartbeat probe result."""
    probe_path = _telemetry_dir() / "heartbeats.jsonl"
    if not probe_path.exists():
        return {"healthy": True}
    try:
        lines = probe_path.read_text().strip().splitlines()
        if lines:
            latest = json.loads(lines[-1])
            # Only use if recent (last 10 minutes)
            ts = datetime.fromisoformat(latest["timestamp"])
            if (datetime.now(ET) - ts).total_seconds() < 600:
                return latest
    except Exception:
        pass
    return {"healthy": True}


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------


def _telemetry_dir() -> Path:
    d = find_repo_root() / TELEMETRY_DIR
    d.mkdir(parents=True, exist_ok=True)
    return d


def _daily_log_path(dt: datetime | None = None) -> Path:
    dt = dt or datetime.now(ET)
    return _telemetry_dir() / f"usage_{dt.strftime('%Y-%m-%d')}.jsonl"


def _record_invocation(
    result: dict, daemon: str, task_id: str, ticker: str,
    start_time: datetime, elapsed_seconds: float,
    rate_limited: bool, returncode: int, stderr_snippet: str,
) -> None:
    usage = result.get("usage", {})
    model_usage = result.get("modelUsage", {})
    model = list(model_usage.keys())[0] if model_usage else ""

    record = {
        "timestamp": start_time.isoformat(),
        "daemon": daemon,
        "task_id": task_id,
        "ticker": ticker,
        "model": model,
        "duration_ms": result.get("duration_ms", int(elapsed_seconds * 1000)),
        "duration_api_ms": result.get("duration_api_ms", 0),
        "num_turns": result.get("num_turns", 0),
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
        "cache_read_tokens": usage.get("cache_read_input_tokens", 0),
        "cache_creation_tokens": usage.get("cache_creation_input_tokens", 0),
        "total_cost_usd": result.get("total_cost_usd", 0),
        "session_id": result.get("session_id", ""),
        "stop_reason": result.get("stop_reason", ""),
        "is_error": result.get("is_error", False),
        "rate_limited": rate_limited,
        "timed_out": result.get("timed_out", False),
        "returncode": returncode,
        "stderr": stderr_snippet,
        "service_tier": usage.get("service_tier", ""),
        "web_search_requests": usage.get("server_tool_use", {}).get("web_search_requests", 0),
        "web_fetch_requests": usage.get("server_tool_use", {}).get("web_fetch_requests", 0),
    }

    try:
        with open(_daily_log_path(start_time), "a") as f:
            f.write(json.dumps(record, default=str) + "\n")
    except Exception as e:
        logger.debug("Failed to write telemetry: %s", e)


# ---------------------------------------------------------------------------
# Aggregation and reporting
# ---------------------------------------------------------------------------


def _load_daily_records(date_str: str | None = None) -> list[dict]:
    dt = datetime.now(ET) if not date_str else datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=ET)
    path = _daily_log_path(dt)
    if not path.exists():
        return []
    records = []
    for line in path.read_text().strip().splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def get_usage_report(date_str: str | None = None) -> dict:
    records = _load_daily_records(date_str)
    if not records:
        return {"date": date_str or datetime.now(ET).strftime("%Y-%m-%d"), "total_invocations": 0}

    total_input = sum(r.get("input_tokens", 0) for r in records)
    total_output = sum(r.get("output_tokens", 0) for r in records)
    total_cache_read = sum(r.get("cache_read_tokens", 0) for r in records)
    total_cache_create = sum(r.get("cache_creation_tokens", 0) for r in records)
    total_cost = sum(r.get("total_cost_usd", 0) for r in records)
    total_duration = sum(r.get("duration_ms", 0) for r in records)
    rate_limits = sum(1 for r in records if r.get("rate_limited"))
    errors = sum(1 for r in records if r.get("is_error"))
    timeouts = sum(1 for r in records if r.get("timed_out"))
    web_searches = sum(r.get("web_search_requests", 0) for r in records)
    web_fetches = sum(r.get("web_fetch_requests", 0) for r in records)

    by_daemon: dict[str, dict] = {}
    for r in records:
        d = r.get("daemon", "unknown")
        if d not in by_daemon:
            by_daemon[d] = {"invocations": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0, "duration_ms": 0, "rate_limits": 0, "errors": 0}
        by_daemon[d]["invocations"] += 1
        by_daemon[d]["input_tokens"] += r.get("input_tokens", 0)
        by_daemon[d]["output_tokens"] += r.get("output_tokens", 0)
        by_daemon[d]["cost_usd"] += r.get("total_cost_usd", 0)
        by_daemon[d]["duration_ms"] += r.get("duration_ms", 0)
        if r.get("rate_limited"):
            by_daemon[d]["rate_limits"] += 1
        if r.get("is_error"):
            by_daemon[d]["errors"] += 1

    by_model: dict[str, dict] = {}
    for r in records:
        m = r.get("model", "unknown")
        if m not in by_model:
            by_model[m] = {"invocations": 0, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0}
        by_model[m]["invocations"] += 1
        by_model[m]["input_tokens"] += r.get("input_tokens", 0)
        by_model[m]["output_tokens"] += r.get("output_tokens", 0)
        by_model[m]["cost_usd"] += r.get("total_cost_usd", 0)

    by_hour: dict[int, int] = {}
    for r in records:
        try:
            h = datetime.fromisoformat(r["timestamp"]).hour
            by_hour[h] = by_hour.get(h, 0) + 1
        except Exception:
            pass

    capacity = get_capacity_estimate()

    return {
        "date": date_str or datetime.now(ET).strftime("%Y-%m-%d"),
        "total_invocations": len(records),
        "total_input_tokens": total_input,
        "total_output_tokens": total_output,
        "total_cache_read_tokens": total_cache_read,
        "total_cache_creation_tokens": total_cache_create,
        "total_cost_usd": round(total_cost, 4),
        "total_duration_minutes": round(total_duration / 60_000, 1),
        "rate_limit_hits": rate_limits,
        "errors": errors,
        "timeouts": timeouts,
        "web_searches": web_searches,
        "web_fetches": web_fetches,
        "by_daemon": by_daemon,
        "by_model": by_model,
        "by_hour": dict(sorted(by_hour.items())),
        "capacity": capacity,
    }


# ---------------------------------------------------------------------------
# CLI display
# ---------------------------------------------------------------------------


def sync_telemetry_to_s3(date_str: str | None = None) -> int:
    """Sync telemetry files to S3. Returns number of files uploaded."""
    from cli.s3 import get_s3_client, upload_file

    s3 = get_s3_client()
    tel_dir = _telemetry_dir()
    uploaded = 0

    for path in tel_dir.glob("*.jsonl"):
        s3_key = f"data/telemetry/{path.name}"
        try:
            upload_file(s3, path, s3_key)
            uploaded += 1
        except Exception:
            pass

    # Also sync calibration
    cal_path = _calibration_path()
    if cal_path.exists():
        try:
            upload_file(s3, cal_path, f"data/telemetry/{cal_path.name}")
            uploaded += 1
        except Exception:
            pass

    # Sync heartbeats
    hb_path = tel_dir / "heartbeats.jsonl"
    if hb_path.exists():
        try:
            upload_file(s3, hb_path, "data/telemetry/heartbeats.jsonl")
            uploaded += 1
        except Exception:
            pass

    return uploaded


def print_usage_report(date_str: str | None = None, as_json: bool = False, live: bool = False) -> None:
    import click

    if live:
        _live_display()
        return

    report = get_usage_report(date_str)

    if as_json:
        click.echo(json.dumps(report, indent=2, default=str))
        return

    click.echo(f"\n{'='*60}")
    click.echo(f"  Claude CLI Usage Report — {report['date']}")
    click.echo(f"{'='*60}")

    if report["total_invocations"] == 0:
        click.echo("  No telemetry data for this date.")
        return

    # Capacity gauge
    cap = report.get("capacity", {})
    if cap:
        pct = cap.get("estimated_pct", 0)
        target = cap.get("target_pct", 80)
        cal_label = "calibrated" if cap.get("calibrated") else "estimated"
        bar_len = 30
        filled = int(pct / 100 * bar_len)
        target_pos = int(target / 100 * bar_len)

        bar = ""
        for i in range(bar_len):
            if i < filled:
                if pct >= 80:
                    bar += "█"  # Over target
                else:
                    bar += "▓"
            elif i == target_pos:
                bar += "┃"  # 80% target marker
            else:
                bar += "░"

        remaining_tok = cap.get("remaining_budget_tokens", 0)
        remaining_cost = cap.get("remaining_budget_cost", 0)

        click.echo(f"\n  CAPACITY ({cal_label})")
        click.echo(f"  [{bar}] {pct}% of window")
        click.echo(f"  Budget: {cap.get('window_budget_tokens', 0):,} tok (${cap.get('window_budget_cost', 0):.2f})")
        click.echo(f"  Used:   {cap.get('current_window_used_tokens', 0):,} tok (${cap.get('current_window_used_cost', 0):.2f})")
        click.echo(f"  Left:   {remaining_tok:,} tok (${remaining_cost:.2f})")
        if cap.get("heartbeat_latency_ms"):
            health = "healthy" if cap.get("heartbeat_healthy") else "THROTTLED"
            click.echo(f"  Probe:  {cap['heartbeat_latency_ms']}ms ({health})")

    click.echo(f"\n  TODAY'S TOTALS")
    click.echo(f"  Invocations:  {report['total_invocations']}")
    click.echo(f"  Output tokens: {report['total_output_tokens']:,}")
    click.echo(f"  Duration:     {report['total_duration_minutes']:.1f} min")
    click.echo(f"  Equiv cost:   ${report['total_cost_usd']:.2f}")
    click.echo(f"  Rate limits:  {report['rate_limit_hits']}")
    click.echo(f"  Errors:       {report['errors']}")

    if report.get("by_daemon"):
        click.echo(f"\n  {'DAEMON':<22} {'Calls':>5} {'Out Tok':>9} {'Cost':>7} {'RLim':>4}")
        click.echo(f"  {'-'*50}")
        for daemon, stats in sorted(report["by_daemon"].items(), key=lambda x: -x[1]["cost_usd"]):
            click.echo(
                f"  {daemon:<22} {stats['invocations']:>5} "
                f"{stats['output_tokens']:>9,} ${stats['cost_usd']:>5.2f} {stats['rate_limits']:>4}"
            )

    if report.get("by_model"):
        click.echo(f"\n  {'MODEL':<30} {'Calls':>5} {'Out Tok':>9}")
        click.echo(f"  {'-'*48}")
        for model, stats in sorted(report["by_model"].items()):
            short = model.split("[")[0] if "[" in model else model
            if not short:
                short = "(unknown)"
            click.echo(f"  {short:<30} {stats['invocations']:>5} {stats['output_tokens']:>9,}")

    if report.get("by_hour"):
        click.echo(f"\n  HOURLY (ET):")
        max_count = max(report["by_hour"].values()) if report["by_hour"] else 1
        for hour in range(24):
            count = report["by_hour"].get(hour, 0)
            if count > 0:
                bar = "▓" * int(count / max_count * 25)
                click.echo(f"  {hour:02d}:00  {bar} {count}")

    click.echo()


def _live_display() -> None:
    """Live-updating capacity display. Refreshes every 30s with heartbeat probes."""
    import click

    click.echo("Live capacity monitor (Ctrl+C to stop)\n")

    while True:
        # Run heartbeat
        probe = run_heartbeat()

        # Get capacity
        cap = get_capacity_estimate()
        pct = cap.get("estimated_pct", 0)
        target = cap.get("target_pct", 80)

        bar_len = 40
        filled = int(pct / 100 * bar_len)
        target_pos = int(target / 100 * bar_len)

        bar = ""
        for i in range(bar_len):
            if i < filled:
                bar += "█" if pct >= 80 else "▓"
            elif i == target_pos:
                bar += "┃"
            else:
                bar += "░"

        now = datetime.now(ET).strftime("%H:%M:%S ET")
        health = "OK" if probe.get("healthy") else "THROTTLED" if probe.get("throttled") else "RATE LIMITED"
        latency = probe.get("latency_ms", 0)

        # Clear line and print
        click.echo(f"\r  {now} [{bar}] {pct:>3}%  "
                    f"| {cap.get('current_window_used_tokens', 0):>7,} / {cap.get('window_budget_tokens', 0):>7,} tok  "
                    f"| probe: {latency}ms {health}  ", nl=False)

        time.sleep(30)
