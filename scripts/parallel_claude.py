#!/usr/bin/env python3
"""Launch Claude sessions in parallel across ticker workspaces.

Usage:
    python scripts/parallel_claude.py --prompt "analyze <ticker>" --tickers NVDA TSM
    python scripts/parallel_claude.py --prompt "analyze <ticker>" --tickers NVDA TSM --max-parallel 2

Resume a session:
    claude --resume <session-id>
"""

import argparse
import os
import shutil
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = REPO_ROOT / "workspace"


def _find_claude() -> str:
    """Resolve the claude binary path (may be an alias not on PATH)."""
    found = shutil.which("claude")
    if found:
        return found
    # Common install location
    local_bin = Path.home() / ".local" / "bin" / "claude"
    if local_bin.exists():
        return str(local_bin)
    print("ERROR: could not find 'claude' binary", file=sys.stderr)
    sys.exit(1)


CLAUDE_BIN = _find_claude()


def run_ticker(ticker: str, prompt: str, session_id: str) -> tuple[str, str, bool, str]:
    """Run claude for a single ticker. Returns (ticker, session_id, success, output)."""
    workspace = WORKSPACE_DIR / ticker
    ticker_prompt = prompt.replace("<ticker>", ticker)

    env = os.environ.copy()
    env.pop("CLAUDE_CODE_ENTRYPOINT", None)
    env.pop("CLAUDECODE", None)

    try:
        result = subprocess.run(
            [CLAUDE_BIN, "-p", ticker_prompt, "--allowedTools", "*", "--session-id", session_id],
            cwd=workspace,
            capture_output=True,
            text=True,
            env=env,
        )
        output = result.stdout + result.stderr
        return (ticker, session_id, result.returncode == 0, output)
    except Exception as e:
        return (ticker, session_id, False, str(e))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Claude sessions in parallel across ticker workspaces")
    parser.add_argument("--prompt", required=True, help="Prompt template (must contain <ticker>)")
    parser.add_argument("--tickers", required=True, nargs="+", help="Tickers to process")
    parser.add_argument("--max-parallel", type=int, default=4, help="Max parallel sessions (default: 4)")
    args = parser.parse_args()

    if "<ticker>" not in args.prompt:
        print("ERROR: --prompt must contain <ticker> placeholder", file=sys.stderr)
        sys.exit(1)

    # Validate workspace dirs exist
    missing = [t for t in args.tickers if not (WORKSPACE_DIR / t).is_dir()]
    if missing:
        print(f"ERROR: workspace dirs not found for: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    # Build session map
    sessions = {ticker: str(uuid.uuid4()) for ticker in args.tickers}

    # Print session IDs upfront for resumability
    print(f"Queued {len(args.tickers)} sessions, running {args.max_parallel} at a time\n")
    for ticker, sid in sessions.items():
        print(f"  {ticker}: {sid}")
    print()

    # Run in parallel
    results: list[tuple[str, str, bool, str]] = []
    with ThreadPoolExecutor(max_workers=args.max_parallel) as pool:
        futures = {
            pool.submit(run_ticker, ticker, args.prompt, sid): ticker
            for ticker, sid in sessions.items()
        }
        for future in as_completed(futures):
            ticker, sid, success, output = future.result()
            status = "done" if success else "FAILED"
            print(f"[{ticker}] {status}")
            if not success and output.strip():
                for line in output.strip().splitlines()[:10]:
                    print(f"  {line}")
            results.append((ticker, sid, success, output))

    # Summary
    print(f"\n{'Ticker':<10} {'Status':<10} {'Session ID'}")
    print("-" * 60)
    for ticker, sid, success, _ in sorted(results):
        status = "OK" if success else "FAIL"
        print(f"{ticker:<10} {status:<10} {sid}")


if __name__ == "__main__":
    main()
