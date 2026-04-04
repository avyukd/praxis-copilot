"""Full local CLI-based filing/PR pipeline — zero API billing.

End-to-end pipeline running entirely on the local machine:
1. Poll EDGAR + newswires (reuse Lambda poller modules)
2. Fetch + extract filing text (reuse Lambda extractor modules)
3. Analyze using Claude CLI (Max subscription, not Sonnet API)
4. Store results to S3 (same paths as Lambda pipeline)
5. Send alerts via SNS

Replaces the entire Lambda pipeline. AWS S3 remains the data store.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import click
import yaml

from cli.s3 import BUCKET, download_file, get_s3_client, list_prefix, upload_file

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Claude CLI analysis (replaces LiteLLM/Sonnet API)
# ---------------------------------------------------------------------------

ANALYSIS_PROMPT = """\
You are a senior equity analyst specializing in small-cap and micro-cap stocks.

Given a filing or press release, you must:
1. Identify what NEW information is being disclosed — focus on the DELTA vs what was already known.
2. Assess how MATERIAL this information is for the stock price.
3. Classify as BUY, SELL, or NEUTRAL.
4. Assign magnitude 0.0-1.0.

Classification guidelines:
- BUY: Positive earnings surprise, accretive acquisition, FDA approval, major contract,
  significant revenue beat, insider buying, strategic partnership, debt reduction.
- SELL: Earnings miss, impairment, going concern, auditor change, SEC investigation,
  dilutive offering, customer loss, management departure, covenant breach.
- NEUTRAL: Routine filings, administrative changes, immaterial events, mixed signals.

Magnitude scale:
- 0.0-0.2: Minor/routine (board appointment, minor amendment)
- 0.2-0.5: Moderate (guidance update, moderate contract win)
- 0.5-0.8: Significant (major deal, phase 3 data, earnings surprise >20%)
- 0.8-1.0: Transformative (M&A, FDA approval, bankruptcy)

You MUST respond with ONLY a JSON object (no markdown, no explanation outside JSON):
{
  "classification": "BUY" | "SELL" | "NEUTRAL",
  "magnitude": <float 0.0-1.0>,
  "new_information": "<what is genuinely new — 2-3 sentences>",
  "materiality": "<how material and why — quantify vs market cap/revenue if possible>",
  "explanation": "<1 paragraph analyst summary>"
}
"""


def _find_claude() -> str:
    """Find the claude CLI binary."""
    found = shutil.which("claude")
    if found:
        return found
    local = Path.home() / ".local" / "bin" / "claude"
    if local.exists():
        return str(local)
    raise FileNotFoundError("Claude CLI not found")


def _cli_env() -> dict:
    """Build env dict for Claude CLI subprocesses (strips API keys)."""
    env = os.environ.copy()
    env.pop("ANTHROPIC_API_KEY", None)
    env.pop("CLAUDE_API_KEY", None)
    env.pop("CLAUDE_CODE_ENTRYPOINT", None)
    env.pop("CLAUDECODE", None)
    return env


def analyze_with_cli(ticker: str, form_type: str, text: str, model: str = "sonnet") -> dict | None:
    """Analyze filing text using Claude CLI. Returns analysis dict or None.

    *model* can be 'sonnet', 'haiku', or 'opus'. Default is sonnet (fast + smart enough).
    """
    claude_bin = _find_claude()

    # Truncate
    if len(text) > 30_000:
        text = text[:30_000] + "\n\n[TRUNCATED]"

    user_msg = f"Ticker: {ticker}\nForm type: {form_type}\n\n--- FILING TEXT ---\n{text}"
    full_prompt = f"{ANALYSIS_PROMPT}\n\n{user_msg}"

    cmd = [claude_bin, "-p", full_prompt, "--dangerously-skip-permissions"]
    if model and model != "opus":
        cmd.extend(["--model", model])

    try:
        from cli.telemetry import track_claude_call

        result_json = track_claude_call(
            cmd, env=_cli_env(), timeout=120,
            daemon="scanner", task_id=f"analyze_{ticker}", ticker=ticker,
        )

        if result_json.get("is_error"):
            if result_json.get("rate_limited"):
                logger.warning("Rate limited analyzing %s", ticker)
                try:
                    from cli.queue_capacity import CapacityTracker
                    ct = CapacityTracker()
                    ct.on_rate_limit()
                except Exception:
                    pass
            return None

        raw = result_json.get("result", "").strip()
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            logger.error("No JSON in Claude output for %s", ticker)
            return None

        analysis = json.loads(match.group())
        analysis["analyzed_at"] = datetime.now(ET).isoformat()
        analysis["analyzer"] = "cli"
        return analysis

    except subprocess.TimeoutExpired:
        logger.error("Claude CLI timed out for %s", ticker)
        return None
    except Exception as e:
        logger.error("Analysis failed for %s: %s", ticker, e)
        return None


def prescreen_with_cli(ticker: str, text: str) -> str:
    """Quick prescreen using haiku. Returns 'POSITIVE', 'NEUTRAL', or 'NEGATIVE'."""
    prompt = (
        f"You are screening SEC filings for materiality. "
        f"Read this filing for {ticker} and respond with exactly one word: "
        f"POSITIVE, NEUTRAL, or NEGATIVE.\n\n{text[:5000]}"
    )
    try:
        from cli.telemetry import track_claude_call

        cmd = [_find_claude(), "-p", prompt, "--dangerously-skip-permissions", "--model", "haiku"]
        result_json = track_claude_call(
            cmd, env=_cli_env(), timeout=30,
            daemon="scanner", task_id=f"prescreen_{ticker}", ticker=ticker,
        )
        response = result_json.get("result", "").strip().upper()
        if "POSITIVE" in response:
            return "POSITIVE"
        if "NEGATIVE" in response:
            return "NEGATIVE"
        return "NEUTRAL"
    except Exception:
        return "NEUTRAL"


# ---------------------------------------------------------------------------
# Full local pipeline: poll → fetch → extract → analyze → alert
# ---------------------------------------------------------------------------


def _ensure_src_path() -> None:
    """Ensure repo root is on sys.path so Lambda module imports (from src.modules...) work."""
    import sys
    # Lambda modules use `from src.modules...` — need repo root on path
    repo_root = str(Path(__file__).resolve().parent.parent.parent)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)


def run_full_pipeline(
    *,
    lookback_minutes: int = 120,
    max_parallel: int = 4,
    prescreen: bool = True,
    alert_threshold: float = 0.5,
    dry_run: bool = False,
    model: str = "sonnet",
) -> dict:
    """Run the full local pipeline: poll EDGAR + newswires, extract, analyze, alert.

    Uses the Lambda module code directly for polling and extraction,
    and Claude CLI for analysis.
    """
    _ensure_src_path()
    from src.modules.events.eight_k_scanner.edgar.poller import poll_new_8k_filings
    from src.modules.events.eight_k_scanner.newswire.gnw import poll_gnw

    GNW_FEED_URLS = [
        "https://www.globenewswire.com/RssFeed/exchange/NYSE",
        "https://www.globenewswire.com/RssFeed/exchange/NASDAQ",
        "https://www.globenewswire.com/RssFeed/exchange/TSX",
        "https://www.globenewswire.com/RssFeed/exchange/TSXV",
    ]

    s3 = get_s3_client()
    stats = {"polled": 0, "extracted": 0, "analyzed": 0, "alerted": 0, "skipped": 0}

    click.echo(f"=== Local Filing Pipeline ===")
    click.echo(f"  Lookback: {lookback_minutes} min | Parallel: {max_parallel}")
    click.echo(f"  Prescreen: {prescreen} | Alert threshold: {alert_threshold}")
    click.echo()

    # --- Stage 1: Poll ---
    click.echo("[1/4] Polling for new filings and press releases...")

    # Poll EDGAR
    try:
        filings = poll_new_8k_filings(lookback_minutes=lookback_minutes)
        click.echo(f"  EDGAR: {len(filings)} new filing(s)")
        stats["polled"] += len(filings)
    except Exception as e:
        logger.error("EDGAR poll failed: %s", e)
        filings = []

    # Poll GNW press releases
    try:
        press_releases = poll_gnw(GNW_FEED_URLS)
        click.echo(f"  GNW: {len(press_releases)} new release(s)")
        stats["polled"] += len(press_releases)
    except Exception as e:
        logger.error("GNW poll failed: %s", e)
        press_releases = []

    # Poll Newsfile (Canadian)
    try:
        from src.modules.events.eight_k_scanner.newswire.newsfile import poll_newsfile
        ca_releases = poll_newsfile()
        click.echo(f"  Newsfile: {len(ca_releases)} new release(s)")
        stats["polled"] += len(ca_releases)
        press_releases.extend(ca_releases)
    except Exception as e:
        logger.debug("Newsfile poll failed: %s", e)

    if not filings and not press_releases:
        click.echo("  Nothing new found.")
        return stats

    # --- Filter: remove low-liquidity and large-cap ---
    pre_filter = len(filings) + len(press_releases)
    filings = _filter_filings(filings)
    press_releases = _filter_press_releases(press_releases)
    filtered_out = pre_filter - len(filings) - len(press_releases)
    if filtered_out > 0:
        click.echo(f"  Filtered out {filtered_out} (ADTV < $1K or mcap > $1B)")
    stats["polled"] = len(filings) + len(press_releases)

    if dry_run:
        click.echo(f"\n[DRY RUN] Would process {len(filings)} filings + {len(press_releases)} PRs")
        for f in filings:
            click.echo(f"  Filing: {f.ticker} {f.form_type} ({f.accession_number})")
        for pr in press_releases:
            click.echo(f"  PR: {pr.ticker} — {pr.title[:60]}")
        return stats

    # --- Stage 2: Fetch + Extract ---
    click.echo(f"\n[2/4] Fetching and extracting text...")

    items_to_analyze = []

    # Process filings
    for filing in filings:
        try:
            item = _fetch_and_extract_filing(s3, filing)
            if item:
                items_to_analyze.append(item)
                stats["extracted"] += 1
        except Exception as e:
            logger.error("Fetch/extract failed for %s: %s", filing.accession_number, e)

    # Process press releases
    for pr in press_releases:
        try:
            item = _store_press_release(s3, pr)
            if item:
                items_to_analyze.append(item)
                stats["extracted"] += 1
        except Exception as e:
            logger.error("PR storage failed for %s: %s", pr.ticker, e)

    click.echo(f"  {stats['extracted']} item(s) ready for analysis")

    # --- Stage 3: Analyze ---
    click.echo(f"\n[3/4] Analyzing with Claude CLI...")

    results = []
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {}
        for item in items_to_analyze:
            future = executor.submit(_analyze_pipeline_item, s3, item, prescreen, model)
            futures[future] = item

        for future in as_completed(futures):
            item = futures[future]
            try:
                analysis = future.result()
                if analysis:
                    stats["analyzed"] += 1
                    results.append((item, analysis))
                    click.echo(
                        f"  {analysis['classification']} (mag={analysis['magnitude']:.2f}): "
                        f"{item['ticker']}"
                    )
                else:
                    stats["skipped"] += 1
            except Exception as e:
                logger.error("Analysis error for %s: %s", item.get("ticker", "?"), e)
                stats["skipped"] += 1

    # --- Stage 4: Alert ---
    click.echo(f"\n[4/4] Sending alerts...")

    for item, analysis in results:
        classification = (analysis.get("classification") or "").upper()
        if classification in ("SELL", "HOLD"):
            continue
        if analysis["magnitude"] >= alert_threshold:
            _send_alert(item, analysis)
            stats["alerted"] += 1

    click.echo(f"\n=== Pipeline Complete ===")
    click.echo(f"  Polled: {stats['polled']} | Extracted: {stats['extracted']} | "
               f"Analyzed: {stats['analyzed']} | Alerted: {stats['alerted']} | "
               f"Skipped: {stats['skipped']}")

    return stats


# ---------------------------------------------------------------------------
# Pipeline helpers
# ---------------------------------------------------------------------------


def _filter_filings(filings: list) -> list:
    """Filter out filings that don't meet liquidity/size criteria."""
    try:
        _ensure_src_path()
        from src.modules.events.eight_k_scanner.financials import lookup_market_cap
    except ImportError:
        return filings  # Can't filter without financials module

    filtered = []
    for f in filings:
        ticker = f.ticker
        if not ticker:
            continue
        try:
            mcap = lookup_market_cap(ticker)
            if mcap and mcap > 1_000_000_000:  # > $1B
                continue
        except Exception:
            pass  # If we can't check, let it through
        filtered.append(f)
    return filtered


def _filter_press_releases(releases: list) -> list:
    """Filter out press releases that don't meet criteria."""
    # For PRs we can't easily check market cap without extra API calls.
    # Just filter out tickers that are clearly large-cap indices/ETFs.
    skip_tickers = {"SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "GLD", "SLV", "TLT"}
    return [pr for pr in releases if pr.ticker and pr.ticker.upper() not in skip_tickers]


def _split_items(text: str) -> dict[str, str]:
    """Split 8-K text into items by Item number headers."""
    pattern = r"\n\s*(?:Item|ITEM)\s+(\d+\.\d+)"
    parts = re.split(pattern, text)
    items = {}
    for i in range(1, len(parts) - 1, 2):
        item_num = parts[i]
        item_text = parts[i + 1].strip()
        if item_text and len(item_text) > 50:
            items[item_num] = item_text[:20_000]
    return items


def _fetch_and_extract_filing(s3, filing) -> dict | None:
    """Fetch a filing from EDGAR, extract text, store to S3."""
    from src.modules.events.eight_k_scanner.edgar.fetcher import fetch_filing

    # Fetch from EDGAR
    doc = fetch_filing(filing.cik, filing.accession_number)
    if not doc:
        return None

    # Store raw to S3
    key_prefix = f"data/raw/filings/{filing.cik}/{filing.accession_number.replace('-', '')}"

    # Build and store index.json
    index = {
        "cik": filing.cik,
        "accession_number": filing.accession_number,
        "ticker": filing.ticker,
        "company_name": filing.company_name,
        "form_type": filing.form_type,
        "filed_date": filing.filed_date,
        "items_detected": filing.items,
        "source": "edgar",
        "fetched_at": datetime.now(ET).isoformat(),
    }
    _upload_json(s3, index, f"{key_prefix}/index.json")

    # Store raw HTML docs
    for filename, content in doc.documents.items():
        tmp = Path(tempfile.mktemp(suffix=f"_{filename}"))
        if isinstance(content, bytes):
            tmp.write_bytes(content)
        else:
            tmp.write_text(content)
        upload_file(s3, tmp, f"{key_prefix}/{filename}")
        tmp.unlink()

    # Extract text from primary document
    primary_doc_name = doc.metadata.primary_doc if doc.metadata else None
    primary_text = ""
    if primary_doc_name and primary_doc_name in doc.documents:
        content = doc.documents[primary_doc_name]
        if isinstance(content, bytes):
            content = content.decode("utf-8", errors="replace")
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(content, "lxml")
        primary_text = soup.get_text(separator="\n", strip=True)
    elif doc.documents:
        # Fallback: try the first HTML document
        for fname, content in doc.documents.items():
            if fname.endswith((".htm", ".html")):
                if isinstance(content, bytes):
                    content = content.decode("utf-8", errors="replace")
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(content, "lxml")
                primary_text = soup.get_text(separator="\n", strip=True)
                break

    if not primary_text:
        return None

    # Try to split into items for 8-K
    items = {}
    if filing.form_type in ("8-K", "8-K/A"):
        items = _split_items(primary_text)

    extracted = {
        "cik": filing.cik,
        "accession_number": filing.accession_number,
        "ticker": filing.ticker,
        "form_type": filing.form_type,
        "items": items,
        "text": primary_text if not items else "",
        "total_chars": len(primary_text),
    }
    _upload_json(s3, extracted, f"{key_prefix}/extracted.json")

    return {
        "key_prefix": key_prefix,
        "ticker": filing.ticker,
        "form_type": filing.form_type,
        "text": primary_text,
        "items": items,
        "source": "filing",
    }


def _store_press_release(s3, pr) -> dict | None:
    """Store a press release to S3 and return item for analysis."""
    if not pr.ticker:
        return None

    # Fetch the actual text
    try:
        from src.modules.events.eight_k_scanner.newswire.fetcher import fetch_release
        fetched = fetch_release(pr.url, pr.source)
        if not fetched or not fetched.text:
            return None
        text = fetched.text
    except Exception as e:
        logger.debug("Failed to fetch PR text for %s: %s", pr.ticker, e)
        return None

    # Build full ticker with exchange suffix for non-US exchanges
    ticker = pr.ticker
    exchange = (pr.exchange or "").upper()
    _suffix_map = {"TSX": ".TO", "TSXV": ".V", "TSX-V": ".V", "ASX": ".AX",
                   "LSE": ".L", "HKEX": ".HK"}
    if exchange in _suffix_map and not any(ticker.endswith(s) for s in _suffix_map.values()):
        ticker = f"{ticker}{_suffix_map[exchange]}"

    source = pr.source or "gnw"
    release_id = pr.release_id or pr.url.split("/")[-1]
    # Use original pr.ticker for S3 path (backwards compatible), but full ticker for display
    key_prefix = f"data/raw/press_releases/{source}/{pr.ticker}/{release_id}"

    # Check if already processed
    try:
        download_file(s3, f"{key_prefix}/analysis.json")
        return None  # Already analyzed
    except Exception:
        pass  # Not yet analyzed

    # Store index
    index = {
        "ticker": ticker,
        "headline": pr.title,
        "url": pr.url,
        "published_at": pr.published_at if isinstance(pr.published_at, str) else str(pr.published_at or ""),
        "source": source,
        "exchange": pr.exchange,
        "fetched_at": datetime.now(ET).isoformat(),
    }
    _upload_json(s3, index, f"{key_prefix}/index.json")

    # Store release text
    tmp = Path(tempfile.mktemp(suffix=".txt"))
    tmp.write_text(text)
    upload_file(s3, tmp, f"{key_prefix}/release.txt")
    tmp.unlink()

    # Store extracted
    extracted = {
        "ticker": ticker,
        "form_type": "PR",
        "text": text,
        "total_chars": len(text),
    }
    _upload_json(s3, extracted, f"{key_prefix}/extracted.json")

    return {
        "key_prefix": key_prefix,
        "ticker": ticker,
        "form_type": "PR",
        "text": text,
        "items": {},
        "source": "press_release",
    }


def _analyze_pipeline_item(s3, item: dict, prescreen: bool, model: str = "sonnet") -> dict | None:
    """Analyze a single pipeline item."""
    ticker = item["ticker"]
    form_type = item["form_type"]

    # Build text from items or raw text
    text_parts = []
    if item.get("items"):
        for num, text in item["items"].items():
            text_parts.append(f"[Item {num}]\n{text}")
    if item.get("text"):
        text_parts.append(item["text"])

    full_text = "\n\n".join(text_parts)
    if not full_text.strip():
        return None

    # Prescreen
    if prescreen:
        screen = prescreen_with_cli(ticker, full_text)
        if screen == "NEGATIVE":
            screening = {"result": "NEGATIVE", "screened_at": datetime.now(ET).isoformat(), "analyzer": "cli"}
            _upload_json(s3, screening, f"{item['key_prefix']}/screening.json")
            return None

    # Full analysis
    analysis = analyze_with_cli(ticker, form_type, full_text, model=model)
    if not analysis:
        return None

    # Upload to S3
    _upload_json(s3, analysis, f"{item['key_prefix']}/analysis.json")

    # Update index with analyzed_at
    try:
        index_raw = download_file(s3, f"{item['key_prefix']}/index.json")
        index = json.loads(index_raw)
        index["analyzed_at"] = analysis["analyzed_at"]
        if analysis["magnitude"] >= 0.5:
            index["alert_sent_at"] = datetime.now(ET).isoformat()
        _upload_json(s3, index, f"{item['key_prefix']}/index.json")
    except Exception:
        pass

    return analysis


def _send_alert(item: dict, analysis: dict) -> None:
    """Send SNS alert for a high-magnitude analysis."""
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not topic_arn:
        click.echo(f"    [NO SNS] {analysis['classification']} {item['ticker']} (mag={analysis['magnitude']:.2f})")
        return

    try:
        import boto3
        sns = boto3.client("sns")

        ticker = item['ticker']
        _exch_map = {".AX": "ASX", ".TO": "TSX", ".V": "TSXV", ".L": "LSE",
                     ".CO": "Copenhagen", ".SW": "SIX", ".HK": "HKEX"}
        exch = next((v for k, v in _exch_map.items() if ticker.upper().endswith(k)), "")
        exch_note = f" [{exch}]" if exch else ""

        # Look up market cap
        mcap_str = ""
        try:
            import sys
            repo_root = str(Path(__file__).resolve().parent.parent.parent)
            if repo_root not in sys.path:
                sys.path.insert(0, repo_root)
            from src.modules.events.eight_k_scanner.financials import lookup_market_cap
            mcap = lookup_market_cap(ticker)
            if mcap:
                if mcap >= 1_000_000_000:
                    mcap_str = f"${mcap / 1_000_000_000:.1f}B"
                else:
                    mcap_str = f"${mcap / 1_000_000:.0f}M"
        except Exception:
            pass

        mcap_line = f" | Mcap: {mcap_str}" if mcap_str else ""

        subject = (
            f"{analysis['classification']} ALERT: {ticker}{exch_note} "
            f"(mag={analysis['magnitude']:.2f})"
        )
        message = (
            f"{analysis['classification']} — {ticker}{exch_note} ({item['form_type']})\n"
            f"Magnitude: {analysis['magnitude']:.2f}{mcap_line}\n\n"
            f"New information:\n{analysis.get('new_information', '')}\n\n"
            f"Materiality:\n{analysis.get('materiality', '')}\n\n"
            f"Analysis:\n{analysis.get('explanation', '')}"
        )

        sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)
        click.echo(f"    Alerted: {item['ticker']} ({analysis['classification']})")
    except Exception as e:
        logger.error("SNS alert failed for %s: %s", item["ticker"], e)


def _upload_json(s3, data: dict, key: str) -> None:
    """Upload a dict as JSON to S3."""
    tmp = Path(tempfile.mktemp(suffix=".json"))
    tmp.write_text(json.dumps(data, indent=2, default=str))
    upload_file(s3, tmp, key)
    tmp.unlink()


# ---------------------------------------------------------------------------
# S3-only analysis (for items already extracted by Lambda)
# ---------------------------------------------------------------------------


def scan_unanalyzed(
    *,
    lookback_hours: int = 24,
    max_parallel: int = 4,
    prescreen: bool = True,
    dry_run: bool = False,
    model: str = "sonnet",
) -> dict:
    """Find items in S3 with extracted.json but no analysis.json and analyze them."""
    from cli.s3 import list_prefix_objects

    s3 = get_s3_client()
    cutoff = datetime.now(ET) - timedelta(hours=lookback_hours)

    click.echo(f"Scanning S3 for unanalyzed items (lookback: {lookback_hours}h)")

    items = []
    for prefix in ["data/raw/filings/", "data/raw/press_releases/"]:
        objects = list_prefix_objects(s3, prefix)
        dirs: dict[str, dict] = {}
        for obj in objects:
            key = obj["Key"]
            parts = key.rsplit("/", 1)
            if len(parts) != 2:
                continue
            parent, filename = parts
            if parent not in dirs:
                dirs[parent] = {"files": set(), "last_modified": obj.get("LastModified")}
            dirs[parent]["files"].add(filename)
            if obj.get("LastModified") and (
                dirs[parent]["last_modified"] is None
                or obj["LastModified"] > dirs[parent]["last_modified"]
            ):
                dirs[parent]["last_modified"] = obj["LastModified"]

        for parent, info in dirs.items():
            if "extracted.json" in info["files"] and "analysis.json" not in info["files"]:
                if info["last_modified"] and info["last_modified"].replace(tzinfo=None) < cutoff.replace(tzinfo=None):
                    continue
                ticker = ""
                parts = parent.split("/")
                if "press_releases" in parent and len(parts) >= 5:
                    ticker = parts[4]
                items.append({"key_prefix": parent, "ticker": ticker})

    click.echo(f"  Found {len(items)} unanalyzed item(s)")

    if dry_run:
        for item in items:
            click.echo(f"  [DRY RUN] {item['key_prefix']}")
        return {"found": len(items)}

    analyzed = 0
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {}
        for item in items:
            try:
                extracted = json.loads(download_file(s3, f"{item['key_prefix']}/extracted.json"))
            except Exception:
                continue
            ticker = item.get("ticker") or extracted.get("ticker", "")
            form_type = extracted.get("form_type", "PR")
            text_parts = []
            if extracted.get("items"):
                for num, txt in extracted["items"].items():
                    text_parts.append(f"[Item {num}]\n{txt}")
            if extracted.get("text"):
                text_parts.append(extracted["text"])
            full_text = "\n\n".join(text_parts)
            if not full_text.strip():
                continue

            item["ticker"] = ticker
            item["form_type"] = form_type
            item["text"] = full_text
            item["items"] = extracted.get("items", {})
            future = executor.submit(_analyze_pipeline_item, s3, item, prescreen, model)
            futures[future] = item

        for future in as_completed(futures):
            item = futures[future]
            try:
                result = future.result()
                if result:
                    analyzed += 1
                    click.echo(
                        f"  {result['classification']} (mag={result['magnitude']:.2f}): "
                        f"{item['ticker']}"
                    )
            except Exception as e:
                logger.error("Error: %s", e)

    click.echo(f"\nDone: {analyzed} analyzed")
    return {"found": len(items), "analyzed": analyzed}


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


@click.group("scanner")
def scanner():
    """Local CLI-based filing/PR scanner (zero API billing)."""
    pass


@scanner.command("run")
@click.option("--lookback", type=int, default=120, show_default=True, help="Lookback minutes for polling")
@click.option("--max-parallel", type=int, default=4, show_default=True, help="Max concurrent analyses")
@click.option("--no-prescreen", is_flag=True, help="Skip prescreen stage")
@click.option("--alert-threshold", type=float, default=0.5, show_default=True, help="Min magnitude for SNS alert")
@click.option("--model", type=click.Choice(["sonnet", "haiku", "opus"]), default="sonnet", show_default=True, help="Claude model for analysis")
@click.option("--dry-run", is_flag=True, help="Poll only, don't analyze")
def scanner_run(lookback: int, max_parallel: int, no_prescreen: bool, alert_threshold: float, model: str, dry_run: bool):
    """Run the full local pipeline: poll → extract → analyze → alert.

    \b
    Polls EDGAR and press release feeds, extracts filing text,
    analyzes using Claude CLI (Max subscription, zero API cost),
    and sends SNS alerts for high-magnitude findings.

    \b
    Examples:
      praxis scanner run
      praxis scanner run --lookback 60 --dry-run
      praxis scanner run --max-parallel 8 --alert-threshold 0.3
    """
    run_full_pipeline(
        lookback_minutes=lookback,
        max_parallel=max_parallel,
        prescreen=not no_prescreen,
        alert_threshold=alert_threshold,
        dry_run=dry_run,
        model=model,
    )


@scanner.command("backfill")
@click.option("--lookback", type=int, default=24, show_default=True, help="Lookback hours")
@click.option("--max-parallel", type=int, default=4, show_default=True, help="Max concurrent analyses")
@click.option("--no-prescreen", is_flag=True, help="Skip prescreen")
@click.option("--dry-run", is_flag=True, help="Find items but don't analyze")
def scanner_backfill(lookback: int, max_parallel: int, no_prescreen: bool, dry_run: bool):
    """Analyze items already in S3 that are missing analysis.json.

    \b
    Useful for backfilling analysis on items that Lambda extracted
    but didn't analyze (e.g., during an outage or to re-analyze with CLI).

    \b
    Examples:
      praxis scanner backfill
      praxis scanner backfill --lookback 48
    """
    scan_unanalyzed(
        lookback_hours=lookback,
        max_parallel=max_parallel,
        prescreen=not no_prescreen,
        dry_run=dry_run,
    )


@scanner.command("analyze")
@click.argument("s3_path")
def scanner_analyze(s3_path: str):
    """Analyze a specific filing/PR by S3 path.

    \b
    Example:
      praxis scanner analyze data/raw/press_releases/gnw/IMMX/gnw-3264634
    """
    s3 = get_s3_client()
    try:
        extracted = json.loads(download_file(s3, f"{s3_path}/extracted.json"))
    except Exception:
        click.echo(f"No extracted.json found at {s3_path}/")
        return

    ticker = extracted.get("ticker", "")
    form_type = extracted.get("form_type", "PR")
    text_parts = []
    if extracted.get("items"):
        for num, txt in extracted["items"].items():
            text_parts.append(f"[Item {num}]\n{txt}")
    if extracted.get("text"):
        text_parts.append(extracted["text"])

    full_text = "\n\n".join(text_parts)
    result = analyze_with_cli(ticker, form_type, full_text)
    if result:
        _upload_json(s3, result, f"{s3_path}/analysis.json")
        click.echo(json.dumps(result, indent=2, default=str))
    else:
        click.echo("Analysis failed.")


@scanner.command("daemon")
@click.option("--poll-interval", type=int, default=1800, show_default=True, help="Seconds between polls")
@click.option("--start-hour", type=int, default=5, show_default=True, help="Start hour (local time)")
@click.option("--end-hour", type=int, default=14, show_default=True, help="End hour (local time)")
@click.option("--after-hours-sweep", type=int, default=21, show_default=True, help="After-hours sweep hour (local, 0 to disable)")
@click.option("--max-parallel", type=int, default=4, show_default=True, help="Max concurrent analyses")
@click.option("--alert-threshold", type=float, default=0.5, show_default=True, help="Min magnitude for SNS alert")
@click.option("--model", type=click.Choice(["sonnet", "haiku", "opus"]), default="sonnet", show_default=True, help="Claude model for analysis")
def scanner_daemon(poll_interval: int, start_hour: int, end_hour: int, after_hours_sweep: int, max_parallel: int, alert_threshold: float, model: str):
    """Run the scanner as a continuous daemon.

    \b
    Polls EDGAR + newswires every 30 min during market hours, analyzes
    with Claude CLI (zero API cost), uploads to S3, sends SNS alerts.

    \b
    Also runs an after-hours sweep at 9 PM local (default) to catch
    everything from after the close, so you're ready for the next morning.

    \b
    Default hours: 5 AM - 2 PM local (8 AM - 5 PM ET on PDT)
    After-hours: 9 PM local (midnight ET)

    \b
    Examples:
      praxis scanner daemon
      praxis scanner daemon --poll-interval 900 --model haiku
      praxis scanner daemon --after-hours-sweep 0  # disable
    """
    import time as _time

    click.echo(f"Scanner daemon starting")
    click.echo(f"  Market window: {start_hour}:00 - {end_hour}:00 local")
    if after_hours_sweep:
        click.echo(f"  After-hours sweep: {after_hours_sweep}:00 local")
    click.echo(f"  Poll interval: {poll_interval}s | Max parallel: {max_parallel}")
    click.echo(f"  Alert threshold: {alert_threshold} | Model: {model}")
    click.echo()

    from cli.env_loader import load_env
    load_env()

    after_hours_done_today = False
    last_date = ""
    was_throttled = False

    from cli.queue_capacity import CapacityTracker
    capacity = CapacityTracker()

    def _market_cadence(now) -> int:
        """Return poll interval based on market hours (ET-aware)."""
        from zoneinfo import ZoneInfo
        now_et = now.astimezone(ZoneInfo("America/New_York")) if now.tzinfo else now
        h = now_et.hour + now_et.minute / 60.0
        if 6.5 <= h < 9.5:    # Pre-market + open: aggressive
            return 300         # 5 min
        if 9.5 <= h < 16.0:   # Market hours
            return 600         # 10 min
        if 16.0 <= h < 20.0:  # After-hours
            return 900         # 15 min
        return poll_interval   # Default (off-hours)

    try:
        while True:
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")

            # Reset daily flags
            if today != last_date:
                after_hours_done_today = False
                last_date = today

            # No work on weekends
            if now.weekday() >= 5:
                _time.sleep(600)
                continue

            # Hourly digest email (market hours: 9 AM - 4 PM ET)
            try:
                from zoneinfo import ZoneInfo
                now_et = now.astimezone(ZoneInfo("America/New_York")) if now.tzinfo else now
                et_hour = now_et.hour
                if 9 <= et_hour < 16:
                    hour_key = f"hourly_{today}_{et_hour}"
                    if not hasattr(run_full_pipeline, '_hourly_sent'):
                        run_full_pipeline._hourly_sent = set()
                    if hour_key not in run_full_pipeline._hourly_sent:
                        from cli.hourly_digest import send_hourly_digest
                        send_hourly_digest()
                        run_full_pipeline._hourly_sent.add(hour_key)
            except Exception:
                pass

            # Check capacity — back off if rate limited
            if not capacity.should_run():
                click.echo(f"[{now.strftime('%H:%M:%S')}] Capacity throttled (backoff={capacity.current_backoff_seconds}s, hits={len(capacity.rate_limit_hits)}), waiting...")
                was_throttled = True
                _time.sleep(300)
                continue

            # Coming back from throttle — backfill missed items FIRST
            if was_throttled:
                click.echo(f"\n[{now.strftime('%H:%M:%S')}] Back online after throttle — backfilling missed items...")
                try:
                    scan_unanalyzed(
                        lookback_hours=6,
                        max_parallel=max_parallel,
                        prescreen=True,
                        model=model,
                    )
                except Exception as e:
                    logger.error("Post-throttle backfill failed: %s", e)
                was_throttled = False

            in_market_window = start_hour <= now.hour < end_hour
            in_after_hours = (
                after_hours_sweep
                and now.hour >= after_hours_sweep
                and not after_hours_done_today
            )

            if not in_market_window and not in_after_hours:
                _time.sleep(300)
                continue

            if in_after_hours:
                click.echo(f"\n[{now.strftime('%H:%M:%S')}] === AFTER-HOURS SWEEP ===")
                click.echo(f"  Catching everything since market close...")
                try:
                    run_full_pipeline(
                        lookback_minutes=480,
                        max_parallel=max_parallel,
                        prescreen=True,
                        alert_threshold=alert_threshold,
                        model=model,
                    )
                    scan_unanalyzed(
                        lookback_hours=12,
                        max_parallel=max_parallel,
                        prescreen=True,
                        model=model,
                    )
                except Exception as e:
                    logger.error("After-hours sweep failed: %s", e)
                    click.echo(f"  ERROR: {e}")
                after_hours_done_today = True
                # Sync telemetry to S3 at end of day
                try:
                    from cli.telemetry import sync_telemetry_to_s3
                    uploaded = sync_telemetry_to_s3()
                    if uploaded:
                        click.echo(f"  Synced {uploaded} telemetry file(s) to S3")
                except Exception:
                    pass
                click.echo(f"[{datetime.now().strftime('%H:%M:%S')}] After-hours sweep complete.")
                _time.sleep(poll_interval)
                continue

            # Regular market hours scan with adaptive cadence
            current_cadence = _market_cadence(now)
            click.echo(f"\n[{now.strftime('%H:%M:%S')}] Scan cycle (cadence: {current_cadence}s)...")
            try:
                run_full_pipeline(
                    lookback_minutes=max(current_cadence // 60 + 30, 60),
                    max_parallel=max_parallel,
                    prescreen=True,
                    alert_threshold=alert_threshold,
                    model=model,
                )
                scan_unanalyzed(
                    lookback_hours=4,
                    max_parallel=max_parallel,
                    prescreen=True,
                    model=model,
                )
            except Exception as e:
                logger.error("Scan cycle failed: %s", e)
                click.echo(f"  ERROR: {e}")

            click.echo(f"\n[{datetime.now().strftime('%H:%M:%S')}] Sleeping {current_cadence}s until next cycle...")
            _time.sleep(current_cadence)

    except KeyboardInterrupt:
        click.echo("\nScanner daemon stopped.")


@scanner.command("schedule")
def scanner_schedule():
    """Install the launchd plist to run the scanner daemon continuously."""
    import shutil as _shutil
    from cli.config_utils import find_repo_root

    plist_name = "com.praxis.scanner.plist"
    source = find_repo_root() / plist_name
    if not source.exists():
        click.echo(f"Plist not found at {source}")
        return

    dest_dir = Path.home() / "Library" / "LaunchAgents"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / plist_name

    _shutil.copy2(source, dest)
    click.echo(f"Installed plist to {dest}")

    result = subprocess.run(["launchctl", "load", str(dest)], capture_output=True, text=True)
    if result.returncode == 0:
        click.echo("Loaded into launchd. Scanner daemon is now running.")
    else:
        click.echo(f"launchctl load failed: {result.stderr}")


@scanner.command("unschedule")
def scanner_unschedule():
    """Remove the scanner launchd plist."""
    plist_name = "com.praxis.scanner.plist"
    dest = Path.home() / "Library" / "LaunchAgents" / plist_name

    if not dest.exists():
        click.echo("No plist found. Not currently scheduled.")
        return

    subprocess.run(["launchctl", "unload", str(dest)], capture_output=True, text=True)
    dest.unlink()
    click.echo("Unloaded and removed scanner plist.")
