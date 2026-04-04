"""Thesis-driven monitoring — auto-extract watches from research memos.

Parses memo.yaml invalidation conditions and key_assumptions into
actionable monitoring targets. Three watch types:

- price_level: alert when price crosses a specific level (FREE, no LLM)
- news_watch: periodic haiku scan for keywords (CHEAP)
- event_watch: track upcoming catalysts with proximity ramping
- context: free-text thesis context for analyst investigations

Auto-created when research completes. No manual approval needed.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import click
import yaml
from pydantic import BaseModel, Field

from cli.config_utils import find_repo_root, get_config_dir

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ThesisWatch(BaseModel):
    ticker: str
    source: str = "memo"
    memo_date: str = ""
    type: str = "context"  # price_level, news_watch, event_watch, context
    description: str = ""
    urgency: str = "medium"  # low, medium, high

    # price_level fields
    level: float | None = None
    direction: str = ""  # above, below

    # news_watch fields
    keywords: list[str] = Field(default_factory=list)
    check_cadence: str = "daily"

    # event_watch fields
    event_type: str = ""
    expected_date: str = ""
    ramp_days: int = 14

    # context fields
    text: str = ""


class ExposureFactor(BaseModel):
    name: str
    tickers: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


def _watches_path() -> Path:
    return get_config_dir() / "thesis_watches.yaml"


def _exposure_path() -> Path:
    return get_config_dir() / "exposure_map.yaml"


def load_watches() -> list[ThesisWatch]:
    path = _watches_path()
    if not path.exists():
        return []
    try:
        raw = yaml.safe_load(path.read_text()) or {}
        return [ThesisWatch(**w) for w in raw.get("watches", [])]
    except Exception:
        return []


def save_watches(watches: list[ThesisWatch]) -> None:
    path = _watches_path()
    # Deduplicate by ticker + type + description
    seen = set()
    unique = []
    for w in watches:
        key = f"{w.ticker}_{w.type}_{w.description[:50]}"
        if key not in seen:
            seen.add(key)
            unique.append(w)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"watches": [w.model_dump() for w in unique]}
    path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


def load_exposure_map() -> list[ExposureFactor]:
    path = _exposure_path()
    if not path.exists():
        return []
    try:
        raw = yaml.safe_load(path.read_text()) or {}
        return [ExposureFactor(**f) for f in raw.get("factors", [])]
    except Exception:
        return []


def save_exposure_map(factors: list[ExposureFactor]) -> None:
    path = _exposure_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"factors": [f.model_dump() for f in factors]}
    path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


# ---------------------------------------------------------------------------
# Extraction from memos
# ---------------------------------------------------------------------------

# Patterns for price level extraction
PRICE_PATTERN = re.compile(
    r"(?:at|below|above|under|over|exceeds?|drops?\s+(?:below|to))\s+"
    r"\$?([\d,]+(?:\.\d+)?)",
    re.IGNORECASE,
)

# Factor keywords for exposure map
FACTOR_KEYWORDS = {
    "oil_energy": {
        "keywords": ["oil", "crude", "WTI", "Brent", "natural gas", "LNG", "energy services",
                      "oilfield", "frac", "drilling", "petroleum"],
        "name": "oil_energy",
    },
    "interest_rates": {
        "keywords": ["interest rate", "Fed", "Federal Reserve", "yield curve", "NIM",
                      "net interest margin", "rate cut", "rate hike", "FOMC", "monetary policy"],
        "name": "interest_rates",
    },
    "china_trade": {
        "keywords": ["China", "Chinese", "CHIPS Act", "tariff", "trade war", "semiconductor",
                      "supply chain", "reshoring", "onshoring"],
        "name": "china_trade",
    },
    "gold_precious": {
        "keywords": ["gold", "silver", "precious metals", "mining", "ounce", "bullion",
                      "exploration", "drill results"],
        "name": "gold_precious",
    },
    "biotech_fda": {
        "keywords": ["FDA", "PDUFA", "NDA", "BLA", "Phase 2", "Phase 3", "clinical trial",
                      "drug approval", "pivotal trial", "breakthrough therapy"],
        "name": "biotech_fda",
    },
    "rin_biofuel": {
        "keywords": ["RIN", "RVO", "renewable fuel", "biofuel", "EPA", "ethanol",
                      "biodiesel", "renewable diesel"],
        "name": "rin_biofuel",
    },
    "real_estate": {
        "keywords": ["real estate", "REIT", "mortgage", "housing", "property",
                      "occupancy", "rent", "lease"],
        "name": "real_estate",
    },
    "crypto_digital": {
        "keywords": ["bitcoin", "crypto", "blockchain", "digital asset", "mining rig"],
        "name": "crypto_digital",
    },
}


def extract_watches_from_memo(ticker: str, memo: dict) -> list[ThesisWatch]:
    """Extract monitoring targets from a memo.yaml dict.

    Deterministic — no LLM calls. Returns list of ThesisWatch objects.
    """
    watches: list[ThesisWatch] = []
    raw_date = memo.get("date", date.today().isoformat())
    memo_date = str(raw_date) if raw_date else date.today().isoformat()
    valuation = memo.get("valuation", {}) or {}
    invalidation = valuation.get("invalidation", []) or []
    key_assumptions = valuation.get("key_assumptions", []) or []

    all_conditions = invalidation + key_assumptions

    for condition in all_conditions:
        if not isinstance(condition, str):
            continue

        is_invalidation = condition in invalidation
        urgency = "high" if is_invalidation else "medium"

        # Try to extract price level
        price_match = PRICE_PATTERN.search(condition)
        if price_match:
            try:
                level = float(price_match.group(1).replace(",", ""))
                direction = "below" if any(w in condition.lower() for w in ["below", "under", "drops"]) else "above"
                watches.append(ThesisWatch(
                    ticker=ticker,
                    memo_date=memo_date,
                    type="price_level",
                    level=level,
                    direction=direction,
                    description=condition,
                    urgency=urgency,
                ))
                continue
            except ValueError:
                pass

        # Check for FDA/clinical event
        if any(kw in condition.lower() for kw in ["fda", "pdufa", "nda", "bla", "approval", "phase 2", "phase 3", "clinical", "pivotal"]):
            # Try to extract date
            date_match = re.search(r"(Q[1-4]\s*20\d{2}|20\d{2}-\d{2}(?:-\d{2})?|H[12]\s*20\d{2})", condition)
            expected = ""
            if date_match:
                raw_date = date_match.group(1)
                expected = _normalize_date(raw_date)

            event_type = "fda_decision" if any(w in condition.lower() for w in ["fda", "pdufa", "approval"]) else "clinical_data"
            watches.append(ThesisWatch(
                ticker=ticker,
                memo_date=memo_date,
                type="event_watch",
                event_type=event_type,
                expected_date=expected,
                description=condition,
                urgency=urgency,
                ramp_days=14,
            ))
            continue

        # Check for earnings/financial threshold
        if any(kw in condition.lower() for kw in ["revenue", "earnings", "ebitda", "eps", "growth", "margin"]):
            watches.append(ThesisWatch(
                ticker=ticker,
                memo_date=memo_date,
                type="context",
                text=condition,
                description=f"Financial threshold: {condition}",
                urgency=urgency,
            ))
            continue

        # Check for equity/dilution
        if any(kw in condition.lower() for kw in ["equity", "dilution", "offering", "share", "warrant", "convertible"]):
            watches.append(ThesisWatch(
                ticker=ticker,
                memo_date=memo_date,
                type="news_watch",
                keywords=[ticker, "offering", "dilution", "equity raise"],
                description=condition,
                urgency=urgency,
                check_cadence="daily",
            ))
            continue

        # Check for commodity/macro keywords
        has_commodity = False
        for factor_id, factor_info in FACTOR_KEYWORDS.items():
            if any(kw.lower() in condition.lower() for kw in factor_info["keywords"]):
                watches.append(ThesisWatch(
                    ticker=ticker,
                    memo_date=memo_date,
                    type="news_watch",
                    keywords=[kw for kw in factor_info["keywords"] if kw.lower() in condition.lower()][:3],
                    description=condition,
                    urgency=urgency,
                    check_cadence="daily",
                ))
                has_commodity = True
                break

        if has_commodity:
            continue

        # Default: store as context (free-text for analyst)
        watches.append(ThesisWatch(
            ticker=ticker,
            memo_date=memo_date,
            type="context",
            text=condition,
            description=condition,
            urgency=urgency,
        ))

    return watches


def _normalize_date(raw: str) -> str:
    """Convert 'Q3 2026' or 'H2 2026' to approximate YYYY-MM-DD."""
    raw = raw.strip()
    q_match = re.match(r"Q([1-4])\s*(\d{4})", raw)
    if q_match:
        q, year = int(q_match.group(1)), q_match.group(2)
        month = {1: "03", 2: "06", 3: "09", 4: "12"}[q]
        return f"{year}-{month}-15"
    h_match = re.match(r"H([12])\s*(\d{4})", raw)
    if h_match:
        h, year = int(h_match.group(1)), h_match.group(2)
        month = "06" if h == 1 else "12"
        return f"{year}-{month}-15"
    return raw


# ---------------------------------------------------------------------------
# Exposure map
# ---------------------------------------------------------------------------


def build_exposure_map() -> list[ExposureFactor]:
    """Build factor exposure map from all workspace memos."""
    repo_root = find_repo_root()
    workspace = repo_root / "workspace"

    factor_tickers: dict[str, set[str]] = {}

    for ticker_dir in workspace.iterdir():
        if not ticker_dir.is_dir() or ticker_dir.name in ("queue", "analyst", "macro"):
            continue
        memo_path = ticker_dir / "memo.yaml"
        if not memo_path.exists():
            continue

        try:
            memo = yaml.safe_load(memo_path.read_text()) or {}
        except Exception:
            continue

        # Combine thesis + assumptions for keyword matching
        text = " ".join([
            memo.get("thesis_summary", ""),
            " ".join(memo.get("valuation", {}).get("key_assumptions", []) or []),
        ]).lower()

        for factor_id, factor_info in FACTOR_KEYWORDS.items():
            if any(kw.lower() in text for kw in factor_info["keywords"]):
                if factor_id not in factor_tickers:
                    factor_tickers[factor_id] = set()
                factor_tickers[factor_id].add(ticker_dir.name)

    factors = []
    for factor_id, tickers in sorted(factor_tickers.items()):
        info = FACTOR_KEYWORDS[factor_id]
        factors.append(ExposureFactor(
            name=info["name"],
            tickers=sorted(tickers),
            keywords=info["keywords"],
        ))

    return factors


# ---------------------------------------------------------------------------
# Backfill from existing memos
# ---------------------------------------------------------------------------


def refresh_all_watches() -> tuple[int, int]:
    """Re-extract watches from all workspace memos. Returns (tickers, watches)."""
    repo_root = find_repo_root()
    workspace = repo_root / "workspace"
    all_watches: list[ThesisWatch] = []
    ticker_count = 0

    for ticker_dir in workspace.iterdir():
        if not ticker_dir.is_dir() or ticker_dir.name in ("queue", "analyst", "macro"):
            continue
        memo_path = ticker_dir / "memo.yaml"
        if not memo_path.exists():
            continue

        try:
            memo = yaml.safe_load(memo_path.read_text()) or {}
        except Exception:
            continue

        watches = extract_watches_from_memo(ticker_dir.name, memo)
        if watches:
            all_watches.extend(watches)
            ticker_count += 1

    save_watches(all_watches)

    # Also rebuild exposure map
    factors = build_exposure_map()
    save_exposure_map(factors)

    return ticker_count, len(all_watches)


def get_watches_for_ticker(ticker: str) -> list[ThesisWatch]:
    """Get all active watches for a ticker."""
    watches = load_watches()
    return [w for w in watches if w.ticker.upper() == ticker.upper()]


def get_price_watches() -> list[ThesisWatch]:
    """Get all price-level watches for the fast loop."""
    watches = load_watches()
    return [w for w in watches if w.type == "price_level" and w.level is not None]


def get_news_watches() -> list[ThesisWatch]:
    """Get all news watches for the medium loop."""
    watches = load_watches()
    return [w for w in watches if w.type == "news_watch" and w.keywords]


def get_event_watches() -> list[ThesisWatch]:
    """Get all event watches."""
    watches = load_watches()
    return [w for w in watches if w.type == "event_watch"]


def get_exposed_tickers(factor_name: str) -> list[str]:
    """Get tickers exposed to a specific factor."""
    factors = load_exposure_map()
    for f in factors:
        if f.name == factor_name:
            return f.tickers
    return []


# ---------------------------------------------------------------------------
# Email notification for new watches
# ---------------------------------------------------------------------------


def notify_new_watches(ticker: str, watches: list[ThesisWatch]) -> None:
    """Email notification when thesis watches are created."""
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not topic_arn or not watches:
        return

    price_watches = [w for w in watches if w.type == "price_level"]
    news_watches = [w for w in watches if w.type == "news_watch"]
    event_watches = [w for w in watches if w.type == "event_watch"]
    context_watches = [w for w in watches if w.type == "context"]

    subject = f"[PRAXIS WATCHES] {ticker}: {len(watches)} monitors created"
    lines = [f"Thesis monitors auto-created for {ticker}:\n"]

    if price_watches:
        lines.append(f"PRICE LEVELS ({len(price_watches)}):")
        for w in price_watches:
            lines.append(f"  • Alert if {w.direction} ${w.level:.2f}: {w.description[:80]}")

    if news_watches:
        lines.append(f"\nNEWS WATCHES ({len(news_watches)}):")
        for w in news_watches:
            lines.append(f"  • Keywords: {', '.join(w.keywords[:3])}: {w.description[:80]}")

    if event_watches:
        lines.append(f"\nEVENT WATCHES ({len(event_watches)}):")
        for w in event_watches:
            date_str = f" ({w.expected_date})" if w.expected_date else ""
            lines.append(f"  • {w.event_type}{date_str}: {w.description[:80]}")

    if context_watches:
        lines.append(f"\nCONTEXT ({len(context_watches)}):")
        for w in context_watches:
            lines.append(f"  • {w.description[:80]}")

    body = "\n".join(lines)

    try:
        import boto3
        sns = boto3.client("sns", region_name="us-east-1")
        sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=body)
    except Exception as e:
        logger.error("Watch notification failed: %s", e)


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


@click.group("watches")
def watches_cli():
    """Thesis-driven monitoring — auto-extracted from research memos."""
    pass


@watches_cli.command("list")
@click.option("--ticker", "-t", default=None, help="Filter by ticker")
@click.option("--type", "watch_type", default=None, help="Filter by type (price_level, news_watch, event_watch, context)")
def watches_list(ticker: str | None, watch_type: str | None):
    """List active thesis watches."""
    watches = load_watches()
    if ticker:
        watches = [w for w in watches if w.ticker.upper() == ticker.upper()]
    if watch_type:
        watches = [w for w in watches if w.type == watch_type]

    if not watches:
        click.echo("No thesis watches found. Run 'praxis watches refresh' to extract from memos.")
        return

    click.echo(f"Thesis watches ({len(watches)}):\n")

    by_type = {}
    for w in watches:
        by_type.setdefault(w.type, []).append(w)

    for wtype, type_watches in sorted(by_type.items()):
        click.echo(f"  {wtype.upper()} ({len(type_watches)}):")
        for w in type_watches:
            extra = ""
            if w.type == "price_level":
                extra = f" | {w.direction} ${w.level:.2f}"
            elif w.type == "news_watch":
                extra = f" | keywords: {', '.join(w.keywords[:3])}"
            elif w.type == "event_watch":
                extra = f" | {w.event_type} {w.expected_date or ''}"
            click.echo(f"    [{w.ticker}] {w.description[:60]}{extra}")
        click.echo()


@watches_cli.command("refresh")
def watches_refresh():
    """Re-extract watches from all workspace memos."""
    click.echo("Extracting thesis watches from memos...")
    tickers, count = refresh_all_watches()
    click.echo(f"Extracted {count} watches from {tickers} tickers.")
    click.echo("Run 'praxis watches list' to see them.")


@watches_cli.command("exposure")
def watches_exposure():
    """Show factor exposure map."""
    factors = load_exposure_map()
    if not factors:
        click.echo("No exposure map. Run 'praxis watches refresh' first.")
        return

    click.echo(f"Factor Exposure Map ({len(factors)} factors):\n")
    for f in factors:
        click.echo(f"  {f.name} ({len(f.tickers)} tickers):")
        click.echo(f"    {', '.join(f.tickers)}")
        click.echo()


@watches_cli.command("check")
def watches_check():
    """Run all price-level watches now."""
    from cli.entry_watchdog import _get_price

    price_watches = get_price_watches()
    if not price_watches:
        click.echo("No price-level watches.")
        return

    click.echo(f"Checking {len(price_watches)} price watches...")
    triggered = 0
    for w in price_watches:
        price = _get_price(w.ticker)
        if price is None:
            continue

        hit = False
        if w.direction == "below" and price <= w.level:
            hit = True
        elif w.direction == "above" and price >= w.level:
            hit = True

        if hit:
            triggered += 1
            click.echo(f"  ⚠ {w.ticker} at ${price:.2f} — {w.direction} ${w.level:.2f}: {w.description[:60]}")

    click.echo(f"\n{triggered} watch(es) triggered.")
