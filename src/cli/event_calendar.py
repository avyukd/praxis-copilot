"""Shared event calendar — earnings, FDA dates, investor days, conferences.

Persists to config/events.yaml. Any Claude session can read/write events.
The analyst agent checks this when investigating price moves.

Events are also surfaced in CLAUDE.md for research sessions so they
know about upcoming catalysts.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import click
import yaml
from pydantic import BaseModel, Field

from cli.config_utils import get_config_dir

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

EVENT_TYPES = [
    "earnings",
    "clinical_data",
    "fda_decision",
    "investor_day",
    "conference",
    "ex_dividend",
    "lockup_expiry",
    "court_hearing",
    "shareholder_vote",
    "regulatory",
    "product_launch",
    "contract_deadline",
    "other",
]


class CalendarEvent(BaseModel):
    ticker: str
    date: str  # YYYY-MM-DD
    type: str = "other"
    description: str = ""
    source: str = ""  # who added it: analyst_agent, filing_research, manual, etc
    added_at: str = Field(default_factory=lambda: date.today().isoformat())


class EventCalendar(BaseModel):
    events: list[CalendarEvent] = Field(default_factory=list)


def _calendar_path() -> Path:
    return get_config_dir() / "events.yaml"


def load_calendar() -> EventCalendar:
    path = _calendar_path()
    if path.exists():
        try:
            raw = yaml.safe_load(path.read_text()) or {}
            events_raw = raw.get("events", [])
            events = [CalendarEvent(**e) if isinstance(e, dict) else e for e in events_raw]
            return EventCalendar(events=events)
        except Exception:
            return EventCalendar()
    return EventCalendar()


def save_calendar(cal: EventCalendar) -> None:
    path = _calendar_path()
    # Sort by date
    cal.events.sort(key=lambda e: e.date)
    # Deduplicate
    seen = set()
    unique = []
    for e in cal.events:
        key = f"{e.ticker}_{e.date}_{e.type}_{e.description[:50]}"
        if key not in seen:
            seen.add(key)
            unique.append(e)
    cal.events = unique
    data = {"events": [e.model_dump() for e in cal.events]}
    path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


def add_event(
    ticker: str,
    event_date: str,
    event_type: str = "other",
    description: str = "",
    source: str = "manual",
) -> CalendarEvent:
    """Add an event to the calendar. Deduplicates automatically."""
    cal = load_calendar()
    event = CalendarEvent(
        ticker=ticker.upper(),
        date=event_date,
        type=event_type,
        description=description,
        source=source,
    )
    cal.events.append(event)
    save_calendar(cal)
    return event


def get_events_for_ticker(ticker: str, days_ahead: int = 30) -> list[CalendarEvent]:
    """Get upcoming events for a ticker within the next N days."""
    cal = load_calendar()
    today = date.today()
    cutoff = date(today.year, today.month, today.day)

    results = []
    for e in cal.events:
        if e.ticker.upper() != ticker.upper():
            continue
        try:
            event_date = date.fromisoformat(e.date)
            days_until = (event_date - cutoff).days
            if -7 <= days_until <= days_ahead:  # Include events up to 7 days ago (recent)
                results.append(e)
        except ValueError:
            continue
    return results


def get_upcoming_events(days_ahead: int = 7) -> list[CalendarEvent]:
    """Get all upcoming events within the next N days."""
    cal = load_calendar()
    today = date.today()
    cutoff = date(today.year, today.month, today.day)

    results = []
    for e in cal.events:
        try:
            event_date = date.fromisoformat(e.date)
            days_until = (event_date - cutoff).days
            if 0 <= days_until <= days_ahead:
                results.append(e)
        except ValueError:
            continue
    return sorted(results, key=lambda e: e.date)


def format_events_for_prompt(ticker: str) -> str:
    """Format upcoming events as context for a Claude research prompt."""
    events = get_events_for_ticker(ticker, days_ahead=60)
    if not events:
        return ""

    lines = [f"\nUPCOMING EVENTS FOR {ticker}:"]
    for e in events:
        try:
            event_date = date.fromisoformat(e.date)
            days_until = (event_date - date.today()).days
            timing = f"in {days_until} days" if days_until > 0 else "TODAY" if days_until == 0 else f"{abs(days_until)} days ago"
        except ValueError:
            timing = e.date
        lines.append(f"  • {e.date} ({timing}): {e.type.upper()} — {e.description}")
    return "\n".join(lines)


def format_all_upcoming_for_prompt(days_ahead: int = 14) -> str:
    """Format all upcoming events for the analyst agent."""
    events = get_upcoming_events(days_ahead)
    if not events:
        return ""

    lines = [f"\nEVENT CALENDAR (next {days_ahead} days):"]
    current_date = ""
    for e in events:
        if e.date != current_date:
            current_date = e.date
            try:
                dt = date.fromisoformat(e.date)
                days_until = (dt - date.today()).days
                day_label = "TODAY" if days_until == 0 else f"in {days_until}d"
            except ValueError:
                day_label = ""
            lines.append(f"\n  {e.date} ({day_label}):")
        lines.append(f"    [{e.ticker}] {e.type}: {e.description}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


@click.group("events")
def events_cli():
    """Event calendar — track earnings, FDA dates, catalysts."""
    pass


@events_cli.command("add")
@click.argument("ticker")
@click.argument("event_date")
@click.option("--type", "event_type", type=click.Choice(EVENT_TYPES), default="other", help="Event type")
@click.option("--desc", "-d", default="", help="Description")
def events_add(ticker: str, event_date: str, event_type: str, desc: str):
    """Add an event to the calendar.

    \b
    Examples:
      praxis events add IMMX 2026-09-30 --type clinical_data -d "NEXICART-2 topline results"
      praxis events add AAPL 2026-07-31 --type earnings -d "Q3 FY2026 earnings"
      praxis events add CLMT 2026-04-15 --type regulatory -d "EPA RVO final rule implementation"
    """
    event = add_event(ticker, event_date, event_type, desc, source="manual")
    click.echo(f"Added: [{event.ticker}] {event.date} {event.type}: {event.description}")


@events_cli.command("list")
@click.option("--ticker", "-t", default=None, help="Filter by ticker")
@click.option("--days", type=int, default=30, help="Days ahead to show")
def events_list(ticker: str | None, days: int):
    """Show upcoming events.

    \b
    Examples:
      praxis events list
      praxis events list --ticker IMMX
      praxis events list --days 7
    """
    if ticker:
        events = get_events_for_ticker(ticker, days)
    else:
        events = get_upcoming_events(days)

    if not events:
        click.echo("No upcoming events.")
        return

    click.echo(f"Upcoming events ({len(events)}):")
    for e in events:
        try:
            days_until = (date.fromisoformat(e.date) - date.today()).days
            timing = f"in {days_until}d" if days_until > 0 else "TODAY" if days_until == 0 else f"{abs(days_until)}d ago"
        except ValueError:
            timing = "?"
        click.echo(f"  {e.date} ({timing}) [{e.ticker}] {e.type}: {e.description}")


@events_cli.command("remove")
@click.argument("ticker")
@click.argument("event_date")
def events_remove(ticker: str, event_date: str):
    """Remove an event by ticker and date."""
    cal = load_calendar()
    before = len(cal.events)
    cal.events = [e for e in cal.events if not (e.ticker.upper() == ticker.upper() and e.date == event_date)]
    after = len(cal.events)
    save_calendar(cal)
    removed = before - after
    click.echo(f"Removed {removed} event(s) for {ticker} on {event_date}")
