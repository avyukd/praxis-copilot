"""Interactive helpers for praxis pipeline day browsing."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import date
from shutil import get_terminal_size

import click

from cli.pipeline_status import PipelineItem, PipelineTrace, build_pipeline_trace
from cli.s3 import download_file


def trace_payload(trace: PipelineTrace) -> dict:
    return {
        "source_type": trace.source_type,
        "key_prefix": trace.key_prefix,
        "item_id": trace.item_id,
        "ticker": trace.ticker,
        "cik": trace.cik,
        "form_type": trace.form_type,
        "source": trace.source,
        "stage": trace.stage,
        "files": trace.files,
        "arrived_at": trace.arrived_at,
        "extracted_at": trace.extracted_at,
        "analyzed_at": trace.analyzed_at,
        "screening_at": trace.screening_at,
        "alert_sent_at": trace.alert_sent_at,
        "analysis": {
            "classification": trace.analysis_classification,
            "magnitude": trace.analysis_magnitude,
            "summary": trace.analysis_summary,
        },
        "extracted": {
            "total_chars": trace.extracted_total_chars,
            "items": trace.extracted_items,
        },
    }


def build_interactive_trace_text(s3_client, trace: PipelineTrace) -> str:
    payload = trace_payload(trace)
    lines = [
        f"Pipeline trace for id={trace.item_id}",
        "=" * 80,
        json.dumps(payload, indent=2),
        "",
        "Artifacts:",
    ]

    for name in ("index.json", "extracted.json", "screening.json", "analysis.json"):
        if name not in trace.files:
            continue
        key = f"{trace.key_prefix}/{name}"
        lines.append("")
        lines.append("-" * 80)
        lines.append(name)
        lines.append("-" * 80)
        try:
            body = download_file(s3_client, key).decode("utf-8", errors="replace")
            lines.append(body.rstrip())
        except Exception as exc:
            lines.append(f"(unable to read {key}: {exc})")

    return "\n".join(lines).rstrip() + "\n"


def open_text_in_pager(text: str) -> None:
    less_bin = shutil.which("less")
    if less_bin:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, suffix=".txt") as tmp:
            tmp.write(text)
            tmp_path = tmp.name
        try:
            subprocess.run([less_bin, "-R", tmp_path], check=False)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        return

    click.echo_via_pager(text)


def _truncate(value: str, width: int) -> str:
    if width <= 0:
        return ""
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    return value[: width - 3] + "..."


def _format_selected_details(item: PipelineItem) -> list[str]:
    return [
        f"stage: {item.stage}",
        f"type: {item.source_type}",
        f"ticker: {item.ticker or '-'}",
        f"item_id: {item.item_id}",
        f"age_minutes: {item.age_minutes}",
        f"form: {item.form_type or '-'}",
        f"source: {item.source or '-'}",
        f"cik: {item.cik or '-'}",
        f"prefix: {item.key_prefix}",
    ]


def _viewport_bounds(total_items: int, selected_index: int, window_size: int) -> tuple[int, int]:
    if total_items <= 0:
        return 0, 0
    window_size = max(1, window_size)
    start = max(0, min(selected_index - (window_size // 2), total_items - window_size))
    end = min(total_items, start + window_size)
    return start, end


def _read_navigation_key() -> str:
    ch = click.getchar()
    if ch == "\x1b":
        next_ch = click.getchar()
        if next_ch != "[":
            return "escape"
        final_ch = click.getchar()
        if final_ch in {"5", "6"}:
            tail = click.getchar()
            if tail == "~":
                return {"5": "page_up", "6": "page_down"}[final_ch]
            return "escape"
        return {
            "A": "up",
            "B": "down",
        }.get(final_ch, "escape")
    if ch in {"k", "K"}:
        return "up"
    if ch in {"j", "J"}:
        return "down"
    if ch in {"g"}:
        return "home"
    if ch in {"G"}:
        return "end"
    if ch in {"q", "Q"}:
        return "quit"
    if ch in {"\r", "\n"}:
        return "open"
    if ch == " ":
        return "page_down"
    return "unknown"


def _move_selection(selected_index: int, key: str, total_items: int, page_size: int) -> int:
    if total_items <= 0:
        return 0

    max_index = total_items - 1
    if key == "up":
        return max(0, selected_index - 1)
    if key == "down":
        return min(max_index, selected_index + 1)
    if key == "page_up":
        return max(0, selected_index - max(1, page_size))
    if key == "page_down":
        return min(max_index, selected_index + max(1, page_size))
    if key == "home":
        return 0
    if key == "end":
        return max_index
    return selected_index


def _render_day_view(items: list[PipelineItem], target_day: date, source: str, selected_index: int) -> str:
    terminal_size = get_terminal_size((120, 40))
    width = max(80, terminal_size.columns)
    height = max(24, terminal_size.lines)
    list_height = max(5, height - 18)
    start, end = _viewport_bounds(len(items), selected_index, list_height)

    lines = [
        f"Praxis Pipeline Interactive  {target_day.isoformat()} ET  source={source}",
        "Navigate: up/down arrows or j/k, Enter=open, g/G=top/bottom, Space/PgDn, q=quit",
        "",
    ]

    if not items:
        lines.append("No filings/releases found for this day and source.")
        return "\n".join(lines)

    header = f"{' ':2} {'stage':13} {'type':14} {'ticker':8} {'item_id':28} {'age_m':>6}"
    lines.append(_truncate(header, width))
    lines.append("-" * min(width, len(header)))

    for idx in range(start, end):
        item = items[idx]
        marker = ">" if idx == selected_index else " "
        row = (
            f"{marker} "
            f"{item.stage[:13]:13} "
            f"{item.source_type[:14]:14} "
            f"{(item.ticker or '-')[:8]:8} "
            f"{item.item_id[:28]:28} "
            f"{item.age_minutes:>6}"
        )
        lines.append(_truncate(row, width))

    lines.append("")
    lines.append("-" * min(width, 80))
    lines.extend(_truncate(line, width) for line in _format_selected_details(items[selected_index]))
    if start > 0 or end < len(items):
        lines.append("")
        lines.append(_truncate(f"Showing {start + 1}-{end} of {len(items)}", width))

    return "\n".join(lines)


def interactive_pipeline_day_view(s3_client, items: list[PipelineItem], target_day: date, source: str) -> None:
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise click.ClickException("Interactive mode requires a TTY (run from a terminal).")

    if not items:
        click.clear()
        click.echo(_render_day_view(items, target_day, source, selected_index=0))
        click.pause(info="Press any key to exit")
        return

    selected_index = 0
    while True:
        terminal_size = get_terminal_size((120, 40))
        page_size = max(1, terminal_size.lines - 18)
        click.clear()
        click.echo(_render_day_view(items, target_day, source, selected_index))
        key = _read_navigation_key()
        if key == "quit":
            return
        if key == "open":
            item = items[selected_index]
            trace = build_pipeline_trace(s3_client, source_type=item.source_type, key_prefix=item.key_prefix)
            open_text_in_pager(build_interactive_trace_text(s3_client, trace))
            continue
        if key == "unknown":
            continue
        selected_index = _move_selection(selected_index, key, len(items), page_size=page_size)
