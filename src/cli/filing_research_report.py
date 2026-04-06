"""Generate a self-contained HTML dashboard for filing research results."""

from __future__ import annotations

import html
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import click
import pandas as pd
import requests
import yaml

from cli.config_utils import find_repo_root
from cli.filing_research import (
    SKIP_DECISIONS,
    FilingDecision,
    FilingResearchState,
    TrackedFiling,
    _load_state,
)
from cli.market_data import EODHD_BASE, get_eodhd_api_key, to_eodhd_symbol
from cli.s3 import download_file, get_s3_client as _get_s3

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Price data
# ---------------------------------------------------------------------------


def _fetch_ohlc_eodhd(ticker: str, days: int = 30) -> list[dict] | None:
    """Fetch daily OHLC from EODHD. Returns None on failure."""
    try:
        api_key = get_eodhd_api_key()
    except Exception:
        return None
    try:
        end = date.today()
        start = end - timedelta(days=days)
        resp = requests.get(
            f"{EODHD_BASE}/eod/{to_eodhd_symbol(ticker)}",
            params={
                "api_token": api_key,
                "period": "d",
                "from": start.isoformat(),
                "to": end.isoformat(),
                "fmt": "json",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or not data:
            return None
        return [
            {
                "date": d["date"],
                "open": d.get("open"),
                "high": d.get("high"),
                "low": d.get("low"),
                "close": d.get("adjusted_close", d.get("close")),
                "volume": d.get("volume"),
            }
            for d in data
        ]
    except Exception:
        logger.debug("EODHD fetch failed for %s", ticker, exc_info=True)
        return None


def _fetch_ohlc_yfinance(ticker: str, days: int = 30) -> list[dict] | None:
    """Fetch daily OHLC from yfinance. Returns None on failure."""
    try:
        import yfinance as yf

        end = date.today()
        start = end - timedelta(days=days)
        df = yf.download(ticker, start=start.isoformat(), end=end.isoformat(), progress=False)
        if df is None or df.empty:
            return None
        # Flatten multi-level columns from newer yfinance versions
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        results = []
        for idx, row in df.iterrows():
            results.append(
                {
                    "date": idx.strftime("%Y-%m-%d"),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row["Volume"]),
                }
            )
        return results if results else None
    except Exception:
        logger.debug("yfinance fetch failed for %s", ticker, exc_info=True)
        return None


def fetch_ohlc(ticker: str, days: int = 30) -> list[dict]:
    """Fetch OHLC with fallback chain: EODHD -> yfinance -> empty."""
    data = _fetch_ohlc_eodhd(ticker, days)
    if data:
        return data
    data = _fetch_ohlc_yfinance(ticker, days)
    if data:
        return data
    return []


def fetch_all_ohlc(tickers: list[str], days: int = 30) -> dict[str, list[dict]]:
    """Fetch OHLC for multiple tickers in parallel, with local cache."""
    cache_path = find_repo_root() / "data" / "ohlc_cache.yaml"
    cache: dict[str, dict] = {}
    today = date.today().isoformat()

    # Load cache
    if cache_path.exists():
        try:
            cache = yaml.safe_load(cache_path.read_text()) or {}
        except Exception:
            cache = {}

    results: dict[str, list[dict]] = {}
    tickers_to_fetch: list[str] = []

    for t in tickers:
        cached = cache.get(t)
        if cached and cached.get("date") == today and cached.get("data"):
            results[t] = cached["data"]
        else:
            tickers_to_fetch.append(t)

    if tickers_to_fetch:
        # Serialize fetches — yfinance is not thread-safe and returns
        # corrupted data when multiple downloads run in parallel.
        for ticker in tickers_to_fetch:
            try:
                data = fetch_ohlc(ticker, days)
                results[ticker] = data
                cache[ticker] = {"date": today, "data": data}
            except Exception:
                results[ticker] = []

        # Save cache
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(yaml.dump(cache, default_flow_style=False))
        except Exception:
            pass

    return results


# ---------------------------------------------------------------------------
# SVG chart
# ---------------------------------------------------------------------------


def render_price_svg(ohlc: list[dict], width: int = 400, height: int = 120) -> str:
    """Render a self-contained SVG line chart of closing prices."""
    if not ohlc:
        return (
            f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">'
            f'<text x="{width // 2}" y="{height // 2}" text-anchor="middle" '
            f'fill="#666" font-size="13">No price data</text></svg>'
        )

    closes = [d["close"] for d in ohlc if d.get("close") is not None]
    if not closes:
        return (
            f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">'
            f'<text x="{width // 2}" y="{height // 2}" text-anchor="middle" '
            f'fill="#666" font-size="13">No price data</text></svg>'
        )

    n = len(closes)
    min_c, max_c = min(closes), max(closes)
    price_range = max_c - min_c if max_c != min_c else 1.0

    ml, mr, mt, mb = 55, 10, 15, 25  # margins
    cw = width - ml - mr
    ch = height - mt - mb

    def px(i: int) -> float:
        return ml + (i / max(n - 1, 1)) * cw

    def py(v: float) -> float:
        return mt + (1 - (v - min_c) / price_range) * ch

    points = " ".join(f"{px(i):.1f},{py(v):.1f}" for i, v in enumerate(closes))
    poly_points = points + f" {px(n - 1):.1f},{mt + ch:.1f} {ml:.1f},{mt + ch:.1f}"

    color = "#4ade80" if closes[-1] >= closes[0] else "#f87171"
    fill_opacity = "0.15"

    pct = ((closes[-1] - closes[0]) / closes[0]) * 100 if closes[0] else 0
    pct_sign = "+" if pct >= 0 else ""

    first_date = ohlc[0].get("date", "")
    last_date = ohlc[-1].get("date", "")

    svg = f"""\
<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
  <polygon points="{poly_points}" fill="{color}" fill-opacity="{fill_opacity}" />
  <polyline points="{points}" fill="none" stroke="{color}" stroke-width="1.5" />
  <text x="{ml - 5}" y="{mt + 4}" text-anchor="end" fill="#999" font-size="10">${max_c:,.2f}</text>
  <text x="{ml - 5}" y="{mt + ch}" text-anchor="end" fill="#999" font-size="10">${min_c:,.2f}</text>
  <text x="{ml}" y="{height - 4}" fill="#666" font-size="9">{first_date}</text>
  <text x="{width - mr}" y="{height - 4}" text-anchor="end" fill="#666" font-size="9">{last_date}</text>
  <text x="{width - mr}" y="{mt + 4}" text-anchor="end" fill="{color}" font-size="11" font-weight="bold">${closes[-1]:,.2f} ({pct_sign}{pct:.1f}%)</text>
</svg>"""
    return svg


# ---------------------------------------------------------------------------
# Memo loading
# ---------------------------------------------------------------------------


def _load_alert_analysis(key_prefix: str) -> dict | None:
    """Load the original PR/8-K analysis from S3."""
    try:
        import json
        s3 = _get_s3()
        analysis_key = f"{key_prefix}/analysis.json"
        raw = download_file(s3, analysis_key)
        return json.loads(raw)
    except Exception:
        logger.debug("Failed to load analysis for %s", key_prefix, exc_info=True)
        return None


def _load_all_analyses(filings: list[TrackedFiling]) -> dict[str, dict | None]:
    """Load alert analyses for all researched filings."""
    results: dict[str, dict | None] = {}
    for f in filings:
        results[f.ticker.upper()] = _load_alert_analysis(f.key_prefix)
    return results


def _load_memo_yaml(ticker: str) -> dict | None:
    from cli.memo_reader import read_memo_yaml
    return read_memo_yaml(ticker)


def _load_memo_md(ticker: str) -> str | None:
    from cli.memo_reader import read_memo_md
    return read_memo_md(ticker)
    return None


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------


_CLASSIFICATION_COLORS = {
    "BUY": "#4ade80",
    "SPECULATIVE_BUY": "#86efac",
    "SELL": "#f87171",
    "HOLD": "#fbbf24",
    "NEUTRAL": "#fbbf24",
    "TOO_HARD": "#9ca3af",
    "TOO HARD": "#9ca3af",
}


def _badge_color(classification: str) -> str:
    return _CLASSIFICATION_COLORS.get(classification.upper().strip(), "#9ca3af")


def _render_memo_html(memo_md: str | None) -> str:
    """Lightweight markdown to HTML for memo content."""
    if not memo_md:
        return '<p class="empty">No memo available</p>'

    # Apply inline formatting before escaping (we'll escape cell/text content individually)
    lines = memo_md.split("\n")
    out: list[str] = []
    in_table = False
    in_list = False
    is_first_header_row = False

    for line in lines:
        stripped = line.strip()

        # Table rows
        if stripped.startswith("|") and stripped.endswith("|"):
            cells = [c.strip() for c in stripped.strip("|").split("|")]
            # Skip separator rows (---|---|---)
            if all(re.match(r"^[-:]+$", c) for c in cells):
                is_first_header_row = False
                continue
            if not in_table:
                if in_list:
                    out.append("</ul>")
                    in_list = False
                out.append('<table class="memo-table">')
                in_table = True
                is_first_header_row = True
            tag = "th" if is_first_header_row else "td"
            escaped_cells = [_inline_fmt(html.escape(c)) for c in cells]
            out.append("<tr>" + "".join(f"<{tag}>{c}</{tag}>" for c in escaped_cells) + "</tr>")
            if is_first_header_row:
                is_first_header_row = False
            continue
        elif in_table:
            out.append("</table>")
            in_table = False

        escaped = html.escape(stripped)

        # Headers
        if stripped.startswith("#### "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h4>{_inline_fmt(html.escape(stripped[5:]))}</h4>")
        elif stripped.startswith("### "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h3>{_inline_fmt(html.escape(stripped[4:]))}</h3>")
        elif stripped.startswith("## "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h2>{_inline_fmt(html.escape(stripped[3:]))}</h2>")
        elif stripped.startswith("# "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h1>{_inline_fmt(html.escape(stripped[2:]))}</h1>")
        elif stripped == "---":
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append("<hr>")
        elif stripped.startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{_inline_fmt(html.escape(stripped[2:]))}</li>")
        elif stripped == "":
            if in_list:
                out.append("</ul>")
                in_list = False
        else:
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<p>{_inline_fmt(escaped)}</p>")

    if in_table:
        out.append("</table>")
    if in_list:
        out.append("</ul>")

    return "\n".join(out)


def _inline_fmt(text: str) -> str:
    """Apply bold/italic formatting to already-escaped text."""
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\*(.+?)\*", r"<em>\1</em>", text)
    return text


def _compute_adtv(ohlc: list[dict]) -> str | None:
    """Compute average daily trading volume (dollar volume) from OHLC data."""
    if not ohlc:
        return None
    dollar_vols = []
    for d in ohlc[-20:]:  # Last 20 trading days
        vol = d.get("volume")
        close = d.get("close")
        if vol and close:
            dollar_vols.append(vol * close)
    if not dollar_vols:
        return None
    adtv = sum(dollar_vols) / len(dollar_vols)
    if adtv >= 1_000_000:
        return f"${adtv / 1_000_000:.1f}M"
    if adtv >= 1_000:
        return f"${adtv / 1_000:.0f}K"
    return f"${adtv:.0f}"


def _render_filing_card(
    filing: TrackedFiling,
    rank: int,
    svg: str,
    memo_yaml: dict | None,
    memo_md: str | None,
    adtv: str | None = None,
    analysis: dict | None = None,
) -> str:
    ticker = html.escape(filing.ticker)
    form_type = html.escape(filing.form_type or "")
    classification = (memo_yaml or {}).get("decision", filing.classification) or filing.classification or ""
    badge_col = _badge_color(classification)
    mag = f"{filing.magnitude:.2f}" if filing.magnitude is not None else "—"

    thesis = html.escape(
        (memo_yaml or {}).get("thesis_summary", "") or filing.summary or ""
    )

    # Scores
    scores = (memo_yaml or {}).get("scores", {}) or {}
    tac_score = scores.get("tactical")
    fun_score = scores.get("fundamental")
    score_html = ""
    if tac_score is not None:
        score_html += f'<span class="score-tag tac-score" title="Tactical opportunity score (1-10)">Tactical: {tac_score}/10</span>'
    if fun_score is not None:
        score_html += f'<span class="score-tag fun-score" title="Fundamental case score (1-10)">Fundamental: {fun_score}/10</span>'

    # Valuation info
    valuation = (memo_yaml or {}).get("valuation", {}) or {}
    fv = valuation.get("fair_value_estimate")
    entry = valuation.get("entry_range", [None, None])
    val_html = ""
    if fv is not None:
        val_html += f'<span class="val-tag">FV: ${fv}</span>'
    if entry and entry[0] is not None:
        val_html += f'<span class="val-tag">Entry: ${entry[0]}–${entry[1]}</span>'

    # Tactical section
    tactical = (memo_yaml or {}).get("tactical", {}) or {}
    tactical_html = ""
    if tactical and tactical.get("setup"):
        tac_parts = []
        if tactical.get("setup"):
            tac_parts.append(f'<div class="tac-setup">{html.escape(tactical["setup"])}</div>')
        details = []
        if tactical.get("entry_trigger"):
            details.append(f'<span class="tac-detail"><b>Entry:</b> {html.escape(tactical["entry_trigger"])}</span>')
        if tactical.get("timeframe"):
            details.append(f'<span class="tac-detail"><b>Timeframe:</b> {html.escape(tactical["timeframe"])}</span>')
        if tactical.get("risk_reward"):
            details.append(f'<span class="tac-detail"><b>R/R:</b> {html.escape(tactical["risk_reward"])}</span>')
        if tactical.get("catalyst"):
            details.append(f'<span class="tac-detail"><b>Catalyst:</b> {html.escape(tactical["catalyst"])}</span>')
        if tactical.get("invalidation"):
            details.append(f'<span class="tac-detail"><b>Stop:</b> {html.escape(tactical["invalidation"])}</span>')
        if details:
            tac_parts.append(f'<div class="tac-details">{"".join(details)}</div>')
        tactical_html = f'<div class="tactical-box">{"".join(tac_parts)}</div>'

    # Session ID for resume
    session_id = filing.research_session_id or ""
    session_html = ""
    if session_id:
        session_html = (
            f'<span class="session-tag" title="claude --resume {session_id}">'
            f'<code>{session_id[:8]}</code></span>'
        )

    memo_content = _render_memo_html(memo_md)

    # Alert analysis dropdown
    alert_html = ""
    if analysis:
        new_info = html.escape(analysis.get("new_information", "") or "")
        materiality = html.escape(analysis.get("materiality", "") or "")
        explanation = html.escape(analysis.get("explanation", "") or "")
        alert_class = html.escape(analysis.get("classification", "") or "")
        alert_mag = analysis.get("magnitude", "")

        alert_html = f"""\
  <details class="memo-expand alert-expand">
    <summary>Original Alert ({alert_class}, mag {alert_mag})</summary>
    <div class="alert-content">
      <h3>What Happened</h3>
      <p>{new_info}</p>
      <h3>Materiality Assessment</h3>
      <p>{materiality}</p>
      <h3>Analysis</h3>
      <p>{explanation}</p>
    </div>
  </details>"""

    return f"""\
<div class="card">
  <div class="card-header">
    <span class="rank">#{rank}</span>
    <span class="ticker">{ticker}</span>
    <span class="badge" style="background:{badge_col}">{html.escape(classification.upper())}</span>
    <span class="form-type">{form_type}</span>
    <span class="magnitude">mag {mag}</span>
    {f'<span class="adtv-tag" title="Avg daily trading volume (20d)">ADTV {adtv}</span>' if adtv else ''}
    {score_html}
    {val_html}
    {session_html}
  </div>
  <div class="card-body">
    <div class="chart-col">{svg}</div>
    <div class="text-col">
      <p class="thesis">{thesis}</p>
      {tactical_html}
    </div>
  </div>
  <details class="memo-expand">
    <summary>Full Memo</summary>
    <div class="memo-content">{memo_content}</div>
  </details>
  {alert_html}
</div>"""


def _source_label(filing: TrackedFiling) -> str:
    """Human-readable source label from key_prefix and form_type."""
    kp = filing.key_prefix or ""
    if filing.form_type:
        return filing.form_type
    if "newsfile" in kp:
        return "Canadian PR"
    if "/gnw/" in kp:
        return "US PR"
    if "press_releases" in kp:
        return "PR"
    return "—"


def _render_skipped_table(skipped: list[TrackedFiling]) -> str:
    if not skipped:
        return ""
    rows = []
    for f in sorted(skipped, key=lambda x: x.discovered_at, reverse=True):
        ticker = html.escape(f.ticker or "—")
        source = html.escape(_source_label(f))
        cls = html.escape(f.classification[:10] if f.classification else "—")
        mag = f"{f.magnitude:.2f}" if f.magnitude is not None else "—"
        time_str = f.discovered_at.strftime("%H:%M") if f.discovered_at else "—"
        reason = html.escape(f.decision_reason or "")

        # Distinguish prescreen rejections
        if f.decision == FilingDecision.SKIP_SCREENED:
            reason_cls = "skip-screened"
        elif f.decision == FilingDecision.SKIP_NOT_ANALYZED:
            reason_cls = "skip-pending"
        else:
            reason_cls = ""

        rows.append(
            f'<tr class="{reason_cls}">'
            f"<td>{time_str}</td><td>{ticker}</td><td>{source}</td>"
            f"<td>{cls}</td><td>{mag}</td><td>{reason}</td></tr>"
        )
    return f"""\
<details class="skipped-section">
  <summary>Skipped Filings ({len(skipped)})</summary>
  <table class="skipped-table">
    <thead><tr><th>Time</th><th>Ticker</th><th>Source</th><th>Class</th><th>Mag</th><th>Reason</th></tr></thead>
    <tbody>{"".join(rows)}</tbody>
  </table>
</details>"""


def _css() -> str:
    return """\
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: #0f172a; color: #e2e8f0; padding: 20px 30px;
  max-width: 1100px; margin: 0 auto;
}
h1.title { font-size: 22px; margin-bottom: 4px; }
.subtitle { color: #94a3b8; font-size: 13px; margin-bottom: 20px; }
.summary-bar {
  display: flex; gap: 20px; padding: 12px 16px; margin-bottom: 24px;
  background: #1e293b; border-radius: 8px; font-size: 13px;
}
.summary-bar .stat { color: #94a3b8; }
.summary-bar .stat b { color: #e2e8f0; }
.card {
  background: #1e293b; border-radius: 10px; margin-bottom: 16px;
  border: 1px solid #334155; overflow: hidden;
}
.card-header {
  display: flex; align-items: center; gap: 10px;
  padding: 12px 16px; border-bottom: 1px solid #334155;
  flex-wrap: wrap;
}
.rank { color: #64748b; font-weight: 700; font-size: 15px; min-width: 28px; }
.ticker { font-weight: 700; font-size: 18px; color: #f8fafc; }
.badge {
  font-size: 11px; font-weight: 600; padding: 2px 8px;
  border-radius: 4px; color: #0f172a; text-transform: uppercase;
}
.form-type { color: #94a3b8; font-size: 13px; }
.magnitude { color: #cbd5e1; font-size: 13px; }
.adtv-tag { font-size: 11px; color: #94a3b8; background: #334155; padding: 2px 6px; border-radius: 3px; }
.session-tag {
  font-size: 10px; color: #64748b; background: #1e293b; border: 1px solid #334155;
  padding: 1px 5px; border-radius: 3px; cursor: pointer; margin-left: auto;
}
.session-tag:hover { color: #94a3b8; border-color: #475569; }
.session-tag code { font-size: 10px; }
.score-tag {
  font-size: 11px; font-weight: 600; padding: 2px 6px;
  border-radius: 3px; font-family: monospace;
}
.tac-score { background: #1e3a5f; color: #60a5fa; }
.fun-score { background: #14532d; color: #4ade80; }
.val-tag {
  font-size: 11px; color: #94a3b8; background: #334155;
  padding: 2px 6px; border-radius: 3px;
}
.card-body { display: flex; gap: 16px; padding: 16px; align-items: flex-start; }
.chart-col { flex-shrink: 0; }
.text-col { flex: 1; min-width: 0; }
.thesis { font-size: 14px; line-height: 1.5; margin-bottom: 8px; }
.rationale { font-size: 12px; color: #94a3b8; }
.tactical-box {
  background: #172554; border: 1px solid #1e40af; border-radius: 6px;
  padding: 10px 14px; margin: 10px 0;
}
.tac-setup { font-size: 13px; color: #93c5fd; margin-bottom: 6px; font-weight: 500; }
.tac-details { display: flex; flex-wrap: wrap; gap: 8px 16px; }
.tac-detail { font-size: 12px; color: #bfdbfe; }
.tac-detail b { color: #60a5fa; }
.memo-expand { border-top: 1px solid #334155; }
.memo-expand summary {
  padding: 10px 16px; cursor: pointer; font-size: 13px;
  color: #64748b; user-select: none;
}
.memo-expand summary:hover { color: #94a3b8; }
.memo-content {
  padding: 24px 32px; font-size: 14px; line-height: 1.7;
  color: #cbd5e1; max-width: 800px;
}
.memo-content h1 {
  font-size: 20px; color: #f8fafc; margin: 24px 0 12px 0;
  padding-bottom: 6px; border-bottom: 1px solid #334155;
}
.memo-content h2 {
  font-size: 17px; color: #f1f5f9; margin: 20px 0 10px 0;
}
.memo-content h3 { font-size: 15px; color: #e2e8f0; margin: 16px 0 8px 0; }
.memo-content h4 { font-size: 14px; color: #e2e8f0; margin: 14px 0 6px 0; }
.memo-content p { margin-bottom: 8px; }
.memo-content strong { color: #f1f5f9; }
.memo-content em { color: #94a3b8; font-style: italic; }
.memo-content .memo-table {
  border-collapse: collapse; margin: 12px 0; width: 100%;
  font-size: 13px;
}
.memo-content .memo-table th {
  text-align: left; padding: 8px 12px; color: #e2e8f0;
  background: #1e293b; border: 1px solid #475569;
  font-weight: 600; white-space: nowrap;
}
.memo-content .memo-table td {
  padding: 6px 12px; border: 1px solid #334155;
  vertical-align: top;
}
.memo-content .memo-table tr:nth-child(even) td { background: rgba(51,65,85,0.3); }
.memo-content ul {
  margin: 8px 0 8px 20px; padding: 0;
}
.memo-content li { margin-bottom: 4px; }
.memo-content hr {
  border: none; border-top: 1px solid #334155; margin: 16px 0;
}
.memo-content .empty { color: #64748b; font-style: italic; }
.alert-expand { border-top: 1px solid #334155; }
.alert-content {
  padding: 20px 28px; font-size: 14px; line-height: 1.6;
  color: #cbd5e1; max-width: 800px;
}
.alert-content h3 {
  font-size: 14px; color: #93c5fd; margin: 14px 0 6px 0;
  text-transform: uppercase; letter-spacing: 0.5px;
}
.alert-content h3:first-child { margin-top: 0; }
.alert-content p { margin-bottom: 10px; }
.skipped-section { margin-top: 30px; }
.skipped-section summary {
  cursor: pointer; font-size: 14px; color: #64748b;
  padding: 10px 0; user-select: none;
}
.skipped-table {
  width: 100%; border-collapse: collapse; font-size: 12px; margin-top: 8px;
}
.skipped-table th {
  text-align: left; padding: 6px 10px; color: #94a3b8;
  border-bottom: 1px solid #334155;
}
.skipped-table td { padding: 4px 10px; color: #cbd5e1; border-bottom: 1px solid #1e293b; }
.skip-screened td { color: #64748b; }
.skip-pending td { color: #fbbf24; }
.briefing {
  background: #172554; border: 1px solid #1e40af; border-radius: 10px;
  padding: 16px 20px; margin-bottom: 24px;
}
.briefing-title { font-size: 16px; color: #93c5fd; margin-bottom: 12px; }
.briefing-empty { color: #64748b; font-style: italic; }
.briefing-item {
  padding: 8px 0; border-bottom: 1px solid #1e3a5f;
}
.briefing-item:last-child { border-bottom: none; }
.briefing-ticker { font-weight: 700; font-size: 15px; color: #f8fafc; }
.briefing-form { font-size: 12px; color: #64748b; }
.briefing-scores { font-size: 11px; color: #60a5fa; font-family: monospace; margin-left: 8px; }
.briefing-thesis { font-size: 13px; color: #cbd5e1; margin: 4px 0 0 0; line-height: 1.4; }
"""


def _render_briefing(
    researched: list[TrackedFiling],
    memo_yamls: dict[str, dict | None],
) -> str:
    """Render a quick morning briefing — top opportunities in 30 seconds."""
    if not researched:
        return ""

    # Find the top opportunities: BUY decisions sorted by tactical score
    buys = []
    for f in researched:
        t = f.ticker.upper()
        memo = memo_yamls.get(t) or {}
        decision = (memo.get("decision") or f.classification or "").upper().strip()
        if decision not in ("BUY", "SPECULATIVE_BUY", "SPECULATIVE BUY"):
            continue
        scores = memo.get("scores", {}) or {}
        tac = scores.get("tactical") or 0
        fun = scores.get("fundamental") or 0
        thesis = memo.get("thesis_summary") or f.summary or ""
        buys.append((t, tac, fun, thesis, f.form_type or "PR"))

    if not buys:
        return '<div class="briefing"><p class="briefing-empty">No BUY recommendations today.</p></div>'

    buys.sort(key=lambda x: (-x[1], -x[2]))
    top = buys[:5]

    items = []
    for ticker, tac, fun, thesis, form in top:
        thesis_short = thesis[:150] + "..." if len(thesis) > 150 else thesis
        items.append(
            f'<div class="briefing-item">'
            f'<span class="briefing-ticker">{html.escape(ticker)}</span> '
            f'<span class="briefing-form">{html.escape(form)}</span> '
            f'<span class="briefing-scores">T:{tac} F:{fun}</span>'
            f'<p class="briefing-thesis">{html.escape(thesis_short)}</p>'
            f'</div>'
        )

    return f"""\
<div class="briefing">
  <h2 class="briefing-title">Morning Briefing — Top {len(top)} Opportunities</h2>
  {"".join(items)}
</div>"""


_DECISION_RANK = {
    "BUY": 0,
    "SPECULATIVE_BUY": 1,
    "SPECULATIVE BUY": 1,
    "NEUTRAL": 2,
    "TOO_HARD": 3,
    "TOO HARD": 3,
    "HOLD": 4,
    "SELL": 5,
}


def _sort_key(filing: TrackedFiling, memo_yamls: dict[str, dict | None]) -> tuple[int, int, int, float]:
    """Sort key: BUY-first by decision, then tactical score desc, fundamental desc, magnitude desc."""
    memo = memo_yamls.get(filing.ticker.upper()) or {}
    decision = (memo.get("decision") or filing.classification or "").upper().strip()
    decision_rank = _DECISION_RANK.get(decision, 3)
    scores = memo.get("scores", {}) or {}
    tac = scores.get("tactical") or 0
    fun = scores.get("fundamental") or 0
    mag = filing.magnitude or 0
    return (decision_rank, -tac, -fun, -mag)


def generate_report(
    state: FilingResearchState,
    ohlc_data: dict[str, list[dict]],
    memo_yamls: dict[str, dict | None],
    memo_mds: dict[str, str | None],
    analyses: dict[str, dict | None] | None = None,
) -> str:
    """Generate the full HTML report string."""
    now_str = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")

    # Summary stats
    total = len(state.filings)
    researched = [f for f in state.filings.values() if f.decision == FilingDecision.RESEARCH_DONE]
    failed = [f for f in state.filings.values() if f.decision == FilingDecision.RESEARCH_FAILED]
    skipped = [f for f in state.filings.values() if f.decision in SKIP_DECISIONS]

    summary_html = f"""\
<div class="summary-bar">
  <div class="stat"><b>{total}</b> filings discovered</div>
  <div class="stat"><b>{len(researched)}</b> researched</div>
  <div class="stat"><b>{len(failed)}</b> failed</div>
  <div class="stat"><b>{len(skipped)}</b> skipped</div>
</div>"""

    # Morning briefing — top opportunities
    briefing_html = _render_briefing(researched, memo_yamls)

    # Sort by tactical score (primary), fundamental score (tiebreaker), magnitude (fallback)
    sorted_filings = sorted(researched, key=lambda f: _sort_key(f, memo_yamls))

    cards_html = []
    for rank, f in enumerate(sorted_filings, 1):
        t = f.ticker.upper()
        ticker_ohlc = ohlc_data.get(t, [])
        svg = render_price_svg(ticker_ohlc)
        adtv = _compute_adtv(ticker_ohlc)
        analysis = (analyses or {}).get(t)
        cards_html.append(
            _render_filing_card(f, rank, svg, memo_yamls.get(t), memo_mds.get(t), adtv=adtv, analysis=analysis)
        )

    skipped_html = _render_skipped_table(skipped)

    # Failed filings section
    failed_html = ""
    if failed:
        rows = []
        for f in failed:
            rows.append(
                f"<tr><td>{html.escape(f.ticker)}</td><td>{html.escape(f.form_type or '')}</td>"
                f"<td>{html.escape(f.decision_reason or 'unknown')}</td></tr>"
            )
        failed_html = f"""\
<details class="skipped-section" style="margin-top:16px">
  <summary>Failed Research ({len(failed)})</summary>
  <table class="skipped-table">
    <thead><tr><th>Ticker</th><th>Form</th><th>Reason</th></tr></thead>
    <tbody>{"".join(rows)}</tbody>
  </table>
</details>"""

    return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Filing Research — {html.escape(state.run_date)}</title>
  <style>{_css()}</style>
</head>
<body>
  <h1 class="title">Filing Research Report</h1>
  <div class="subtitle">{html.escape(state.run_date)} &middot; Generated {html.escape(now_str)}</div>
  {summary_html}
  {briefing_html}
  {"".join(cards_html) if cards_html else '<p style="color:#64748b">No researched filings for this date.</p>'}
  {failed_html}
  {skipped_html}
  <script>
  document.querySelectorAll('.session-tag').forEach(el => {{
    el.addEventListener('click', () => {{
      const cmd = el.title;
      navigator.clipboard.writeText(cmd).then(() => {{
        el.style.borderColor = '#4ade80';
        el.style.color = '#4ade80';
        setTimeout(() => {{ el.style.borderColor = ''; el.style.color = ''; }}, 1000);
      }});
    }});
  }});
  // Auto-refresh: reload page every 60s if the file has been regenerated
  let lastLen = document.documentElement.innerHTML.length;
  setInterval(() => {{
    fetch(window.location.href).then(r => r.text()).then(html => {{
      if (html.length !== lastLen) {{
        // Preserve scroll position across refresh
        const scrollY = window.scrollY;
        const openDetails = [...document.querySelectorAll('details[open]')].map(
          d => d.querySelector('summary')?.textContent
        );
        document.documentElement.innerHTML = new DOMParser()
          .parseFromString(html, 'text/html').documentElement.innerHTML;
        window.scrollTo(0, scrollY);
        lastLen = html.length;
      }}
    }}).catch(() => {{}});
  }}, 60000);
  </script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def generate_and_write_report(
    *,
    date_str: str | None = None,
    output_path: str | None = None,
    skip_charts: bool = False,
    open_browser: bool = True,
    quiet: bool = False,
) -> Path | None:
    """Generate the HTML report for a given date.

    If *open_browser* is True (default), opens the report in the browser.
    If *quiet* is True, suppresses click.echo output (for daemon auto-regen).
    """

    def _echo(msg: str) -> None:
        if not quiet:
            click.echo(msg)

    now_et = datetime.now(ET)
    run_date = date_str or now_et.strftime("%Y-%m-%d")

    state = _load_state(run_date)
    if not state.filings and not state.started_at:
        _echo(f"No filing research state found for {run_date}.")
        return None

    researched = [
        f for f in state.filings.values()
        if f.decision == FilingDecision.RESEARCH_DONE
    ]
    tickers = [f.ticker.upper() for f in researched]

    _echo(f"Generating report for {run_date} ({len(researched)} researched filings)")

    # Load memos
    _echo("  Loading memos...")
    memo_yamls: dict[str, dict | None] = {}
    memo_mds: dict[str, str | None] = {}
    for t in tickers:
        memo_yamls[t] = _load_memo_yaml(t)
        memo_mds[t] = _load_memo_md(t)

    # Fetch price data
    if skip_charts:
        _echo("  Skipping price charts")
        ohlc_data: dict[str, list[dict]] = {}
    else:
        _echo("  Fetching price data...")
        ohlc_data = fetch_all_ohlc(tickers)

    # Load alert analyses from S3
    _echo("  Loading alert analyses...")
    analyses = _load_all_analyses(researched)

    # Generate HTML (sorted by scores from memo.yaml)
    report_html = generate_report(state, ohlc_data, memo_yamls, memo_mds, analyses=analyses)

    # Write
    if output_path:
        out = Path(output_path)
    else:
        out = find_repo_root() / "data" / f"filing_research_report_{run_date}.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report_html)

    _echo(f"Report written to {out}")
    if open_browser:
        import webbrowser
        webbrowser.open(f"file://{out.resolve()}")
    return out
