# Macro Workspace

Shared macro context area consumed by the macro-analyst agent during research sessions.

---

## Location

- **S3 (durable):** `s3://praxis-copilot/data/context/macro/view.md`
- **Local (working copy):** synced to local disk for Claude Code sessions

---

## Structure

`view.md` is a human-maintained document containing the current macro worldview: rate environment, cycle positioning, sector views, geopolitical risks, and any other macro context relevant to investment analysis.

Supplementary files (data tables, charts, reference notes) can live alongside it under `context/macro/`.

---

## Maintenance

Human maintains the macro workspace via Claude Code sessions — supplements with new data, refines views through conversation. There is no automated update; the macro view is a human artifact.

---

## Consumption

The **macro-analyst** specialist agent reads `view.md` as input when analyzing each stock. It interprets the macro context in terms of company-specific exposure (rate sensitivity, cycle positioning, sector headwinds/tailwinds, etc.).

---

## Versioning

Each version of `view.md` is tracked by content hash. When a research session runs, the memo records which macro view hash it was written against. If the macro view is later updated, dependent memos become stale (see [research-session.md](research-session.md) — Artifact Versioning & Staleness).
