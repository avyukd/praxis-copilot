# praxis-copilot — System Specification (v0)

## Purpose

praxis-copilot is a personal investment operating system: structured memory + monitoring + controlled reasoning.

It exists to reduce cognitive load, improve signal-to-noise, and augment (not replace) human decision-making. The human remains the portfolio manager.

## Design Principles

1. Deterministic first, reasoning second
2. Delta-driven analysis only
3. Human approval for state changes
4. Minimal configuration surface
5. Event-driven backbone
6. LLM as amplifier, not foundation

## MVP Pipeline

**event → analyze → monitor → manage**

Each step is human-in-the-loop. The system is split into a **Data Plane** (deterministic ingestion, normalization, event emission) and a **Control Plane** (policy, reasoning, alerting). See [architecture.md](architecture.md).

## MVP Module: 8K Scanner

The first module implements the event stage of the pipeline — polling SEC 8-K filings and press releases, extracting structured data, and surfacing actionable items via LLM screening.

See [modules/events/8k-scanner.md](modules/events/8k-scanner.md).

## Sub-Specs

- [architecture.md](architecture.md) — Data/Control plane definitions, AWS stack, module pipeline
- [config.md](config.md) — Configuration surface: scheduling, thresholds, universe, policy
- [modules/events/8k-scanner.md](modules/events/8k-scanner.md) — 8K/PR scanner module spec
- [modules/events/event-dispatch.md](modules/events/event-dispatch.md) — Event routing, ticker registry, CIK↔ticker mapping
- [modules/events/news-scanner.md](modules/events/news-scanner.md) — Daily SERP sweep + agent triage for thesis-relevant news
- [modules/analyze/research-session.md](modules/analyze/research-session.md) — Deep research session module spec
- [modules/analyze/macro-workspace.md](modules/analyze/macro-workspace.md) — Shared macro context workspace
- [modules/monitor/monitors.md](modules/monitor/monitors.md) — Monitor module spec: DSL, collectors, snapshots, evaluator, escalation
- [modules/manage/portfolio-manager.md](modules/manage/portfolio-manager.md) — Portfolio manager: price/volume/technicals + thesis context, actionable alerts

## Non-Goals (Initial Version)

- Automated trading execution
- Tick-level capture
- Full web indexing
- Autonomous strategy mutation
- Multi-LLM orchestration framework
