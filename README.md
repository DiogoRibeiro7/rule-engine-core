# Rule Engine Core

This repository contains the core runtime for a generic declarative rule engine.
It currently provides an executable in-memory replay engine and is being
extended toward a fully implemented sink delivery system.

## Current State

- Canonical runtime model: keyed execution with domain-specific identifiers supplied by the caller.
- Entities are keyed by caller-supplied identifiers, with `rule_id` used as the per-rule namespace.
- Declarative rules now compile into executable in-memory runtime objects.
- Replay evaluation supports `event`, `window`, `absence`, `composite`, and `scheduled` triggers.
- Unit tests assert alert behavior, timer expiry, and lookback handling.
- A first-class sink contract now exists, with `stdout` and file sinks implemented.
- Sink delivery is not fully implemented yet; webhook, queue, and object-storage adapters are still pending.

## Repository layout

- `rule_engine/` — generic Python reference implementation.
- `tests/` — unit tests for rule semantics and timing behavior.
- `sample_rules/` — sample declarative rules used as reference fixtures.
- `sample_data/` — NDJSON fixtures for replay-based tests and demos.
- `ROADMAP.md` — prioritized next steps for stabilizing and extending the engine.

## Scope

What this repo is:

- a core rule-evaluation runtime
- a declarative YAML rule compiler/executor
- a replay engine for deterministic testing and validation
- the base for sink delivery adapters, with `stdout` and file support already present

What this repo is not yet:

- a production streaming platform
- a complete sink delivery system
- a workflow orchestration tool
- a UI or rule-management product

## Quick start

Install the development dependencies:

```bash
python -m pip install -r requirements.txt
```

Run the reference tests:

```bash
python -m pytest
```

Run a declarative YAML rule demo:

```bash
python -m rule_engine.runner sample_rules/source_gap.yaml
```

Replay a declarative YAML rule against a sample NDJSON event fixture:

```bash
python -m rule_engine.runner sample_rules/source_gap.yaml --events sample_data/source_gap_events.ndjson
```

Replay a timer-driven rule and advance the engine past the final event:

```bash
python -m rule_engine.runner sample_rules/dual_source_gap.yaml --events sample_data/dual_source_gap_events.ndjson --until 2023-11-15T12:26:40+00:00
```

## Roadmap Alignment

This repository is organized around a single canonical runtime model. The
runtime package is generic and can be reused for domains that fit the same
event-and-timer evaluation model.

The current development target is no longer just a reference runtime. The end
goal is a production-capable core with a fully implemented sink delivery
system. The detailed plan for that work lives in `ROADMAP.md`.

## Maintenance Rule

`README.md` should describe the current repo truth, not the intended future
state. When the runtime surface, supported rule language, or sink delivery
capabilities change, update this file in the same change set.

