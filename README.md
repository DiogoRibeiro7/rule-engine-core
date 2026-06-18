# Rule Engine Core

This repository contains a generic declarative rule engine reference implementation.
The repository was refactored from conflicting design notes and a routing-only
scaffold into an executable in-memory replay engine.

## What changed

- Canonical runtime model: keyed execution with domain-specific identifiers supplied by the caller.
- Entities are keyed by caller-supplied identifiers, with `rule_id` used as the per-rule namespace.
- Declarative rules now compile into executable in-memory runtime objects.
- Replay evaluation supports `event`, `window`, `absence`, `composite`, and `scheduled` triggers.
- Unit tests assert alert behavior, timer expiry, and lookback handling.

## Repository layout

- `rule_engine/` — generic Python reference implementation.
- `tests/` — unit tests for rule semantics and timing behavior.
- `sample_rules/` — sample declarative rules used as reference fixtures.
- `sample_data/` — NDJSON fixtures for replay-based tests and demos.
- `ROADMAP.md` — prioritized next steps for stabilizing and extending the engine.

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

## Notes

This repository is organized around a single canonical runtime model. The
runtime package is generic and can be reused for domains that fit the same
event-and-timer evaluation model. The Python runtime is an executable reference
for rule evaluation, not a production Flink runner or a fully implemented sink
delivery system. If you extend it, keep the declarative syntax and runtime
behavior aligned inside `rule_engine/`.

