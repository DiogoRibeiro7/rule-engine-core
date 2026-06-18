# Roadmap

This roadmap now reflects the repository after the initial core build-out.
The original multi-phase implementation plan is effectively complete, so the
remaining work is the post-core backlog rather than the initial construction
sequence.

## North Star

Build a small, trustworthy declarative rule engine that:

- executes rules deterministically in memory
- keeps YAML semantics aligned with runtime behavior
- is easy to extend and test
- delivers rule outcomes through real, reliable sink integrations

## Current State

- The repo has a working in-memory replay engine in `rule_engine/`.
- Supported trigger families are `event`, `window`, `absence`, `composite`, and `scheduled`.
- Tests cover core alert behavior and replay timing.
- The package is now generic; the sample rules are only reference fixtures.
- Declarative rules now validate against a formal schema before execution.
- Trigger/duration/cron edge cases now fail fast during compilation.
- The exact supported declarative subset is now documented in-repo.
- Compile-time rule loading is now split from execution through dedicated compiler/runtime entry points.
- Engine startup and scheduling behavior now have explicit runtime configuration.
- A lightweight embedding API now exists for YAML, file-based, and precompiled engine construction.
- Typed rule metadata and evaluation result objects now exist for embedding use cases.
- Public runtime models are now separated from execution logic into clearer module boundaries.
- Formatter/linter configuration is now defined in `pyproject.toml` and exercised in CI.
- Type checking is now defined in `pyproject.toml` and exercised in CI for the core package.
- Fixture-driven golden replay tests now pin sample scenario output at the JSON-report level.
- The repo now ships a small examples section with checked-in neutral-domain rules and event fixtures.
- The repo now has explicit contribution notes and a top-level changelog.
- Sink configs now validate against explicit grammar for the implemented sink types.
- Sink dispatch now uses explicit typed sink config objects instead of opaque runtime dictionaries.
- The implemented sinks now share an explicit versioned delivery envelope with a documented idempotency key contract.
- Integration tests now cover the implemented sink adapters across success, retry, and dead-letter paths.
- The repo now has explicit production-boundary decisions for cron scope, replay-first execution, maintained sink surface, and generic-only examples.
- Common sink-registry setup is now exposed through helper constructors for embedding code.

## Completed Foundations

The original planned phases are complete at the repository level:

1. Stabilize the core
2. Complete the declarative language
3. Improve runtime structure
4. Developer experience
5. Build the sink delivery system
6. Production boundary decisions

The rest of this file captures the remaining backlog after that baseline.

## Post-Core Backlog

### 1. Operational Hardening

Goal: make the existing sink system safer to operate in less toy-like
environments without expanding the core into a platform.

Candidate work:

- Add explicit delivery timeout coverage for file/object-storage paths where it
  makes sense.
- Add stronger dead-letter persistence options and retention guidance.
- Add optional structured sink metrics export helpers for downstream embedding
  code.
- Add more failure-mode coverage around partial transport exceptions and adapter
  metadata consistency.

### 2. Backend Depth

Goal: strengthen the current adapter implementations without widening the
maintained sink surface prematurely.

Candidate work:

- Replace or wrap the in-memory queue transport with a clearer SQS-style example
  adapter boundary.
- Add richer object-storage key strategies and collision guidance.
- Add webhook request signing or auth-header examples without baking product
  policy into the core.

### 3. Embedding Ergonomics

Goal: make downstream use cleaner for Python callers without adding a service
runtime.

Candidate work:

- Add helper constructors for common sink-registry setups.
- Add richer typed report/query helpers for downstream inspection.
- Add more examples showing programmatic embedding and sink composition.

### 4. Documentation Tightening

Goal: keep public docs aligned as the repo matures.

Candidate work:

- Add a short upgrade/migration note pattern to `CHANGELOG.md`.
- Expand `docs/delivery-contract.md` with sample payloads per sink.
- Add a concise architecture note for the compile/runtime/sink boundaries.

## Recommended Next Steps

1. Decide whether operational hardening is still in scope for this repo or
   should stay in downstream wrappers.
2. If yes, implement one concrete hardening slice instead of reopening broad
   architecture: webhook auth examples, dead-letter persistence options, or
   richer queue transport boundaries.
3. Keep `README.md`, `ROADMAP.md`, and `docs/scope-boundary.md` aligned whenever
   that choice changes.
