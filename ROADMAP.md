# Roadmap

This roadmap assumes the repository evolves from a compact reference
implementation into a production-capable core with a fully implemented sink
delivery system.

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
- Sink configs now validate against explicit grammar for the implemented sink types.
- The delivery layer is executable, but production-grade backend breadth and policy depth are still incomplete.

## Phase 1: Stabilize The Core

Goal: make the current runtime harder to break and easier to reason about.

Deliverables:

- Add `.gitignore` for `__pycache__/`, `.pytest_cache/`, `.benchmarks/`, and similar artifacts.
- Add stricter YAML validation with explicit error messages for malformed rules.
- Introduce a formal rule schema and validate rule files before execution.
- Tighten duration parsing and trigger parsing edge cases.
- Add negative tests for invalid rules, unsupported operators, and bad cron expressions.
- Document the exact supported rule-language subset in the repo.

Exit criteria:

- Invalid rules fail fast with readable messages.
- Runtime behavior for every supported trigger is specified and tested.

## Phase 2: Complete The Declarative Language

Goal: close the biggest gaps between the YAML surface and the actual engine.

Deliverables:

- Implement the aggregation functions that are worth keeping long term.
- Define and enforce the supported condition grammar.
- Make template rendering explicit and predictable, including missing-variable behavior.
- Define and enforce the sink configuration grammar instead of treating sink payloads as opaque dictionaries.
- Introduce a stable sink adapter interface and start with low-risk executable sinks such as stdout, file, and webhook.

Exit criteria:

- Every field accepted by the YAML format is either executed or removed.
- There is no dead declarative surface area.

## Phase 3: Improve Runtime Structure

Goal: reduce incidental complexity and make extension safer.

Deliverables:

- Separate compile-time rule loading from runtime execution more cleanly.
- Replace any remaining implicit global behavior with explicit engine configuration.
- Introduce a lightweight public API for embedding the engine from other Python code.
- Add typed runtime/result objects for alerts, evaluations, and rule metadata.
- Review naming and module boundaries inside `rule_engine/` for long-term clarity.

Exit criteria:

- The runtime can be embedded without going through the CLI.
- Internal responsibilities are obvious from module boundaries.

## Phase 4: Developer Experience

Goal: make the repo pleasant to work on and hard to misuse.

Deliverables:

- Add formatter and linter configuration.
- Add type-checking to CI.
- Add fixture-driven golden tests for replay scenarios.
- Add a small examples section for multiple neutral domains.
- Add a changelog and contribution notes.

Exit criteria:

- A new contributor can run checks and understand the supported workflow quickly.

## Phase 5: Build The Sink Delivery System

Goal: implement real sink delivery with reliability semantics instead of leaving
delivery as metadata attached to alerts.

Deliverables:

- Add a first-class sink dispatch layer inside `rule_engine/`.
- Support sink registration and configuration through explicit typed adapters.
- Implement at least these concrete sinks:
  - stdout or console sink
  - file or NDJSON sink
  - webhook sink
  - SQS-style queue sink
  - object-storage sink
- Define a delivery contract for each sink:
  - payload shape
  - retryable vs terminal errors
  - timeout handling
  - idempotency expectations
  - structured delivery result reporting
- Add retry policy support with bounded retries and backoff.
- Add dead-letter or failed-delivery recording for undeliverable events.
- Add delivery metrics and structured logging around success, failure, latency, and retry counts.
- Add integration tests for each sink type and failure mode.

Exit criteria:

- Sink definitions in rules execute through real adapters.
- Delivery failures are observable and classified correctly.
- The engine can deliver alerts end to end without custom downstream glue code.

## Phase 6: Production Boundary Decisions

Goal: decide what this repo is, and what it is not.

Options:

- Keep it as a reference engine only.
- Add adapter layers for real inputs and outputs while keeping the core in-memory.
- Split the core engine from any streaming or infrastructure-specific integration work.

Questions to answer:

- Should cron support remain intentionally narrow, or become more complete?
- Should rule execution stay replay-based only, or support a live process loop?
- Which sinks are mandatory for the supported product surface, and which stay optional adapters?
- Should domain-specific rule packs live here or in separate example repos?

Exit criteria:

- The repo has a clear scope boundary and does not drift back into speculative architecture.

## Priority Order

1. Phase 1: Stabilize the core
2. Phase 2: Complete the declarative language
3. Phase 3: Improve runtime structure
4. Phase 4: Developer experience
5. Phase 5: Build the sink delivery system
6. Phase 6: Production boundary decisions

## Immediate Next Steps

1. Add `.gitignore` and clean generated artifacts from the repo.
2. Add schema-backed validation for YAML rules.
3. Add failure-mode tests for malformed rules and unsupported expressions.
4. Audit the YAML surface and remove any fields the runtime will not support.
5. Design the sink adapter contract before implementing individual sink backends.
