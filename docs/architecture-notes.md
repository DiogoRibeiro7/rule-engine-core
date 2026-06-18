# Architecture Notes

This note explains the current internal boundary lines in `rule-engine-core`.
It is intentionally short and maps directly to the modules that make up the
runtime.

## 1. Compile-Time Boundary

Compile-time code turns declarative rule definitions into executable runtime
objects.

Primary modules:

- `rule_engine.declarative`
- `rule_engine.compiler`
- `rule_engine.runner` for file-oriented loading helpers

Responsibilities:

- load YAML rule documents
- validate the supported declarative schema
- normalize sink configuration grammar
- reject unsupported trigger, condition, duration, and cron shapes
- compile validated rules into `CompiledRule` instances

What compile-time code does not do:

- manage entity state
- process events
- deliver alerts

## 2. Runtime Boundary

Runtime code owns deterministic replay evaluation and alert emission.

Primary modules:

- `rule_engine.runtime`
- `rule_engine.models`
- `rule_engine.types`
- `rule_engine.window`

Responsibilities:

- maintain per-entity rule state
- process ordered events
- advance timers deterministically
- evaluate event, window, absence, composite, and scheduled triggers
- build alert payloads and delivery reports

What runtime code does not do:

- parse infrastructure-specific live inputs
- run a long-lived service loop
- know about domain-specific rule packs

## 3. Sink Boundary

Sink code owns delivery transport behavior after the runtime emits an alert.

Primary module:

- `rule_engine.sinks`

Responsibilities:

- typed sink config objects
- shared delivery envelope construction
- adapter registration and dispatch
- retry policy and backoff
- dead-letter recording
- delivery metrics and structured delivery logs
- concrete adapters for `stdout`, `file`, `webhook`, `queue`, and
  `object_storage`

What sink code does not do:

- decide whether a rule should fire
- mutate rule state
- define domain-specific downstream contracts beyond the documented delivery
  envelope

## 4. Embedding Boundary

Embedding code gives downstream Python callers a smaller surface than the raw
 runtime modules.

Primary module:

- `rule_engine.api`

Responsibilities:

- create engines from YAML strings, files, or precompiled rules
- expose typed evaluation results
- expose helper constructors for standard sink-registry setup

What embedding code does not do:

- add new evaluation semantics
- replace the compile/runtime separation

## 5. CLI Boundary

The CLI is a thin adapter over compile-time loading and replay execution.

Primary module:

- `rule_engine.runner`

Responsibilities:

- load rule files and NDJSON event fixtures
- run deterministic replay
- print compiled models, alerts, schemas, and delivery reports

What the CLI does not do:

- act as a production daemon
- become the canonical embedding API

## Design Intent

The repo stays understandable because each layer has a narrow job:

1. declarative input becomes compiled rules
2. compiled rules and replay events become emitted alerts
3. emitted alerts become delivery attempts and delivery reports

If a future change crosses those lines, it should be treated as a scope change,
not just an implementation detail.
