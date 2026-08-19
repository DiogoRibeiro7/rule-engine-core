# Changelog

This project follows a simple keep-a-changelog style.

The repository is still pre-`1.0`, so entries describe concrete repo changes
rather than a strict compatibility promise.

## Entry Pattern

Use the following sections when they apply:

- `Added`
- `Changed`
- `Fixed`
- `Removed`
- `Upgrade Notes`

`Upgrade Notes` should be present whenever a downstream embedder or integration
may need to change code, config, or expectations after pulling a new version.

Suggested `Upgrade Notes` format:

```md
### Upgrade Notes

- If you were constructing `SinkRegistry` manually for the standard adapter
  set, prefer `create_sink_registry(...)`.
- Webhook sink configs can now declare `auth_token` and `signature_secret`.
  Existing configs remain valid.
```

## Unreleased

### Added

- An optional per-rule `emit` block with `cooldown`, `repeat_every`, and
  `resolve`, turning a rule from fire-on-every-match into one that tracks alert
  episodes. `repeat_every` is timer-driven, so a reminder fires with no new
  events.
- Delivered payloads now carry `lifecycle` (`firing`, `repeat`, or `resolved`)
  and `correlation_id`, which is stable across one episode so a resolution can
  be joined back to the alert that opened it.
- Episode state is covered by engine snapshots, so a cooldown still suppresses
  and a pending reminder still fires after a restart.
- `CompiledEngine.reload()` swaps the rule set on a live engine under an explicit
  state-migration policy: `preserve` keeps state where the rule's structure is
  unchanged, `reset` discards it, and `drain` keeps the previous definition
  running for entities with an open alert episode until it resolves.
- `reload(activate_at=...)` stages a swap until the watermark reaches that
  instant, with `CompiledEngine.last_reload_report()` returning the result once
  it applies.
- A typed `ReloadReport` recording each rule as preserved, reset, draining,
  added, or removed, with `to_dict()`/`to_json()` exports.

### Changed

- Alert metadata gained `lifecycle` and `correlation_id`, which also appear in
  the delivered payload. The golden replay fixture was regenerated accordingly.

### Upgrade Notes

- Rules with no `emit` block are unaffected: every qualifying evaluation still
  emits, with no episode tracking or suppression.
- Consumers parsing delivered payloads will see two new fields. A rule without
  an `emit` block always reports `lifecycle: firing`, and its `correlation_id`
  is stable per entity rather than per episode, so episode correlation requires
  declaring `emit`.
- Alert acknowledgement is explicitly not supported; it needs an inbound
  operator API that is outside this repo's scope.
- Snapshots do not carry in-progress drains or staged reloads. Let a drain
  finish, or re-issue the reload after restoring.

## 0.2.0 - 2026-08-18

Event-time correctness and state recovery.

### Added

- `allowed_lateness`, an optional per-rule duration declaring how far behind the
  watermark an event may arrive and still be considered. Defaults to `0s`, and
  accepts `0s` explicitly.
- `EngineConfig.late_event_policy` (`reject` by default, or `drop`) governing
  events later than any rule tolerates.
- `CompiledEngine.late_event_metrics()` returning a typed `LateEventMetrics`
  with totals, per-rule breakdown, and `to_dict()`/`to_json()` exports.
- `CompiledEngine.watermark` exposes the current event-time watermark.
- `CompiledEngine.snapshot()` and `CompiledEngine.restore()` for checkpoint and
  recovery, with a typed, versioned, JSON-serializable `EngineSnapshot` covering
  the watermark, per-entity rule state, pending timers, in-flight window
  buffers, and late-event counters.
- `CompiledRule.state_fingerprint()`, a structural hash used to refuse a restore
  into a rule whose windows, timers, or sources changed.
- Watermark regression tests covering out-of-order events, backward
  `advance_to` targets, and batches that predate the watermark.

### Changed

- Event-time order is now enforced. `process_event` rejects an event that
  predates the current watermark, and `advance_to` rejects a target earlier
  than it. Both raise before mutating any state, so a rejected event leaves
  the engine untouched. Previously both assigned the watermark unconditionally,
  so an out-of-order event moved it backward and could retroactively change
  timer behaviour.
- `sample_rules/source_gap.yaml` now posts to the reserved `example.com`
  documentation domain instead of a domain-specific host.
- Late events within tolerance are inserted into window buffers in timestamp
  order rather than appended, because `delta` and `rate` read
  `values[-1] - values[0]` and appending would have corrupted them.
- `parse_duration` takes an `allow_zero` flag so tolerance fields can accept
  `0s` while window and timeout durations still require a positive value.

### Fixed

- A condition operand referencing a value that is absent — an aggregation over an
  empty window, or a metric the event does not carry — raised `TypeError` when
  compared. Such an operand is now unsatisfied, so advancing a window rule past
  its last event no longer crashes and absent data cannot satisfy a threshold.
- CI ran `mypy` without PyYAML stubs and failed on an `import-untyped` error;
  `types-PyYAML` is now pinned as a dev dependency.

### Upgrade Notes

- If you feed events through `process_event` directly, they must arrive in
  non-decreasing event-time order. Sort them first, or use `replay()`, which
  sorts a batch for you. Events exactly at the current watermark are still
  accepted.
- If you relied on `advance_to` accepting an earlier target as a no-op, it now
  raises. Track the current position with `CompiledEngine.watermark`.
- Out-of-order events still raise by default. To tolerate them, declare
  `allowed_lateness` on the rules that should accept them; to discard them
  instead of raising, set `EngineConfig.late_event_policy='drop'`.
- A tolerated late event does not recompute a window that has already closed.
  That needs alert retraction and is tracked in `ROADMAP.md`.

## 0.1.0 - 2026-08-18

First tagged release. Archived on Zenodo with a DOI.

### Added

- Initial public reference implementation of the in-memory declarative rule
  engine core.
- Formal declarative rule schema validation with path-aware YAML errors.
- Compile-time validation for trigger fields, durations, cron expressions, and
  supported condition operators.
- Dedicated compiler/runtime split with a lightweight embedding API.
- Typed runtime metadata, evaluation results, and delivery reports.
- First-class sink dispatch with retry, backoff, dead-letter, delivery metrics,
  and structured delivery logs.
- File-backed dead-letter retention and stronger local persistence options for
  embedding code.
- Consistent sink failure metadata across file, queue, object-storage, and
  webhook delivery paths.
- Explicit timeout handling for file and object-storage sinks.
- Formatter, linter, and type-checking configuration enforced in CI.
- Golden replay fixtures for sample scenarios.
- Neutral multi-domain examples with checked-in sample rules and event data.
- Top-level contribution notes and changelog policy.
- A reusable changelog upgrade-note pattern for future releases.
- `.zenodo.json` and `CITATION.cff` so tagged releases archive to Zenodo with
  a DOI and machine-readable citation metadata.
- Release and archiving steps in `CONTRIBUTING.md`.

### Changed

- Repository naming and public docs are now generic and no longer tied to a
  domain-specific engine name.
- README and roadmap now track repo truth instead of aspirational features.
- `ROADMAP.md` is now a sequenced depth plan (late events, checkpointing,
  alert lifecycle, rule versioning, temporal sequences, explainability,
  backtesting, partitioning) rather than an integration backlog, with an
  explicit list of deferred non-goals.
- README is now restructured around what the runtime is and how it works,
  with a worked example, trigger/sink reference tables, and incremental
  "now supports" notes moved here instead.
- README now documents that the CLI runs without a sink registry, so declared
  sinks report as `unsupported` rather than delivering.

### Fixed

- CLI `main()` declared `argv` as `Iterable[str] | None`, which
  `argparse.parse_args` rejects under `mypy`; it is now `Sequence[str] | None`.
