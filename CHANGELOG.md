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

- `CompiledEngine.watermark` exposes the current event-time watermark.
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

### Fixed

- CI ran `mypy` without PyYAML stubs and failed on an `import-untyped` error;
  `types-PyYAML` is now pinned as a dev dependency.

### Upgrade Notes

- If you feed events through `process_event` directly, they must arrive in
  non-decreasing event-time order. Sort them first, or use `replay()`, which
  sorts a batch for you. Events exactly at the current watermark are still
  accepted.
- If you relied on `advance_to` accepting an earlier target as a no-op, it now
  raises. Track the current position with `CompiledEngine.watermark`.

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
