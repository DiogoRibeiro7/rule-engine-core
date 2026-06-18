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

- Formal declarative rule schema validation with path-aware YAML errors.
- Compile-time validation for trigger fields, durations, cron expressions, and
  supported condition operators.
- Dedicated compiler/runtime split with a lightweight embedding API.
- Typed runtime metadata, evaluation results, and delivery reports.
- First-class sink dispatch with retry, backoff, dead-letter, delivery metrics,
  and structured delivery logs.
- File-backed dead-letter retention and stronger local persistence options for
  embedding code.
- Formatter, linter, and type-checking configuration enforced in CI.
- Golden replay fixtures for sample scenarios.
- Neutral multi-domain examples with checked-in sample rules and event data.
- Top-level contribution notes and changelog policy.
- A reusable changelog upgrade-note pattern for future releases.

### Changed

- Repository naming and public docs are now generic and no longer tied to a
  domain-specific engine name.
- README and roadmap now track repo truth instead of aspirational features.

## 0.1.0

### Added

- Initial public reference implementation of the in-memory declarative rule
  engine core.
