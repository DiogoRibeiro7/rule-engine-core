# Contributing

## Scope

This repository is a compact core runtime for declarative rule evaluation. Keep
changes aligned with the current repo boundary:

- deterministic in-memory execution
- declarative rule compilation and validation
- sink delivery behavior that is actually implemented here
- docs and examples that match the executable surface

Do not add speculative product layers, domain-specific branding, or unsupported
declarative syntax as documentation-only placeholders.

## Local Setup

Install the development toolchain:

```bash
python -m pip install -e .[dev]
```

Run the full local check stack before opening a change:

```bash
python -m pytest -q
python -m ruff check .
python -m mypy
```

Format when needed:

```bash
python -m ruff format .
```

## Change Rules

- Update `README.md` in the same change set whenever the runtime surface,
  supported rule language, or sink delivery behavior changes.
- Update `ROADMAP.md` when a roadmap item is completed or materially re-scoped.
- Update `CHANGELOG.md` for any user-visible capability, behavior, or workflow
  change.
- Keep examples executable. If you add or change files under
  `sample_rules/examples/` or `sample_data/examples/`, keep the docs and tests
  aligned.
- Prefer small, mechanically verifiable changes over broad speculative edits.

## Tests

The repo uses a few different test shapes for different risks:

- unit tests for engine and rule semantics
- golden replay fixtures for stable JSON replay outputs
- compile checks for example rules

If a change affects alert output, delivery reporting, or replay shape, update
or extend the golden fixtures under `tests/fixtures/replay/`.

## Style Notes

- Python version target is `3.11+`.
- Ruff handles formatting and linting.
- Mypy covers the `rule_engine` package.
- Keep public naming generic. Avoid domain-specific terms in repo-facing code
  and docs.

## Pull Requests

A good change set includes:

- the implementation
- matching tests or fixture updates
- README/roadmap/changelog updates when applicable
- a commit message that describes the concrete repo change
