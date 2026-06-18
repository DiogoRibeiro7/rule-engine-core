# Scope Boundary

This document defines what `rule-engine-core` is responsible for today and
where the repository stops.

## Decision Summary

### 1. Cron support stays intentionally narrow

Decision:

- Keep the current daily time-of-day cron subset (`minute hour * * *`).
- Do not expand to full cron syntax inside this repository unless a concrete
  product requirement appears.

Reasoning:

- The runtime is deterministic because the scheduler surface is small.
- Full cron support would add parser complexity and edge cases that are outside
  the current core value of the repo.

Implication:

- More complex schedules belong in a wrapper or integration layer, not in the
  core engine for now.

### 2. Rule execution stays replay-first

Decision:

- Keep replay-based evaluation as the primary execution model.
- Do not add a long-running live process loop in the core package right now.

Reasoning:

- Replay is the current source of determinism, testability, and simple engine
  semantics.
- A live loop introduces infrastructure concerns such as checkpoints,
  lifecycle management, health probes, and backpressure.

Implication:

- If live execution is needed later, it should wrap the existing core engine
  rather than folding process orchestration into it prematurely.

### 3. Mandatory sink surface is the current five adapters

Decision:

- Treat `stdout`, `file`, `webhook`, `queue`, and `object_storage` as the
  maintained sink surface for this repository.
- Additional sinks are optional adapters, not part of the mandatory core
  promise.

Reasoning:

- These five cover the main delivery shapes the repo already implements:
  console, local persistence, HTTP push, queue handoff, and object archival.
- Expanding the sink matrix too early would increase maintenance without
  improving the core runtime model.

Implication:

- New sink adapters should be added only when they fit the existing delivery
  contract and have clear test coverage.

### 4. Domain-specific rule packs stay outside this repo

Decision:

- Keep this repository generic.
- Domain-specific rule packs, fixtures, and branded examples should live in
  separate repos or downstream application layers.

Reasoning:

- The repo is now explicitly positioned as a reusable core.
- Pulling domain packs back into this codebase would erode naming discipline and
  confuse the product boundary.

Implication:

- In-repo examples should remain neutral, small, and documentation-oriented.

## What This Repo Is

- A deterministic in-memory rule-evaluation core
- A declarative YAML compiler and replay runtime
- A typed sink-dispatch layer with documented delivery semantics
- A testable embedding surface for downstream Python code

## What This Repo Is Not

- A streaming platform
- A workflow orchestrator
- A hosted control plane
- A domain-specific application package
- A full infrastructure integration suite

## Future Change Bar

Any future change that expands scope should answer all of the following before
it lands:

- Does it preserve deterministic replay behavior?
- Does it fit the current core package boundary instead of adding orchestration?
- Can it be expressed through the existing delivery/runtime contracts?
- Does it stay generic and avoid domain-specific naming or assets?

If the answer is no, that change probably belongs in a wrapper project instead
of `rule-engine-core`.
