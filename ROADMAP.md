# Roadmap

## Positioning

The target this roadmap builds toward:

> **A deterministic, typed, event-time rule engine with explainable temporal
> semantics.**

Not: *a Python rules package with many integrations.* That distinction drives
every prioritization decision below. Work is preferred when it deepens the
temporal or operational semantics of the engine, and deferred when it only
widens the surface area.

The initial core build-out is complete. The engine compiles and validates
declarative rules, replays them deterministically, and delivers alerts through
five maintained sink adapters with retry, dead-letter, and metrics support. See
`CHANGELOG.md` for the detailed history and `README.md` for the current
capability surface.

The remaining work is a sequence of depth increases, not a backlog of
integrations.

## Sequence

Each stage assumes the previous one. The ordering is deliberate: temporal
correctness comes before durability, durability before lifecycle, and
expressiveness before tooling that explains it.

```text
correctness fixes                 [done]
  → late-event semantics          [done]
    → checkpoint / recovery        [done]
      → suppression + alert lifecycle [done]
        → rule versioning + hot reload [done]
          → temporal sequences      [done]
            → explainability        [done]
              → simulation / backtesting [done]
                → partitioned execution  [done]
```

---

### Stage 0 — Correctness fixes — complete

Prerequisite for everything below.

- **Backward watermark movement is now rejected.** `process_event` and
  `advance_to` previously assigned the watermark unconditionally, so an
  out-of-order event moved it backward and could retroactively change timer
  behaviour. Both paths now raise before mutating any state, and
  `CompiledEngine.watermark` exposes the current value. Stage 1 replaces this
  rejection with deliberate policy.
- **CI is green.** Two separate problems were involved: `main(argv)` was typed
  as `Iterable[str] | None` where `parse_args` requires `Sequence[str] | None`,
  and CI ran `mypy` without PyYAML stubs. Both are fixed and `types-PyYAML` is
  pinned as a dev dependency.
- **The sample rules are domain-neutral.** `sample_rules/source_gap.yaml` posted
  to `hooks.hospital.internal`; it now uses the reserved `example.com`
  documentation domain.

Any further findings from correctness review belong here before Stage 1 starts.

---

### Stage 1 — Late-event and out-of-order handling — complete

**Goal.** Make the engine honest about the distinction that defines stream
processing: event time is not processing time.

**Delivered.**

- `allowed_lateness`, a per-rule duration compared inclusively, accepting `0s`
  explicitly.
- Tolerated late events are folded into rule state in place: the watermark never
  moves backward, no timers re-fire, window buffers take the event in timestamp
  order, and `last_seen` only ever advances.
- `EngineConfig.late_event_policy` — `reject` (default) or `drop` — for events
  later than any rule tolerates, with `late_event_metrics()` reporting totals and
  a per-rule breakdown.
- `EngineConfig.recompute_late_windows` reopens windows that have already closed.
  A window that no longer holds is retracted under the original episode's
  correlation id; one that now holds fires late; an unchanged verdict emits
  nothing. Verdicts are retained only for the window duration plus
  `allowed_lateness`, so the record stays bounded and snapshottable.
- Lateness is measured against each entity's own progress, so an entity running
  ahead cannot make another entity's in-order events look late.
  `entity_watermarks()` reports the per-entity positions.

**Two constraints the implementation settled.**

*Timer progress stays global, deliberately.* Per-entity timers sound like the
obvious completion of per-entity watermarks, but an entity that goes silent never
advances its own clock — so its absence alert would never fire, which is the one
thing an absence rule exists to do. Lateness is per entity; time is not.

*Explicit clock advancement is not the same as traffic.* `advance_to()` and
`initial_watermark` raise the bar for every entity, because both assert that time
has moved. Another entity's events do not. That distinction is what makes
per-entity lateness safe rather than a way to smuggle stale data in.

**Not carried further.** Per-*source* watermarks are not tracked; lateness is per
entity, not per sensor type. Recompute applies to window rules only, since event
rules already evaluate a tolerated late event directly and absence and composite
state only ever advances.

---

### Stage 2 — Checkpoint and recovery — complete

**Goal.** Let a long-running embedder stop and resume without losing state.

**Delivered.**

- `CompiledEngine.snapshot()` returns a typed `EngineSnapshot` capturing the
  watermark, per-entity rule state, pending timers, in-flight window buffers,
  and late-event counters. It is JSON-serializable and version-stamped, and an
  unknown version is refused on read.
- `CompiledEngine.restore(snapshot, rules, ...)` rebuilds an engine. The
  snapshot watermark takes precedence over `EngineConfig.initial_watermark`.
- `CompiledRule.state_fingerprint()`, a hash of the fields that give retained
  state its meaning — trigger type, entity filter, sources, window duration and
  slide, timeouts, cron, lookback. Restoring into a rule whose fingerprint
  changed raises. Message templates, severities, sinks, and condition operands
  are excluded, so cosmetic edits do not invalidate a checkpoint.
- Rules only in the snapshot are dropped and rules only in the new set start
  empty, so rules can be added and removed between checkpoints.

This fingerprint is the same structural comparison Stage 4 needs to decide
whether a reloaded rule can preserve its state, so that work now has its
foundation.

**Verified.** Parameterised recovery tests split an event stream at every
boundary, serialize the snapshot through JSON, restore into a fresh engine, and
assert the emitted alerts are identical to an uninterrupted replay — covering
event rules, pending absence timers, in-flight windows, and multiple rules
across multiple entities.

---

### Stage 3 — Suppression, cooldowns, and alert lifecycle — complete

**Goal.** The most valuable missing *operational* capability. A technically
correct rule engine with no lifecycle model still floods everything downstream.

**Delivered.**

```yaml
emit:
  cooldown: 30m
  repeat_every: 2h
  resolve: true
```

- `cooldown` throttles emissions within an episode; `repeat_every` re-emits on a
  timer, so a reminder fires even with no new events; `resolve` emits a closing
  alert when the condition clears.
- Emissions are labelled `firing`, `repeat`, or `resolved`, and every emission in
  one episode shares a `correlation_id` derived from the rule, the entity, and
  the instant the episode opened. Both fields ride in the delivered payload, so
  a consumer can join a resolution back to the alert that opened it.
- Episode state is part of the Stage 2 snapshot, so a cooldown still suppresses
  and a pending reminder still fires after a restart.
- Emission policy is deliberately excluded from the state fingerprint, so
  retuning a cooldown does not invalidate an existing checkpoint.
- Rules with no `emit` block are untouched: every qualifying evaluation emits,
  with no episode tracking.

**Not implemented: acknowledgement.** The original sketch listed an
`acknowledged` state. Acknowledging an alert requires an inbound operator API,
which contradicts the repo's own boundary against being a rule-management
product. That state belongs in the alerting system consuming these payloads, and
`docs/rule-language.md` says so explicitly rather than leaving it implied.

**Unblocks.** `recompute` from Stage 1 needed retraction before a late event
could reopen a closed window. The episode model now provides the correlation
handle that a retraction would use.

---

### Stage 4 — Rule versioning and hot reload — complete

**Goal.** Configuration-driven systems eventually have to manage rule lifecycle.
Swap `rule-v1` for `rule-v2` without recreating the process.

**Delivered.**

```python
report = engine.reload(new_rules, policy="preserve", activate_at=None)
```

- `preserve` keeps state where the rule's structure is unchanged and discards it
  otherwise; `reset` always discards; `drain` keeps the previous definition
  running for entities with an open alert episode until it resolves, so an alert
  that fired under the old rule is closed by the old rule.
- "Compatible" is the Stage 2 state fingerprint, exactly as anticipated: trigger
  type, entity filter, sources, window geometry, timeouts, cron, lookback.
  Thresholds, messages, severities, sinks, and `emit` are excluded, so ordinary
  tuning does not cost state.
- `activate_at` stages a swap until the watermark reaches that instant;
  `last_reload_report()` returns the result once it applies.
- A typed `ReloadReport` records each rule as `preserved`, `reset`, `draining`,
  `added`, or `removed`, with the draining entities named.

**Known limitation.** Snapshots do not carry in-progress drains or staged
reloads: a restore puts every entity on the rules passed to `restore()`.
Serializing a draining definition would mean serializing whole compiled rules
into the snapshot, which is a bigger change than it is worth right now. The
behaviour is pinned by test so it cannot drift silently.

---

### Stage 5 — Temporal sequences and correlation — complete

**Goal.** The largest single increase in expressive power: ordered temporal
patterns, and negated correlation.

**Delivered.** A `sequence` trigger with a required `within` bound:

```yaml
trigger:
  type: sequence
  within: 5m
sequence:
  - sensor_type: access_denied
  - sensor_type: access_denied
  - sensor_type: access_granted
without:
  sensor_type: credential_reset
```

- Ordered matching with skip-till-next semantics: an event that is not the next
  expected step is ignored rather than breaking a partial match.
- `without` cancels every partial match in flight for that entity, covering the
  negated form.
- Matches are non-overlapping. A completed match consumes the entity's partial
  state, so a burst produces one alert rather than a cascade.
- Partial matches expire against `within`, which is why the bound is required
  rather than optional: it is what keeps state bounded per entity, and therefore
  snapshottable. Partial matches are carried in snapshots and covered by the
  state fingerprint.
- Sequence rules compose with `emit`, so a matched pattern can be throttled,
  repeated, and resolved like any other alert.

**Restrictions are the design.** No quantifiers, no alternation, no nesting, no
per-step conditions, no unbounded patterns. Repeated steps are written out
explicitly. `docs/rule-language.md` states each of these as an explicit
non-feature rather than leaving them to be discovered.

---

### Stage 6 — Explainability and rule tracing — complete

**Goal.** Declarative systems are hard to debug precisely because the logic is
data.

**Delivered.** `engine.explain(event)` returns a typed `ExplainResult`: one
`RuleExplanation` per rule, each a list of `ExplainCheck` predicates with the
value actually observed.

- **The non-firing case is covered properly.** `first_failure()` returns the
  check that stopped the rule, with its observed value, which is the half that
  is genuinely useful and genuinely harder.
- Every trigger family reports something meaningful: a failing operand, a
  sequence that was ignored, advanced, or cancelled, an absence timer with the
  time left before it fires, a composite's per-source silence, a window's
  aggregate over the current buffer.
- Suppression is explained rather than silently indistinguishable from
  not-matching: a suppressed rule reports how long its cooldown has left.
- `to_dict()`/`to_json()` are the primary output and `render()` is one view over
  the same structure, consistent with the other typed results.

**Read-only by construction.** Deciding suppression previously mutated episode
state, so that decision was split into a pure function shared by the emission
path and by explain. Nothing is registered, no watermark moves, and nothing is
delivered, so explain is safe on a live engine. Tests assert the engine is
byte-identical afterwards, including for sequence partial matches.

---

### Stage 7 — Simulation and backtesting — complete

**Goal.** Build on deterministic replay to make rule changes safe to ship.

**Delivered.**

```python
report = engine.simulate(events, from_time=..., to_time=...)
comparison = CompiledEngine.compare(events, baseline_rules, candidate_rules)
```

- Per-rule statistics: evaluations, alerts, fires, repeats, resolutions,
  suppressions, entities affected, first and last alert, and mean and maximum
  episode duration. `fire_rate` is alerts per evaluation.
- Counting suppressions needed a counter the engine did not have, so
  `suppressed_counts()` was added and is carried through snapshots. It is an
  operational metric in its own right, not only a simulation input.
- `compare()` answers "is this rule change safe?" without deploying it: alerts
  only under one version, alerts shared, and per-rule deltas in alert and
  suppression volume. Alerts are matched on rule, entity, timestamp, and
  lifecycle, so an unchanged alert reads as shared rather than as one removed
  and one added.

**Clean-room by construction.** A backtest runs in a fresh engine built from the
same rules. The live engine is untouched, and the result depends on the stream
rather than on state the caller happens to be carrying — asserted by a test that
compares a fresh engine against a used one and requires identical reports.

---

### Stage 8 — Partitioning and keyed state — complete

**Goal.** Make the entity model explicit rather than assumed.

**Delivered.**

```yaml
partition_by:
  - customer_id
  - device_id
```

- The partition key becomes the identity for a rule: its state, timers, alert
  episodes, and the `entity_id` reported on its alerts. Composite keys join with
  `|`. Values are read from an event's new `attributes` map, then from its
  built-in fields.
- Omitting `partition_by` is exactly `partition_by: [entity_id]`, which is why
  every pre-existing test passed unchanged through the restructure.
- Ordering and state isolation hold within a partition: a cooldown in one cannot
  suppress another, and absence timers run independently per partition.
- The partition scheme is part of the state fingerprint, so changing it is
  refused on restore rather than silently reinterpreting state keyed by
  something else.
- A rule declaring `partition_by` must use `entity_id: "*"` in every source,
  rejected at compile time. A custom key replaces `entity_id` as the identity,
  so an entity filter alongside it would be ambiguous.
- An event missing a partition field is skipped by that rule rather than being
  forced into a wrong partition, and `explain()` omits the rule instead of
  reporting a misleading result.

**Explicit non-goal, restated in the docs.** This is a state-model change. It is
a clean path toward parallel execution later, and the engine remains
single-process and in-memory. Nothing here makes it distributed.

---

## Not prioritized

Deliberately postponed. Each would grow the repository without deepening the
engineering:

- Kafka, Redis, or database integrations
- Kubernetes or deployment tooling
- A graphical rule builder
- Additional HTTP integrations beyond the maintained five sinks
- A custom expression programming language
- AI-generated rules

The five-adapter sink surface stays fixed. `docs/scope-boundary.md` records the
reasoning; changes to that boundary belong there first.

## Maintenance rule

Update this file when a stage completes or is materially re-scoped, and keep
`README.md` and `docs/scope-boundary.md` aligned in the same change set.
Per-change detail belongs in `CHANGELOG.md`, not here.
