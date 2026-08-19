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
  → late-event semantics          [partial]
    → checkpoint / recovery        [done]
      → suppression + alert lifecycle [done]
        → rule versioning + hot reload [done]
          → temporal sequences      [done]
            → explainability        [done]
              → simulation / backtesting
                → partitioned execution
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

### Stage 1 — Late-event and out-of-order handling — partially complete

**Goal.** Make the engine honest about the distinction that defines stream
processing: event time is not processing time.

Stage 0 made the ordering assumption explicit by rejecting violations. Stage 1
replaces blanket rejection with declared tolerance.

**Delivered.**

- `allowed_lateness`, a per-rule duration, compared inclusively. It accepts
  `0s` explicitly, which required `parse_duration` to grow an `allow_zero` flag.
- Tolerated late events are folded into rule state in place: the watermark never
  moves backward, no timers re-fire, `event` triggers are evaluated, window and
  scheduled buffers receive the event in timestamp order, and `last_seen` only
  ever advances.
- `EngineConfig.late_event_policy` — `reject` (default) or `drop` — governing
  events later than any rule tolerates. Engine-level rather than per-rule,
  because it decides the fate of an event that no rule can use.
- `CompiledEngine.late_event_metrics()`, a typed `LateEventMetrics` carrying
  totals, a per-rule breakdown, and structured exports.

**Remaining.**

- **`recompute`.** Reopening a window that has already closed means retracting
  the alert it emitted, so this is blocked on the Stage 3 lifecycle work. The
  policy value is deliberately rejected rather than accepted and silently
  degraded to plain acceptance.
- **Per-source and per-entity watermarks.** The watermark is still engine-wide.
  Splitting it changes when every timer fires, making it a timer-machinery
  refactor rather than an extension of the lateness work, so it is better done
  on its own. Until then, one lagging entity holds the watermark back for all
  of them.

**Done when.** Shuffled arrival converges with in-order replay for window rules
as well as event rules — which is exactly what `recompute` buys — and
watermarks are tracked per entity.

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

### Stage 7 — Simulation and backtesting

**Goal.** Build on deterministic replay to make rule changes safe to ship.

**Shape.**

```python
report = engine.simulate(events, from_time=..., to_time=...)
```

Producing per-rule statistics:

$$N_{\text{evaluations}},\quad N_{\text{fires}},\quad N_{\text{suppressed}},\quad N_{\text{resolved}},\quad \text{latency},\quad N_{\text{entities}}$$

Plus A/B comparison of $R_{v1}$ against $R_{v2}$ over the same event stream —
the strongest feature in this list for anyone changing production rules, and a
natural consumer of the Stage 4 versioning work.

**Done when.** A comparison report shows which alerts appear only under one
version, which are shared, and how suppression volume differs — enough to answer
"is this rule change safe?" without deploying it.

---

### Stage 8 — Partitioning and keyed state

**Goal.** Make the entity model explicit rather than assumed:

$$K(e) \rightarrow \text{independent rule state}$$

Today `SensorEvent.entity_id` is a single string, and every source in a rule must
share one `entity_id` filter. Partitioning is effectively hardcoded to one field.

**Shape.**

```yaml
partition_by:
  - customer_id
  - device_id
```

with guaranteed ordering and state isolation *within* each partition.

**Explicit non-goal.** This creates a clean path toward parallel execution
later. It does not make the engine distributed, and the documentation should not
imply that it does.

**Done when.** Composite partition keys work end to end, state isolation between
partitions is asserted by test, and `docs/scope-boundary.md` records that
partitioning is a state-model change rather than a distribution mechanism.

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
