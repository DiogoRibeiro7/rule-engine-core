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
correctness fixes
  → late-event semantics
    → checkpoint / recovery
      → suppression + alert lifecycle
        → rule versioning + hot reload
          → temporal sequences
            → explainability
              → simulation / backtesting
                → partitioned execution
```

---

### Stage 0 — Correctness fixes

Prerequisite for everything below. Known items:

- **Backward watermark movement is silently accepted.** `_apply_event` assigns
  `self._watermark = timestamp` unconditionally, so an out-of-order event moves
  the watermark backward and can retroactively change timer behaviour. Until
  Stage 1 defines a deliberate policy, this should be rejected explicitly rather
  than tolerated implicitly.
- **`mypy` fails on `main`.** `runner.py` types `main(argv)` as
  `Iterable[str] | None`, but `ArgumentParser.parse_args` requires
  `Sequence[str] | None`. CI runs `mypy`, so this is a red build.
- **A domain-specific URL survives in a sample rule.** `sample_rules/source_gap.yaml`
  posts to `hooks.hospital.internal`, which contradicts the repo's
  generic-examples boundary.

Any further findings from correctness review belong here before Stage 1 starts.

---

### Stage 1 — Late-event and out-of-order handling

**Goal.** Make the engine honest about the distinction that defines stream
processing:

$$\text{event time} \neq \text{processing time}$$

Today the engine assumes arrival order equals event order. Stage 0 makes that
assumption explicit by rejecting violations; Stage 1 replaces rejection with
deliberate policy.

**Shape.**

- `allowed_lateness` as a rule-level duration.
- Watermarks tracked per source and per entity rather than one global value.
- An explicit late-event policy: `drop`, `accept`, or `recompute`.

Under `recompute`, a late event reopens the affected window, re-evaluates it,
and reconciles any alert already emitted — which is why alert lifecycle
(Stage 3) has to be able to retract as well as emit.

**Done when.** A test suite replays a stream in shuffled arrival order and
asserts that, for each policy, output matches the documented expectation, and
that `accept` and `recompute` converge on the same result as in-order replay.

---

### Stage 2 — Checkpoint and recovery

**Goal.** Let a long-running embedder stop and resume without losing state.
More valuable to a serious rule engine than any additional adapter.

**Shape.** Serialize and restore the full state tuple:

$$(\text{watermark},\ \text{windows},\ \text{timers},\ \text{entity state},\ \text{dedupe state})$$

```python
snapshot = engine.snapshot()
engine = CompiledEngine.restore(snapshot, rules=[...])
```

Snapshots should be versioned and JSON-serializable, consistent with the typed
export helpers already present on delivery reports and metrics.

**Done when.** A recovery test splits an event stream at an arbitrary point,
snapshots, restores into a fresh engine, replays the remainder, and asserts the
output is identical to an uninterrupted replay — including pending timers and
in-flight windows.

---

### Stage 3 — Suppression, cooldowns, and alert lifecycle

**Goal.** The most valuable missing *operational* capability. A technically
correct rule engine with no lifecycle model still floods everything downstream.

**Shape.**

```yaml
emit:
  cooldown: 30m
  repeat_every: 2h
  resolve: true
```

Alerts gain an explicit state machine:

```text
inactive → firing → acknowledged/suppressed → resolved
```

This changes the delivery contract: sinks currently receive fire-and-forget
alerts, and will need to carry lifecycle transitions and correlate repeats and
resolutions to the originating alert. The existing idempotency key is the
natural correlation handle. `docs/delivery-contract.md` must be updated in the
same change set.

**Done when.** Cooldown suppresses within the window, `repeat_every` re-emits
after it, `resolve` emits a resolution when the condition clears, and lifecycle
state survives the Stage 2 snapshot round-trip.

---

### Stage 4 — Rule versioning and hot reload

**Goal.** Configuration-driven systems eventually have to manage rule
lifecycle. Support `rule-v1 → rule-v2` without recreating the process.

**Shape.** Explicit state-migration policies per reload:

- reset state,
- preserve compatible state,
- drain the old version,
- activate the new version at watermark $t$.

"Compatible" needs a definition, not a heuristic: a structural fingerprint of
the parts of a rule that own state (trigger type, window geometry, partition
key), computed at compile time. A change to any of those forces reset or drain;
changes elsewhere — message templates, sinks, severity — can preserve state.

**Done when.** Each policy has a test asserting exactly which state survives a
reload, and reloading a rule mid-replay produces documented, deterministic
output.

---

### Stage 5 — Temporal sequences and correlation

**Goal.** The largest single increase in expressive power. Support ordered
temporal patterns:

$$A \rightarrow B \rightarrow C \quad \text{within } 10\,\text{min}$$

and negated correlations:

$$A \land B \quad \text{without } C \text{ for } 5\,\text{min}$$

**Shape.** A deliberately restricted grammar — not a general CEP language:

```yaml
sequence:
  - event: login_failure
  - event: login_failure
  - event: login_success
within: 5m
```

**Constraints.** The restriction is the design. No unbounded backtracking, no
regex-style quantifiers, no user-defined expression language. Every pattern must
have a bounded window, so partial-match state stays bounded per entity — which
matters because that state has to be snapshottable under Stage 2.

`docs/rule-language.md` is the contract and must be extended in the same change
set.

**Done when.** Sequence matching is correct under out-of-order arrival within
`allowed_lateness`, partial-match state is bounded and snapshottable, and the
unsupported edges are documented as explicitly as the supported ones.

---

### Stage 6 — Explainability and rule tracing

**Goal.** Declarative systems are hard to debug precisely because the logic is
data. An explain mode fits this repository especially well and makes it far
easier to demonstrate.

**Shape.**

```python
result = engine.explain(event)
```

```text
rule: temperature-spike
matched filters:
    source == sensor-a        ✓
    temperature > 40          ✓
aggregation:
    mean(last 5m) = 43.2
    required > 42             ✓
suppression:
    cooldown expired          ✓
outcome:
    alert emitted
```

**The hard requirement.** Explaining why a rule *did not* fire is the valuable
half and the harder half. A non-firing rule must report the first predicate that
failed, with the actual observed value — not merely "no match".

This is why explainability sits after Stages 3 and 5: an explanation is only
useful if it can also say "suppressed by cooldown until 14:32" or "sequence
matched 2 of 3 steps, expired at 14:05".

**Done when.** Both firing and non-firing paths produce a structured, typed
trace with `to_dict()`/`to_json()` exports, consistent with the existing typed
result objects, and the text rendering above is one view over that structure
rather than the primary output.

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
