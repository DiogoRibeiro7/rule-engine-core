# Embedding Examples

This document shows the intended Python embedding patterns for
`rule-engine-core`.

## 1. Build From YAML Strings

Use this when your application already has rule text in memory.

```python
from rule_engine import build_engine_from_yaml
from rule_engine.types import SensorEvent

yaml_text = """
rule_id: source_primary_spike
trigger:
  type: event
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
  operands:
    - metric: value
      operator: gt
      value: 180
actions:
  - severity: critical
    message: "Primary source spike for {{entity_id}}: {{value}}"
    sinks: []
"""

embedded = build_engine_from_yaml([yaml_text])
alerts = embedded.replay(
    [
        SensorEvent(
            entity_id="entity-1",
            sensor_type="source_primary",
            value=185.0,
            timestamp_ms=1704067200000,
        )
    ]
)
```

## 2. Build From Files

Use this when your embedding application treats the repository rule files as
the source of truth.

```python
from rule_engine import build_engine_from_files

embedded = build_engine_from_files(
    [
        "sample_rules/source_gap.yaml",
        "sample_rules/dual_source_gap.yaml",
    ]
)

metadata = embedded.rule_metadata()
```

## 3. Build From Precompiled Rules

Use this when you want an explicit compile step separate from runtime
construction.

```python
from rule_engine import compile_yaml_rule, create_engine

compiled_rule = compile_yaml_rule(yaml_text)
embedded = create_engine([compiled_rule])
```

## 4. Use The Standard Sink Registry

Use the registry helper when you want the maintained sink set without manual
adapter wiring.

```python
from rule_engine import build_engine_from_yaml, create_sink_registry

sink_registry = create_sink_registry(
    dead_letter_path="output/dead_letters.ndjson",
    dead_letter_max_records=1000,
    dead_letter_fsync=True,
)
embedded = build_engine_from_yaml(
    [yaml_text],
    sink_registry=sink_registry,
)
```

Use `dead_letter_max_records` to cap local retention when the file is only a
fallback buffer and `dead_letter_fsync=True` when you prefer stronger
single-process durability over write throughput.

## 5. Override Specific Transports

Use this when you want the standard adapter set but need custom queue or object
storage behavior.

```python
from rule_engine import create_sink_registry
from rule_engine.sinks import InMemoryQueueTransport

queue_transport = InMemoryQueueTransport()
sink_registry = create_sink_registry(
    queue_transport=queue_transport,
)
```

## 6. Inspect Typed Delivery Reports

Use `evaluate(...)` or `replay_with_report(...)` when you need delivery
observability in addition to the alerts.

```python
result = embedded.evaluate(events)

if result.has_failures:
    failed_entries = result.delivery_report.failed_entries()
    by_queue = result.delivery_report.metrics_for("queue")
    dead_letters = result.delivery_report.dead_letter_entries()
```

Useful helpers:

- `result.alert_count`
- `result.has_failures`
- `result.delivery_report.has_failures`
- `result.delivery_report.has_dead_letters`
- `result.delivery_report.sink_types()`
- `result.delivery_report.metrics_for(sink_type)`
- `result.delivery_report.entries_for_sink(sink_type)`
- `result.delivery_report.failed_entries()`
- `result.delivery_report.dead_letter_entries()`

## 7. Checkpoint And Resume

`snapshot()` captures the watermark, per-entity rule state, pending timers,
in-flight window buffers, and late-event counters. `restore()` rebuilds an
engine from it. Both sides are JSON-serializable, so a checkpoint can be written
to disk or an object store between runs.

```python
from rule_engine import CompiledEngine, EngineSnapshot

engine = CompiledEngine(compiled_rules)
engine.replay(first_batch)

with open("checkpoint.json", "w", encoding="utf-8") as handle:
    handle.write(engine.snapshot().to_json(indent=2))

# ... later, in a new process ...

with open("checkpoint.json", "r", encoding="utf-8") as handle:
    snapshot = EngineSnapshot.from_json(handle.read())

resumed = CompiledEngine.restore(snapshot, compiled_rules)
alerts = resumed.replay(next_batch)
```

Resuming from a checkpoint produces the same alerts as an uninterrupted replay
of the whole stream, including timers that were pending and windows that were
still open when the snapshot was taken.

### Rule changes between checkpoints

A snapshot records a structural fingerprint of each rule, covering the fields
that give retained state its meaning: trigger type, entity filter, sources,
window duration and slide, timeouts, cron, and lookback. Restoring into a rule
whose fingerprint changed raises, because the stored state no longer describes
the same thing.

Cosmetic edits do not invalidate a checkpoint. Message templates, severities,
sinks, and condition operands are excluded from the fingerprint, so tuning a
threshold or rewording an alert lets an existing snapshot restore cleanly.

Rules present only in the snapshot are dropped, and rules present only in the
new rule set start with empty state, so adding and removing rules between
checkpoints is supported. The snapshot watermark takes precedence over
`EngineConfig.initial_watermark`.

## 8. Reload Rules Without Restarting

`reload()` swaps the rule set on a live engine and reports what happened to each
rule's retained state.

```python
report = engine.reload(new_rules, policy="preserve")

for outcome in report.outcomes:
    print(outcome.rule_id, outcome.outcome, outcome.compatible)
```

### Policies

| Policy | Effect on retained state |
| --- | --- |
| `preserve` | Kept where the rule's structure is unchanged; discarded otherwise |
| `reset` | Always discarded |
| `drain` | Previous definition stays active for entities with an open alert episode until it resolves |

"Structure" is the same fingerprint checkpoints use: trigger type, entity filter,
sources, window duration and slide, timeouts, cron, and lookback. Retuning a
threshold, rewording a message, changing a severity, or adjusting `emit` keeps
state under `preserve`; changing a window or a timeout does not.

Each rule is reported as `preserved`, `reset`, `draining`, `added`, or
`removed`. Rules dropped from the set lose their state; rules newly added start
empty for entities that already exist.

### Draining

`drain` matters for rules with an `emit` block, because it is defined in terms of
open alert episodes. An entity mid-episode keeps the old definition until that
episode resolves, so an alert that fired under the old rule is closed by the old
rule rather than being orphaned. Entities with no open episode switch at once.

```python
engine.reload(new_rules, policy="drain")
engine.draining_rule_ids()   # ["source_gap"] until the open episodes close
```

### Staged activation

```python
engine.reload(new_rules, policy="reset", activate_at=cutover_time)
```

The swap is held until the watermark reaches `activate_at`, then applied
automatically. `reload()` returns a report with `applied=False`; the applied
report is available afterwards from `engine.last_reload_report()`.

### Limitation

Snapshots do not carry in-progress drains or staged reloads. A restore puts every
entity on the rules passed to `restore()`. Let a drain finish, or re-issue the
reload after restoring.

## 9. Explain Why A Rule Did Or Did Not Fire

`explain()` reports what every rule would do with an event, without changing
anything: no state is registered or mutated, no watermark moves, and nothing is
delivered. It is safe to call on a live engine.

```python
result = engine.explain(event)

print(result.render())
result.to_dict()            # same structure, for tooling
result.emitting_rule_ids()  # ["temperature_spike"]
```

```text
event: entity=facility-1 at 2024-01-01T12:02:00+00:00

rule: temperature_spike  [window]
    entity matches *                         PASS   observed=facility-1
    sensor_type in sensor_a                  PASS   observed=sensor_a
    events in the last 0:05:00               PASS   observed=3
        includes this event
    avg > 42                                 PASS   observed=43.13333333333333
    not suppressed                           PASS
  outcome: would_emit
    would emit as firing
```

The non-firing case is the useful half. `first_failure()` returns the check that
stopped the rule, with the value actually observed:

```python
failure = result.by_rule("reading_spike").first_failure()
failure.label      # "value > 40"
failure.observed   # 12.0
```

### Outcomes

| Outcome | Meaning |
| --- | --- |
| `would_emit` | Every check passed |
| `entity_not_matched` | The rule's entity filter excludes this entity |
| `source_not_matched` | The event's sensor type is not one of the rule's sources |
| `condition_not_met` | A condition operand failed; `first_failure()` names it |
| `suppressed` | The rule would fire but its cooldown has not elapsed |
| `waiting` | An absence timer is running but has not reached its deadline |
| `timer_not_started` | An absence rule has never seen a reading |
| `timer_reset` | This event is a reading, so the absence timer restarts |
| `already_fired` | The absence or composite alert is already active |
| `ignored` | Not the next expected step of a sequence |
| `advanced` | A sequence moved forward but is not complete |
| `cancelled` | A `without` event cancelled the sequence matches in flight |
| `scheduled` | Scheduled rules run on their cron, not on events |

The text rendering is one view over the structure, not the primary output.
`to_dict()` and `to_json()` give the same information for downstream tooling.

## 10. When To Use Which Surface

- Use `build_engine_from_yaml(...)` for the smallest embedding surface.
- Use `build_engine_from_files(...)` when files are the deployment artifact.
- Use `compile_yaml_rule(...)` plus `create_engine(...)` when compile-time and
  runtime should stay separate in your host application.
- Use `evaluate(...)` when you want both alerts and a typed delivery report.
- Use `replay(...)` when you only need emitted alerts.
