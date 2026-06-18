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
)
embedded = build_engine_from_yaml(
    [yaml_text],
    sink_registry=sink_registry,
)
```

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

## 7. When To Use Which Surface

- Use `build_engine_from_yaml(...)` for the smallest embedding surface.
- Use `build_engine_from_files(...)` when files are the deployment artifact.
- Use `compile_yaml_rule(...)` plus `create_engine(...)` when compile-time and
  runtime should stay separate in your host application.
- Use `evaluate(...)` when you want both alerts and a typed delivery report.
- Use `replay(...)` when you only need emitted alerts.
