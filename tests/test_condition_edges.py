from datetime import datetime, timedelta

from rule_engine.compiler import compile_rule
from rule_engine.declarative import load_rule_yaml
from rule_engine.runtime import CompiledEngine
from rule_engine.types import SensorEvent

BASE = datetime(2024, 1, 1, 12, 0, 0)

WINDOW_RULE = """
rule_id: window_mean
description: Emit when the windowed mean is high
trigger:
  type: window
  duration: 10m
  slide: 5m
sources:
  - sensor_type: source_alpha
    entity_id: "*"
aggregations:
  - id: avg
    function: mean
    field: value
condition:
  operator: AND
  operands:
    - metric: avg
      operator: gt
      value: 40
actions:
  - severity: warning
    message: "High mean"
    sinks: []
"""

MISSING_METRIC_RULE = """
rule_id: missing_metric
description: References a metric no event carries
trigger:
  type: event
sources:
  - sensor_type: source_alpha
    entity_id: "*"
condition:
  operator: AND
  operands:
    - metric: not_a_real_field
      operator: gt
      value: 1
actions:
  - severity: warning
    message: "Should never fire"
    sinks: []
"""


def event_at(offset_seconds: int, value: float = 1.0) -> SensorEvent:
    moment = BASE + timedelta(seconds=offset_seconds)
    return SensorEvent(
        entity_id="entity-1",
        sensor_type="source_alpha",
        value=value,
        timestamp_ms=int(moment.timestamp() * 1000),
    )


def build(yaml_text: str) -> CompiledEngine:
    return CompiledEngine([compile_rule(load_rule_yaml(yaml_text))])


def test_window_advancing_past_the_last_event_does_not_raise():
    """An aggregation over an empty window yields None; comparing it raised TypeError."""
    engine = build(WINDOW_RULE)

    alerts = engine.replay(
        [event_at(0, 50.0), event_at(120, 60.0)],
        until=BASE + timedelta(minutes=45),
    )

    assert all(alert.rule_id == "window_mean" for alert in alerts)


def test_an_empty_window_does_not_emit():
    """Absent data must not satisfy a threshold; silence is not a spike."""
    engine = build(WINDOW_RULE)

    alerts = engine.replay([event_at(0, 5.0)], until=BASE + timedelta(minutes=45))

    assert alerts == []


def test_a_metric_absent_from_the_event_is_unsatisfied():
    engine = build(MISSING_METRIC_RULE)

    alerts = engine.replay([event_at(0, 500.0)])

    assert alerts == []
