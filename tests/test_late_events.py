from datetime import datetime, timedelta

import pytest

from rule_engine.compiler import compile_rule
from rule_engine.declarative import load_rule_yaml
from rule_engine.runtime import CompiledEngine, EngineConfig
from rule_engine.types import SensorEvent

BASE = datetime(2024, 1, 1, 12, 0, 0)

EVENT_RULE = """
rule_id: reading_spike
description: Emit when a reading exceeds the threshold
allowed_lateness: {lateness}
trigger:
  type: event
sources:
  - sensor_type: source_alpha
    entity_id: "*"
condition:
  operator: AND
  operands:
    - metric: value
      operator: gt
      value: 10
actions:
  - severity: warning
    message: "High reading {{{{value}}}}"
    sinks: []
"""

WINDOW_RULE = """
rule_id: windowed_delta
description: Emit when the delta across the window is large
allowed_lateness: 10m
trigger:
  type: window
  duration: 30m
  slide: 30m
sources:
  - sensor_type: source_alpha
    entity_id: "*"
aggregations:
  - id: spread
    function: delta
    field: value
condition:
  operator: AND
  operands:
    - metric: spread
      operator: gt
      value: 100
actions:
  - severity: warning
    message: "Large spread"
    sinks: []
"""


def build(yaml_text: str, **config_kwargs) -> CompiledEngine:
    rule = compile_rule(load_rule_yaml(yaml_text))
    config = EngineConfig(**config_kwargs) if config_kwargs else None
    return CompiledEngine([rule], config=config)


def event_at(offset_seconds: int, value: float = 1.0) -> SensorEvent:
    moment = BASE + timedelta(seconds=offset_seconds)
    return SensorEvent(
        entity_id="entity-1",
        sensor_type="source_alpha",
        value=value,
        timestamp_ms=int(moment.timestamp() * 1000),
    )


def test_default_tolerance_is_zero_and_still_rejects():
    engine = build(EVENT_RULE.format(lateness="0s"))
    engine.process_event(event_at(600))

    with pytest.raises(ValueError, match="allowed_lateness"):
        engine.process_event(event_at(300))

    assert engine.late_event_metrics().rejected == 1


def test_a_rule_without_allowed_lateness_behaves_the_same():
    yaml_text = EVENT_RULE.replace("allowed_lateness: {lateness}\n", "")
    engine = build(yaml_text)
    engine.process_event(event_at(600))

    with pytest.raises(ValueError):
        engine.process_event(event_at(300))


def test_late_event_within_tolerance_is_evaluated():
    engine = build(EVENT_RULE.format(lateness="10m"))
    engine.process_event(event_at(600, value=1.0))

    alerts = engine.process_event(event_at(300, value=50.0))

    assert len(alerts) == 1
    assert alerts[0].alert.message == "High reading 50.0"


def test_a_tolerated_late_event_does_not_rewind_the_watermark():
    engine = build(EVENT_RULE.format(lateness="10m"))
    engine.process_event(event_at(600))
    before = engine.watermark

    engine.process_event(event_at(300, value=50.0))

    assert engine.watermark == before


def test_event_beyond_tolerance_is_rejected_by_default():
    engine = build(EVENT_RULE.format(lateness="1m"))
    engine.process_event(event_at(600))

    with pytest.raises(ValueError, match="exceeds the largest declared allowed_lateness"):
        engine.process_event(event_at(0))


def test_drop_policy_discards_instead_of_raising():
    engine = build(EVENT_RULE.format(lateness="1m"), late_event_policy="drop")
    engine.process_event(event_at(600))

    alerts = engine.process_event(event_at(0, value=50.0))

    assert alerts == []
    metrics = engine.late_event_metrics()
    assert metrics.dropped == 1
    assert metrics.rejected == 0
    assert metrics.accepted == 0


def test_metrics_track_totals_and_per_rule_counts():
    engine = build(EVENT_RULE.format(lateness="10m"), late_event_policy="drop")
    engine.process_event(event_at(600))

    engine.process_event(event_at(300, value=50.0))
    engine.process_event(event_at(-60, value=50.0))

    metrics = engine.late_event_metrics()
    assert metrics.total == 2
    assert metrics.accepted == 1
    assert metrics.dropped == 1
    assert metrics.per_rule_accepted == {"reading_spike": 1}
    assert metrics.to_dict()["total"] == 2


def test_lateness_equal_to_the_tolerance_is_accepted():
    """The boundary is inclusive: lateness == allowed_lateness is still in range."""
    engine = build(EVENT_RULE.format(lateness="10m"))
    engine.process_event(event_at(600))

    alerts = engine.process_event(event_at(0, value=50.0))

    assert len(alerts) == 1
    assert engine.late_event_metrics().accepted == 1


def test_explicit_zero_tolerance_is_accepted_by_the_compiler():
    engine = build(EVENT_RULE.format(lateness="0s"))

    assert engine.rules[0].allowed_lateness == timedelta(0)


def test_an_unknown_policy_is_rejected_at_construction():
    rule = compile_rule(load_rule_yaml(EVENT_RULE.format(lateness="1m")))

    with pytest.raises(ValueError, match="Unsupported late_event_policy"):
        CompiledEngine([rule], config=EngineConfig(late_event_policy="recompute"))


def test_out_of_order_arrival_converges_with_in_order_replay():
    """With tolerance covering the spread, arrival order must not change output."""
    in_order = build(EVENT_RULE.format(lateness="30m"))
    shuffled = build(EVENT_RULE.format(lateness="30m"))

    events = [event_at(0, 50.0), event_at(120, 5.0), event_at(240, 60.0), event_at(360, 70.0)]
    ordered_alerts = [a.alert.message for a in in_order.replay(events)]

    arrival = [events[3], events[0], events[2], events[1]]
    shuffled_alerts = []
    for event in arrival:
        shuffled_alerts.extend(a.alert.message for a in shuffled.process_event(event))

    assert sorted(shuffled_alerts) == sorted(ordered_alerts)


def test_late_event_is_inserted_in_order_so_delta_stays_correct():
    """delta reads values[-1] - values[0]; appending a late event would corrupt it."""
    engine = build(WINDOW_RULE)
    engine.process_event(event_at(60, value=100.0))
    engine.process_event(event_at(600, value=250.0))

    engine.process_event(event_at(300, value=999.0))

    rule_id = engine.rules[0].rule_id
    buffered = engine._entities["entity-1"][rule_id].buffered_events
    assert [e.timestamp for e in buffered] == sorted(e.timestamp for e in buffered)
    assert [e.value for e in buffered] == [100.0, 999.0, 250.0]


def test_late_event_does_not_drag_last_seen_backward():
    engine = build(EVENT_RULE.format(lateness="10m"))
    engine.process_event(event_at(600))
    rule_id = engine.rules[0].rule_id
    newest = engine._entities["entity-1"][rule_id].last_seen["source_alpha"]

    engine.process_event(event_at(300))

    assert engine._entities["entity-1"][rule_id].last_seen["source_alpha"] == newest
