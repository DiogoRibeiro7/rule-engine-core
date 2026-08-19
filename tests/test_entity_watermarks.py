from datetime import datetime, timedelta

import pytest

from rule_engine.compiler import compile_rule
from rule_engine.declarative import load_rule_yaml
from rule_engine.models import EngineSnapshot
from rule_engine.runtime import CompiledEngine, EngineConfig
from rule_engine.types import SensorEvent

BASE = datetime(2024, 1, 1, 12, 0, 0)

EVENT_RULE = """
rule_id: reading_spike
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
    message: "High reading"
    sinks: []
"""

ABSENCE_RULE = """
rule_id: source_gap
trigger:
  type: absence
  timeout: 10m
sources:
  - sensor_type: source_alpha
    entity_id: "*"
condition:
  operator: AND
actions:
  - severity: warning
    message: "Silent"
    sinks: []
"""


def build(yaml_text=EVENT_RULE, **config):
    rules = [compile_rule(load_rule_yaml(yaml_text))]
    return CompiledEngine(rules, config=EngineConfig(**config) if config else None)


def ev(minutes: float, entity_id: str, value: float = 50.0) -> SensorEvent:
    moment = BASE + timedelta(minutes=minutes)
    return SensorEvent(
        entity_id=entity_id,
        sensor_type="source_alpha",
        value=value,
        timestamp_ms=int(moment.timestamp() * 1000),
    )


# --- the behaviour this replaces ----------------------------------------------


def test_a_fast_entity_no_longer_makes_another_entity_look_late():
    """entity-b is in order for itself; entity-a running ahead must not reject it."""
    engine = build()
    engine.process_event(ev(0, "entity-b"))
    engine.process_event(ev(120, "entity-a"))

    alerts = engine.process_event(ev(5, "entity-b"))

    assert len(alerts) == 1
    assert engine.late_event_metrics().total == 0


def test_an_entity_going_backwards_against_itself_is_still_late():
    engine = build()
    engine.process_event(ev(60, "entity-a"))

    with pytest.raises(ValueError, match="allowed_lateness"):
        engine.process_event(ev(30, "entity-a"))


def test_each_entity_tracks_its_own_progress():
    engine = build()
    engine.process_event(ev(10, "entity-a"))
    engine.process_event(ev(20, "entity-b"))

    watermarks = engine.entity_watermarks()

    assert watermarks["entity-a"] == ev(10, "entity-a").timestamp
    assert watermarks["entity-b"] == ev(20, "entity-b").timestamp


def test_a_new_entity_starting_behind_the_others_is_accepted():
    engine = build()
    engine.process_event(ev(120, "entity-a"))

    alerts = engine.process_event(ev(1, "entity-newcomer"))

    assert len(alerts) == 1


# --- explicit clock advancement still applies to everyone ---------------------


def test_advance_to_raises_the_bar_for_every_entity():
    """advance_to is a statement that time moved, unlike another entity's traffic."""
    engine = build()
    engine.process_event(ev(0, "entity-a"))
    engine.advance_to(BASE + timedelta(minutes=120))

    with pytest.raises(ValueError, match="allowed_lateness"):
        engine.process_event(ev(5, "entity-b"))


def test_the_configured_initial_watermark_still_applies_to_every_entity():
    engine = build(initial_watermark=BASE + timedelta(minutes=60))

    with pytest.raises(ValueError, match="allowed_lateness"):
        engine.process_event(ev(5, "entity-a"))


# --- timers stay global, which is what keeps absence detection working --------


def test_an_absence_alert_still_fires_for_an_entity_that_went_silent():
    """Timer progress must stay global: a silent entity never advances its own clock."""
    engine = build(ABSENCE_RULE)
    engine.process_event(ev(0, "entity-quiet"))

    alerts = engine.replay([ev(minutes, "entity-busy") for minutes in range(0, 40, 5)])

    assert [alert.entity_id for alert in alerts] == ["entity-quiet"]


# --- interaction with the rest of the engine ----------------------------------


def test_out_of_order_across_entities_is_not_counted_as_late():
    engine = build()
    engine.process_event(ev(60, "entity-a"))
    engine.process_event(ev(10, "entity-b"))

    metrics = engine.late_event_metrics()

    assert metrics.total == 0
    assert metrics.accepted == 0


def test_genuine_lateness_is_still_counted():
    text = EVENT_RULE.replace("rule_id:", "allowed_lateness: 30m\nrule_id:", 1)
    engine = build(text)
    engine.process_event(ev(60, "entity-a"))

    engine.process_event(ev(50, "entity-a"))

    assert engine.late_event_metrics().total == 1


def test_entity_progress_survives_a_snapshot_round_trip():
    engine = build()
    engine.process_event(ev(60, "entity-a"))
    engine.process_event(ev(10, "entity-b"))

    resumed = CompiledEngine.restore(
        EngineSnapshot.from_json(engine.snapshot().to_json()),
        [compile_rule(load_rule_yaml(EVENT_RULE))],
    )

    assert resumed.entity_watermarks() == engine.entity_watermarks()
    assert len(resumed.process_event(ev(20, "entity-b"))) == 1


def test_the_floor_survives_a_snapshot_round_trip():
    engine = build()
    engine.process_event(ev(0, "entity-a"))
    engine.advance_to(BASE + timedelta(minutes=120))

    resumed = CompiledEngine.restore(
        engine.snapshot(), [compile_rule(load_rule_yaml(EVENT_RULE))]
    )

    with pytest.raises(ValueError, match="allowed_lateness"):
        resumed.process_event(ev(5, "entity-b"))


def test_replay_still_sorts_and_accepts_a_shuffled_batch():
    engine = build()
    shuffled = [ev(20, "entity-a"), ev(0, "entity-b"), ev(10, "entity-a")]

    alerts = engine.replay(shuffled)

    assert len(alerts) == 3
