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
description: Emit when a reading exceeds the threshold
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
    message: "High reading {{value}}"
    sinks: []
"""

ABSENCE_RULE = """
rule_id: source_gap
description: Alert when a source goes quiet
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
    message: "No source_alpha for {{entity_id}}"
    sinks: []
"""

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


def compile_rules(*yaml_texts):
    return [compile_rule(load_rule_yaml(text)) for text in yaml_texts]


def event_at(offset_seconds: int, value: float = 1.0, entity_id: str = "entity-1") -> SensorEvent:
    moment = BASE + timedelta(seconds=offset_seconds)
    return SensorEvent(
        entity_id=entity_id,
        sensor_type="source_alpha",
        value=value,
        timestamp_ms=int(moment.timestamp() * 1000),
    )


def messages(alerts):
    return [(a.entity_id, a.rule_id, a.timestamp, a.alert.message) for a in alerts]


def run_uninterrupted(yaml_texts, events, until=None):
    engine = CompiledEngine(compile_rules(*yaml_texts))
    return messages(engine.replay(events, until=until))


def run_with_restart(yaml_texts, events, split, until=None):
    """Replay the first slice, snapshot through JSON, then resume in a new engine."""
    first = CompiledEngine(compile_rules(*yaml_texts))
    emitted = messages(first.replay(events[:split]))

    payload = first.snapshot().to_json()
    resumed = CompiledEngine.restore(
        EngineSnapshot.from_json(payload), compile_rules(*yaml_texts)
    )

    emitted.extend(messages(resumed.replay(events[split:], until=until)))
    return emitted


@pytest.mark.parametrize("split", [1, 2, 3, 4])
def test_event_rule_recovery_matches_uninterrupted_replay(split):
    events = [event_at(0, 50.0), event_at(60, 5.0), event_at(120, 80.0), event_at(180, 90.0)]

    assert run_with_restart([EVENT_RULE], events, split) == run_uninterrupted([EVENT_RULE], events)


@pytest.mark.parametrize("split", [1, 2])
def test_pending_absence_timer_survives_recovery(split):
    """The absence timer must still fire after the restart, at the same instant."""
    events = [event_at(0), event_at(60)]
    until = BASE + timedelta(minutes=30)

    restarted = run_with_restart([ABSENCE_RULE], events, split, until=until)
    uninterrupted = run_uninterrupted([ABSENCE_RULE], events, until=until)

    assert restarted == uninterrupted
    assert len(uninterrupted) == 1


@pytest.mark.parametrize("split", [1, 2, 3])
def test_in_flight_window_survives_recovery(split):
    events = [event_at(0, 50.0), event_at(120, 60.0), event_at(240, 70.0)]
    until = BASE + timedelta(minutes=20)

    assert run_with_restart([WINDOW_RULE], events, split, until=until) == run_uninterrupted(
        [WINDOW_RULE], events, until=until
    )


def test_recovery_across_multiple_rules_and_entities():
    events = [
        event_at(0, 50.0, "entity-1"),
        event_at(30, 5.0, "entity-2"),
        event_at(90, 80.0, "entity-1"),
        event_at(150, 60.0, "entity-2"),
    ]
    rules = [EVENT_RULE, ABSENCE_RULE, WINDOW_RULE]
    until = BASE + timedelta(minutes=30)

    assert run_with_restart(rules, events, 2, until=until) == run_uninterrupted(
        rules, events, until=until
    )


def test_snapshot_round_trips_through_json():
    engine = CompiledEngine(compile_rules(EVENT_RULE, ABSENCE_RULE))
    engine.replay([event_at(0, 50.0), event_at(60, 5.0)])

    restored = EngineSnapshot.from_json(engine.snapshot().to_json())

    assert restored.to_dict() == engine.snapshot().to_dict()


def test_watermark_survives_the_round_trip():
    engine = CompiledEngine(compile_rules(EVENT_RULE))
    engine.replay([event_at(0), event_at(600)])

    resumed = CompiledEngine.restore(engine.snapshot(), compile_rules(EVENT_RULE))

    assert resumed.watermark == engine.watermark


def test_late_event_counters_survive_the_round_trip():
    rule = EVENT_RULE.replace("rule_id:", "allowed_lateness: 10m\nrule_id:", 1)
    engine = CompiledEngine(compile_rules(rule))
    engine.process_event(event_at(600))
    engine.process_event(event_at(300, value=50.0))

    resumed = CompiledEngine.restore(engine.snapshot(), compile_rules(rule))

    assert resumed.late_event_metrics().to_dict() == engine.late_event_metrics().to_dict()


def test_restoring_into_a_structurally_changed_rule_raises():
    engine = CompiledEngine(compile_rules(ABSENCE_RULE))
    engine.replay([event_at(0)])

    changed = compile_rules(ABSENCE_RULE.replace("timeout: 10m", "timeout: 45m"))

    with pytest.raises(ValueError, match="has changed shape"):
        CompiledEngine.restore(engine.snapshot(), changed)


def test_cosmetic_rule_edits_do_not_invalidate_a_snapshot():
    """Message and severity changes do not alter what retained state means."""
    engine = CompiledEngine(compile_rules(ABSENCE_RULE))
    engine.replay([event_at(0)])

    edited = compile_rules(
        ABSENCE_RULE.replace("severity: warning", "severity: critical").replace(
            "No source_alpha for", "Source went quiet for"
        )
    )
    resumed = CompiledEngine.restore(engine.snapshot(), edited)

    assert resumed.watermark == engine.watermark


def test_a_removed_rule_drops_its_state_and_an_added_rule_starts_empty():
    engine = CompiledEngine(compile_rules(EVENT_RULE, ABSENCE_RULE))
    engine.replay([event_at(0, 50.0)])

    resumed = CompiledEngine.restore(engine.snapshot(), compile_rules(EVENT_RULE, WINDOW_RULE))

    assert {rule.rule_id for rule in resumed.rules} == {"reading_spike", "window_mean"}
    assert resumed.watermark == engine.watermark


def test_snapshot_watermark_wins_over_configured_initial_watermark():
    engine = CompiledEngine(compile_rules(EVENT_RULE))
    engine.replay([event_at(600)])

    resumed = CompiledEngine.restore(
        engine.snapshot(),
        compile_rules(EVENT_RULE),
        config=EngineConfig(initial_watermark=BASE - timedelta(hours=5)),
    )

    assert resumed.watermark == engine.watermark
