from datetime import datetime, timedelta

from rule_engine.compiler import compile_rule
from rule_engine.declarative import load_rule_yaml
from rule_engine.runtime import CompiledEngine
from rule_engine.types import SensorEvent

BASE = datetime(2024, 1, 1, 12, 0, 0)

EVENT_RULE = """
rule_id: reading_spike
description: Emit when a reading exceeds the threshold
{emit}
trigger:
  type: event
sources:
  - sensor_type: source_alpha
    entity_id: "{entity}"
condition:
  operator: AND
  operands:
    - metric: value
      operator: gt
      value: 40
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

SEQUENCE_RULE = """
rule_id: denied_then_granted
trigger:
  type: sequence
  within: 5m
sources:
  - sensor_type: denied
    entity_id: "*"
  - sensor_type: granted
    entity_id: "*"
  - sensor_type: reset
    entity_id: "*"
sequence:
  - sensor_type: denied
  - sensor_type: granted
without:
  sensor_type: reset
actions:
  - severity: critical
    message: "Pattern"
    sinks: []
"""


def event_engine(emit="", entity="*"):
    text = EVENT_RULE.format(emit=emit, entity=entity)
    return CompiledEngine([compile_rule(load_rule_yaml(text))])


def ev(minutes: float, value: float = 50.0, sensor_type="source_alpha", entity_id="entity-1"):
    moment = BASE + timedelta(minutes=minutes)
    return SensorEvent(
        entity_id=entity_id,
        sensor_type=sensor_type,
        value=value,
        timestamp_ms=int(moment.timestamp() * 1000),
    )


# --- the firing path ----------------------------------------------------------


def test_a_matching_event_reports_would_emit():
    engine = event_engine()

    explanation = engine.explain(ev(0, 50.0)).by_rule("reading_spike")

    assert explanation.would_emit
    assert explanation.first_failure() is None


def test_the_observed_value_is_reported_on_a_passing_check():
    engine = event_engine()

    explanation = engine.explain(ev(0, 50.0)).by_rule("reading_spike")
    check = [c for c in explanation.checks if c.label.startswith("value")][0]

    assert check.observed == 50.0
    assert check.expected == 40


# --- the non-firing path, which is the valuable half --------------------------


def test_a_failing_threshold_names_the_predicate_and_the_observed_value():
    engine = event_engine()

    explanation = engine.explain(ev(0, 12.0)).by_rule("reading_spike")
    failure = explanation.first_failure()

    assert explanation.outcome == "condition_not_met"
    assert failure.label == "value > 40"
    assert failure.observed == 12.0


def test_a_non_matching_sensor_type_is_reported():
    engine = event_engine()

    explanation = engine.explain(ev(0, 99.0, sensor_type="other")).by_rule("reading_spike")

    assert explanation.outcome == "source_not_matched"
    assert explanation.first_failure().observed == "other"


def test_a_non_matching_entity_is_reported():
    engine = event_engine(entity="entity-9")

    explanation = engine.explain(ev(0, 99.0)).by_rule("reading_spike")

    assert explanation.outcome == "entity_not_matched"
    assert explanation.first_failure().observed == "entity-1"


def test_suppression_is_reported_with_the_time_remaining():
    engine = event_engine(emit="emit:\n  cooldown: 30m")
    engine.replay([ev(0, 50.0)])

    explanation = engine.explain(ev(5, 50.0)).by_rule("reading_spike")

    assert explanation.outcome == "suppressed"
    assert "left to run" in explanation.detail
    assert explanation.first_failure().label == "not suppressed"


# --- other trigger families ---------------------------------------------------


def test_an_absence_rule_reports_time_left_before_it_fires():
    engine = CompiledEngine([compile_rule(load_rule_yaml(ABSENCE_RULE))])
    engine.replay([ev(0)])

    explanation = engine.explain(ev(5, sensor_type="unrelated")).by_rule("source_gap")

    assert explanation.outcome == "waiting"
    assert "left before the timer fires" in explanation.detail


def test_an_absence_rule_with_no_reading_yet_says_so():
    engine = CompiledEngine([compile_rule(load_rule_yaml(ABSENCE_RULE))])

    explanation = engine.explain(ev(0, sensor_type="unrelated")).by_rule("source_gap")

    assert explanation.outcome == "timer_not_started"


def test_a_matching_reading_reports_the_absence_timer_resetting():
    engine = CompiledEngine([compile_rule(load_rule_yaml(ABSENCE_RULE))])
    engine.replay([ev(0)])

    explanation = engine.explain(ev(1)).by_rule("source_gap")

    assert explanation.outcome == "timer_reset"


def test_a_sequence_reports_how_far_the_pattern_got():
    engine = CompiledEngine([compile_rule(load_rule_yaml(SEQUENCE_RULE))])
    engine.replay([ev(0, sensor_type="denied")])

    explanation = engine.explain(ev(1, sensor_type="granted")).by_rule("denied_then_granted")

    assert explanation.outcome == "would_emit"


def test_a_sequence_reports_an_ignored_event():
    engine = CompiledEngine([compile_rule(load_rule_yaml(SEQUENCE_RULE))])

    explanation = engine.explain(ev(0, sensor_type="granted")).by_rule("denied_then_granted")

    assert explanation.outcome == "ignored"


def test_a_sequence_reports_cancellation():
    engine = CompiledEngine([compile_rule(load_rule_yaml(SEQUENCE_RULE))])
    engine.replay([ev(0, sensor_type="denied")])

    explanation = engine.explain(ev(1, sensor_type="reset")).by_rule("denied_then_granted")

    assert explanation.outcome == "cancelled"


# --- explain must not change anything -----------------------------------------


def test_explain_does_not_move_the_watermark():
    engine = event_engine()
    engine.replay([ev(0, 50.0)])
    before = engine.watermark

    engine.explain(ev(60, 50.0))

    assert engine.watermark == before


def test_explain_does_not_open_an_episode():
    engine = event_engine(emit="emit:\n  cooldown: 30m")

    engine.explain(ev(0, 50.0))

    assert engine._entities == {}


def test_explain_does_not_register_a_new_entity():
    engine = event_engine()
    engine.replay([ev(0, 50.0)])

    engine.explain(ev(1, 50.0, entity_id="brand-new"))

    assert "brand-new" not in engine._entities


def test_explain_does_not_advance_a_sequence():
    engine = CompiledEngine([compile_rule(load_rule_yaml(SEQUENCE_RULE))])
    engine.replay([ev(0, sensor_type="denied")])
    before = list(engine._entities["entity-1"]["denied_then_granted"].partial_matches)

    engine.explain(ev(1, sensor_type="granted"))

    assert engine._entities["entity-1"]["denied_then_granted"].partial_matches == before


def test_explaining_a_suppressed_rule_twice_gives_the_same_answer():
    engine = event_engine(emit="emit:\n  cooldown: 30m")
    engine.replay([ev(0, 50.0)])

    first = engine.explain(ev(5, 50.0)).to_dict()
    second = engine.explain(ev(5, 50.0)).to_dict()

    assert first == second


# --- output shape -------------------------------------------------------------


def test_the_result_serializes_and_renders():
    engine = event_engine()

    result = engine.explain(ev(0, 50.0))

    assert result.to_dict()["rules"][0]["rule_id"] == "reading_spike"
    assert "reading_spike" in result.render()
    assert result.emitting_rule_ids() == ["reading_spike"]


def test_every_rule_is_explained_not_just_the_matching_one():
    rules = [
        compile_rule(load_rule_yaml(EVENT_RULE.format(emit="", entity="*"))),
        compile_rule(load_rule_yaml(ABSENCE_RULE)),
    ]
    engine = CompiledEngine(rules)

    result = engine.explain(ev(0, 50.0))

    assert {entry.rule_id for entry in result.rules} == {"reading_spike", "source_gap"}
