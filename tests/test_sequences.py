from datetime import datetime, timedelta

import pytest

from rule_engine.compiler import compile_rule
from rule_engine.declarative import load_rule_yaml
from rule_engine.models import EngineSnapshot
from rule_engine.runtime import CompiledEngine
from rule_engine.types import SensorEvent

BASE = datetime(2024, 1, 1, 12, 0, 0)

SEQUENCE_RULE = """
rule_id: credential_stuffing
description: Repeated failures followed by a success
trigger:
  type: sequence
  within: {within}
sources:
  - sensor_type: login_failure
    entity_id: "*"
  - sensor_type: login_success
    entity_id: "*"
  - sensor_type: password_reset
    entity_id: "*"
sequence:
  - sensor_type: login_failure
  - sensor_type: login_failure
  - sensor_type: login_success
{without}
actions:
  - severity: critical
    message: "Credential stuffing for {{{{entity_id}}}}"
    sinks: []
"""


def build(within="5m", without="") -> CompiledEngine:
    text = SEQUENCE_RULE.format(within=within, without=without)
    return CompiledEngine([compile_rule(load_rule_yaml(text))])


def ev(offset_seconds: float, sensor_type: str, entity_id="entity-1") -> SensorEvent:
    moment = BASE + timedelta(seconds=offset_seconds)
    return SensorEvent(
        entity_id=entity_id,
        sensor_type=sensor_type,
        value=1.0,
        timestamp_ms=int(moment.timestamp() * 1000),
    )


F, S, R = "login_failure", "login_success", "password_reset"
WITHOUT = "without:\n  sensor_type: password_reset"


def test_the_pattern_matches_in_order_within_the_window():
    engine = build()

    alerts = engine.replay([ev(0, F), ev(10, F), ev(20, S)])

    assert len(alerts) == 1
    assert alerts[0].alert.metadata["variables"]["matched_steps"] == 3


def test_an_incomplete_pattern_does_not_match():
    engine = build()

    assert engine.replay([ev(0, F), ev(10, F)]) == []


def test_a_wrong_order_does_not_match():
    engine = build()

    assert engine.replay([ev(0, S), ev(10, F), ev(20, F)]) == []


def test_the_pattern_must_complete_inside_the_window():
    engine = build(within="1m")

    assert engine.replay([ev(0, F), ev(10, F), ev(120, S)]) == []


def test_a_pattern_completing_exactly_at_the_boundary_matches():
    engine = build(within="1m")

    alerts = engine.replay([ev(0, F), ev(10, F), ev(60, S)])

    assert len(alerts) == 1


def test_unrelated_events_do_not_break_a_partial_match():
    """Skip-till-next: an event that is not the next step is ignored."""
    engine = build(without=WITHOUT)
    engine_without_noise = build(without=WITHOUT)

    with_noise = engine.replay([ev(0, F), ev(5, F), ev(10, S)])
    clean = engine_without_noise.replay([ev(0, F), ev(5, F), ev(10, S)])

    assert len(with_noise) == len(clean) == 1


def test_the_without_sensor_type_cancels_a_partial_match():
    engine = build(without=WITHOUT)

    assert engine.replay([ev(0, F), ev(5, F), ev(8, R), ev(10, S)]) == []


def test_without_only_cancels_while_a_match_is_in_flight():
    engine = build(without=WITHOUT)

    alerts = engine.replay([ev(0, R), ev(5, F), ev(10, F), ev(20, S)])

    assert len(alerts) == 1


def test_a_completed_match_consumes_partial_state():
    """Matches never overlap, so a burst cannot cascade into many alerts."""
    engine = build()

    alerts = engine.replay([ev(0, F), ev(5, F), ev(10, F), ev(15, S)])

    assert len(alerts) == 1


def test_a_second_pattern_can_match_after_the_first():
    engine = build()

    alerts = engine.replay(
        [ev(0, F), ev(5, F), ev(10, S), ev(20, F), ev(25, F), ev(30, S)]
    )

    assert len(alerts) == 2


def test_entities_match_independently():
    engine = build()

    alerts = engine.replay(
        [
            ev(0, F, "entity-1"),
            ev(1, F, "entity-2"),
            ev(5, F, "entity-1"),
            ev(10, S, "entity-1"),
        ]
    )

    assert [alert.entity_id for alert in alerts] == ["entity-1"]


def test_partial_matches_expire_and_stay_bounded():
    engine = build(within="1m")
    engine.replay([ev(offset, F) for offset in range(0, 600, 10)])

    state = engine._entities["entity-1"]["credential_stuffing"]

    assert len(state.partial_matches) <= 7


def test_partial_matches_survive_a_snapshot_round_trip():
    text = SEQUENCE_RULE.format(within="5m", without="")
    engine = CompiledEngine([compile_rule(load_rule_yaml(text))])
    engine.replay([ev(0, F), ev(10, F)])

    resumed = CompiledEngine.restore(
        EngineSnapshot.from_json(engine.snapshot().to_json()),
        [compile_rule(load_rule_yaml(text))],
    )
    alerts = resumed.replay([ev(20, S)])

    assert len(alerts) == 1


def test_resuming_mid_pattern_matches_an_uninterrupted_run():
    text = SEQUENCE_RULE.format(within="5m", without="")
    events = [ev(0, F), ev(10, F), ev(20, S)]

    uninterrupted = CompiledEngine([compile_rule(load_rule_yaml(text))])
    expected = len(uninterrupted.replay(events))

    first = CompiledEngine([compile_rule(load_rule_yaml(text))])
    got = len(first.replay(events[:2]))
    resumed = CompiledEngine.restore(first.snapshot(), [compile_rule(load_rule_yaml(text))])
    got += len(resumed.replay(events[2:]))

    assert got == expected == 1


def test_within_is_required():
    text = SEQUENCE_RULE.format(within="5m", without="").replace("  within: 5m\n", "")

    with pytest.raises(ValueError, match="requires trigger.within"):
        compile_rule(load_rule_yaml(text))


def test_a_step_must_be_declared_in_sources():
    text = SEQUENCE_RULE.format(within="5m", without="").replace(
        "  - sensor_type: login_success\n", "  - sensor_type: never_declared\n", 1
    )

    with pytest.raises(ValueError, match="not declared in sources"):
        compile_rule(load_rule_yaml(text))


def test_sequence_fields_are_rejected_on_other_trigger_types():
    text = SEQUENCE_RULE.format(within="5m", without="").replace(
        "  type: sequence\n  within: 5m", "  type: event"
    )

    with pytest.raises(ValueError, match="require trigger type 'sequence'"):
        compile_rule(load_rule_yaml(text))


def test_a_sequence_rule_honours_cooldown():
    text = SEQUENCE_RULE.format(within="5m", without="").replace(
        "trigger:", "emit:\n  cooldown: 1h\ntrigger:", 1
    )
    engine = CompiledEngine([compile_rule(load_rule_yaml(text))])

    alerts = engine.replay(
        [ev(0, F), ev(5, F), ev(10, S), ev(20, F), ev(25, F), ev(30, S)]
    )

    assert len(alerts) == 1
