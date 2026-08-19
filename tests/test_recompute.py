from datetime import datetime, timedelta

from rule_engine.compiler import compile_rule
from rule_engine.declarative import load_rule_yaml
from rule_engine.models import EngineSnapshot
from rule_engine.runtime import CompiledEngine, EngineConfig
from rule_engine.types import SensorEvent

BASE = datetime(2024, 1, 1, 12, 0, 0)

WINDOW_RULE = """
rule_id: windowed_mean
description: Emit when the windowed mean exceeds the threshold
allowed_lateness: 10m
trigger:
  type: window
  duration: 5m
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


def build(recompute: bool) -> CompiledEngine:
    return CompiledEngine(
        [compile_rule(load_rule_yaml(WINDOW_RULE))],
        config=EngineConfig(recompute_late_windows=recompute),
    )


def ev(minutes: float, value: float) -> SensorEvent:
    moment = BASE + timedelta(minutes=minutes)
    return SensorEvent(
        entity_id="entity-1",
        sensor_type="source_alpha",
        value=value,
        timestamp_ms=int(moment.timestamp() * 1000),
    )


def lifecycles(alerts):
    return [alert.alert.metadata["lifecycle"] for alert in alerts]


def test_recompute_is_off_by_default():
    assert EngineConfig().recompute_late_windows is False


def test_a_late_event_can_retract_a_closed_window():
    """The window fired on a high mean; a late low reading pulls it under."""
    engine = build(recompute=True)
    engine.replay([ev(1, 42.0)], until=BASE + timedelta(minutes=7))

    late = engine.process_event(ev(2, 1.0))

    assert lifecycles(late) == ["retracted"]


def test_without_recompute_the_closed_window_stands():
    engine = build(recompute=False)
    engine.replay([ev(1, 90.0)], until=BASE + timedelta(minutes=7))

    late = engine.process_event(ev(2, 1.0))

    assert late == []


def test_a_late_event_can_fire_a_window_that_had_not_fired():
    engine = build(recompute=True)
    engine.replay([ev(1, 10.0)], until=BASE + timedelta(minutes=7))

    late = engine.process_event(ev(2, 200.0))

    assert lifecycles(late) == ["firing"]


def test_a_late_event_that_changes_nothing_stays_silent():
    """mean(90, 95) is still over the threshold, so the verdict is unchanged."""
    engine = build(recompute=True)
    engine.replay([ev(1, 90.0)], until=BASE + timedelta(minutes=7))

    late = engine.process_event(ev(2, 95.0))

    assert late == []


def test_a_late_event_that_moves_the_value_but_not_the_verdict_stays_silent():
    """mean(90, 1) is 45.5: lower, but still over 40, so nothing is emitted."""
    engine = build(recompute=True)
    engine.replay([ev(1, 90.0)], until=BASE + timedelta(minutes=7))

    late = engine.process_event(ev(2, 1.0))

    assert late == []


def test_a_late_event_outside_any_closed_window_changes_nothing():
    engine = build(recompute=True)
    engine.replay([ev(1, 90.0)], until=BASE + timedelta(minutes=7))

    late = engine.process_event(ev(6, 1.0))

    assert late == []


def test_a_retraction_reuses_the_firing_correlation_id():
    engine = build(recompute=True)
    fired = engine.replay([ev(1, 42.0)], until=BASE + timedelta(minutes=7))

    retracted = engine.process_event(ev(2, 1.0))

    assert fired[0].alert.metadata["correlation_id"] == (
        retracted[0].alert.metadata["correlation_id"]
    )


def test_a_retracted_window_can_fire_again_if_a_later_event_restores_it():
    engine = build(recompute=True)
    engine.replay([ev(1, 42.0)], until=BASE + timedelta(minutes=7))
    engine.process_event(ev(2, 1.0))

    restored = engine.process_event(ev(3, 300.0))

    assert lifecycles(restored) == ["firing"]


def test_closed_window_records_stay_bounded():
    engine = build(recompute=True)
    engine.replay([ev(minutes, 90.0) for minutes in range(0, 120, 5)])

    state = engine._entities["entity-1"]["windowed_mean"]

    assert len(state.closed_windows) <= 4


def test_closed_window_verdicts_survive_a_snapshot_round_trip():
    engine = build(recompute=True)
    engine.replay([ev(1, 42.0)], until=BASE + timedelta(minutes=7))

    resumed = CompiledEngine.restore(
        EngineSnapshot.from_json(engine.snapshot().to_json()),
        [compile_rule(load_rule_yaml(WINDOW_RULE))],
        config=EngineConfig(recompute_late_windows=True),
    )
    late = resumed.process_event(ev(2, 1.0))

    assert lifecycles(late) == ["retracted"]


def test_recompute_respects_allowed_lateness():
    """Beyond tolerance the event is rejected before any recompute happens."""
    engine = build(recompute=True)
    engine.replay([ev(1, 90.0)], until=BASE + timedelta(minutes=60))

    try:
        engine.process_event(ev(2, 1.0))
    except ValueError as error:
        assert "allowed_lateness" in str(error)
    else:  # pragma: no cover - the tolerance must apply
        raise AssertionError("an event beyond tolerance should not have been accepted")


def test_an_in_order_simulation_has_nothing_to_retract():
    """simulate() sorts its input, so no event is late and nothing is reopened."""
    engine = build(recompute=True)

    stats = engine.simulate([ev(2, 1.0), ev(1, 42.0)]).by_rule("windowed_mean")

    assert stats.retractions == 0
