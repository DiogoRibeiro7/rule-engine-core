from datetime import datetime, timedelta

from rule_engine.compiler import compile_rule
from rule_engine.declarative import load_rule_yaml
from rule_engine.runtime import CompiledEngine
from rule_engine.types import SensorEvent

BASE = datetime(2024, 1, 1, 12, 0, 0)

RULE = """
rule_id: reading_spike
description: Emit when a reading exceeds the threshold
{emit}
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
      value: {threshold}
actions:
  - severity: warning
    message: "High reading"
    sinks: []
"""

QUIET_RULE = """
rule_id: never_fires
trigger:
  type: event
sources:
  - sensor_type: source_beta
    entity_id: "*"
condition:
  operator: AND
  operands:
    - metric: value
      operator: gt
      value: 100000
actions:
  - severity: warning
    message: "Never"
    sinks: []
"""


def rules(threshold=40, emit=""):
    return [compile_rule(load_rule_yaml(RULE.format(threshold=threshold, emit=emit)))]


def ev(minutes: float, value: float = 50.0, entity_id="entity-1", sensor_type="source_alpha"):
    moment = BASE + timedelta(minutes=minutes)
    return SensorEvent(
        entity_id=entity_id,
        sensor_type=sensor_type,
        value=value,
        timestamp_ms=int(moment.timestamp() * 1000),
    )


# --- basic statistics ---------------------------------------------------------


def test_counts_evaluations_alerts_and_entities():
    engine = CompiledEngine(rules())
    events = [ev(0, 50.0, "entity-1"), ev(1, 5.0, "entity-1"), ev(2, 60.0, "entity-2")]

    stats = engine.simulate(events).by_rule("reading_spike")

    assert stats.evaluations == 3
    assert stats.alerts == 2
    assert stats.fires == 2
    assert stats.entity_count == 2
    assert stats.entities == ["entity-1", "entity-2"]


def test_counts_repeats_resolutions_and_suppressions():
    engine = CompiledEngine(rules(emit="emit:\n  cooldown: 10m\n  resolve: true"))
    events = [ev(0, 50.0), ev(5, 60.0), ev(15, 70.0), ev(20, 1.0)]

    stats = engine.simulate(events).by_rule("reading_spike")

    assert stats.fires == 1
    assert stats.repeats == 1
    assert stats.resolutions == 1
    assert stats.suppressed == 1


def test_reports_episode_duration():
    engine = CompiledEngine(rules(emit="emit:\n  resolve: true"))

    stats = engine.simulate([ev(0, 50.0), ev(20, 1.0)]).by_rule("reading_spike")

    assert stats.mean_episode_seconds == 1200.0
    assert stats.max_episode_seconds == 1200.0


def test_a_rule_that_never_fires_is_still_reported():
    engine = CompiledEngine(rules() + [compile_rule(load_rule_yaml(QUIET_RULE))])

    report = engine.simulate([ev(0, 50.0)])
    quiet = report.by_rule("never_fires")

    assert quiet is not None
    assert quiet.alerts == 0
    assert quiet.evaluations == 0
    assert quiet.fire_rate is None


def test_fire_rate_is_alerts_per_evaluation():
    engine = CompiledEngine(rules())

    stats = engine.simulate([ev(0, 50.0), ev(1, 1.0)]).by_rule("reading_spike")

    assert stats.fire_rate == 0.5


def test_the_report_records_the_stream_size_and_serializes():
    engine = CompiledEngine(rules())

    report = engine.simulate([ev(0, 50.0), ev(1, 5.0)])

    assert report.event_count == 2
    assert report.alert_count == 1
    assert report.elapsed_ms >= 0.0
    assert report.to_dict()["rules"][0]["rule_id"] == "reading_spike"


def test_noisiest_rules_ranks_by_alert_volume():
    engine = CompiledEngine(rules() + [compile_rule(load_rule_yaml(QUIET_RULE))])

    report = engine.simulate([ev(0, 50.0), ev(1, 60.0)])

    assert report.noisiest_rules(1)[0].rule_id == "reading_spike"


# --- windowing ----------------------------------------------------------------


def test_from_time_and_to_time_bound_the_stream():
    engine = CompiledEngine(rules())
    events = [ev(0, 50.0), ev(10, 50.0), ev(20, 50.0)]

    report = engine.simulate(
        events, from_time=BASE + timedelta(minutes=5), to_time=BASE + timedelta(minutes=15)
    )

    assert report.event_count == 1
    assert report.alert_count == 1


def test_events_are_sorted_before_simulating():
    engine = CompiledEngine(rules())
    shuffled = [ev(20, 50.0), ev(0, 50.0), ev(10, 50.0)]

    report = engine.simulate(shuffled)

    assert report.alert_count == 3


# --- isolation ----------------------------------------------------------------


def test_simulate_does_not_touch_the_live_engine():
    engine = CompiledEngine(rules(emit="emit:\n  cooldown: 30m"))
    engine.replay([ev(0, 50.0)])
    before = engine.watermark

    engine.simulate([ev(100, 50.0), ev(200, 50.0)])

    assert engine.watermark == before
    assert engine.suppressed_counts() == {}


def test_simulate_starts_from_a_clean_state():
    """A backtest must depend on the stream, not on whatever state is carried."""
    fresh = CompiledEngine(rules(emit="emit:\n  cooldown: 30m"))
    used = CompiledEngine(rules(emit="emit:\n  cooldown: 30m"))
    used.replay([ev(0, 50.0)])

    events = [ev(100, 50.0), ev(105, 50.0)]

    from_fresh = fresh.simulate(events).to_dict()
    from_used = used.simulate(events).to_dict()

    # elapsed_ms is wall clock, so it is the one field that legitimately differs
    from_fresh.pop("elapsed_ms")
    from_used.pop("elapsed_ms")
    assert from_fresh == from_used


def test_simulating_twice_gives_the_same_answer():
    engine = CompiledEngine(rules(emit="emit:\n  cooldown: 10m"))
    events = [ev(0, 50.0), ev(5, 50.0), ev(20, 50.0)]

    first = engine.simulate(events).to_dict()
    second = engine.simulate(events).to_dict()

    first.pop("elapsed_ms")
    second.pop("elapsed_ms")
    assert first == second


# --- comparison ---------------------------------------------------------------


def test_comparison_reports_which_alerts_each_version_produces():
    events = [ev(0, 50.0), ev(10, 70.0), ev(20, 45.0)]

    comparison = CompiledEngine.compare(events, rules(threshold=40), rules(threshold=65))

    assert comparison.baseline.alert_count == 3
    assert comparison.candidate.alert_count == 1
    assert comparison.alert_delta == -2
    assert comparison.shared == 1
    assert len(comparison.only_baseline) == 2
    assert comparison.only_candidate == []


def test_comparison_names_the_alerts_only_one_version_produces():
    events = [ev(0, 50.0)]

    comparison = CompiledEngine.compare(events, rules(threshold=40), rules(threshold=65))

    assert comparison.only_baseline[0]["rule_id"] == "reading_spike"
    assert comparison.only_baseline[0]["entity_id"] == "entity-1"
    assert comparison.only_baseline[0]["lifecycle"] == "firing"


def test_comparison_reports_per_rule_deltas():
    events = [ev(0, 50.0), ev(10, 70.0)]

    comparison = CompiledEngine.compare(events, rules(threshold=40), rules(threshold=65))

    assert comparison.rule_deltas()["reading_spike"]["alerts"] == -1


def test_an_identical_rule_set_produces_no_difference():
    events = [ev(0, 50.0), ev(10, 70.0)]

    comparison = CompiledEngine.compare(events, rules(threshold=40), rules(threshold=40))

    assert comparison.alert_delta == 0
    assert comparison.only_baseline == []
    assert comparison.only_candidate == []
    assert comparison.shared == 2


def test_comparison_shows_a_cooldown_reducing_volume():
    events = [ev(minutes, 50.0) for minutes in range(0, 30, 5)]

    comparison = CompiledEngine.compare(
        events, rules(), rules(emit="emit:\n  cooldown: 1h")
    )

    assert comparison.alert_delta < 0
    assert comparison.rule_deltas()["reading_spike"]["suppressed"] > 0


def test_comparison_serializes():
    comparison = CompiledEngine.compare([ev(0, 50.0)], rules(40), rules(65))

    payload = comparison.to_dict()

    assert payload["alert_delta"] == -1
    assert "baseline" in payload and "candidate" in payload
