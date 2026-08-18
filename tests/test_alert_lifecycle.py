from datetime import datetime, timedelta

from rule_engine.compiler import compile_rule
from rule_engine.declarative import load_rule_yaml
from rule_engine.models import EngineSnapshot
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
description: Alert when a source goes quiet
emit:
  resolve: true
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
    message: "No source_alpha"
    sinks: []
"""


def build(emit: str = "") -> CompiledEngine:
    return CompiledEngine([compile_rule(load_rule_yaml(EVENT_RULE.format(emit=emit)))])


def event_at(offset_minutes: float, value: float = 50.0) -> SensorEvent:
    moment = BASE + timedelta(minutes=offset_minutes)
    return SensorEvent(
        entity_id="entity-1",
        sensor_type="source_alpha",
        value=value,
        timestamp_ms=int(moment.timestamp() * 1000),
    )


def lifecycles(alerts):
    return [alert.alert.metadata["lifecycle"] for alert in alerts]


def test_a_rule_without_an_emit_block_emits_every_time():
    """Unconfigured rules keep the original behaviour: no episodes, no throttling."""
    engine = build()

    alerts = engine.replay([event_at(0), event_at(1), event_at(2)])

    assert lifecycles(alerts) == ["firing", "firing", "firing"]


def test_cooldown_suppresses_within_the_window():
    engine = build("emit:\n  cooldown: 30m")

    alerts = engine.replay([event_at(0), event_at(5), event_at(10), event_at(20)])

    assert lifecycles(alerts) == ["firing"]


def test_cooldown_allows_emission_once_it_elapses():
    engine = build("emit:\n  cooldown: 30m")

    alerts = engine.replay([event_at(0), event_at(10), event_at(31), event_at(40)])

    assert lifecycles(alerts) == ["firing", "repeat"]


def test_repeat_every_re_emits_without_any_new_event():
    """The reminder is timer-driven, so it must fire on advance alone."""
    engine = build("emit:\n  repeat_every: 1h")
    engine.replay([event_at(0)])

    alerts = engine.advance_to(BASE + timedelta(hours=3, minutes=1))

    assert lifecycles(alerts) == ["repeat", "repeat", "repeat"]


def test_resolve_emits_when_the_condition_clears():
    engine = build("emit:\n  resolve: true")

    alerts = engine.replay([event_at(0, 50.0), event_at(5, 1.0)])

    assert lifecycles(alerts) == ["firing", "resolved"]


def test_no_resolution_without_an_open_episode():
    engine = build("emit:\n  resolve: true")

    alerts = engine.replay([event_at(0, 1.0), event_at(5, 2.0)])

    assert alerts == []


def test_a_new_episode_starts_after_a_resolution():
    engine = build("emit:\n  resolve: true\n  cooldown: 30m")

    alerts = engine.replay([event_at(0, 50.0), event_at(5, 1.0), event_at(10, 50.0)])

    assert lifecycles(alerts) == ["firing", "resolved", "firing"]


def test_repeats_and_resolution_share_the_firing_correlation_id():
    engine = build("emit:\n  cooldown: 10m\n  resolve: true")

    alerts = engine.replay([event_at(0, 50.0), event_at(11, 50.0), event_at(20, 1.0)])

    assert lifecycles(alerts) == ["firing", "repeat", "resolved"]
    ids = {alert.alert.metadata["correlation_id"] for alert in alerts}
    assert len(ids) == 1


def test_a_new_episode_gets_a_different_correlation_id():
    engine = build("emit:\n  resolve: true")

    alerts = engine.replay([event_at(0, 50.0), event_at(5, 1.0), event_at(10, 50.0)])

    assert alerts[0].alert.metadata["correlation_id"] != alerts[2].alert.metadata["correlation_id"]


def test_absence_alert_resolves_when_the_source_returns():
    engine = CompiledEngine([compile_rule(load_rule_yaml(ABSENCE_RULE))])

    alerts = engine.replay([event_at(0), event_at(45)])

    assert lifecycles(alerts) == ["firing", "resolved"]
    ids = {alert.alert.metadata["correlation_id"] for alert in alerts}
    assert len(ids) == 1


def test_lifecycle_state_survives_a_snapshot_round_trip():
    """Cooldown must still suppress after a restart, not reset to firing."""
    rule_yaml = EVENT_RULE.format(emit="emit:\n  cooldown: 30m")
    rules = [compile_rule(load_rule_yaml(rule_yaml))]
    engine = CompiledEngine(rules)
    engine.replay([event_at(0)])

    resumed = CompiledEngine.restore(
        EngineSnapshot.from_json(engine.snapshot().to_json()),
        [compile_rule(load_rule_yaml(rule_yaml))],
    )
    suppressed = resumed.replay([event_at(5)])
    later = resumed.replay([event_at(31)])

    assert suppressed == []
    assert lifecycles(later) == ["repeat"]


def test_a_pending_repeat_timer_survives_a_snapshot_round_trip():
    rule_yaml = EVENT_RULE.format(emit="emit:\n  repeat_every: 1h")
    engine = CompiledEngine([compile_rule(load_rule_yaml(rule_yaml))])
    engine.replay([event_at(0)])

    resumed = CompiledEngine.restore(
        EngineSnapshot.from_json(engine.snapshot().to_json()),
        [compile_rule(load_rule_yaml(rule_yaml))],
    )
    alerts = resumed.advance_to(BASE + timedelta(hours=1, minutes=1))

    assert lifecycles(alerts) == ["repeat"]


def test_resuming_matches_an_uninterrupted_run():
    rule_yaml = EVENT_RULE.format(emit="emit:\n  cooldown: 20m\n  resolve: true")
    events = [event_at(0, 50.0), event_at(5, 50.0), event_at(25, 50.0), event_at(30, 1.0)]

    uninterrupted = CompiledEngine([compile_rule(load_rule_yaml(rule_yaml))])
    expected = lifecycles(uninterrupted.replay(events))

    first = CompiledEngine([compile_rule(load_rule_yaml(rule_yaml))])
    actual = lifecycles(first.replay(events[:2]))
    resumed = CompiledEngine.restore(first.snapshot(), [compile_rule(load_rule_yaml(rule_yaml))])
    actual.extend(lifecycles(resumed.replay(events[2:])))

    assert actual == expected


def test_cosmetic_emit_changes_do_not_invalidate_a_snapshot():
    """Emission policy is not part of the state fingerprint."""
    engine = build("emit:\n  cooldown: 30m")
    engine.replay([event_at(0)])

    retuned = [compile_rule(load_rule_yaml(EVENT_RULE.format(emit="emit:\n  cooldown: 45m")))]
    resumed = CompiledEngine.restore(engine.snapshot(), retuned)

    assert resumed.watermark == engine.watermark
