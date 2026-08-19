from datetime import datetime, timedelta

import pytest

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
  type: {trigger}
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
  - severity: {severity}
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
  timeout: {timeout}
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

SECOND_RULE = """
rule_id: other_rule
description: A second, unrelated rule
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
      value: 1000
actions:
  - severity: warning
    message: "Beta spike"
    sinks: []
"""


RESOLVE_EMIT = """emit:
  resolve: true"""


def rule(emit="", trigger="event", threshold=10, severity="warning"):
    return compile_rule(
        load_rule_yaml(
            RULE.format(emit=emit, trigger=trigger, threshold=threshold, severity=severity)
        )
    )


def absence_rule(timeout="10m"):
    return compile_rule(load_rule_yaml(ABSENCE_RULE.format(timeout=timeout)))


def event_at(offset_minutes: float, value: float = 50.0, entity_id="entity-1") -> SensorEvent:
    moment = BASE + timedelta(minutes=offset_minutes)
    return SensorEvent(
        entity_id=entity_id,
        sensor_type="source_alpha",
        value=value,
        timestamp_ms=int(moment.timestamp() * 1000),
    )


def lifecycles(alerts):
    return [alert.alert.metadata["lifecycle"] for alert in alerts]


def episode_of(engine, entity_id="entity-1", rule_id="reading_spike"):
    return engine._entities[entity_id][rule_id].episode_started


# --- policy: preserve ---------------------------------------------------------


def test_preserve_keeps_state_when_only_emission_policy_changed():
    engine = CompiledEngine([rule(emit="emit:\n  cooldown: 30m")])
    engine.replay([event_at(0)])
    started = episode_of(engine)

    report = engine.reload([rule(emit="emit:\n  cooldown: 45m")], policy="preserve")

    assert report.by_rule("reading_spike").outcome == "preserved"
    assert episode_of(engine) == started


def test_preserve_keeps_state_when_only_severity_changed():
    engine = CompiledEngine([rule(emit="emit:\n  cooldown: 30m", severity="warning")])
    engine.replay([event_at(0)])

    report = engine.reload(
        [rule(emit="emit:\n  cooldown: 30m", severity="critical")], policy="preserve"
    )

    assert report.by_rule("reading_spike").compatible is True
    assert episode_of(engine) is not None


def test_preserve_discards_state_when_the_structure_changed():
    engine = CompiledEngine([absence_rule("10m")])
    engine.replay([event_at(0)])

    report = engine.reload([absence_rule("45m")], policy="preserve")
    outcome = report.by_rule("source_gap")

    assert outcome.outcome == "reset"
    assert outcome.compatible is False
    # the slot is rebuilt empty rather than removed, so the pending timer is gone
    assert engine._entities["entity-1"]["source_gap"].last_seen == {}


def test_a_preserved_cooldown_still_suppresses_after_reload():
    engine = CompiledEngine([rule(emit="emit:\n  cooldown: 30m")])
    engine.replay([event_at(0)])

    engine.reload([rule(emit="emit:\n  cooldown: 30m", severity="critical")], policy="preserve")

    assert engine.replay([event_at(5)]) == []


# --- policy: reset ------------------------------------------------------------


def test_reset_discards_state_even_when_compatible():
    engine = CompiledEngine([rule(emit="emit:\n  cooldown: 30m")])
    engine.replay([event_at(0)])

    report = engine.reload([rule(emit="emit:\n  cooldown: 30m")], policy="reset")
    outcome = report.by_rule("reading_spike")

    assert outcome.outcome == "reset"
    assert outcome.compatible is True
    assert engine._entities["entity-1"]["reading_spike"].episode_started is None


def test_reset_lets_a_suppressed_rule_fire_again_immediately():
    engine = CompiledEngine([rule(emit="emit:\n  cooldown: 30m")])
    engine.replay([event_at(0)])

    engine.reload([rule(emit="emit:\n  cooldown: 30m")], policy="reset")

    assert lifecycles(engine.replay([event_at(5)])) == ["firing"]


# --- policy: drain ------------------------------------------------------------


def test_drain_keeps_the_old_definition_for_an_open_episode():
    engine = CompiledEngine([rule(emit="emit:\n  cooldown: 30m\n  resolve: true", threshold=10)])
    engine.replay([event_at(0, 50.0)])

    report = engine.reload(
        [rule(emit="emit:\n  cooldown: 30m\n  resolve: true", threshold=100)], policy="drain"
    )
    outcome = report.by_rule("reading_spike")

    assert outcome.outcome == "draining"
    assert outcome.draining_entities == ["entity-1"]
    assert engine.draining_rule_ids() == ["reading_spike"]


def test_drain_ends_once_the_episode_resolves():
    engine = CompiledEngine([rule(emit="emit:\n  resolve: true", threshold=10)])
    engine.replay([event_at(0, 50.0)])
    engine.reload([rule(emit="emit:\n  resolve: true", threshold=100)], policy="drain")

    resolved = engine.replay([event_at(5, 1.0)])

    assert lifecycles(resolved) == ["resolved"]
    assert engine.draining_rule_ids() == []


def test_after_draining_the_new_threshold_applies():
    engine = CompiledEngine([rule(emit="emit:\n  resolve: true", threshold=10)])
    engine.replay([event_at(0, 50.0)])
    engine.reload([rule(emit="emit:\n  resolve: true", threshold=100)], policy="drain")
    engine.replay([event_at(5, 1.0)])

    under_new_threshold = engine.replay([event_at(10, 50.0)])
    over_new_threshold = engine.replay([event_at(15, 500.0)])

    assert under_new_threshold == []
    assert lifecycles(over_new_threshold) == ["firing"]


def test_drain_swaps_immediately_for_entities_without_an_open_episode():
    engine = CompiledEngine([rule(emit="emit:\n  resolve: true", threshold=10)])
    engine.replay([event_at(0, 50.0, "entity-1"), event_at(0, 1.0, "entity-2")])

    report = engine.reload(
        [rule(emit="emit:\n  resolve: true", threshold=100)], policy="drain"
    )

    assert report.by_rule("reading_spike").draining_entities == ["entity-1"]
    assert "entity-2" not in engine._draining_entities["reading_spike"]


def test_drain_without_any_open_episode_behaves_like_preserve():
    engine = CompiledEngine([rule(emit="emit:\n  resolve: true")])
    engine.replay([event_at(0, 1.0)])

    report = engine.reload([rule(emit="emit:\n  resolve: true")], policy="drain")

    assert report.by_rule("reading_spike").outcome == "preserved"
    assert engine.draining_rule_ids() == []


# --- adding and removing rules ------------------------------------------------


def test_a_removed_rule_loses_its_state():
    engine = CompiledEngine([rule(), compile_rule(load_rule_yaml(SECOND_RULE))])
    engine.replay([event_at(0)])

    report = engine.reload([rule()], policy="preserve")

    assert report.by_rule("other_rule").outcome == "removed"
    assert "other_rule" not in engine._entities["entity-1"]
    assert engine.rule_metadata()[0].rule_id == "reading_spike"


def test_an_added_rule_starts_with_empty_state_for_existing_entities():
    engine = CompiledEngine([rule()])
    engine.replay([event_at(0)])

    report = engine.reload([rule(), compile_rule(load_rule_yaml(SECOND_RULE))], policy="preserve")

    assert report.by_rule("other_rule").outcome == "added"
    assert engine._entities["entity-1"]["other_rule"].episode_started is None


# --- staged activation --------------------------------------------------------


def test_a_staged_reload_does_not_apply_immediately():
    engine = CompiledEngine([rule(threshold=10)])
    engine.replay([event_at(0, 50.0)])

    report = engine.reload(
        [rule(threshold=100)], policy="reset", activate_at=BASE + timedelta(hours=1)
    )

    assert report.applied is False
    assert report.activate_at is not None
    assert lifecycles(engine.replay([event_at(5, 50.0)])) == ["firing"]


def test_a_staged_reload_applies_once_the_watermark_reaches_it():
    engine = CompiledEngine([rule(threshold=10)])
    engine.replay([event_at(0, 50.0)])
    engine.reload([rule(threshold=100)], policy="reset", activate_at=BASE + timedelta(hours=1))

    engine.advance_to(BASE + timedelta(hours=2))

    applied = engine.last_reload_report()
    assert applied.applied is True
    assert engine.replay([event_at(150, 50.0)]) == []
    assert lifecycles(engine.replay([event_at(160, 500.0)])) == ["firing"]


def test_an_activation_already_in_the_past_applies_at_once():
    engine = CompiledEngine([rule(threshold=10)])
    engine.replay([event_at(60, 50.0)])

    report = engine.reload([rule(threshold=100)], policy="reset", activate_at=BASE)

    assert report.applied is True


# --- guards -------------------------------------------------------------------


def test_a_snapshot_does_not_carry_an_in_progress_drain():
    """Documented limitation: drains and staged reloads are not snapshot state.

    A snapshot records rule state, not which definition an entity is running.
    Restoring puts every entity on the rules passed to restore().
    """
    engine = CompiledEngine([rule(emit=RESOLVE_EMIT, threshold=10)])
    engine.replay([event_at(0, 50.0)])
    engine.reload([rule(emit=RESOLVE_EMIT, threshold=100)], policy="drain")
    assert engine.draining_rule_ids() == ["reading_spike"]

    resumed = CompiledEngine.restore(
        engine.snapshot(), [rule(emit=RESOLVE_EMIT, threshold=100)]
    )

    assert resumed.draining_rule_ids() == []
    assert episode_of(resumed) is not None


def test_a_snapshot_does_not_carry_a_staged_reload():
    engine = CompiledEngine([rule(threshold=10)])
    engine.replay([event_at(0, 50.0)])
    engine.reload([rule(threshold=100)], policy="reset", activate_at=BASE + timedelta(hours=1))

    resumed = CompiledEngine.restore(engine.snapshot(), [rule(threshold=10)])
    resumed.advance_to(BASE + timedelta(hours=2))

    assert resumed.last_reload_report() is None


def test_an_unknown_policy_is_rejected():
    engine = CompiledEngine([rule()])

    with pytest.raises(ValueError, match="Unsupported reload policy"):
        engine.reload([rule()], policy="merge")


def test_the_report_serializes():
    engine = CompiledEngine([rule()])
    engine.replay([event_at(0)])

    payload = engine.reload([rule()], policy="preserve").to_dict()

    assert payload["applied"] is True
    assert payload["outcomes"][0]["rule_id"] == "reading_spike"


def test_reload_is_deterministic_mid_replay():
    """The same reload at the same point must produce the same alerts."""

    def run():
        engine = CompiledEngine([rule(emit="emit:\n  cooldown: 30m", threshold=10)])
        first = lifecycles(engine.replay([event_at(0, 50.0)]))
        engine.reload([rule(emit="emit:\n  cooldown: 30m", threshold=100)], policy="preserve")
        return first + lifecycles(engine.replay([event_at(40, 500.0)]))

    assert run() == run()
