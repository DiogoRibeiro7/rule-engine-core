from datetime import datetime, timedelta

import pytest

from rule_engine.compiler import compile_rule
from rule_engine.declarative import load_rule_yaml
from rule_engine.models import EngineSnapshot
from rule_engine.runtime import CompiledEngine
from rule_engine.types import SensorEvent

BASE = datetime(2024, 1, 1, 12, 0, 0)

PARTITIONED_RULE = """
rule_id: reading_spike
description: Emit when a reading exceeds the threshold
{emit}
partition_by:
{keys}
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
      value: 40
actions:
  - severity: warning
    message: "High reading"
    sinks: []
"""

PLAIN_RULE = """
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
      value: 40
actions:
  - severity: warning
    message: "High reading"
    sinks: []
"""

ABSENCE_PARTITIONED = """
rule_id: source_gap
partition_by:
  - customer_id
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


def partitioned(keys=("customer_id",), emit=""):
    block = chr(10).join("  - " + key for key in keys)
    return [compile_rule(load_rule_yaml(PARTITIONED_RULE.format(keys=block, emit=emit)))]


def ev(minutes: float, value: float = 50.0, entity_id="device-1", **attributes):
    moment = BASE + timedelta(minutes=minutes)
    return SensorEvent(
        entity_id=entity_id,
        sensor_type="source_alpha",
        value=value,
        timestamp_ms=int(moment.timestamp() * 1000),
        attributes=attributes,
    )


# --- the default is unchanged -------------------------------------------------


def test_a_rule_without_partition_by_is_keyed_by_entity_id():
    engine = CompiledEngine([compile_rule(load_rule_yaml(PLAIN_RULE))])

    alerts = engine.replay([ev(0, 50.0, entity_id="device-1")])

    assert alerts[0].entity_id == "device-1"
    assert set(engine._entities) == {"device-1"}


def test_the_default_partition_is_entity_id():
    rule = compile_rule(load_rule_yaml(PLAIN_RULE))

    assert rule.partition_by == ["entity_id"]


# --- custom partitions --------------------------------------------------------


def test_state_is_keyed_by_the_declared_field():
    engine = CompiledEngine(partitioned())

    engine.replay(
        [
            ev(0, 50.0, entity_id="device-1", customer_id="acme"),
            ev(1, 50.0, entity_id="device-2", customer_id="acme"),
        ]
    )

    assert set(engine._entities) == {"acme"}


def test_alerts_are_identified_by_the_partition_key():
    engine = CompiledEngine(partitioned())

    alerts = engine.replay([ev(0, 50.0, entity_id="device-1", customer_id="acme")])

    assert alerts[0].entity_id == "acme"


def test_a_composite_key_joins_the_declared_fields():
    engine = CompiledEngine(partitioned(keys=("customer_id", "region")))

    engine.replay([ev(0, 50.0, customer_id="acme", region="eu")])

    assert set(engine._entities) == {"acme|eu"}


def test_partitions_keep_independent_state():
    """A cooldown in one partition must not suppress another."""
    engine = CompiledEngine(partitioned(emit="emit:\n  cooldown: 1h"))

    alerts = engine.replay(
        [
            ev(0, 50.0, customer_id="acme"),
            ev(1, 50.0, customer_id="globex"),
            ev(2, 50.0, customer_id="acme"),
        ]
    )

    assert [alert.entity_id for alert in alerts] == ["acme", "globex"]


def test_different_entities_sharing_a_partition_share_state():
    engine = CompiledEngine(partitioned(emit="emit:\n  cooldown: 1h"))

    alerts = engine.replay(
        [
            ev(0, 50.0, entity_id="device-1", customer_id="acme"),
            ev(1, 50.0, entity_id="device-2", customer_id="acme"),
        ]
    )

    assert len(alerts) == 1


def test_an_absence_timer_is_tracked_per_partition():
    engine = CompiledEngine([compile_rule(load_rule_yaml(ABSENCE_PARTITIONED))])
    engine.replay(
        [ev(0, customer_id="acme"), ev(0, customer_id="globex")],
        until=BASE + timedelta(minutes=30),
    )

    fired = sorted(
        alert.entity_id for alert in engine.replay([], until=BASE + timedelta(minutes=30))
    )

    assert fired == []
    assert set(engine._entities) == {"acme", "globex"}


def test_an_absence_alert_fires_once_per_partition():
    engine = CompiledEngine([compile_rule(load_rule_yaml(ABSENCE_PARTITIONED))])

    alerts = engine.replay(
        [ev(0, customer_id="acme"), ev(0, customer_id="globex")],
        until=BASE + timedelta(minutes=30),
    )

    assert sorted(alert.entity_id for alert in alerts) == ["acme", "globex"]


# --- events that cannot be placed ---------------------------------------------


def test_an_event_missing_the_partition_field_is_skipped():
    engine = CompiledEngine(partitioned())

    alerts = engine.replay([ev(0, 50.0)])

    assert alerts == []
    assert engine._entities == {}


def test_a_partitionable_event_still_matches_alongside_one_that_is_not():
    engine = CompiledEngine(partitioned())

    alerts = engine.replay([ev(0, 50.0), ev(1, 50.0, customer_id="acme")])

    assert [alert.entity_id for alert in alerts] == ["acme"]


# --- validation ---------------------------------------------------------------


def test_partition_by_requires_a_wildcard_entity_filter():
    text = PARTITIONED_RULE.format(keys="  - customer_id", emit="").replace(
        'entity_id: "*"', 'entity_id: "device-1"'
    )

    with pytest.raises(ValueError, match="every source must use"):
        compile_rule(load_rule_yaml(text))


# --- interaction with the rest of the engine ----------------------------------


def test_partition_scheme_is_part_of_the_state_fingerprint():
    one = partitioned(keys=("customer_id",))[0]
    two = partitioned(keys=("customer_id", "region"))[0]

    assert one.state_fingerprint() != two.state_fingerprint()


def test_changing_the_partition_scheme_is_refused_on_restore():
    engine = CompiledEngine(partitioned(keys=("customer_id",)))
    engine.replay([ev(0, 50.0, customer_id="acme")])

    with pytest.raises(ValueError, match="has changed shape"):
        CompiledEngine.restore(engine.snapshot(), partitioned(keys=("customer_id", "region")))


def test_partitioned_state_survives_a_snapshot_round_trip():
    rules = partitioned(emit="emit:\n  cooldown: 1h")
    engine = CompiledEngine(rules)
    engine.replay([ev(0, 50.0, customer_id="acme")])

    resumed = CompiledEngine.restore(
        EngineSnapshot.from_json(engine.snapshot().to_json()),
        partitioned(emit="emit:\n  cooldown: 1h"),
    )
    suppressed = resumed.replay([ev(5, 50.0, customer_id="acme")])
    other = resumed.replay([ev(6, 50.0, customer_id="globex")])

    assert suppressed == []
    assert [alert.entity_id for alert in other] == ["globex"]


def test_explain_reports_the_partition_as_the_identity():
    engine = CompiledEngine(partitioned())

    explanation = engine.explain(ev(0, 50.0, customer_id="acme")).by_rule("reading_spike")

    assert explanation.entity_id == "acme"
    assert explanation.would_emit


def test_explain_skips_a_rule_that_cannot_place_the_event():
    engine = CompiledEngine(partitioned())

    result = engine.explain(ev(0, 50.0))

    assert result.rules == []


def test_simulation_counts_only_placeable_events_as_evaluations():
    engine = CompiledEngine(partitioned())
    events = [ev(0, 50.0, customer_id="acme"), ev(1, 50.0)]

    stats = engine.simulate(events).by_rule("reading_spike")

    assert stats.evaluations == 1
    assert stats.entities == ["acme"]
