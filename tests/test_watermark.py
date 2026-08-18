from datetime import datetime, timedelta

import pytest

from rule_engine.compiler import compile_rule
from rule_engine.declarative import load_rule_yaml
from rule_engine.runtime import CompiledEngine, EngineConfig
from rule_engine.types import SensorEvent

RULE_YAML = """
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
    message: "High reading for {{entity_id}}"
    sinks: []
"""

BASE = datetime(2024, 1, 1, 12, 0, 0)


def build_engine(**config_kwargs) -> CompiledEngine:
    rule = compile_rule(load_rule_yaml(RULE_YAML))
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


def test_process_event_rejects_out_of_order_event():
    engine = build_engine()
    engine.process_event(event_at(60))

    with pytest.raises(ValueError, match="watermark"):
        engine.process_event(event_at(30))


def test_advance_to_rejects_backward_target():
    engine = build_engine()
    engine.process_event(event_at(60))

    with pytest.raises(ValueError, match="watermark"):
        engine.advance_to(BASE + timedelta(seconds=30))


def test_watermark_is_unchanged_after_a_rejected_event():
    engine = build_engine()
    engine.process_event(event_at(60))
    before = engine.watermark

    with pytest.raises(ValueError):
        engine.process_event(event_at(30))

    assert engine.watermark == before


def test_event_at_the_current_watermark_is_accepted():
    engine = build_engine()
    engine.process_event(event_at(60))

    engine.process_event(event_at(60, value=99.0))

    assert engine.watermark == event_at(60).timestamp


def test_events_before_the_initial_watermark_are_rejected():
    engine = build_engine(initial_watermark=BASE + timedelta(seconds=120))

    with pytest.raises(ValueError, match="watermark"):
        engine.process_event(event_at(60))


def test_replay_sorts_events_so_shuffled_input_is_accepted():
    engine = build_engine()
    shuffled = [event_at(90), event_at(30), event_at(60)]

    engine.replay(shuffled)

    assert engine.watermark == event_at(90).timestamp


def test_replay_rejects_a_batch_that_predates_the_watermark():
    engine = build_engine()
    engine.replay([event_at(60), event_at(90)])

    with pytest.raises(ValueError, match="watermark"):
        engine.replay([event_at(30)])
