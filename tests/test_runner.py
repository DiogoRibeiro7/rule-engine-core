import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rule_engine.declarative import load_rule_yaml
from rule_engine.runner import (
    emit_replay_report_json,
    RuntimeRule,
    generate_json_schema,
    generate_rule_json_schema,
    load_declarative_rules,
    replay_events,
    replay_events_with_report,
)
from rule_engine.compiler import compile_rule, load_and_compile_rule_files
from rule_engine.runtime import CompiledEngine, DeclarativeEngine
from rule_engine.types import SensorEvent


def _ts(year: int, month: int, day: int, hour: int, minute: int) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=UTC).timestamp() * 1000)


def test_loads_runtime_metadata_for_sample_rule():
    rule_path = (
        Path(__file__).resolve().parents[1] / "sample_rules" / "source_gap.yaml"
    )
    rules = load_declarative_rules([rule_path])

    assert len(rules) == 1
    runtime_rule = rules[0]
    assert isinstance(runtime_rule, RuntimeRule)
    assert runtime_rule.rule_id == "source_gap_48h"
    assert runtime_rule.trigger_type == "absence"
    assert runtime_rule.entity_id_filter == "*"
    assert runtime_rule.sensor_types == ["source_alpha"]


def test_load_and_compile_rule_files_returns_compiled_rules():
    rule_path = (
        Path(__file__).resolve().parents[1] / "sample_rules" / "source_gap.yaml"
    )

    compiled_rules = load_and_compile_rule_files([rule_path])

    assert len(compiled_rules) == 1
    assert compiled_rules[0].rule_id == "source_gap_48h"
    assert compiled_rules[0].trigger_type == "absence"


def test_generate_json_schema_has_runtime_rule_structure():
    schema = generate_json_schema()

    assert schema["type"] == "array"
    assert schema["items"]["type"] == "object"
    assert "rule_id" in schema["items"]["properties"]
    assert "trigger_type" in schema["items"]["properties"]


def test_generate_rule_json_schema_has_declarative_rule_structure():
    schema = generate_rule_json_schema()

    assert schema["type"] == "object"
    assert "rule_id" in schema["properties"]
    assert "actions" in schema["properties"]


def test_replay_absence_fixture_emits_no_alert_without_timeout_boundary():
    base_path = Path(__file__).resolve().parents[1]
    rule_path = base_path / "sample_rules" / "source_gap.yaml"
    event_path = base_path / "sample_data" / "source_gap_events.ndjson"

    alerts = replay_events([rule_path], event_path)
    assert alerts == []


def test_replay_composite_fixture_emits_alert_when_advanced_past_timeout():
    base_path = Path(__file__).resolve().parents[1]
    rule_path = base_path / "sample_rules" / "dual_source_gap.yaml"
    event_path = base_path / "sample_data" / "dual_source_gap_events.ndjson"
    until = datetime(2023, 11, 15, 12, 26, 40, tzinfo=UTC)

    alerts = replay_events([rule_path], event_path, until=until)

    assert len(alerts) == 1
    assert alerts[0].rule_id == "dual_source_gap"
    assert alerts[0].alert.severity == "warning"


def test_replay_events_with_report_returns_delivery_summary():
    base_path = Path(__file__).resolve().parents[1]
    rule_path = base_path / "sample_rules" / "dual_source_gap.yaml"
    event_path = base_path / "sample_data" / "dual_source_gap_events.ndjson"
    until = datetime(2023, 11, 15, 12, 26, 40, tzinfo=UTC)

    alerts, report = replay_events_with_report([rule_path], event_path, until=until)

    assert len(alerts) == 1
    assert report.alert_count == 1
    assert report.delivery_metrics.overall.total_requests == 1
    assert report.delivery_metrics.overall.unsupported == 1
    assert len(report.delivery_log) == 1
    assert report.delivery_log[0].sink_type == "queue"
    assert report.delivery_log[0].status == "unsupported"


def test_emit_replay_report_json_includes_alerts_and_delivery_report():
    base_path = Path(__file__).resolve().parents[1]
    rule_path = base_path / "sample_rules" / "dual_source_gap.yaml"
    event_path = base_path / "sample_data" / "dual_source_gap_events.ndjson"
    until = datetime(2023, 11, 15, 12, 26, 40, tzinfo=UTC)

    alerts, report = replay_events_with_report([rule_path], event_path, until=until)
    payload = json.loads(emit_replay_report_json(alerts, report))

    assert len(payload["alerts"]) == 1
    assert payload["delivery_report"]["alert_count"] == 1
    assert payload["delivery_report"]["delivery_metrics"]["overall"]["unsupported"] == 1
    assert payload["delivery_report"]["delivery_log"][0]["sink_type"] == "queue"


def test_event_rule_emits_alert():
    yaml_text = """
rule_id: source_primary_spike
description: Event spike
trigger:
  type: event
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
  operands:
    - metric: value
      operator: gt
      value: 180
actions:
  - severity: critical
    message: "Primary source spike for {{entity_id}}: {{value}}"
    sinks: []
"""
    runtime = DeclarativeEngine([load_rule_yaml(yaml_text)])

    alerts = runtime.replay(
        [
            SensorEvent(
                entity_id="entity-1",
                sensor_type="source_primary",
                value=185.0,
                timestamp_ms=_ts(2024, 1, 1, 0, 0),
            )
        ]
    )

    assert len(alerts) == 1
    assert alerts[0].alert.message == "Primary source spike for entity-1: 185.0"


def test_compiled_engine_replays_compiled_rules():
    yaml_text = """
rule_id: source_primary_spike
description: Event spike
trigger:
  type: event
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
  operands:
    - metric: value
      operator: gt
      value: 180
actions:
  - severity: critical
    message: "Primary source spike for {{entity_id}}: {{value}}"
    sinks: []
"""
    compiled_rule = compile_rule(load_rule_yaml(yaml_text))
    engine = CompiledEngine([compiled_rule])

    alerts = engine.replay(
        [
            SensorEvent(
                entity_id="entity-1",
                sensor_type="source_primary",
                value=185.0,
                timestamp_ms=_ts(2024, 1, 1, 0, 0),
            )
        ]
    )

    assert len(alerts) == 1
    assert alerts[0].rule_id == "source_primary_spike"
    assert alerts[0].alert.message == "Primary source spike for entity-1: 185.0"


def test_window_rule_emits_after_window_close():
    yaml_text = """
rule_id: source_primary_sustained_high
description: High mean source_primary over 10 minutes
trigger:
  type: window
  duration: 10m
  slide: 10m
sources:
  - sensor_type: source_primary
    entity_id: "*"
aggregations:
  - id: mean_hr
    function: mean
    field: value
condition:
  operator: AND
  operands:
    - metric: mean_hr
      operator: gte
      value: 100
actions:
  - severity: warning
    message: "Window mean source_primary {{mean_hr}}"
    sinks: []
"""
    engine = DeclarativeEngine([load_rule_yaml(yaml_text)])
    alerts = engine.replay(
        [
            SensorEvent("entity-1", "source_primary", 120.0, _ts(2024, 1, 1, 0, 1)),
            SensorEvent("entity-1", "source_primary", 110.0, _ts(2024, 1, 1, 0, 5)),
        ],
        until=datetime(2024, 1, 1, 0, 10, tzinfo=UTC),
    )

    assert len(alerts) == 1
    assert alerts[0].timestamp == datetime(2024, 1, 1, 0, 10, tzinfo=UTC)
    assert alerts[0].alert.severity == "warning"


def test_scheduled_rule_uses_lookback_window():
    yaml_text = """
rule_id: scheduled_source_review
description: Scheduled source review
trigger:
  type: scheduled
  cron: 0 8 * * *
  lookback: 2h
sources:
  - sensor_type: source_primary
    entity_id: "*"
aggregations:
  - id: sample_count
    function: count
    field: value
condition:
  operator: AND
  operands:
    - metric: sample_count
      operator: gte
      value: 2
actions:
  - severity: info
    message: "Scheduled review with {{sample_count}} samples"
    sinks: []
"""
    engine = DeclarativeEngine([load_rule_yaml(yaml_text)])
    alerts = engine.replay(
        [
            SensorEvent("entity-1", "source_primary", 95.0, _ts(2024, 1, 1, 6, 30)),
            SensorEvent("entity-1", "source_primary", 97.0, _ts(2024, 1, 1, 7, 30)),
        ],
        until=datetime(2024, 1, 1, 8, 0, tzinfo=UTC),
    )

    assert len(alerts) == 1
    assert alerts[0].timestamp == datetime(2024, 1, 1, 8, 0, tzinfo=UTC)
    assert alerts[0].alert.message == "Scheduled review with 2 samples"


def test_absence_rule_emits_after_timeout():
    yaml_text = """
rule_id: source_alpha_gap
description: Missing source_alpha
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
    message: "No source_alpha for {{entity_id}} in {{duration}}"
    sinks: []
"""
    engine = DeclarativeEngine([load_rule_yaml(yaml_text)])
    alerts = engine.replay(
        [
            SensorEvent("entity-1", "source_alpha", 96.0, _ts(2024, 1, 1, 0, 0)),
        ],
        until=datetime(2024, 1, 1, 0, 10, tzinfo=UTC),
    )

    assert len(alerts) == 1
    assert alerts[0].timestamp == datetime(2024, 1, 1, 0, 10, tzinfo=UTC)
    assert alerts[0].alert.severity == "warning"


def test_window_rule_rejects_zero_duration():
    yaml_text = """
rule_id: bad_window
trigger:
  type: window
  duration: 0m
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
actions:
  - severity: warning
    message: "bad"
    sinks: []
"""
    with pytest.raises(ValueError, match="Duration must be greater than zero: 0m"):
        DeclarativeEngine([load_rule_yaml(yaml_text)])


def test_window_rule_rejects_slide_longer_than_duration():
    yaml_text = """
rule_id: bad_window
trigger:
  type: window
  duration: 5m
  slide: 10m
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
actions:
  - severity: warning
    message: "bad"
    sinks: []
"""
    with pytest.raises(ValueError, match="trigger.slide to be less than or equal to trigger.duration"):
        DeclarativeEngine([load_rule_yaml(yaml_text)])


def test_event_rule_rejects_window_only_trigger_fields():
    yaml_text = """
rule_id: bad_event
trigger:
  type: event
  duration: 5m
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
actions:
  - severity: warning
    message: "bad"
    sinks: []
"""
    with pytest.raises(ValueError, match="trigger type 'event' does not support fields: duration"):
        DeclarativeEngine([load_rule_yaml(yaml_text)])


def test_scheduled_rule_rejects_out_of_range_cron():
    yaml_text = """
rule_id: bad_schedule
trigger:
  type: scheduled
  cron: 61 24 * * *
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
actions:
  - severity: warning
    message: "bad"
    sinks: []
"""
    with pytest.raises(ValueError, match="Unsupported cron expression: 61 24 \\* \\* \\*"):
        DeclarativeEngine([load_rule_yaml(yaml_text)])


def test_rule_rejects_unsupported_condition_operator():
    yaml_text = """
rule_id: bad_condition
trigger:
  type: event
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: XOR
  operands:
    - metric: value
      operator: gt
      value: 180
actions:
  - severity: warning
    message: "bad"
    sinks: []
"""
    with pytest.raises(ValueError, match="unsupported condition operator 'XOR'"):
        DeclarativeEngine([load_rule_yaml(yaml_text)])


def test_rule_rejects_unsupported_operand_operator():
    yaml_text = """
rule_id: bad_operand
trigger:
  type: event
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
  operands:
    - metric: value
      operator: between
      value: 180
actions:
  - severity: warning
    message: "bad"
    sinks: []
"""
    with pytest.raises(ValueError, match="operand 1 has unsupported operator 'between'"):
        DeclarativeEngine([load_rule_yaml(yaml_text)])
