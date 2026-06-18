from datetime import UTC, datetime
from pathlib import Path

from rule_engine.declarative import load_rule_yaml
from rule_engine.runtime import DeclarativeEngine
from rule_engine.sinks import DeliveryRequest, FileSink, SinkRegistry, StdoutSink
from rule_engine.types import SensorEvent


def _ts(year: int, month: int, day: int, hour: int, minute: int) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=UTC).timestamp() * 1000)


def test_sink_registry_reports_unsupported_sink():
    registry = SinkRegistry()
    result = registry.deliver(
        DeliveryRequest(
            sink_type="webhook",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="hello",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={"type": "webhook"},
        )
    )

    assert result.status == "unsupported"
    assert result.retryable is False


def test_stdout_sink_delivers_message():
    sink = StdoutSink()
    result = sink.deliver(
        DeliveryRequest(
            sink_type="stdout",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="info",
            message="delivered",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={"type": "stdout"},
        )
    )

    assert result.status == "delivered"
    assert len(sink.messages) == 1
    assert "entity=entity-1" in sink.messages[0]


def test_file_sink_writes_ndjson_record(tmp_path: Path):
    target = tmp_path / "alerts.ndjson"
    sink = FileSink()
    result = sink.deliver(
        DeliveryRequest(
            sink_type="file",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="persisted",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={"k": "v"},
            config={"type": "file", "path": str(target)},
        )
    )

    assert result.status == "delivered"
    content = target.read_text(encoding="utf-8")
    assert '"entity_id": "entity-1"' in content
    assert '"message": "persisted"' in content


def test_engine_records_delivery_results_for_stdout_sink():
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
    sinks:
      - type: stdout
"""
    engine = DeclarativeEngine([load_rule_yaml(yaml_text)])
    stdout_sink = StdoutSink()
    engine.sink_registry.register(stdout_sink)

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
    assert len(alerts[0].delivery_results) == 1
    assert alerts[0].delivery_results[0].status == "delivered"
    assert len(stdout_sink.messages) == 1
