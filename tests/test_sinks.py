from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from rule_engine.declarative import load_rule_yaml
from rule_engine.runtime import DeclarativeEngine
from rule_engine.sinks import (
    DeadLetterRecord,
    DeliveryRequest,
    DeliveryResult,
    FileSink,
    FileDeadLetterStore,
    FileObjectStorageTransport,
    InMemoryDeadLetterStore,
    InMemoryQueueTransport,
    ObjectStorageSink,
    QueueSink,
    RetryPolicy,
    SinkRegistry,
    StdoutSink,
    WebhookSink,
)
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


def test_retry_policy_defaults_to_single_attempt():
    policy = RetryPolicy.from_config({})
    assert policy.max_attempts == 1
    assert policy.base_delay_s == 0.0


def test_retry_policy_parses_backoff_config():
    policy = RetryPolicy.from_config(
        {
            "retry": {
                "max_attempts": 4,
                "base_delay_s": 0.5,
                "multiplier": 3,
                "max_delay_s": 2,
            }
        }
    )

    assert policy.max_attempts == 4
    assert policy.backoff_delay(2) == 0.5
    assert policy.backoff_delay(3) == 1.5
    assert policy.backoff_delay(4) == 2.0


def test_sink_registry_retries_retryable_failures():
    class FlakySink:
        sink_type = "flaky"

        def __init__(self):
            self.calls = 0

        def deliver(self, request):
            self.calls += 1
            if self.calls < 3:
                return DeliveryResult(
                    sink_type="flaky",
                    status="failed",
                    detail="temporary",
                    retryable=True,
                )
            return DeliveryResult(
                sink_type="flaky",
                status="delivered",
                detail="ok",
            )

    sink = FlakySink()
    registry = SinkRegistry(adapters=[sink])
    result = registry.deliver(
        DeliveryRequest(
            sink_type="flaky",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="hello",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={"type": "flaky", "retry": {"max_attempts": 3}},
        )
    )

    assert result.status == "delivered"
    assert result.metadata["attempt"] == 3
    assert result.metadata["backoff_schedule_s"] == [0.0, 0.0]
    assert sink.calls == 3


def test_sink_registry_records_backoff_schedule():
    class FlakySink:
        sink_type = "flaky"

        def __init__(self):
            self.calls = 0

        def deliver(self, request):
            self.calls += 1
            if self.calls < 3:
                return DeliveryResult(
                    sink_type="flaky",
                    status="failed",
                    detail="temporary",
                    retryable=True,
                )
            return DeliveryResult(
                sink_type="flaky",
                status="delivered",
                detail="ok",
            )

    sink = FlakySink()
    registry = SinkRegistry(adapters=[sink])
    result = registry.deliver(
        DeliveryRequest(
            sink_type="flaky",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="hello",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={
                "type": "flaky",
                "retry": {
                    "max_attempts": 3,
                    "base_delay_s": 0.25,
                    "multiplier": 2.0,
                    "max_delay_s": 1.0,
                },
            },
        )
    )

    assert result.status == "delivered"
    assert result.metadata["backoff_schedule_s"] == [0.25, 0.5]


def test_sink_registry_can_sleep_between_retries():
    class FlakySink:
        sink_type = "flaky"

        def __init__(self):
            self.calls = 0

        def deliver(self, request):
            self.calls += 1
            if self.calls == 1:
                return DeliveryResult(
                    sink_type="flaky",
                    status="failed",
                    detail="temporary",
                    retryable=True,
                )
            return DeliveryResult(
                sink_type="flaky",
                status="delivered",
                detail="ok",
            )

    sink = FlakySink()
    registry = SinkRegistry(adapters=[sink])
    with patch("rule_engine.sinks.time.sleep") as sleep_mock:
        result = registry.deliver(
            DeliveryRequest(
                sink_type="flaky",
                rule_id="rule-1",
                entity_id="entity-1",
                severity="warning",
                message="hello",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                payload={},
                config={
                    "type": "flaky",
                    "retry": {
                        "max_attempts": 2,
                        "base_delay_s": 0.25,
                        "sleep": True,
                    },
                },
            )
        )

    assert result.status == "delivered"
    sleep_mock.assert_called_once_with(0.25)


def test_sink_registry_records_dead_letter_after_final_failure():
    class FailingSink:
        sink_type = "failing"

        def __init__(self):
            self.calls = 0

        def deliver(self, request):
            self.calls += 1
            return DeliveryResult(
                sink_type="failing",
                status="failed",
                detail="still failing",
                retryable=True,
            )

    sink = FailingSink()
    dead_letters = InMemoryDeadLetterStore()
    registry = SinkRegistry(adapters=[sink], dead_letter_store=dead_letters)
    result = registry.deliver(
        DeliveryRequest(
            sink_type="failing",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="hello",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={"x": 1},
            config={"type": "failing", "retry": {"max_attempts": 2}},
        )
    )

    assert result.status == "failed"
    assert result.metadata["attempt"] == 2
    assert sink.calls == 2
    assert len(dead_letters.records) == 1
    assert dead_letters.records[0].rule_id == "rule-1"


def test_sink_registry_reports_delivery_metrics_summary():
    class FlakySink:
        sink_type = "flaky"

        def __init__(self):
            self.calls = 0

        def deliver(self, request):
            self.calls += 1
            if self.calls == 1:
                return DeliveryResult(
                    sink_type="flaky",
                    status="failed",
                    detail="temporary",
                    retryable=True,
                )
            return DeliveryResult(
                sink_type="flaky",
                status="delivered",
                detail="ok",
            )

    class FailingSink:
        sink_type = "failing"

        def deliver(self, request):
            return DeliveryResult(
                sink_type="failing",
                status="failed",
                detail="still failing",
                retryable=False,
            )

    registry = SinkRegistry(adapters=[FlakySink(), FailingSink()])
    registry.deliver(
        DeliveryRequest(
            sink_type="flaky",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="hello",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={"type": "flaky", "retry": {"max_attempts": 2, "base_delay_s": 0.25}},
        )
    )
    registry.deliver(
        DeliveryRequest(
            sink_type="failing",
            rule_id="rule-2",
            entity_id="entity-2",
            severity="critical",
            message="bad",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={"type": "failing"},
        )
    )
    registry.deliver(
        DeliveryRequest(
            sink_type="missing",
            rule_id="rule-3",
            entity_id="entity-3",
            severity="info",
            message="missing",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={"type": "missing"},
        )
    )

    snapshot = registry.metrics()

    assert snapshot.overall.total_requests == 3
    assert snapshot.overall.total_attempts == 3
    assert snapshot.overall.delivered == 1
    assert snapshot.overall.failed == 3
    assert snapshot.overall.retryable_failures == 1
    assert snapshot.overall.retries_attempted == 1
    assert snapshot.overall.dead_letters == 2
    assert snapshot.overall.unsupported == 1

    assert snapshot.by_sink["flaky"].total_requests == 1
    assert snapshot.by_sink["flaky"].total_attempts == 2
    assert snapshot.by_sink["flaky"].delivered == 1
    assert snapshot.by_sink["flaky"].failed == 1
    assert snapshot.by_sink["flaky"].retries_attempted == 1

    assert snapshot.by_sink["failing"].dead_letters == 1
    assert snapshot.by_sink["missing"].unsupported == 1


def test_sink_registry_can_reset_delivery_metrics():
    registry = SinkRegistry()
    registry.deliver(
        DeliveryRequest(
            sink_type="missing",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="info",
            message="hello",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={"type": "missing"},
        )
    )

    registry.reset_metrics()
    snapshot = registry.metrics()

    assert snapshot.overall.total_requests == 0
    assert snapshot.overall.dead_letters == 0
    assert snapshot.by_sink == {}


def test_file_dead_letter_store_writes_record(tmp_path: Path):
    target = tmp_path / "dead_letters.ndjson"
    store = FileDeadLetterStore(target)
    store.record(
        DeadLetterRecord(
            sink_type="queue",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="dead",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={"x": 1},
            config={"type": "queue"},
            result=DeliveryResult(
                sink_type="queue",
                status="failed",
                detail="bad",
                retryable=False,
            ),
        )
    )

    content = target.read_text(encoding="utf-8")
    assert '"rule_id": "rule-1"' in content
    assert '"status": "failed"' in content


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


def test_webhook_sink_delivers_successfully():
    sink = WebhookSink()

    class FakeResponse:
        status = 202

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def getcode(self):
            return self.status

    with patch("rule_engine.sinks.request.urlopen", return_value=FakeResponse()):
        result = sink.deliver(
            DeliveryRequest(
                sink_type="webhook",
                rule_id="rule-1",
                entity_id="entity-1",
                severity="warning",
                message="sent",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                payload={},
                config={"type": "webhook", "url": "https://example.test/hook"},
            )
        )

    assert result.status == "delivered"
    assert result.metadata["status_code"] == 202


def test_webhook_sink_marks_4xx_as_terminal_failure():
    sink = WebhookSink()
    http_error = HTTPError(
        url="https://example.test/hook",
        code=400,
        msg="bad request",
        hdrs=None,
        fp=None,
    )

    with patch("rule_engine.sinks.request.urlopen", side_effect=http_error):
        result = sink.deliver(
            DeliveryRequest(
                sink_type="webhook",
                rule_id="rule-1",
                entity_id="entity-1",
                severity="warning",
                message="sent",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                payload={},
                config={"type": "webhook", "url": "https://example.test/hook"},
            )
        )

    assert result.status == "failed"
    assert result.retryable is False
    assert result.metadata["status_code"] == 400


def test_webhook_sink_marks_transport_errors_as_retryable():
    sink = WebhookSink()

    with patch(
        "rule_engine.sinks.request.urlopen",
        side_effect=URLError("connection refused"),
    ):
        result = sink.deliver(
            DeliveryRequest(
                sink_type="webhook",
                rule_id="rule-1",
                entity_id="entity-1",
                severity="warning",
                message="sent",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                payload={},
                config={"type": "webhook", "url": "https://example.test/hook"},
            )
        )

    assert result.status == "failed"
    assert result.retryable is True


def test_queue_sink_delivers_successfully():
    transport = InMemoryQueueTransport()
    sink = QueueSink(transport=transport)
    result = sink.deliver(
        DeliveryRequest(
            sink_type="queue",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="queued",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={"x": 1},
            config={"type": "queue", "queue": "alerts"},
        )
    )

    assert result.status == "delivered"
    assert result.metadata["queue"] == "alerts"
    assert len(transport.messages) == 1


def test_queue_sink_requires_queue_name():
    sink = QueueSink()
    result = sink.deliver(
        DeliveryRequest(
            sink_type="queue",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="queued",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={"type": "queue"},
        )
    )

    assert result.status == "failed"
    assert result.retryable is False


def test_queue_sink_marks_timeout_as_retryable():
    class TimeoutTransport:
        def send(self, queue, payload, config):
            raise TimeoutError("slow queue")

    sink = QueueSink(transport=TimeoutTransport())
    result = sink.deliver(
        DeliveryRequest(
            sink_type="queue",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="queued",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={"type": "queue", "queue": "alerts"},
        )
    )

    assert result.status == "failed"
    assert result.retryable is True


def test_engine_records_delivery_results_for_queue_sink():
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
      - type: queue
        queue: alerts
"""
    engine = DeclarativeEngine([load_rule_yaml(yaml_text)])
    transport = InMemoryQueueTransport()
    engine.sink_registry.register(QueueSink(transport=transport))

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
    assert len(transport.messages) == 1


def test_object_storage_sink_writes_object(tmp_path: Path):
    transport = FileObjectStorageTransport(root=tmp_path)
    sink = ObjectStorageSink(transport=transport)
    result = sink.deliver(
        DeliveryRequest(
            sink_type="object_storage",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="stored",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={"x": 1},
            config={"type": "object_storage", "bucket": "archive", "prefix": "alerts"},
        )
    )

    assert result.status == "delivered"
    stored_path = Path(result.metadata["path"])
    assert stored_path.exists()
    assert '"entity_id": "entity-1"' in stored_path.read_text(encoding="utf-8")


def test_object_storage_sink_requires_bucket():
    sink = ObjectStorageSink()
    result = sink.deliver(
        DeliveryRequest(
            sink_type="object_storage",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="stored",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={"type": "object_storage"},
        )
    )

    assert result.status == "failed"
    assert result.retryable is False


def test_object_storage_sink_marks_timeout_as_retryable():
    class TimeoutTransport:
        def put_object(self, bucket, key, body, config):
            raise TimeoutError("slow object store")

    sink = ObjectStorageSink(transport=TimeoutTransport())
    result = sink.deliver(
        DeliveryRequest(
            sink_type="object_storage",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="stored",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={"type": "object_storage", "bucket": "archive"},
        )
    )

    assert result.status == "failed"
    assert result.retryable is True


def test_engine_records_delivery_results_for_object_storage_sink(tmp_path: Path):
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
      - type: object_storage
        bucket: archive
        prefix: alerts
"""
    engine = DeclarativeEngine([load_rule_yaml(yaml_text)])
    transport = FileObjectStorageTransport(root=tmp_path)
    engine.sink_registry.register(ObjectStorageSink(transport=transport))

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
    assert len(transport.objects) == 1
