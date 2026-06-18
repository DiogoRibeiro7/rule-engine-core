import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import pytest

from rule_engine.declarative import load_rule_yaml
from rule_engine.runtime import DeclarativeEngine
from rule_engine.sinks import (
    DeadLetterRecord,
    DeliveryRequest,
    DeliveryResult,
    FileDeadLetterStore,
    FileObjectStorageTransport,
    FileSink,
    InMemoryDeadLetterStore,
    InMemoryQueueTransport,
    ObjectStorageSink,
    QueueSink,
    QueueSinkConfig,
    RetryPolicy,
    SinkRegistry,
    StdoutSink,
    StdoutSinkConfig,
    WebhookSink,
    WebhookSinkConfig,
    build_delivery_payload,
    create_sink_registry,
    default_sink_adapters,
    parse_sink_config,
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


def test_default_sink_adapters_builds_expected_adapter_set():
    adapters = default_sink_adapters(include_stdout=False, include_webhook=False)

    assert [adapter.sink_type for adapter in adapters] == [
        "file",
        "queue",
        "object_storage",
    ]


def test_create_sink_registry_can_configure_dead_letter_path(tmp_path: Path):
    registry = create_sink_registry(
        include_stdout=False,
        include_file=False,
        include_webhook=False,
        include_queue=False,
        include_object_storage=False,
        dead_letter_path=tmp_path / "dead_letters.ndjson",
    )

    assert isinstance(registry.dead_letter_store, FileDeadLetterStore)


def test_create_sink_registry_can_use_custom_transports(tmp_path: Path):
    queue_transport = InMemoryQueueTransport()
    object_transport = FileObjectStorageTransport(root=tmp_path)
    registry = create_sink_registry(
        include_stdout=False,
        include_file=False,
        include_webhook=False,
        queue_transport=queue_transport,
        object_storage_transport=object_transport,
    )

    assert registry.get("queue") is not None
    assert registry.get("object_storage") is not None


def test_retry_policy_defaults_to_single_attempt():
    policy = RetryPolicy.from_config(StdoutSinkConfig())
    assert policy.max_attempts == 1
    assert policy.base_delay_s == 0.0


def test_retry_policy_parses_backoff_config():
    policy = RetryPolicy.from_config(
        StdoutSinkConfig(
            retry=parse_sink_config(
                {
                    "type": "stdout",
                    "retry": {
                        "max_attempts": 4,
                        "base_delay_s": 0.5,
                        "multiplier": 3,
                        "max_delay_s": 2,
                    },
                }
            ).retry
        )
    )

    assert policy.max_attempts == 4
    assert policy.backoff_delay(2) == 0.5
    assert policy.backoff_delay(3) == 1.5
    assert policy.backoff_delay(4) == 2.0


def test_parse_sink_config_returns_typed_config():
    config = parse_sink_config(
        {
            "type": "webhook",
            "url": "https://example.test/hook",
            "timeout_s": 2.5,
            "headers": {"X-Test": "1"},
            "method": "put",
            "auth_token": "secret-token",
            "auth_scheme": "Token",
            "signature_secret": "signing-secret",
            "signature_header": "X-Test-Signature",
            "retry": {"max_attempts": 3},
        }
    )

    assert isinstance(config, WebhookSinkConfig)
    assert config.url == "https://example.test/hook"
    assert config.timeout_s == 2.5
    assert config.headers == {"X-Test": "1"}
    assert config.method == "PUT"
    assert config.auth_token == "secret-token"
    assert config.auth_scheme == "Token"
    assert config.signature_secret == "signing-secret"
    assert config.signature_header == "X-Test-Signature"
    assert config.retry.max_attempts == 3


def test_build_delivery_payload_defines_stable_contract():
    payload = build_delivery_payload(
        DeliveryRequest(
            sink_type="queue",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="hello",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={"variables": {"x": 1}},
            config={"type": "queue", "queue": "alerts"},
        )
    )

    assert payload.contract_version == "rule-engine-core.v1"
    assert payload.sink_type == "queue"
    assert payload.entity_id == "entity-1"
    assert payload.rule_id == "rule-1"
    assert payload.timestamp == "2024-01-01T00:00:00+00:00"
    assert payload.payload == {"variables": {"x": 1}}
    assert len(payload.idempotency_key) == 64


def test_sink_registry_coerces_raw_dict_config_before_adapter_dispatch():
    class CapturingSink:
        sink_type = "queue"

        def __init__(self) -> None:
            self.config = None

        def deliver(self, request):
            self.config = request.config
            return DeliveryResult(
                sink_type="queue",
                status="delivered",
                detail="ok",
            )

    sink = CapturingSink()
    registry = SinkRegistry(adapters=[sink])
    registry.deliver(
        DeliveryRequest(
            sink_type="queue",
            rule_id="rule-1",
            entity_id="entity-1",
            severity="warning",
            message="hello",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload={},
            config={"type": "queue", "queue": "alerts", "retryable": True},
        )
    )

    assert isinstance(sink.config, QueueSinkConfig)
    assert sink.config.queue == "alerts"
    assert sink.config.retryable is True


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


def test_sink_registry_records_latency_and_structured_delivery_log():
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

    registry = SinkRegistry(adapters=[FlakySink()])
    with patch(
        "rule_engine.sinks.time.perf_counter",
        side_effect=[1.0, 1.01, 2.0, 2.025],
    ):
        result = registry.deliver(
            DeliveryRequest(
                sink_type="flaky",
                rule_id="rule-1",
                entity_id="entity-1",
                severity="warning",
                message="hello",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                payload={},
                config={"type": "flaky", "retry": {"max_attempts": 2}},
            )
        )

    snapshot = registry.metrics()
    log = registry.delivery_log()

    assert result.status == "delivered"
    assert result.metadata["attempt_latency_ms"] == 25.0
    assert result.metadata["total_latency_ms"] == 35.0
    assert snapshot.overall.total_attempts == 2
    assert snapshot.overall.total_latency_ms == pytest.approx(35.0)
    assert snapshot.overall.max_latency_ms == pytest.approx(25.0)
    assert snapshot.overall.average_latency_ms == pytest.approx(17.5)
    assert len(log) == 1
    assert log[0].sink_type == "flaky"
    assert log[0].status == "delivered"
    assert log[0].attempt_count == 2
    assert log[0].retry_count == 1
    assert log[0].latency_ms == 35.0
    assert log[0].dead_lettered is False


def test_sink_registry_can_clear_delivery_log():
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

    assert len(registry.delivery_log()) == 1

    registry.clear_delivery_log()

    assert registry.delivery_log() == []


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
    payload = [
        line for line in target.read_text(encoding="utf-8").splitlines() if line.strip()
    ][0]
    record = json.loads(payload)
    assert record["contract_version"] == "rule-engine-core.v1"
    assert record["entity_id"] == "entity-1"
    assert record["message"] == "persisted"
    assert record["payload"] == {"k": "v"}
    assert result.metadata["contract_version"] == "rule-engine-core.v1"
    assert "idempotency_key" in result.metadata


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
    captured_request = {}

    class FakeResponse:
        status = 202

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def getcode(self):
            return self.status

    def fake_urlopen(http_request, timeout):
        captured_request["body"] = http_request.data.decode("utf-8")
        captured_request["timeout"] = timeout
        captured_request["headers"] = dict(http_request.header_items())
        return FakeResponse()

    with patch("rule_engine.sinks.request.urlopen", side_effect=fake_urlopen):
        result = sink.deliver(
            DeliveryRequest(
                sink_type="webhook",
                rule_id="rule-1",
                entity_id="entity-1",
                severity="warning",
                message="sent",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                payload={},
                config={
                    "type": "webhook",
                    "url": "https://example.test/hook",
                    "auth_token": "secret-token",
                    "auth_scheme": "Token",
                    "signature_secret": "signing-secret",
                    "signature_header": "X-Test-Signature",
                },
            )
        )

    assert result.status == "delivered"
    assert result.metadata["status_code"] == 202
    payload = json.loads(captured_request["body"])
    assert payload["contract_version"] == "rule-engine-core.v1"
    assert payload["sink_type"] == "webhook"
    assert payload["entity_id"] == "entity-1"
    assert captured_request["timeout"] == 5.0
    assert captured_request["headers"]["Authorization"] == "Token secret-token"
    assert str(captured_request["headers"]["X-test-signature"]).startswith("sha256=")


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
    payload = transport.messages[0]["payload"]
    assert payload["contract_version"] == "rule-engine-core.v1"
    assert payload["sink_type"] == "queue"
    assert payload["entity_id"] == "entity-1"
    assert "idempotency_key" in payload


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
    payload = json.loads(stored_path.read_text(encoding="utf-8"))
    assert payload["contract_version"] == "rule-engine-core.v1"
    assert payload["entity_id"] == "entity-1"
    assert payload["sink_type"] == "object_storage"


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
