import json
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from rule_engine.declarative import load_rule_yaml
from rule_engine.runtime import DeclarativeEngine
from rule_engine.sinks import (
    FileObjectStorageTransport,
    FileSink,
    InMemoryDeadLetterStore,
    InMemoryQueueTransport,
    ObjectStorageSink,
    QueueSink,
    SinkRegistry,
    WebhookSink,
)
from rule_engine.types import SensorEvent


def _ts(year: int, month: int, day: int, hour: int, minute: int) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=UTC).timestamp() * 1000)


def _event_rule_with_sink(sink_yaml: str) -> str:
    return f"""
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
    message: "Primary source spike for {{{{entity_id}}}}: {{{{value}}}}"
    sinks:
{sink_yaml}
"""


def _replay_single_alert(engine: DeclarativeEngine) -> tuple[list, object]:
    alerts, report = engine.replay_with_report(
        [
            SensorEvent(
                entity_id="entity-1",
                sensor_type="source_primary",
                value=185.0,
                timestamp_ms=_ts(2024, 1, 1, 0, 0),
            )
        ]
    )
    return alerts, report


def test_file_sink_integration_emits_delivery_report(tmp_path: Path) -> None:
    target = tmp_path / "alerts.ndjson"
    yaml_text = _event_rule_with_sink(f"      - type: file\n        path: {target.as_posix()}\n")
    engine = DeclarativeEngine(
        [load_rule_yaml(yaml_text)],
        sink_registry=SinkRegistry(adapters=[FileSink()]),
    )

    alerts, report = _replay_single_alert(engine)

    assert len(alerts) == 1
    assert alerts[0].delivery_results[0].status == "delivered"
    lines = [line for line in target.read_text(encoding="utf-8").splitlines() if line.strip()]
    record = json.loads(lines[0])
    assert record["contract_version"] == "rule-engine-core.v1"
    assert report.delivery_metrics.overall.delivered == 1
    assert report.delivery_log[0].sink_type == "file"


def test_queue_sink_integration_emits_transport_message_and_metrics() -> None:
    transport = InMemoryQueueTransport()
    yaml_text = _event_rule_with_sink("      - type: queue\n        queue: alerts\n")
    engine = DeclarativeEngine(
        [load_rule_yaml(yaml_text)],
        sink_registry=SinkRegistry(adapters=[QueueSink(transport=transport)]),
    )

    alerts, report = _replay_single_alert(engine)

    assert len(alerts) == 1
    assert alerts[0].delivery_results[0].status == "delivered"
    assert len(transport.messages) == 1
    assert transport.messages[0]["payload"]["contract_version"] == "rule-engine-core.v1"
    assert report.delivery_metrics.by_sink["queue"].delivered == 1


def test_object_storage_sink_integration_writes_envelope_object(tmp_path: Path) -> None:
    transport = FileObjectStorageTransport(root=tmp_path)
    yaml_text = _event_rule_with_sink(
        "      - type: object_storage\n        bucket: archive\n        prefix: alerts\n"
    )
    engine = DeclarativeEngine(
        [load_rule_yaml(yaml_text)],
        sink_registry=SinkRegistry(adapters=[ObjectStorageSink(transport=transport)]),
    )

    alerts, report = _replay_single_alert(engine)

    assert len(alerts) == 1
    assert alerts[0].delivery_results[0].status == "delivered"
    assert len(transport.objects) == 1
    stored = transport.objects[0]
    body = json.loads(stored["body"])
    assert body["contract_version"] == "rule-engine-core.v1"
    assert stored["bucket"] == "archive"
    assert report.delivery_log[0].sink_type == "object_storage"


def test_webhook_sink_integration_success_uses_real_http_server() -> None:
    received: dict[str, object] = {}

    class SuccessHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers["Content-Length"])
            body = self.rfile.read(length).decode("utf-8")
            received["body"] = json.loads(body)
            received["path"] = self.path
            self.send_response(202)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), SuccessHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/hook"
        yaml_text = _event_rule_with_sink(f"      - type: webhook\n        url: {url}\n")
        engine = DeclarativeEngine(
            [load_rule_yaml(yaml_text)],
            sink_registry=SinkRegistry(adapters=[WebhookSink()]),
        )

        alerts, report = _replay_single_alert(engine)

        assert len(alerts) == 1
        assert alerts[0].delivery_results[0].status == "delivered"
        assert received["path"] == "/hook"
        body = received["body"]
        assert isinstance(body, dict)
        assert body["contract_version"] == "rule-engine-core.v1"
        assert report.delivery_metrics.by_sink["webhook"].delivered == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_webhook_sink_integration_failure_records_dead_letter_and_retries() -> None:
    attempts = {"count": 0}

    class FailureHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            attempts["count"] += 1
            self.send_response(503)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), FailureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        dead_letters = InMemoryDeadLetterStore()
        url = f"http://127.0.0.1:{server.server_port}/hook"
        yaml_text = _event_rule_with_sink(
            "      - type: webhook\n"
            f"        url: {url}\n"
            "        retry:\n"
            "          max_attempts: 2\n"
        )
        engine = DeclarativeEngine(
            [load_rule_yaml(yaml_text)],
            sink_registry=SinkRegistry(
                adapters=[WebhookSink()],
                dead_letter_store=dead_letters,
            ),
        )

        alerts, report = _replay_single_alert(engine)

        assert len(alerts) == 1
        assert alerts[0].delivery_results[0].status == "failed"
        assert attempts["count"] == 2
        assert len(dead_letters.records) == 1
        assert dead_letters.records[0].result.retryable is True
        assert report.delivery_metrics.by_sink["webhook"].retryable_failures == 2
        assert report.delivery_metrics.by_sink["webhook"].dead_letters == 1
        assert report.delivery_log[0].dead_lettered is True
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
