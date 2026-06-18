from __future__ import annotations

import json
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Protocol
from urllib import error, request


@dataclass(frozen=True)
class DeliveryRequest:
    sink_type: str
    rule_id: str
    entity_id: str
    severity: str
    message: str
    timestamp: datetime
    payload: Dict[str, Any]
    config: Dict[str, Any]


@dataclass(frozen=True)
class DeliveryResult:
    sink_type: str
    status: str
    detail: str
    retryable: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DeliveryMetrics:
    total_requests: int = 0
    total_attempts: int = 0
    delivered: int = 0
    failed: int = 0
    unsupported: int = 0
    retryable_failures: int = 0
    retries_attempted: int = 0
    dead_letters: int = 0


@dataclass(frozen=True)
class DeliveryMetricsSnapshot:
    overall: DeliveryMetrics
    by_sink: Dict[str, DeliveryMetrics]


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    base_delay_s: float = 0.0
    multiplier: float = 2.0
    max_delay_s: float | None = None
    sleep_enabled: bool = False

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "RetryPolicy":
        retry_config = config.get("retry", {})
        max_attempts = int(retry_config.get("max_attempts", 1))
        base_delay_s = float(retry_config.get("base_delay_s", 0.0))
        multiplier = float(retry_config.get("multiplier", 2.0))
        max_delay_value = retry_config.get("max_delay_s")
        max_delay_s = float(max_delay_value) if max_delay_value is not None else None
        sleep_enabled = bool(retry_config.get("sleep", False))
        return cls(
            max_attempts=max(1, max_attempts),
            base_delay_s=max(0.0, base_delay_s),
            multiplier=max(1.0, multiplier),
            max_delay_s=max_delay_s if max_delay_s is None else max(0.0, max_delay_s),
            sleep_enabled=sleep_enabled,
        )

    def backoff_delay(self, attempt: int) -> float:
        if attempt <= 1 or self.base_delay_s <= 0:
            return 0.0
        delay = self.base_delay_s * (self.multiplier ** (attempt - 2))
        if self.max_delay_s is not None:
            delay = min(delay, self.max_delay_s)
        return delay


@dataclass(frozen=True)
class DeadLetterRecord:
    sink_type: str
    rule_id: str
    entity_id: str
    severity: str
    message: str
    timestamp: datetime
    payload: Dict[str, Any]
    config: Dict[str, Any]
    result: DeliveryResult


class DeadLetterStore(Protocol):
    def record(self, record: DeadLetterRecord) -> None:
        ...


class InMemoryDeadLetterStore:
    def __init__(self) -> None:
        self.records: List[DeadLetterRecord] = []

    def record(self, record: DeadLetterRecord) -> None:
        self.records.append(record)


class FileDeadLetterStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def record(self, record: DeadLetterRecord) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "sink_type": record.sink_type,
            "rule_id": record.rule_id,
            "entity_id": record.entity_id,
            "severity": record.severity,
            "message": record.message,
            "timestamp": record.timestamp.isoformat(),
            "payload": record.payload,
            "config": record.config,
            "result": {
                "status": record.result.status,
                "detail": record.result.detail,
                "retryable": record.result.retryable,
                "metadata": record.result.metadata,
            },
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")


class SinkAdapter(Protocol):
    sink_type: str

    def deliver(self, request: DeliveryRequest) -> DeliveryResult:
        ...


class SinkRegistry:
    def __init__(
        self,
        adapters: Iterable[SinkAdapter] | None = None,
        dead_letter_store: DeadLetterStore | None = None,
    ):
        self._adapters: Dict[str, SinkAdapter] = {}
        self.dead_letter_store = dead_letter_store
        self._overall_metrics = DeliveryMetrics()
        self._sink_metrics: Dict[str, DeliveryMetrics] = {}
        for adapter in adapters or []:
            self.register(adapter)

    def register(self, adapter: SinkAdapter) -> None:
        self._adapters[adapter.sink_type] = adapter

    def get(self, sink_type: str) -> SinkAdapter | None:
        return self._adapters.get(sink_type)

    def metrics(self) -> DeliveryMetricsSnapshot:
        return DeliveryMetricsSnapshot(
            overall=self._overall_metrics,
            by_sink=dict(self._sink_metrics),
        )

    def reset_metrics(self) -> None:
        self._overall_metrics = DeliveryMetrics()
        self._sink_metrics = {}

    def deliver(self, request: DeliveryRequest) -> DeliveryResult:
        self._increment_metrics(request.sink_type, total_requests=1)
        adapter = self.get(request.sink_type)
        if adapter is None:
            self._increment_metrics(
                request.sink_type,
                failed=1,
                unsupported=1,
            )
            result = DeliveryResult(
                sink_type=request.sink_type,
                status="unsupported",
                detail=f"No adapter registered for sink type '{request.sink_type}'",
            )
            self._record_dead_letter(request, result)
            return result

        retry_policy = RetryPolicy.from_config(request.config)
        last_result: DeliveryResult | None = None
        backoff_schedule: List[float] = []
        for attempt in range(1, retry_policy.max_attempts + 1):
            result = adapter.deliver(request)
            result_is_delivered = result.status == "delivered"
            result_is_retryable_failure = result.status != "delivered" and result.retryable
            self._increment_metrics(
                request.sink_type,
                total_attempts=1,
                delivered=1 if result_is_delivered else 0,
                failed=0 if result_is_delivered else 1,
                retryable_failures=1 if result_is_retryable_failure else 0,
            )
            result.metadata.setdefault("attempt", attempt)
            result.metadata.setdefault("max_attempts", retry_policy.max_attempts)
            result.metadata.setdefault("backoff_schedule_s", list(backoff_schedule))
            last_result = result
            if result_is_delivered:
                return result
            if not result.retryable:
                break
            if attempt < retry_policy.max_attempts:
                delay = retry_policy.backoff_delay(attempt + 1)
                backoff_schedule.append(delay)
                self._increment_metrics(request.sink_type, retries_attempted=1)
                if retry_policy.sleep_enabled and delay > 0:
                    time.sleep(delay)

        assert last_result is not None
        self._record_dead_letter(request, last_result)
        return last_result

    def _record_dead_letter(
        self, request: DeliveryRequest, result: DeliveryResult
    ) -> None:
        self._increment_metrics(request.sink_type, dead_letters=1)
        if self.dead_letter_store is None:
            return
        self.dead_letter_store.record(
            DeadLetterRecord(
                sink_type=request.sink_type,
                rule_id=request.rule_id,
                entity_id=request.entity_id,
                severity=request.severity,
                message=request.message,
                timestamp=request.timestamp,
                payload=request.payload,
                config=request.config,
                result=result,
            )
        )

    def _increment_metrics(self, sink_type: str, **increments: int) -> None:
        self._overall_metrics = self._merge_metrics(self._overall_metrics, increments)
        current = self._sink_metrics.get(sink_type, DeliveryMetrics())
        self._sink_metrics[sink_type] = self._merge_metrics(current, increments)

    @staticmethod
    def _merge_metrics(
        current: DeliveryMetrics,
        increments: Dict[str, int],
    ) -> DeliveryMetrics:
        values = current.__dict__.copy()
        for key, amount in increments.items():
            values[key] = values.get(key, 0) + amount
        return DeliveryMetrics(**values)


class StdoutSink:
    sink_type = "stdout"

    def __init__(self) -> None:
        self.messages: List[str] = []

    def deliver(self, request: DeliveryRequest) -> DeliveryResult:
        rendered = (
            f"{request.timestamp.isoformat()} entity={request.entity_id} "
            f"rule={request.rule_id} severity={request.severity} "
            f"message={request.message}"
        )
        print(rendered)
        self.messages.append(rendered)
        return DeliveryResult(
            sink_type=self.sink_type,
            status="delivered",
            detail="Delivered to stdout",
        )


class FileSink:
    sink_type = "file"

    def deliver(self, request: DeliveryRequest) -> DeliveryResult:
        path_value = request.config.get("path")
        if not path_value:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail="Missing required sink config field 'path'",
            )
        path = Path(path_value)
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "entity_id": request.entity_id,
            "rule_id": request.rule_id,
            "severity": request.severity,
            "message": request.message,
            "timestamp": request.timestamp.isoformat(),
            "payload": request.payload,
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
        return DeliveryResult(
            sink_type=self.sink_type,
            status="delivered",
            detail="Delivered to file",
            metadata={"path": str(path)},
        )


class WebhookSink:
    sink_type = "webhook"

    def deliver(self, req: DeliveryRequest) -> DeliveryResult:
        url = req.config.get("url")
        if not url:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail="Missing required sink config field 'url'",
            )

        timeout_s = float(req.config.get("timeout_s", 5.0))
        payload = {
            "entity_id": req.entity_id,
            "rule_id": req.rule_id,
            "severity": req.severity,
            "message": req.message,
            "timestamp": req.timestamp.isoformat(),
            "payload": req.payload,
        }
        body = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        headers.update(req.config.get("headers", {}))
        method = str(req.config.get("method", "POST")).upper()
        http_request = request.Request(
            url=url,
            data=body,
            headers=headers,
            method=method,
        )

        try:
            with request.urlopen(http_request, timeout=timeout_s) as response:
                status_code = getattr(response, "status", None) or response.getcode()
                return DeliveryResult(
                    sink_type=self.sink_type,
                    status="delivered",
                    detail="Delivered to webhook",
                    metadata={"status_code": status_code, "url": url},
                )
        except error.HTTPError as exc:
            retryable = exc.code >= 500
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"Webhook returned HTTP {exc.code}",
                retryable=retryable,
                metadata={"status_code": exc.code, "url": url},
            )
        except (error.URLError, TimeoutError, socket.timeout) as exc:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"Webhook delivery failed: {exc}",
                retryable=True,
                metadata={"url": url},
            )


class QueueTransport(Protocol):
    def send(self, queue: str, payload: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        ...


class InMemoryQueueTransport:
    def __init__(self) -> None:
        self.messages: List[Dict[str, Any]] = []

    def send(self, queue: str, payload: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        record = {"queue": queue, "payload": payload, "config": config}
        self.messages.append(record)
        return {"message_id": str(len(self.messages))}


class QueueSink:
    sink_type = "queue"

    def __init__(self, transport: QueueTransport | None = None) -> None:
        self.transport = transport or InMemoryQueueTransport()

    def deliver(self, req: DeliveryRequest) -> DeliveryResult:
        queue_name = req.config.get("queue")
        if not queue_name:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail="Missing required sink config field 'queue'",
            )

        payload = {
            "entity_id": req.entity_id,
            "rule_id": req.rule_id,
            "severity": req.severity,
            "message": req.message,
            "timestamp": req.timestamp.isoformat(),
            "payload": req.payload,
        }
        try:
            transport_result = self.transport.send(queue_name, payload, req.config)
            return DeliveryResult(
                sink_type=self.sink_type,
                status="delivered",
                detail="Delivered to queue",
                metadata={
                    "queue": queue_name,
                    **transport_result,
                },
            )
        except TimeoutError as exc:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"Queue delivery timed out: {exc}",
                retryable=True,
                metadata={"queue": queue_name},
            )
        except Exception as exc:
            retryable = bool(req.config.get("retryable", False))
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"Queue delivery failed: {exc}",
                retryable=retryable,
                metadata={"queue": queue_name},
            )


class ObjectStorageTransport(Protocol):
    def put_object(
        self,
        bucket: str,
        key: str,
        body: str,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        ...


class FileObjectStorageTransport:
    def __init__(self, root: str | Path = ".object_store") -> None:
        self.root = Path(root)
        self.objects: List[Dict[str, Any]] = []

    def put_object(
        self,
        bucket: str,
        key: str,
        body: str,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        path = self.root / bucket / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
        self.objects.append({"bucket": bucket, "key": key, "body": body, "config": config})
        return {"bucket": bucket, "key": key, "path": str(path)}


class ObjectStorageSink:
    sink_type = "object_storage"

    def __init__(self, transport: ObjectStorageTransport | None = None) -> None:
        self.transport = transport or FileObjectStorageTransport()

    def deliver(self, req: DeliveryRequest) -> DeliveryResult:
        bucket = req.config.get("bucket")
        if not bucket:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail="Missing required sink config field 'bucket'",
            )

        prefix = str(req.config.get("prefix", "")).strip("/")
        extension = str(req.config.get("extension", "jsonl")).lstrip(".")
        timestamp_token = req.timestamp.strftime("%Y%m%dT%H%M%SZ")
        base_name = f"{req.rule_id}-{timestamp_token}.{extension}"
        key = f"{prefix}/{base_name}" if prefix else base_name
        body = json.dumps(
            {
                "entity_id": req.entity_id,
                "rule_id": req.rule_id,
                "severity": req.severity,
                "message": req.message,
                "timestamp": req.timestamp.isoformat(),
                "payload": req.payload,
            }
        )
        try:
            result = self.transport.put_object(bucket, key, body, req.config)
            return DeliveryResult(
                sink_type=self.sink_type,
                status="delivered",
                detail="Delivered to object storage",
                metadata=result,
            )
        except TimeoutError as exc:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"Object storage delivery timed out: {exc}",
                retryable=True,
                metadata={"bucket": bucket, "key": key},
            )
        except Exception as exc:
            retryable = bool(req.config.get("retryable", False))
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"Object storage delivery failed: {exc}",
                retryable=retryable,
                metadata={"bucket": bucket, "key": key},
            )
