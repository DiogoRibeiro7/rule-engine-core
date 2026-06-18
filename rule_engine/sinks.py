from __future__ import annotations

import hashlib
import hmac
import json
import os
import socket
import time
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Protocol, TypeAlias
from urllib import error, request


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 1
    base_delay_s: float = 0.0
    multiplier: float = 2.0
    max_delay_s: float | None = None
    sleep: bool = False

    @classmethod
    def from_dict(cls, value: Dict[str, Any] | None) -> "RetryConfig":
        retry_config = value or {}
        max_delay_value = retry_config.get("max_delay_s")
        return cls(
            max_attempts=max(1, int(retry_config.get("max_attempts", 1))),
            base_delay_s=max(0.0, float(retry_config.get("base_delay_s", 0.0))),
            multiplier=max(1.0, float(retry_config.get("multiplier", 2.0))),
            max_delay_s=(
                max(0.0, float(max_delay_value)) if max_delay_value is not None else None
            ),
            sleep=bool(retry_config.get("sleep", False)),
        )

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "max_attempts": self.max_attempts,
            "base_delay_s": self.base_delay_s,
            "multiplier": self.multiplier,
            "sleep": self.sleep,
        }
        if self.max_delay_s is not None:
            data["max_delay_s"] = self.max_delay_s
        return data


class SinkConfig(Protocol):
    type: str
    retry: RetryConfig

    def to_dict(self) -> Dict[str, Any]: ...


@dataclass(frozen=True)
class StdoutSinkConfig:
    type: str = "stdout"
    retry: RetryConfig = field(default_factory=RetryConfig)

    def to_dict(self) -> Dict[str, Any]:
        return {"type": self.type, "retry": self.retry.to_dict()}


@dataclass(frozen=True)
class FileSinkConfig:
    path: str
    type: str = "file"
    retry: RetryConfig = field(default_factory=RetryConfig)

    def to_dict(self) -> Dict[str, Any]:
        return {"type": self.type, "path": self.path, "retry": self.retry.to_dict()}


@dataclass(frozen=True)
class WebhookSinkConfig:
    url: str
    timeout_s: float = 5.0
    headers: Dict[str, str] = field(default_factory=dict)
    method: str = "POST"
    auth_token: str | None = None
    auth_scheme: str = "Bearer"
    signature_secret: str | None = None
    signature_header: str = "X-Signature-256"
    type: str = "webhook"
    retry: RetryConfig = field(default_factory=RetryConfig)

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "type": self.type,
            "url": self.url,
            "timeout_s": self.timeout_s,
            "headers": dict(self.headers),
            "method": self.method,
            "auth_scheme": self.auth_scheme,
            "signature_header": self.signature_header,
            "retry": self.retry.to_dict(),
        }
        if self.auth_token is not None:
            data["auth_token"] = self.auth_token
        if self.signature_secret is not None:
            data["signature_secret"] = self.signature_secret
        return data


@dataclass(frozen=True)
class QueueSinkConfig:
    queue: str
    retryable: bool = False
    type: str = "queue"
    retry: RetryConfig = field(default_factory=RetryConfig)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "queue": self.queue,
            "retryable": self.retryable,
            "retry": self.retry.to_dict(),
        }


@dataclass(frozen=True)
class ObjectStorageSinkConfig:
    bucket: str
    prefix: str = ""
    extension: str = "jsonl"
    retryable: bool = False
    type: str = "object_storage"
    retry: RetryConfig = field(default_factory=RetryConfig)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "bucket": self.bucket,
            "prefix": self.prefix,
            "extension": self.extension,
            "retryable": self.retryable,
            "retry": self.retry.to_dict(),
        }


@dataclass(frozen=True)
class GenericSinkConfig:
    type: str
    retry: RetryConfig = field(default_factory=RetryConfig)
    extras: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            **dict(self.extras),
            "retry": self.retry.to_dict(),
        }


AnySinkConfig: TypeAlias = (
    StdoutSinkConfig
    | FileSinkConfig
    | WebhookSinkConfig
    | QueueSinkConfig
    | ObjectStorageSinkConfig
    | GenericSinkConfig
)
SinkConfigLike: TypeAlias = AnySinkConfig | Dict[str, Any]


def parse_sink_config(config: Dict[str, Any]) -> AnySinkConfig:
    sink_type = str(config.get("type", "")).strip()
    retry = RetryConfig.from_dict(config.get("retry"))
    if sink_type == "stdout":
        return StdoutSinkConfig(retry=retry)
    if sink_type == "file":
        return FileSinkConfig(path=str(config.get("path", "")), retry=retry)
    if sink_type == "webhook":
        headers = config.get("headers") or {}
        return WebhookSinkConfig(
            url=str(config.get("url", "")),
            timeout_s=float(config.get("timeout_s", 5.0)),
            headers={str(key): str(value) for key, value in headers.items()},
            method=str(config.get("method", "POST")).upper(),
            auth_token=(
                str(config.get("auth_token")) if config.get("auth_token") is not None else None
            ),
            auth_scheme=str(config.get("auth_scheme", "Bearer")),
            signature_secret=(
                str(config.get("signature_secret"))
                if config.get("signature_secret") is not None
                else None
            ),
            signature_header=str(config.get("signature_header", "X-Signature-256")),
            retry=retry,
        )
    if sink_type == "queue":
        return QueueSinkConfig(
            queue=str(config.get("queue", "")),
            retryable=bool(config.get("retryable", False)),
            retry=retry,
        )
    if sink_type == "object_storage":
        return ObjectStorageSinkConfig(
            bucket=str(config.get("bucket", "")),
            prefix=str(config.get("prefix", "")),
            extension=str(config.get("extension", "jsonl")),
            retryable=bool(config.get("retryable", False)),
            retry=retry,
        )
    extras = {key: value for key, value in config.items() if key not in {"type", "retry"}}
    return GenericSinkConfig(type=sink_type, retry=retry, extras=extras)


def coerce_sink_config(config: SinkConfigLike) -> AnySinkConfig:
    if isinstance(
        config,
        (
            StdoutSinkConfig,
            FileSinkConfig,
            WebhookSinkConfig,
            QueueSinkConfig,
            ObjectStorageSinkConfig,
            GenericSinkConfig,
        ),
    ):
        return config
    return parse_sink_config(config)


def _require_config(config: SinkConfigLike, expected_type: type[Any]) -> AnySinkConfig:
    parsed = coerce_sink_config(config)
    if not isinstance(parsed, expected_type):
        raise ValueError(f"Expected {expected_type.__name__}, got {parsed.type}")
    assert isinstance(
        parsed,
        (
            StdoutSinkConfig,
            FileSinkConfig,
            WebhookSinkConfig,
            QueueSinkConfig,
            ObjectStorageSinkConfig,
            GenericSinkConfig,
        ),
    )
    return parsed


def _require_file_config(config: SinkConfigLike) -> FileSinkConfig:
    parsed = _require_config(config, FileSinkConfig)
    assert isinstance(parsed, FileSinkConfig)
    return parsed


def _require_webhook_config(config: SinkConfigLike) -> WebhookSinkConfig:
    parsed = _require_config(config, WebhookSinkConfig)
    assert isinstance(parsed, WebhookSinkConfig)
    return parsed


def _require_queue_config(config: SinkConfigLike) -> QueueSinkConfig:
    parsed = _require_config(config, QueueSinkConfig)
    assert isinstance(parsed, QueueSinkConfig)
    return parsed


def _require_object_storage_config(config: SinkConfigLike) -> ObjectStorageSinkConfig:
    parsed = _require_config(config, ObjectStorageSinkConfig)
    assert isinstance(parsed, ObjectStorageSinkConfig)
    return parsed


@dataclass(frozen=True)
class DeliveryRequest:
    sink_type: str
    rule_id: str
    entity_id: str
    severity: str
    message: str
    timestamp: datetime
    payload: Dict[str, Any]
    config: SinkConfigLike


@dataclass(frozen=True)
class DeliveryPayload:
    contract_version: str
    sink_type: str
    idempotency_key: str
    entity_id: str
    rule_id: str
    severity: str
    message: str
    timestamp: str
    payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "sink_type": self.sink_type,
            "idempotency_key": self.idempotency_key,
            "entity_id": self.entity_id,
            "rule_id": self.rule_id,
            "severity": self.severity,
            "message": self.message,
            "timestamp": self.timestamp,
            "payload": self.payload,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


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
    total_latency_ms: float = 0.0
    max_latency_ms: float = 0.0

    @property
    def average_latency_ms(self) -> float:
        if self.total_attempts == 0:
            return 0.0
        return self.total_latency_ms / self.total_attempts

    def to_dict(self) -> Dict[str, float]:
        return {
            "total_requests": self.total_requests,
            "total_attempts": self.total_attempts,
            "delivered": self.delivered,
            "failed": self.failed,
            "unsupported": self.unsupported,
            "retryable_failures": self.retryable_failures,
            "retries_attempted": self.retries_attempted,
            "dead_letters": self.dead_letters,
            "total_latency_ms": self.total_latency_ms,
            "max_latency_ms": self.max_latency_ms,
            "average_latency_ms": self.average_latency_ms,
        }


def build_delivery_payload(request: DeliveryRequest) -> DeliveryPayload:
    timestamp = request.timestamp.isoformat()
    raw_key = (
        f"{request.rule_id}|{request.entity_id}|{request.severity}|"
        f"{timestamp}|{request.message}"
    )
    digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    return DeliveryPayload(
        contract_version="rule-engine-core.v1",
        sink_type=request.sink_type,
        idempotency_key=digest,
        entity_id=request.entity_id,
        rule_id=request.rule_id,
        severity=request.severity,
        message=request.message,
        timestamp=timestamp,
        payload=request.payload,
    )


def _payload_metadata(payload: DeliveryPayload, **extra: Any) -> Dict[str, Any]:
    metadata = {
        "contract_version": payload.contract_version,
        "idempotency_key": payload.idempotency_key,
    }
    metadata.update(extra)
    return metadata


def _webhook_signature(secret: str, body: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


@dataclass(frozen=True)
class DeliveryMetricsSnapshot:
    overall: DeliveryMetrics
    by_sink: Dict[str, DeliveryMetrics]

    def sink_types(self) -> List[str]:
        return sorted(self.by_sink)

    def metrics_for(self, sink_type: str) -> DeliveryMetrics:
        return self.by_sink.get(sink_type, DeliveryMetrics())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "overall": self.overall.to_dict(),
            "by_sink": {
                sink_type: metrics.to_dict()
                for sink_type, metrics in sorted(self.by_sink.items())
            },
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


@dataclass(frozen=True)
class DeliveryLogEntry:
    sink_type: str
    rule_id: str
    entity_id: str
    severity: str
    status: str
    detail: str
    attempt_count: int
    retry_count: int
    latency_ms: float
    dead_lettered: bool
    retryable: bool
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sink_type": self.sink_type,
            "rule_id": self.rule_id,
            "entity_id": self.entity_id,
            "severity": self.severity,
            "status": self.status,
            "detail": self.detail,
            "attempt_count": self.attempt_count,
            "retry_count": self.retry_count,
            "latency_ms": self.latency_ms,
            "dead_lettered": self.dead_lettered,
            "retryable": self.retryable,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    base_delay_s: float = 0.0
    multiplier: float = 2.0
    max_delay_s: float | None = None
    sleep_enabled: bool = False

    @classmethod
    def from_config(cls, config: SinkConfigLike) -> "RetryPolicy":
        typed_config = coerce_sink_config(config)
        retry_config = typed_config.retry
        return cls(
            max_attempts=retry_config.max_attempts,
            base_delay_s=retry_config.base_delay_s,
            multiplier=retry_config.multiplier,
            max_delay_s=retry_config.max_delay_s,
            sleep_enabled=retry_config.sleep,
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
    config: SinkConfigLike
    result: DeliveryResult

    def to_dict(self) -> Dict[str, Any]:
        config = coerce_sink_config(self.config)
        return {
            "sink_type": self.sink_type,
            "rule_id": self.rule_id,
            "entity_id": self.entity_id,
            "severity": self.severity,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "payload": self.payload,
            "config": config.to_dict(),
            "result": {
                "status": self.result.status,
                "detail": self.result.detail,
                "retryable": self.result.retryable,
                "metadata": dict(self.result.metadata),
            },
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


class DeadLetterStore(Protocol):
    def record(self, record: DeadLetterRecord) -> None: ...


class InMemoryDeadLetterStore:
    def __init__(self) -> None:
        self.records: List[DeadLetterRecord] = []

    def record(self, record: DeadLetterRecord) -> None:
        self.records.append(record)


class FileDeadLetterStore:
    def __init__(
        self,
        path: str | Path,
        *,
        max_records: int | None = None,
        fsync: bool = False,
    ) -> None:
        self.path = Path(path)
        if max_records is not None and max_records <= 0:
            raise ValueError("max_records must be greater than zero when provided")
        self.max_records = max_records
        self.fsync = fsync

    def record(self, record: DeadLetterRecord) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(record.to_json() + "\n")
            handle.flush()
            if self.fsync:
                try:
                    os.fsync(handle.fileno())
                except OSError:
                    pass
        self._enforce_retention()

    def _enforce_retention(self) -> None:
        if self.max_records is None or not self.path.exists():
            return
        lines = self.path.read_text(encoding="utf-8").splitlines()
        if len(lines) <= self.max_records:
            return
        retained_lines = lines[-self.max_records :]
        content = "\n".join(retained_lines) + "\n"
        self.path.write_text(content, encoding="utf-8")


class SinkAdapter(Protocol):
    sink_type: str

    def deliver(self, request: DeliveryRequest) -> DeliveryResult: ...


def default_sink_adapters(
    *,
    include_stdout: bool = True,
    include_file: bool = True,
    include_webhook: bool = True,
    include_queue: bool = True,
    include_object_storage: bool = True,
    queue_transport: QueueTransport | None = None,
    object_storage_transport: ObjectStorageTransport | None = None,
) -> List[SinkAdapter]:
    adapters: List[SinkAdapter] = []
    if include_stdout:
        adapters.append(StdoutSink())
    if include_file:
        adapters.append(FileSink())
    if include_webhook:
        adapters.append(WebhookSink())
    if include_queue:
        adapters.append(QueueSink(transport=queue_transport))
    if include_object_storage:
        adapters.append(ObjectStorageSink(transport=object_storage_transport))
    return adapters


def create_sink_registry(
    *,
    include_stdout: bool = True,
    include_file: bool = True,
    include_webhook: bool = True,
    include_queue: bool = True,
    include_object_storage: bool = True,
    queue_transport: QueueTransport | None = None,
    object_storage_transport: ObjectStorageTransport | None = None,
    dead_letter_store: DeadLetterStore | None = None,
    dead_letter_path: str | Path | None = None,
    dead_letter_max_records: int | None = None,
    dead_letter_fsync: bool = False,
) -> "SinkRegistry":
    resolved_dead_letter_store = dead_letter_store
    if resolved_dead_letter_store is None and dead_letter_path is not None:
        resolved_dead_letter_store = FileDeadLetterStore(
            dead_letter_path,
            max_records=dead_letter_max_records,
            fsync=dead_letter_fsync,
        )
    return SinkRegistry(
        adapters=default_sink_adapters(
            include_stdout=include_stdout,
            include_file=include_file,
            include_webhook=include_webhook,
            include_queue=include_queue,
            include_object_storage=include_object_storage,
            queue_transport=queue_transport,
            object_storage_transport=object_storage_transport,
        ),
        dead_letter_store=resolved_dead_letter_store,
    )


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
        self._delivery_log: List[DeliveryLogEntry] = []
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

    def delivery_log(self) -> List[DeliveryLogEntry]:
        return list(self._delivery_log)

    def reset_metrics(self) -> None:
        self._overall_metrics = DeliveryMetrics()
        self._sink_metrics = {}

    def clear_delivery_log(self) -> None:
        self._delivery_log = []

    def deliver(self, request: DeliveryRequest) -> DeliveryResult:
        request = replace(request, config=coerce_sink_config(request.config))
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
            dead_lettered = self._record_dead_letter(request, result)
            self._append_delivery_log(
                request=request,
                result=result,
                attempt_count=0,
                latency_ms=0.0,
                dead_lettered=dead_lettered,
            )
            return result

        retry_policy = RetryPolicy.from_config(request.config)
        last_result: DeliveryResult | None = None
        backoff_schedule: List[float] = []
        total_latency_ms = 0.0
        for attempt in range(1, retry_policy.max_attempts + 1):
            started_at = time.perf_counter()
            result = adapter.deliver(request)
            latency_ms = (time.perf_counter() - started_at) * 1000.0
            total_latency_ms += latency_ms
            result_is_delivered = result.status == "delivered"
            result_is_retryable_failure = result.status != "delivered" and result.retryable
            self._increment_metrics(
                request.sink_type,
                total_attempts=1,
                delivered=1 if result_is_delivered else 0,
                failed=0 if result_is_delivered else 1,
                retryable_failures=1 if result_is_retryable_failure else 0,
                total_latency_ms=latency_ms,
                max_latency_ms=latency_ms,
            )
            result.metadata.setdefault("attempt_latency_ms", round(latency_ms, 3))
            result.metadata.setdefault("total_latency_ms", round(total_latency_ms, 3))
            result.metadata.setdefault("attempt", attempt)
            result.metadata.setdefault("max_attempts", retry_policy.max_attempts)
            result.metadata.setdefault("backoff_schedule_s", list(backoff_schedule))
            last_result = result
            if result_is_delivered:
                self._append_delivery_log(
                    request=request,
                    result=result,
                    attempt_count=attempt,
                    latency_ms=total_latency_ms,
                    dead_lettered=False,
                )
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
        dead_lettered = self._record_dead_letter(request, last_result)
        self._append_delivery_log(
            request=request,
            result=last_result,
            attempt_count=int(last_result.metadata.get("attempt", 0)),
            latency_ms=total_latency_ms,
            dead_lettered=dead_lettered,
        )
        return last_result

    def _record_dead_letter(self, request: DeliveryRequest, result: DeliveryResult) -> bool:
        self._increment_metrics(request.sink_type, dead_letters=1)
        if self.dead_letter_store is None:
            return True
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
        return True

    def _append_delivery_log(
        self,
        request: DeliveryRequest,
        result: DeliveryResult,
        attempt_count: int,
        latency_ms: float,
        dead_lettered: bool,
    ) -> None:
        self._delivery_log.append(
            DeliveryLogEntry(
                sink_type=request.sink_type,
                rule_id=request.rule_id,
                entity_id=request.entity_id,
                severity=request.severity,
                status=result.status,
                detail=result.detail,
                attempt_count=attempt_count,
                retry_count=max(0, attempt_count - 1),
                latency_ms=round(latency_ms, 3),
                dead_lettered=dead_lettered,
                retryable=result.retryable,
                metadata=dict(result.metadata),
            )
        )

    def _increment_metrics(self, sink_type: str, **increments: float) -> None:
        self._overall_metrics = self._merge_metrics(self._overall_metrics, increments)
        current = self._sink_metrics.get(sink_type, DeliveryMetrics())
        self._sink_metrics[sink_type] = self._merge_metrics(current, increments)

    @staticmethod
    def _merge_metrics(
        current: DeliveryMetrics,
        increments: Dict[str, float],
    ) -> DeliveryMetrics:
        values = current.__dict__.copy()
        for key, amount in increments.items():
            if key == "max_latency_ms":
                values[key] = max(values.get(key, 0.0), amount)
            else:
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
        config = _require_file_config(request.config)
        if not config.path:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail="Missing required sink config field 'path'",
            )
        path = Path(config.path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = build_delivery_payload(request)
        try:
            with path.open("a", encoding="utf-8") as handle:
                handle.write(payload.to_json() + "\n")
            return DeliveryResult(
                sink_type=self.sink_type,
                status="delivered",
                detail="Delivered to file",
                metadata=_payload_metadata(payload, path=str(path)),
            )
        except OSError as exc:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"File delivery failed: {exc}",
                metadata=_payload_metadata(payload, path=str(path)),
            )


class WebhookSink:
    sink_type = "webhook"

    def deliver(self, req: DeliveryRequest) -> DeliveryResult:
        config = _require_webhook_config(req.config)
        if not config.url:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail="Missing required sink config field 'url'",
            )

        timeout_s = config.timeout_s
        payload = build_delivery_payload(req)
        body = payload.to_json().encode("utf-8")
        headers = {"Content-Type": "application/json"}
        headers.update(config.headers)
        if config.auth_token:
            headers["Authorization"] = f"{config.auth_scheme} {config.auth_token}"
        if config.signature_secret:
            headers[config.signature_header] = _webhook_signature(config.signature_secret, body)
        method = config.method
        http_request = request.Request(
            url=config.url,
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
                    metadata=_payload_metadata(
                        payload,
                        status_code=status_code,
                        url=config.url,
                    ),
                )
        except error.HTTPError as exc:
            retryable = exc.code >= 500
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"Webhook returned HTTP {exc.code}",
                retryable=retryable,
                metadata=_payload_metadata(
                    payload,
                    status_code=exc.code,
                    url=config.url,
                ),
            )
        except (error.URLError, TimeoutError, socket.timeout, OSError) as exc:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"Webhook delivery failed: {exc}",
                retryable=True,
                metadata=_payload_metadata(payload, url=config.url),
            )


class QueueTransport(Protocol):
    def send(
        self, queue: str, payload: Dict[str, Any], config: Dict[str, Any]
    ) -> Dict[str, Any]: ...


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
        config = _require_queue_config(req.config)
        if not config.queue:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail="Missing required sink config field 'queue'",
            )

        payload = build_delivery_payload(req)
        try:
            transport_result = self.transport.send(
                config.queue,
                payload.to_dict(),
                config.to_dict(),
            )
            return DeliveryResult(
                sink_type=self.sink_type,
                status="delivered",
                detail="Delivered to queue",
                metadata=_payload_metadata(payload, queue=config.queue, **transport_result),
            )
        except TimeoutError as exc:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"Queue delivery timed out: {exc}",
                retryable=True,
                metadata=_payload_metadata(payload, queue=config.queue),
            )
        except Exception as exc:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"Queue delivery failed: {exc}",
                retryable=config.retryable,
                metadata=_payload_metadata(payload, queue=config.queue),
            )


class ObjectStorageTransport(Protocol):
    def put_object(
        self,
        bucket: str,
        key: str,
        body: str,
        config: Dict[str, Any],
    ) -> Dict[str, Any]: ...


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
        config = _require_object_storage_config(req.config)
        if not config.bucket:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail="Missing required sink config field 'bucket'",
            )

        prefix = config.prefix.strip("/")
        extension = config.extension.lstrip(".")
        timestamp_token = req.timestamp.strftime("%Y%m%dT%H%M%SZ")
        base_name = f"{req.rule_id}-{timestamp_token}.{extension}"
        key = f"{prefix}/{base_name}" if prefix else base_name
        payload = build_delivery_payload(req)
        body = payload.to_json()
        try:
            result = self.transport.put_object(config.bucket, key, body, config.to_dict())
            return DeliveryResult(
                sink_type=self.sink_type,
                status="delivered",
                detail="Delivered to object storage",
                metadata=_payload_metadata(payload, **result),
            )
        except TimeoutError as exc:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"Object storage delivery timed out: {exc}",
                retryable=True,
                metadata=_payload_metadata(payload, bucket=config.bucket, key=key),
            )
        except Exception as exc:
            return DeliveryResult(
                sink_type=self.sink_type,
                status="failed",
                detail=f"Object storage delivery failed: {exc}",
                retryable=config.retryable,
                metadata=_payload_metadata(payload, bucket=config.bucket, key=key),
            )
