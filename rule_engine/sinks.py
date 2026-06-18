from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Protocol


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


class SinkAdapter(Protocol):
    sink_type: str

    def deliver(self, request: DeliveryRequest) -> DeliveryResult:
        ...


class SinkRegistry:
    def __init__(self, adapters: Iterable[SinkAdapter] | None = None):
        self._adapters: Dict[str, SinkAdapter] = {}
        for adapter in adapters or []:
            self.register(adapter)

    def register(self, adapter: SinkAdapter) -> None:
        self._adapters[adapter.sink_type] = adapter

    def get(self, sink_type: str) -> SinkAdapter | None:
        return self._adapters.get(sink_type)

    def deliver(self, request: DeliveryRequest) -> DeliveryResult:
        adapter = self.get(request.sink_type)
        if adapter is None:
            return DeliveryResult(
                sink_type=request.sink_type,
                status="unsupported",
                detail=f"No adapter registered for sink type '{request.sink_type}'",
            )
        return adapter.deliver(request)


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
