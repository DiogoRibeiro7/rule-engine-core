from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from .sinks import DeliveryLogEntry, DeliveryMetrics, DeliveryMetricsSnapshot, DeliveryResult
from .types import Alert


@dataclass
class EmittedAlert:
    entity_id: str
    rule_id: str
    alert: Alert
    timestamp: datetime
    delivery_results: List[DeliveryResult] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "rule_id": self.rule_id,
            "timestamp": self.timestamp.isoformat(),
            "alert": {
                "severity": self.alert.severity,
                "message": self.alert.message,
                "metadata": self.alert.metadata,
            },
            "delivery_results": [
                {
                    "sink_type": result.sink_type,
                    "status": result.status,
                    "detail": result.detail,
                    "retryable": result.retryable,
                    "metadata": dict(result.metadata),
                }
                for result in self.delivery_results
            ],
        }


@dataclass(frozen=True)
class ReplayDeliveryReport:
    alert_count: int
    delivery_metrics: DeliveryMetricsSnapshot
    delivery_log: List[DeliveryLogEntry]

    @property
    def has_failures(self) -> bool:
        return self.delivery_metrics.overall.failed > 0

    @property
    def has_dead_letters(self) -> bool:
        return self.delivery_metrics.overall.dead_letters > 0

    def sink_types(self) -> List[str]:
        return self.delivery_metrics.sink_types()

    def metrics_for(self, sink_type: str) -> DeliveryMetrics:
        return self.delivery_metrics.metrics_for(sink_type)

    def entries_for_sink(self, sink_type: str) -> List[DeliveryLogEntry]:
        return [entry for entry in self.delivery_log if entry.sink_type == sink_type]

    def failed_entries(self) -> List[DeliveryLogEntry]:
        return [entry for entry in self.delivery_log if entry.status != "delivered"]

    def dead_letter_entries(self) -> List[DeliveryLogEntry]:
        return [entry for entry in self.delivery_log if entry.dead_lettered]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_count": self.alert_count,
            "delivery_metrics": self.delivery_metrics.to_dict(),
            "delivery_log": [entry.to_dict() for entry in self.delivery_log],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


@dataclass(frozen=True)
class RuleMetadata:
    rule_id: str
    description: str
    trigger_type: str
    entity_id_filter: str
    sensor_types: List[str]
    sink_types: List[str]
    aggregation_ids: List[str]


@dataclass(frozen=True)
class EvaluationResult:
    alerts: List[EmittedAlert]
    delivery_report: ReplayDeliveryReport

    @property
    def alert_count(self) -> int:
        return len(self.alerts)

    @property
    def has_failures(self) -> bool:
        return self.delivery_report.has_failures

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alerts": [alert.to_dict() for alert in self.alerts],
            "delivery_report": self.delivery_report.to_dict(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


@dataclass(frozen=True)
class EngineConfig:
    initial_watermark: Optional[datetime] = None
    schedule_start: Optional[datetime] = None
