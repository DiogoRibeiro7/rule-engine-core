from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from .sinks import DeliveryLogEntry, DeliveryMetrics, DeliveryMetricsSnapshot, DeliveryResult
from .types import Alert


@dataclass
class EmittedAlert:
    entity_id: str
    rule_id: str
    alert: Alert
    timestamp: datetime
    delivery_results: List[DeliveryResult] = field(default_factory=list)


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


@dataclass(frozen=True)
class EngineConfig:
    initial_watermark: Optional[datetime] = None
    schedule_start: Optional[datetime] = None
