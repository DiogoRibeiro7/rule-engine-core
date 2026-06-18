from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from .sinks import DeliveryLogEntry, DeliveryMetricsSnapshot, DeliveryResult
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


@dataclass(frozen=True)
class EngineConfig:
    initial_watermark: Optional[datetime] = None
    schedule_start: Optional[datetime] = None
