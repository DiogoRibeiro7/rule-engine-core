from __future__ import annotations

import json
from copy import deepcopy
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


RELOAD_POLICIES = ("reset", "preserve", "drain")


@dataclass
class RuleReloadOutcome:
    """What a reload did to one rule's retained state."""

    rule_id: str
    outcome: str
    compatible: Optional[bool] = None
    draining_entities: List[str] = field(default_factory=list)
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "outcome": self.outcome,
            "compatible": self.compatible,
            "draining_entities": list(self.draining_entities),
            "detail": self.detail,
        }


@dataclass
class ReloadReport:
    """Result of a rule reload, per rule."""

    applied: bool = True
    activate_at: Optional[str] = None
    outcomes: List[RuleReloadOutcome] = field(default_factory=list)

    def by_rule(self, rule_id: str) -> Optional[RuleReloadOutcome]:
        for outcome in self.outcomes:
            if outcome.rule_id == rule_id:
                return outcome
        return None

    def rule_ids_with_outcome(self, outcome: str) -> List[str]:
        return [entry.rule_id for entry in self.outcomes if entry.outcome == outcome]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "applied": self.applied,
            "activate_at": self.activate_at,
            "outcomes": [entry.to_dict() for entry in self.outcomes],
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent)


SNAPSHOT_VERSION = 1


@dataclass
class EngineSnapshot:
    """Serializable engine state: watermark, per-entity rule state, counters.

    ``rule_fingerprints`` records the state-shaping structure of each rule at
    snapshot time so that restoring into a rule whose windows or timers changed
    fails loudly instead of reinterpreting state that no longer means the same
    thing.
    """

    watermark: Optional[str] = None
    entities: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)
    rule_fingerprints: Dict[str, str] = field(default_factory=dict)
    late_event_metrics: Dict[str, Any] = field(default_factory=dict)
    version: int = SNAPSHOT_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "watermark": self.watermark,
            "rule_fingerprints": dict(self.rule_fingerprints),
            "entities": deepcopy(self.entities),
            "late_event_metrics": deepcopy(self.late_event_metrics),
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "EngineSnapshot":
        version = payload.get("version")
        if version != SNAPSHOT_VERSION:
            raise ValueError(
                f"Unsupported snapshot version {version!r}; "
                f"this build reads version {SNAPSHOT_VERSION}"
            )
        return cls(
            watermark=payload.get("watermark"),
            entities=deepcopy(payload.get("entities", {})),
            rule_fingerprints=dict(payload.get("rule_fingerprints", {})),
            late_event_metrics=deepcopy(payload.get("late_event_metrics", {})),
            version=version,
        )

    @classmethod
    def from_json(cls, text: str) -> "EngineSnapshot":
        return cls.from_dict(json.loads(text))


@dataclass
class LateEventMetrics:
    """Counts of events that arrived behind the engine watermark."""

    total: int = 0
    accepted: int = 0
    dropped: int = 0
    rejected: int = 0
    per_rule_accepted: Dict[str, int] = field(default_factory=dict)
    per_rule_dropped: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total": self.total,
            "accepted": self.accepted,
            "dropped": self.dropped,
            "rejected": self.rejected,
            "per_rule_accepted": dict(self.per_rule_accepted),
            "per_rule_dropped": dict(self.per_rule_dropped),
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent)


@dataclass(frozen=True)
class EngineConfig:
    initial_watermark: Optional[datetime] = None
    schedule_start: Optional[datetime] = None
    late_event_policy: str = "reject"
