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


@dataclass
class ExplainCheck:
    """One predicate evaluated while explaining a rule."""

    label: str
    passed: bool
    observed: Any = None
    expected: Any = None
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "passed": self.passed,
            "observed": self.observed,
            "expected": self.expected,
            "detail": self.detail,
        }


@dataclass
class RuleExplanation:
    """Why one rule would or would not emit for a given event."""

    rule_id: str
    entity_id: str
    trigger_type: str
    outcome: str
    checks: List[ExplainCheck] = field(default_factory=list)
    detail: str = ""

    @property
    def would_emit(self) -> bool:
        return self.outcome == "would_emit"

    def first_failure(self) -> Optional[ExplainCheck]:
        """The predicate that stopped this rule, or None if nothing failed."""
        for check in self.checks:
            if not check.passed:
                return check
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "entity_id": self.entity_id,
            "trigger_type": self.trigger_type,
            "outcome": self.outcome,
            "detail": self.detail,
            "checks": [check.to_dict() for check in self.checks],
        }


@dataclass
class ExplainResult:
    """Explanations for every rule that was considered for one event."""

    entity_id: str
    timestamp: str
    rules: List[RuleExplanation] = field(default_factory=list)

    def by_rule(self, rule_id: str) -> Optional[RuleExplanation]:
        for explanation in self.rules:
            if explanation.rule_id == rule_id:
                return explanation
        return None

    def emitting_rule_ids(self) -> List[str]:
        return [entry.rule_id for entry in self.rules if entry.would_emit]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "timestamp": self.timestamp,
            "rules": [entry.to_dict() for entry in self.rules],
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def render(self) -> str:
        """Human-readable view over the same structure."""
        lines: List[str] = [f"event: entity={self.entity_id} at {self.timestamp}"]
        for entry in self.rules:
            lines.append("")
            lines.append(f"rule: {entry.rule_id}  [{entry.trigger_type}]")
            for check in entry.checks:
                mark = "PASS" if check.passed else "FAIL"
                line = f"    {check.label:<40} {mark}"
                if check.observed is not None:
                    line = f"{line}   observed={check.observed}"
                lines.append(line)
                if check.detail:
                    lines.append(f"        {check.detail}")
            lines.append(f"  outcome: {entry.outcome}")
            if entry.detail:
                lines.append(f"    {entry.detail}")
        return chr(10).join(lines)


@dataclass
class RuleSimulationStats:
    """What one rule did over a simulated event stream."""

    rule_id: str
    evaluations: int = 0
    alerts: int = 0
    fires: int = 0
    repeats: int = 0
    resolutions: int = 0
    retractions: int = 0
    suppressed: int = 0
    entities: List[str] = field(default_factory=list)
    first_alert: Optional[str] = None
    last_alert: Optional[str] = None
    mean_episode_seconds: Optional[float] = None
    max_episode_seconds: Optional[float] = None

    @property
    def entity_count(self) -> int:
        return len(self.entities)

    @property
    def fire_rate(self) -> Optional[float]:
        """Alerts per evaluation, or None when the rule was never evaluated."""
        if self.evaluations == 0:
            return None
        return self.alerts / self.evaluations

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "evaluations": self.evaluations,
            "alerts": self.alerts,
            "fires": self.fires,
            "repeats": self.repeats,
            "resolutions": self.resolutions,
            "retractions": self.retractions,
            "suppressed": self.suppressed,
            "entity_count": self.entity_count,
            "entities": list(self.entities),
            "first_alert": self.first_alert,
            "last_alert": self.last_alert,
            "mean_episode_seconds": self.mean_episode_seconds,
            "max_episode_seconds": self.max_episode_seconds,
            "fire_rate": self.fire_rate,
        }


@dataclass
class SimulationReport:
    """Result of replaying a stream against a rule set in a clean engine."""

    event_count: int = 0
    alert_count: int = 0
    from_time: Optional[str] = None
    to_time: Optional[str] = None
    elapsed_ms: float = 0.0
    rules: List[RuleSimulationStats] = field(default_factory=list)

    def by_rule(self, rule_id: str) -> Optional[RuleSimulationStats]:
        for entry in self.rules:
            if entry.rule_id == rule_id:
                return entry
        return None

    def noisiest_rules(self, limit: int = 5) -> List[RuleSimulationStats]:
        return sorted(self.rules, key=lambda entry: entry.alerts, reverse=True)[:limit]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_count": self.event_count,
            "alert_count": self.alert_count,
            "from_time": self.from_time,
            "to_time": self.to_time,
            "elapsed_ms": self.elapsed_ms,
            "rules": [entry.to_dict() for entry in self.rules],
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent)


@dataclass
class SimulationComparison:
    """Two rule sets replayed over the same stream."""

    baseline: SimulationReport = field(default_factory=SimulationReport)
    candidate: SimulationReport = field(default_factory=SimulationReport)
    only_baseline: List[Dict[str, Any]] = field(default_factory=list)
    only_candidate: List[Dict[str, Any]] = field(default_factory=list)
    shared: int = 0

    @property
    def alert_delta(self) -> int:
        return self.candidate.alert_count - self.baseline.alert_count

    def rule_deltas(self) -> Dict[str, Dict[str, int]]:
        """Per-rule change in alerts and suppressions, candidate minus baseline."""
        rule_ids = {entry.rule_id for entry in self.baseline.rules}
        rule_ids |= {entry.rule_id for entry in self.candidate.rules}
        deltas: Dict[str, Dict[str, int]] = {}
        for rule_id in sorted(rule_ids):
            before = self.baseline.by_rule(rule_id)
            after = self.candidate.by_rule(rule_id)
            deltas[rule_id] = {
                "alerts": (after.alerts if after else 0) - (before.alerts if before else 0),
                "suppressed": (after.suppressed if after else 0)
                - (before.suppressed if before else 0),
            }
        return deltas

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_delta": self.alert_delta,
            "shared": self.shared,
            "only_baseline": list(self.only_baseline),
            "only_candidate": list(self.only_candidate),
            "rule_deltas": self.rule_deltas(),
            "baseline": self.baseline.to_dict(),
            "candidate": self.candidate.to_dict(),
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent)


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
    suppressed_counts: Dict[str, int] = field(default_factory=dict)
    entity_watermarks: Dict[str, str] = field(default_factory=dict)
    floor_watermark: Optional[str] = None
    version: int = SNAPSHOT_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "watermark": self.watermark,
            "rule_fingerprints": dict(self.rule_fingerprints),
            "entities": deepcopy(self.entities),
            "late_event_metrics": deepcopy(self.late_event_metrics),
            "suppressed_counts": dict(self.suppressed_counts),
            "entity_watermarks": dict(self.entity_watermarks),
            "floor_watermark": self.floor_watermark,
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
            suppressed_counts=dict(payload.get("suppressed_counts", {})),
            entity_watermarks=dict(payload.get("entity_watermarks", {})),
            floor_watermark=payload.get("floor_watermark"),
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
    recompute_late_windows: bool = False
