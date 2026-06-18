from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Dict, Optional


@dataclass
class SensorEvent:
    entity_id: str
    sensor_type: str
    value: float
    timestamp_ms: int

    @property
    def timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ms / 1000.0, tz=UTC)


@dataclass
class RuleContext:
    entity_id: str
    rule_id: str
    timestamp: datetime
    duration: timedelta = timedelta(0)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Alert:
    severity: str
    message: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StoreRecord:
    fields: Dict[str, Any]
