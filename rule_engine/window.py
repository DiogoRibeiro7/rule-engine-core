from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import List, Optional

from .types import SensorEvent


@dataclass
class EntityWindow:
    entity_id: str
    start: datetime
    end: datetime
    events: List[SensorEvent]

    def __post_init__(self) -> None:
        if self.start.tzinfo is None:
            self.start = self.start.replace(tzinfo=UTC)
        if self.end.tzinfo is None:
            self.end = self.end.replace(tzinfo=UTC)

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    def sensor(self, sensor_type: str) -> List[SensorEvent]:
        return [event for event in self.events if event.sensor_type == sensor_type]

    def last_seen(self, sensor_type: str) -> Optional[datetime]:
        events = self.sensor(sensor_type)
        return max((event.timestamp for event in events), default=None)

    def silence_duration(self, sensor_type: str) -> timedelta:
        last = self.last_seen(sensor_type)
        if last is None:
            return self.duration
        return self.end - last

    def all_sensors(self) -> List[str]:
        return sorted({event.sensor_type for event in self.events})

    def event_count(self, sensor_type: str) -> int:
        return len(self.sensor(sensor_type))
