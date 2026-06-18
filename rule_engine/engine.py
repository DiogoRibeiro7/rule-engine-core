from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Optional

from .registry import EVENT_RULES, SCHEDULED_RULES, WINDOW_RULES
from .types import Alert, RuleContext, SensorEvent, StoreRecord
from .window import EntityWindow


class RuleEngine:
    def process_event(self, event: SensorEvent) -> List[Alert]:
        context = RuleContext(
            entity_id=event.entity_id,
            rule_id="",
            timestamp=event.timestamp,
        )
        alerts: List[Alert] = []
        for rule in EVENT_RULES:
            context.rule_id = rule.rule_id
            result = rule.fn(event, context)
            if isinstance(result, Alert):
                alerts.append(result)
            elif isinstance(result, list):
                alerts.extend(result)
        return alerts

    def process_window(
        self,
        entity_id: str,
        start: datetime,
        end: datetime,
        events: Iterable[SensorEvent],
    ) -> List[Alert]:
        window = EntityWindow(entity_id=entity_id, start=start, end=end, events=list(events))
        context = RuleContext(entity_id=entity_id, rule_id="", timestamp=end, duration=end - start)
        alerts: List[Alert] = []
        for rule in WINDOW_RULES:
            context.rule_id = rule.rule_id
            result = rule.fn(window, context)
            if isinstance(result, Alert):
                alerts.append(result)
            elif isinstance(result, list):
                alerts.extend(result)
        return alerts

    def process_scheduled(
        self,
        entity_id: str,
        fire_time: datetime,
        events: Iterable[SensorEvent],
        rule_id: Optional[str] = None,
    ) -> List[Alert | StoreRecord]:
        buffered = list(events)
        window = EntityWindow(
            entity_id=entity_id,
            start=min((e.timestamp for e in buffered), default=fire_time),
            end=fire_time,
            events=buffered,
        )
        context = RuleContext(
            entity_id=entity_id,
            rule_id=rule_id or "",
            timestamp=fire_time,
            duration=fire_time - window.start,
        )
        results: List[Alert | StoreRecord] = []
        for rule in SCHEDULED_RULES:
            if rule_id and rule.rule_id != rule_id:
                continue
            context.rule_id = rule.rule_id
            result = rule.fn(window, context)
            if isinstance(result, Alert) or isinstance(result, StoreRecord):
                results.append(result)
            elif isinstance(result, list):
                results.extend(result)
        return results
