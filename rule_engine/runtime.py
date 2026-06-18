from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from math import ceil
from statistics import mean, pstdev
from typing import Any, Dict, Iterable, List, Optional

from .declarative import Action, DeclarativeRule
from .sinks import (
    DeliveryLogEntry,
    DeliveryMetricsSnapshot,
    DeliveryRequest,
    DeliveryResult,
    SinkRegistry,
)
from .types import Alert, RuleContext, SensorEvent
from .window import EntityWindow


_DURATION_RE = re.compile(r"^(?P<value>\d+)(?P<unit>[smhd])$")
_TEMPLATE_RE = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")
_TRIGGER_TYPES = {"event", "window", "absence", "composite", "scheduled"}
_CONDITION_OPERATORS = {"AND", "OR"}
_COMPARISON_OPERATORS = {"eq", "ne", "gt", "gte", "lt", "lte"}


def parse_duration(value: Optional[str]) -> Optional[timedelta]:
    if value is None:
        return None
    match = _DURATION_RE.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"Unsupported duration: {value}")
    amount = int(match.group("value"))
    if amount <= 0:
        raise ValueError(f"Duration must be greater than zero: {value}")
    unit = match.group("unit")
    if unit == "s":
        return timedelta(seconds=amount)
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    return timedelta(days=amount)


def _to_epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


@dataclass
class Operand:
    metric: Optional[str] = None
    operator: Optional[str] = None
    value: Any = None
    const: Optional[bool] = None


@dataclass
class Aggregation:
    agg_id: str
    function: str
    field: Optional[str] = None
    input: Optional[str] = None
    percentile: Optional[float] = None
    sub_window: Optional[timedelta] = None


@dataclass
class CompiledRule:
    rule_id: str
    description: str
    trigger_type: str
    entity_id_filter: str
    sensor_types: List[str]
    actions: List[Action]
    condition_operator: Optional[str]
    operands: List[Operand]
    aggregations: List[Aggregation]
    duration: Optional[timedelta] = None
    slide: Optional[timedelta] = None
    timeout: Optional[timedelta] = None
    source_timeouts: Dict[str, timedelta] = field(default_factory=dict)
    cron: Optional[str] = None
    lookback: Optional[timedelta] = None

    @classmethod
    def from_declarative(cls, rule: DeclarativeRule) -> "CompiledRule":
        if not rule.sources:
            raise ValueError(f"Rule {rule.rule_id} has no sources")

        entity_ids = {source.entity_id for source in rule.sources}
        if len(entity_ids) != 1:
            raise ValueError(
                f"Rule {rule.rule_id} must use the same entity_id filter for all sources"
            )

        trigger_type = rule.trigger.type
        if trigger_type not in _TRIGGER_TYPES:
            supported = ", ".join(sorted(_TRIGGER_TYPES))
            raise ValueError(
                f"Rule {rule.rule_id} has unsupported trigger type '{trigger_type}'; supported trigger types: {supported}"
            )
        _validate_trigger_fields(rule)
        duration = parse_duration(rule.trigger.duration)
        slide = parse_duration(rule.trigger.slide) or duration
        timeout = parse_duration(rule.trigger.timeout)
        lookback = parse_duration(rule.trigger.lookback) if trigger_type == "scheduled" else None
        if rule.trigger.cron:
            _parse_cron(rule.trigger.cron)

        aggregations = [
            Aggregation(
                agg_id=entry["id"],
                function=entry["function"],
                field=entry.get("field"),
                input=entry.get("input"),
                percentile=entry.get("percentile"),
                sub_window=parse_duration(entry.get("sub_window")),
            )
            for entry in rule.aggregations
        ]
        operands = [
            Operand(
                metric=operand.get("metric"),
                operator=operand.get("operator"),
                value=operand.get("value"),
                const=operand.get("const"),
            )
            for operand in rule.condition.operands
        ]
        _validate_condition(rule.rule_id, rule.condition.operator, operands)

        if trigger_type == "window":
            if duration is None:
                raise ValueError(f"Window rule {rule.rule_id} requires trigger.duration")
            if slide is None:
                slide = duration
            if slide > duration:
                raise ValueError(
                    f"Window rule {rule.rule_id} requires trigger.slide to be less than or equal to trigger.duration"
                )
        if trigger_type == "absence":
            if timeout is None and rule.sources[0].trigger is not None:
                timeout = parse_duration(rule.sources[0].trigger.timeout)
            if timeout is None:
                raise ValueError(f"Absence rule {rule.rule_id} requires a timeout")
        if trigger_type == "composite":
            source_timeouts: Dict[str, timedelta] = {}
            for source in rule.sources:
                source_timeout = parse_duration(
                    source.trigger.timeout if source.trigger else None
                )
                if source_timeout is None:
                    raise ValueError(
                        f"Composite rule {rule.rule_id} requires per-source absence timeouts"
                    )
                source_timeouts[source.sensor_type] = source_timeout
        else:
            source_timeouts = {}
        if trigger_type == "scheduled" and not rule.trigger.cron:
            raise ValueError(f"Scheduled rule {rule.rule_id} requires trigger.cron")

        return cls(
            rule_id=rule.rule_id,
            description=rule.description,
            trigger_type=trigger_type,
            entity_id_filter=next(iter(entity_ids)),
            sensor_types=[source.sensor_type for source in rule.sources],
            actions=rule.actions,
            condition_operator=rule.condition.operator,
            operands=operands,
            aggregations=aggregations,
            duration=duration,
            slide=slide,
            timeout=timeout,
            source_timeouts=source_timeouts,
            cron=rule.trigger.cron,
            lookback=lookback,
        )

    def matches_event(self, event: SensorEvent) -> bool:
        if self.entity_id_filter != "*" and self.entity_id_filter != event.entity_id:
            return False
        return event.sensor_type in self.sensor_types

    def applies_to_entity(self, entity_id: str) -> bool:
        return self.entity_id_filter in {"*", entity_id}


@dataclass
class RuleState:
    buffered_events: List[SensorEvent] = field(default_factory=list)
    last_seen: Dict[str, datetime] = field(default_factory=dict)
    absence_fired: bool = False
    source_absent: Dict[str, bool] = field(default_factory=dict)
    composite_active: bool = False
    next_window_end: Optional[datetime] = None
    next_schedule_fire: Optional[datetime] = None
    last_schedule_fire: Optional[datetime] = None


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


def _flatten_context(data: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    flattened: Dict[str, Any] = {}
    for key, value in data.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flattened.update(_flatten_context(value, full_key))
        else:
            flattened[full_key] = value
    return flattened


def _render_template(template: str, variables: Dict[str, Any]) -> str:
    flattened = _flatten_context(variables)

    def replacer(match: re.Match[str]) -> str:
        key = match.group(1)
        value = flattened.get(key)
        return str(value) if value is not None else match.group(0)

    return _TEMPLATE_RE.sub(replacer, template)


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_cron(cron: str) -> tuple[int, int]:
    parts = cron.split()
    if len(parts) != 5:
        raise ValueError(f"Unsupported cron expression: {cron}")
    minute, hour, day_of_month, month, day_of_week = parts
    if day_of_month != "*" or month != "*" or day_of_week != "*":
        raise ValueError(f"Unsupported cron expression: {cron}")
    if not minute.isdigit() or not hour.isdigit():
        raise ValueError(f"Unsupported cron expression: {cron}")
    minute_value = int(minute)
    hour_value = int(hour)
    if minute_value not in range(0, 60) or hour_value not in range(0, 24):
        raise ValueError(f"Unsupported cron expression: {cron}")
    return hour_value, minute_value


def _validate_trigger_fields(rule: DeclarativeRule) -> None:
    trigger_type = rule.trigger.type
    disallowed_by_trigger = {
        "event": {
            "duration": rule.trigger.duration,
            "slide": rule.trigger.slide,
            "timeout": rule.trigger.timeout,
            "cron": rule.trigger.cron,
            "lookback": rule.trigger.lookback,
        },
        "window": {
            "timeout": rule.trigger.timeout,
            "cron": rule.trigger.cron,
            "lookback": rule.trigger.lookback,
        },
        "absence": {
            "duration": rule.trigger.duration,
            "slide": rule.trigger.slide,
            "cron": rule.trigger.cron,
            "lookback": rule.trigger.lookback,
        },
        "composite": {
            "duration": rule.trigger.duration,
            "slide": rule.trigger.slide,
            "timeout": rule.trigger.timeout,
            "cron": rule.trigger.cron,
            "lookback": rule.trigger.lookback,
        },
        "scheduled": {
            "duration": rule.trigger.duration,
            "slide": rule.trigger.slide,
            "timeout": rule.trigger.timeout,
        },
    }
    invalid_fields = [
        field_name
        for field_name, field_value in disallowed_by_trigger.get(trigger_type, {}).items()
        if field_value is not None
    ]
    if invalid_fields:
        field_list = ", ".join(sorted(invalid_fields))
        raise ValueError(
            f"Rule {rule.rule_id} trigger type '{trigger_type}' does not support fields: {field_list}"
        )


def _validate_condition(
    rule_id: str,
    operator: Optional[str],
    operands: List[Operand],
) -> None:
    if operator is not None and operator not in _CONDITION_OPERATORS:
        supported = ", ".join(sorted(_CONDITION_OPERATORS))
        raise ValueError(
            f"Rule {rule_id} has unsupported condition operator '{operator}'; supported condition operators: {supported}"
        )
    for index, operand in enumerate(operands, start=1):
        if operand.const is not None:
            continue
        if operand.metric is None or operand.operator is None:
            raise ValueError(
                f"Rule {rule_id} operand {index} requires metric and operator"
            )
        if operand.operator not in _COMPARISON_OPERATORS:
            supported = ", ".join(sorted(_COMPARISON_OPERATORS))
            raise ValueError(
                f"Rule {rule_id} operand {index} has unsupported operator '{operand.operator}'; supported operators: {supported}"
            )


def _next_cron_fire(after: datetime, cron: str) -> datetime:
    hour, minute = _parse_cron(cron)
    candidate = after.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= after:
        candidate += timedelta(days=1)
    return candidate


def _align_window_end(ts: datetime, slide: timedelta) -> datetime:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    seconds = (ts - epoch).total_seconds() / slide.total_seconds()
    aligned = epoch + ceil(seconds) * slide
    return aligned if aligned > ts else aligned + slide


def _percentile(values: List[float], percentile: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (percentile / 100.0) * (len(ordered) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _chunk_values(
    events: List[SensorEvent], field: str, start: datetime, end: datetime, step: timedelta
) -> List[List[float]]:
    buckets: List[List[float]] = []
    bucket_start = start
    while bucket_start < end:
        bucket_end = min(bucket_start + step, end)
        buckets.append(
            [
                float(getattr(event, field))
                for event in events
                if bucket_start <= event.timestamp <= bucket_end
            ]
        )
        bucket_start = bucket_end
    return buckets


def _evaluate_aggregation(
    aggregation: Aggregation,
    window: EntityWindow,
    outputs: Dict[str, Any],
) -> Any:
    if aggregation.field is not None:
        source_values = [float(getattr(event, aggregation.field)) for event in window.events]
    elif aggregation.input is not None:
        source_values = outputs[aggregation.input]
    else:
        raise ValueError(f"Aggregation {aggregation.agg_id} requires field or input")

    if aggregation.sub_window is not None and aggregation.field is not None:
        source_values = [
            bucket
            for bucket in _chunk_values(
                window.events,
                aggregation.field,
                window.start,
                window.end,
                aggregation.sub_window,
            )
            if bucket
        ]

    function = aggregation.function
    if function == "count":
        if aggregation.sub_window is not None:
            return [len(bucket) for bucket in source_values]
        return len(source_values)
    if function == "sum":
        return [sum(bucket) for bucket in source_values] if aggregation.sub_window else sum(source_values)
    if function == "mean":
        if aggregation.sub_window is not None:
            return [mean(bucket) for bucket in source_values]
        return mean(source_values) if source_values else None
    if function == "min":
        if aggregation.sub_window is not None:
            return [min(bucket) for bucket in source_values]
        return min(source_values) if source_values else None
    if function == "max":
        if aggregation.sub_window is not None:
            return [max(bucket) for bucket in source_values]
        return max(source_values) if source_values else None
    if function == "stddev":
        if aggregation.sub_window is not None:
            return [pstdev(bucket) if len(bucket) > 1 else 0.0 for bucket in source_values]
        if not source_values:
            return None
        return pstdev(source_values) if len(source_values) > 1 else 0.0
    if function == "delta":
        if not source_values:
            return None
        if aggregation.sub_window is not None:
            return [bucket[-1] - bucket[0] for bucket in source_values]
        return source_values[-1] - source_values[0]
    if function == "rate":
        delta = _evaluate_aggregation(
            Aggregation(aggregation.agg_id, "delta", field=aggregation.field, input=aggregation.input),
            window,
            outputs,
        )
        if delta is None:
            return None
        duration_seconds = window.duration.total_seconds()
        if aggregation.sub_window is not None:
            return [value / aggregation.sub_window.total_seconds() for value in delta]
        return delta / duration_seconds if duration_seconds else None
    if function == "percentile":
        if not source_values:
            return None
        percentile = aggregation.percentile if aggregation.percentile is not None else 95.0
        if aggregation.sub_window is not None:
            return [_percentile(bucket, percentile) for bucket in source_values]
        return _percentile(source_values, percentile)
    raise ValueError(f"Unsupported aggregation function: {function}")


def _compare(left: Any, operator: str, right: Any) -> bool:
    if operator == "eq":
        return left == right
    if operator == "ne":
        return left != right
    if operator == "gt":
        return left > right
    if operator == "gte":
        return left >= right
    if operator == "lt":
        return left < right
    if operator == "lte":
        return left <= right
    raise ValueError(f"Unsupported operator: {operator}")


def _evaluate_operands(operator: Optional[str], operands: List[Operand], values: Dict[str, Any]) -> bool:
    if not operands:
        return False
    results: List[bool] = []
    for operand in operands:
        if operand.const is not None:
            results.append(operand.const)
            continue
        if operand.metric is None or operand.operator is None:
            raise ValueError("Operand requires metric and operator")
        left = values.get(operand.metric)
        results.append(_compare(left, operand.operator, operand.value))
    if operator == "OR":
        return any(results)
    return all(results)


class CompiledEngine:
    def __init__(self, rules: Iterable[CompiledRule]):
        self.rules = list(rules)
        self._rule_map = {rule.rule_id: rule for rule in self.rules}
        self._entities: Dict[str, Dict[str, RuleState]] = {}
        self._watermark: Optional[datetime] = None
        self.sink_registry = SinkRegistry()

    def replay(
        self, events: Iterable[SensorEvent], until: Optional[datetime] = None
    ) -> List[EmittedAlert]:
        emitted: List[EmittedAlert] = []
        ordered_events = sorted(events, key=lambda event: event.timestamp)
        for event in ordered_events:
            emitted.extend(self.process_event(event))
        if until is not None:
            emitted.extend(self.advance_to(until))
        return emitted

    def replay_with_report(
        self, events: Iterable[SensorEvent], until: Optional[datetime] = None
    ) -> tuple[List[EmittedAlert], ReplayDeliveryReport]:
        self.sink_registry.reset_metrics()
        self.sink_registry.clear_delivery_log()
        alerts = self.replay(events, until=until)
        return alerts, self.delivery_report(alerts)

    def delivery_report(self, alerts: List[EmittedAlert]) -> ReplayDeliveryReport:
        return ReplayDeliveryReport(
            alert_count=len(alerts),
            delivery_metrics=self.sink_registry.metrics(),
            delivery_log=self.sink_registry.delivery_log(),
        )

    def process_event(self, event: SensorEvent) -> List[EmittedAlert]:
        timestamp = event.timestamp
        emitted = self.advance_to(timestamp)
        self._register_entity(event.entity_id)
        entity_states = self._entities[event.entity_id]

        for rule in self.rules:
            if not rule.applies_to_entity(event.entity_id):
                continue
            state = entity_states[rule.rule_id]
            self._prune_buffer(rule, state, timestamp)
            if rule.trigger_type in {"window", "scheduled"}:
                state.buffered_events.append(event)
            if rule.matches_event(event):
                state.last_seen[event.sensor_type] = timestamp
                if rule.trigger_type == "event":
                    emitted.extend(self._evaluate_event_rule(rule, event))
                elif rule.trigger_type == "absence":
                    state.absence_fired = False
                elif rule.trigger_type == "composite":
                    state.source_absent[event.sensor_type] = False
                    state.composite_active = self._composite_condition_active(rule, state)

        self._watermark = timestamp
        return emitted

    def advance_to(self, target: datetime) -> List[EmittedAlert]:
        target = _normalize_datetime(target)
        emitted: List[EmittedAlert] = []
        if self._watermark is None:
            self._watermark = target
            return emitted

        while True:
            next_due = self._next_due_time()
            if next_due is None or next_due > target:
                break
            self._watermark = next_due
            emitted.extend(self._fire_due_timers(next_due))
        self._watermark = target
        return emitted

    def _register_entity(self, entity_id: str) -> None:
        if entity_id in self._entities:
            return
        states: Dict[str, RuleState] = {}
        for rule in self.rules:
            if not rule.applies_to_entity(entity_id):
                continue
            state = RuleState()
            if rule.trigger_type == "composite":
                state.source_absent = {sensor_type: False for sensor_type in rule.sensor_types}
            if rule.trigger_type == "window" and rule.slide is not None:
                state.next_window_end = None
            if rule.trigger_type == "scheduled" and rule.cron is not None:
                now = self._watermark or datetime.now(UTC)
                state.next_schedule_fire = _next_cron_fire(now, rule.cron)
            states[rule.rule_id] = state
        self._entities[entity_id] = states

    def _next_due_time(self) -> Optional[datetime]:
        due_times: List[datetime] = []
        for entity_states in self._entities.values():
            for rule_id, state in entity_states.items():
                rule = self._rule_map[rule_id]
                if rule.trigger_type == "absence":
                    last_seen = state.last_seen.get(rule.sensor_types[0])
                    if last_seen is not None and not state.absence_fired and rule.timeout is not None:
                        due_times.append(last_seen + rule.timeout)
                elif rule.trigger_type == "composite":
                    for sensor_type, timeout in rule.source_timeouts.items():
                        last_seen = state.last_seen.get(sensor_type)
                        if last_seen is not None and not state.source_absent.get(sensor_type, False):
                            due_times.append(last_seen + timeout)
                elif rule.trigger_type == "window":
                    if state.next_window_end is not None:
                        due_times.append(state.next_window_end)
                elif rule.trigger_type == "scheduled":
                    if state.next_schedule_fire is not None:
                        due_times.append(state.next_schedule_fire)
        return min(due_times, default=None)

    def _fire_due_timers(self, fire_time: datetime) -> List[EmittedAlert]:
        emitted: List[EmittedAlert] = []
        for entity_id, entity_states in self._entities.items():
            for rule_id, state in entity_states.items():
                rule = self._rule_map[rule_id]
                if rule.trigger_type == "absence":
                    last_seen = state.last_seen.get(rule.sensor_types[0])
                    if (
                        last_seen is not None
                        and not state.absence_fired
                        and rule.timeout is not None
                        and last_seen + rule.timeout == fire_time
                    ):
                        state.absence_fired = True
                        emitted.extend(self._emit_absence(rule, entity_id, state, fire_time))
                elif rule.trigger_type == "composite":
                    changed = False
                    for sensor_type, timeout in rule.source_timeouts.items():
                        last_seen = state.last_seen.get(sensor_type)
                        if (
                            last_seen is not None
                            and not state.source_absent.get(sensor_type, False)
                            and last_seen + timeout == fire_time
                        ):
                            state.source_absent[sensor_type] = True
                            changed = True
                    if changed:
                        active = self._composite_condition_active(rule, state)
                        if active and not state.composite_active:
                            state.composite_active = True
                            emitted.extend(
                                self._emit_composite(rule, entity_id, state, fire_time)
                            )
                elif rule.trigger_type == "window":
                    if state.next_window_end == fire_time:
                        emitted.extend(self._emit_window(rule, entity_id, state, fire_time))
                        state.next_window_end = fire_time + (rule.slide or timedelta(0))
                elif rule.trigger_type == "scheduled":
                    if state.next_schedule_fire == fire_time:
                        emitted.extend(
                            self._emit_scheduled(rule, entity_id, state, fire_time)
                        )
                        state.last_schedule_fire = fire_time
                        state.next_schedule_fire = _next_cron_fire(fire_time, rule.cron or "")
        return emitted

    def _evaluate_event_rule(self, rule: CompiledRule, event: SensorEvent) -> List[EmittedAlert]:
        values = {
            "entity_id": event.entity_id,
            "sensor_type": event.sensor_type,
            "value": event.value,
            "timestamp_ms": event.timestamp_ms,
            "rule_id": rule.rule_id,
        }
        if not _evaluate_operands(rule.condition_operator, rule.operands, values):
            return []
        context = RuleContext(
            entity_id=event.entity_id,
            rule_id=rule.rule_id,
            timestamp=event.timestamp,
        )
        return self._build_alerts(rule, context, values)

    def _emit_absence(
        self, rule: CompiledRule, entity_id: str, state: RuleState, fire_time: datetime
    ) -> List[EmittedAlert]:
        sensor_type = rule.sensor_types[0]
        last_seen = state.last_seen.get(sensor_type)
        duration = fire_time - last_seen if last_seen is not None else rule.timeout or timedelta(0)
        values = {
            "entity_id": entity_id,
            "rule_id": rule.rule_id,
            "sensor_type": sensor_type,
            "timestamp": fire_time.isoformat(),
            "duration": str(duration),
            "last_seen_ts": last_seen.isoformat() if last_seen is not None else None,
        }
        context = RuleContext(
            entity_id=entity_id,
            rule_id=rule.rule_id,
            timestamp=fire_time,
            duration=duration,
        )
        return self._build_alerts(rule, context, values)

    def _emit_composite(
        self, rule: CompiledRule, entity_id: str, state: RuleState, fire_time: datetime
    ) -> List[EmittedAlert]:
        values: Dict[str, Any] = {
            "entity_id": entity_id,
            "rule_id": rule.rule_id,
            "timestamp": fire_time.isoformat(),
        }
        for sensor_type in rule.sensor_types:
            last_seen = state.last_seen.get(sensor_type)
            duration = fire_time - last_seen if last_seen is not None else None
            values[sensor_type] = {
                "last_seen": last_seen.isoformat() if last_seen is not None else None,
                "duration": str(duration) if duration is not None else None,
                "absent": state.source_absent.get(sensor_type, False),
            }
        context = RuleContext(
            entity_id=entity_id,
            rule_id=rule.rule_id,
            timestamp=fire_time,
        )
        return self._build_alerts(rule, context, values)

    def _emit_window(
        self, rule: CompiledRule, entity_id: str, state: RuleState, fire_time: datetime
    ) -> List[EmittedAlert]:
        duration = rule.duration or timedelta(0)
        start = fire_time - duration
        events = [
            event
            for event in state.buffered_events
            if start <= event.timestamp <= fire_time and event.sensor_type in rule.sensor_types
        ]
        window = EntityWindow(entity_id=entity_id, start=start, end=fire_time, events=events)
        values = self._window_values(rule, window)
        if not _evaluate_operands(rule.condition_operator, rule.operands, values):
            return []
        context = RuleContext(
            entity_id=entity_id,
            rule_id=rule.rule_id,
            timestamp=fire_time,
            duration=duration,
        )
        return self._build_alerts(rule, context, values)

    def _emit_scheduled(
        self, rule: CompiledRule, entity_id: str, state: RuleState, fire_time: datetime
    ) -> List[EmittedAlert]:
        if rule.lookback is not None:
            start = fire_time - rule.lookback
        else:
            start = state.last_schedule_fire or min(
                (event.timestamp for event in state.buffered_events),
                default=fire_time,
            )
        events = [
            event
            for event in state.buffered_events
            if start <= event.timestamp <= fire_time and event.sensor_type in rule.sensor_types
        ]
        window = EntityWindow(entity_id=entity_id, start=start, end=fire_time, events=events)
        values = self._window_values(rule, window)
        if not _evaluate_operands(rule.condition_operator, rule.operands, values):
            return []
        context = RuleContext(
            entity_id=entity_id,
            rule_id=rule.rule_id,
            timestamp=fire_time,
            duration=fire_time - start,
        )
        return self._build_alerts(rule, context, values)

    def _window_values(self, rule: CompiledRule, window: EntityWindow) -> Dict[str, Any]:
        outputs: Dict[str, Any] = {}
        for aggregation in rule.aggregations:
            outputs[aggregation.agg_id] = _evaluate_aggregation(aggregation, window, outputs)
        return {
            "entity_id": window.entity_id,
            "rule_id": rule.rule_id,
            "window_start": window.start.isoformat(),
            "window_end": window.end.isoformat(),
            "timestamp": window.end.isoformat(),
            **outputs,
        }

    def _build_alerts(
        self, rule: CompiledRule, context: RuleContext, variables: Dict[str, Any]
    ) -> List[EmittedAlert]:
        emitted: List[EmittedAlert] = []
        merged = {
            **variables,
            "entity_id": context.entity_id,
            "rule_id": context.rule_id,
            "timestamp": context.timestamp.isoformat(),
            "duration": str(context.duration),
        }
        for action in rule.actions:
            emitted.append(
                EmittedAlert(
                    entity_id=context.entity_id,
                    rule_id=rule.rule_id,
                    timestamp=context.timestamp,
                    alert=Alert(
                        severity=action.severity,
                        message=_render_template(action.message, merged),
                        metadata={
                            "rule_id": rule.rule_id,
                            "entity_id": context.entity_id,
                            "sinks": action.sinks,
                            "variables": merged,
                        },
                    ),
                )
            )
            emitted[-1].delivery_results.extend(
                self._deliver_action_sinks(action, emitted[-1])
            )
        return emitted

    def _deliver_action_sinks(
        self, action: Action, emitted_alert: EmittedAlert
    ) -> List[DeliveryResult]:
        results: List[DeliveryResult] = []
        for sink in action.sinks:
            sink_type = sink.get("type")
            if not sink_type:
                results.append(
                    DeliveryResult(
                        sink_type="unknown",
                        status="failed",
                        detail="Sink config is missing required field 'type'",
                    )
                )
                continue
            results.append(
                self.sink_registry.deliver(
                    DeliveryRequest(
                        sink_type=sink_type,
                        rule_id=emitted_alert.rule_id,
                        entity_id=emitted_alert.entity_id,
                        severity=emitted_alert.alert.severity,
                        message=emitted_alert.alert.message,
                        timestamp=emitted_alert.timestamp,
                        payload=emitted_alert.alert.metadata,
                        config=sink,
                    )
                )
            )
        return results

    def _composite_condition_active(self, rule: CompiledRule, state: RuleState) -> bool:
        values = [state.source_absent.get(sensor_type, False) for sensor_type in rule.sensor_types]
        if rule.condition_operator == "OR":
            return any(values)
        return all(values)

    def _prune_buffer(self, rule: CompiledRule, state: RuleState, now: datetime) -> None:
        lookbacks = [value for value in [rule.duration, rule.lookback] if value is not None]
        if not lookbacks:
            return
        cutoff = now - max(lookbacks)
        state.buffered_events = [event for event in state.buffered_events if event.timestamp >= cutoff]
        if rule.trigger_type == "window" and state.next_window_end is None and rule.slide is not None:
            state.next_window_end = _align_window_end(now, rule.slide)


class DeclarativeEngine(CompiledEngine):
    def __init__(self, rules: Iterable[DeclarativeRule]):
        super().__init__([CompiledRule.from_declarative(rule) for rule in rules])
