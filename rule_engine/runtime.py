from __future__ import annotations

import json
import re
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from math import ceil
from statistics import mean, pstdev
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .declarative import Action, DeclarativeRule
from .models import (
    RELOAD_POLICIES,
    EmittedAlert,
    EngineConfig,
    EngineSnapshot,
    EvaluationResult,
    LateEventMetrics,
    ReloadReport,
    ReplayDeliveryReport,
    RuleMetadata,
    RuleReloadOutcome,
)
from .sinks import (
    DeliveryRequest,
    DeliveryResult,
    SinkRegistry,
)
from .types import Alert, RuleContext, SensorEvent
from .window import EntityWindow

_DURATION_RE = re.compile(r"^(?P<value>\d+)(?P<unit>[smhd])$")
_TEMPLATE_RE = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")
_LATE_EVENT_POLICIES = {"reject", "drop"}
_TRIGGER_TYPES = {"event", "window", "absence", "composite", "scheduled"}
_CONDITION_OPERATORS = {"AND", "OR"}
_COMPARISON_OPERATORS = {"eq", "ne", "gt", "gte", "lt", "lte"}
NumericSeries = List[float]
BucketedNumericSeries = List[NumericSeries]


def parse_duration(value: Optional[str], allow_zero: bool = False) -> Optional[timedelta]:
    """Parse a duration literal such as ``30s`` or ``48h``.

    Window and timeout durations must be positive, so zero is rejected by
    default. Tolerances such as ``allowed_lateness`` pass ``allow_zero=True``,
    where ``0s`` is a meaningful value rather than a mistake.
    """
    if value is None:
        return None
    match = _DURATION_RE.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"Unsupported duration: {value}")
    amount = int(match.group("value"))
    if amount < 0 or (amount == 0 and not allow_zero):
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
    allowed_lateness: timedelta = timedelta(0)
    cooldown: Optional[timedelta] = None
    repeat_every: Optional[timedelta] = None
    resolve: bool = False

    @property
    def has_lifecycle(self) -> bool:
        """Whether this rule tracks alert episodes.

        Rules without an emit block keep the original behaviour of emitting
        on every qualifying evaluation, with no episode state at all.
        """
        return self.cooldown is not None or self.repeat_every is not None or self.resolve

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
                f"Rule {rule.rule_id} has unsupported trigger type "
                f"'{trigger_type}'; supported trigger types: {supported}"
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
                    f"Window rule {rule.rule_id} requires trigger.slide "
                    "to be less than or equal to trigger.duration"
                )
        if trigger_type == "absence":
            if timeout is None and rule.sources[0].trigger is not None:
                timeout = parse_duration(rule.sources[0].trigger.timeout)
            if timeout is None:
                raise ValueError(f"Absence rule {rule.rule_id} requires a timeout")
        if trigger_type == "composite":
            source_timeouts: Dict[str, timedelta] = {}
            for source in rule.sources:
                source_timeout = parse_duration(source.trigger.timeout if source.trigger else None)
                if source_timeout is None:
                    raise ValueError(
                        f"Composite rule {rule.rule_id} requires per-source absence timeouts"
                    )
                source_timeouts[source.sensor_type] = source_timeout
        else:
            source_timeouts = {}
        if trigger_type == "scheduled" and not rule.trigger.cron:
            raise ValueError(f"Scheduled rule {rule.rule_id} requires trigger.cron")

        emit = rule.emit
        cooldown = parse_duration(emit.cooldown) if emit else None
        repeat_every = parse_duration(emit.repeat_every) if emit else None
        resolve = emit.resolve if emit else False

        allowed_lateness = parse_duration(rule.allowed_lateness, allow_zero=True) or timedelta(0)
        if allowed_lateness < timedelta(0):
            raise ValueError(f"Rule {rule.rule_id} requires a non-negative allowed_lateness")

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
            allowed_lateness=allowed_lateness,
            cooldown=cooldown,
            repeat_every=repeat_every,
            resolve=resolve,
        )

    def state_fingerprint(self) -> str:
        """Hash of the rule structure that gives stored state its meaning.

        Covers triggers, windows, timers and sources. Deliberately excludes
        message templates, severities, sinks and condition operands: those
        change what a rule emits, not what its retained state means, so
        editing them must not invalidate a snapshot.
        """
        structure = {
            "trigger_type": self.trigger_type,
            "entity_id_filter": self.entity_id_filter,
            "sensor_types": sorted(self.sensor_types),
            "duration": _seconds_or_none(self.duration),
            "slide": _seconds_or_none(self.slide),
            "timeout": _seconds_or_none(self.timeout),
            "source_timeouts": {
                sensor_type: timeout.total_seconds()
                for sensor_type, timeout in sorted(self.source_timeouts.items())
            },
            "cron": self.cron,
            "lookback": _seconds_or_none(self.lookback),
        }
        canonical = json.dumps(structure, sort_keys=True, separators=(",", ":"))
        return sha256(canonical.encode("utf-8")).hexdigest()[:16]

    def matches_event(self, event: SensorEvent) -> bool:
        if self.entity_id_filter != "*" and self.entity_id_filter != event.entity_id:
            return False
        return event.sensor_type in self.sensor_types

    def applies_to_entity(self, entity_id: str) -> bool:
        return self.entity_id_filter in {"*", entity_id}

    def metadata(self) -> "RuleMetadata":
        return RuleMetadata(
            rule_id=self.rule_id,
            description=self.description,
            trigger_type=self.trigger_type,
            entity_id_filter=self.entity_id_filter,
            sensor_types=list(self.sensor_types),
            sink_types=[
                sink["type"] for action in self.actions for sink in action.sinks if sink.get("type")
            ],
            aggregation_ids=[aggregation.agg_id for aggregation in self.aggregations],
        )


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
    episode_started: Optional[datetime] = None
    last_emit: Optional[datetime] = None
    next_repeat_fire: Optional[datetime] = None


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


def _serialize_rule_state(state: "RuleState") -> Dict[str, Any]:
    return {
        "buffered_events": [
            {
                "entity_id": event.entity_id,
                "sensor_type": event.sensor_type,
                "value": event.value,
                "timestamp_ms": event.timestamp_ms,
            }
            for event in state.buffered_events
        ],
        "last_seen": {
            sensor_type: moment.isoformat() for sensor_type, moment in state.last_seen.items()
        },
        "absence_fired": state.absence_fired,
        "source_absent": dict(state.source_absent),
        "composite_active": state.composite_active,
        "next_window_end": _iso_or_none(state.next_window_end),
        "next_schedule_fire": _iso_or_none(state.next_schedule_fire),
        "last_schedule_fire": _iso_or_none(state.last_schedule_fire),
        "episode_started": _iso_or_none(state.episode_started),
        "last_emit": _iso_or_none(state.last_emit),
        "next_repeat_fire": _iso_or_none(state.next_repeat_fire),
    }


def _deserialize_rule_state(payload: Dict[str, Any]) -> "RuleState":
    return RuleState(
        buffered_events=[
            SensorEvent(
                entity_id=entry["entity_id"],
                sensor_type=entry["sensor_type"],
                value=entry["value"],
                timestamp_ms=entry["timestamp_ms"],
            )
            for entry in payload.get("buffered_events", [])
        ],
        last_seen={
            sensor_type: _normalize_datetime(datetime.fromisoformat(moment))
            for sensor_type, moment in payload.get("last_seen", {}).items()
        },
        absence_fired=payload.get("absence_fired", False),
        source_absent=dict(payload.get("source_absent", {})),
        composite_active=payload.get("composite_active", False),
        next_window_end=_datetime_or_none(payload.get("next_window_end")),
        next_schedule_fire=_datetime_or_none(payload.get("next_schedule_fire")),
        last_schedule_fire=_datetime_or_none(payload.get("last_schedule_fire")),
        episode_started=_datetime_or_none(payload.get("episode_started")),
        last_emit=_datetime_or_none(payload.get("last_emit")),
        next_repeat_fire=_datetime_or_none(payload.get("next_repeat_fire")),
    )


def _seconds_or_none(value: Optional[timedelta]) -> Optional[float]:
    return value.total_seconds() if value is not None else None


def _iso_or_none(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _datetime_or_none(value: Optional[str]) -> Optional[datetime]:
    return _normalize_datetime(datetime.fromisoformat(value)) if value is not None else None


def _insert_event_in_order(events: List[SensorEvent], event: SensorEvent) -> None:
    """Insert into a timestamp-ordered buffer.

    delta and rate read values[-1] - values[0], so a late event appended at the
    end would silently corrupt them.
    """
    index = bisect_right([existing.timestamp for existing in events], event.timestamp)
    events.insert(index, event)


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
            f"Rule {rule.rule_id} trigger type '{trigger_type}' "
            f"does not support fields: {field_list}"
        )


def _validate_condition(
    rule_id: str,
    operator: Optional[str],
    operands: List[Operand],
) -> None:
    if operator is not None and operator not in _CONDITION_OPERATORS:
        supported = ", ".join(sorted(_CONDITION_OPERATORS))
        raise ValueError(
            f"Rule {rule_id} has unsupported condition operator "
            f"'{operator}'; supported condition operators: {supported}"
        )
    for index, operand in enumerate(operands, start=1):
        if operand.const is not None:
            continue
        if operand.metric is None or operand.operator is None:
            raise ValueError(f"Rule {rule_id} operand {index} requires metric and operator")
        if operand.operator not in _COMPARISON_OPERATORS:
            supported = ", ".join(sorted(_COMPARISON_OPERATORS))
            raise ValueError(
                f"Rule {rule_id} operand {index} has unsupported operator "
                f"'{operand.operator}'; supported operators: {supported}"
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
    function = aggregation.function

    if aggregation.sub_window is not None:
        if aggregation.field is None:
            raise ValueError(f"Aggregation {aggregation.agg_id} sub_window requires a source field")
        bucketed_values: BucketedNumericSeries = [
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
        if function == "count":
            return [len(bucket) for bucket in bucketed_values]
        if function == "sum":
            return [sum(bucket) for bucket in bucketed_values]
        if function == "mean":
            return [mean(bucket) for bucket in bucketed_values]
        if function == "min":
            return [min(bucket) for bucket in bucketed_values]
        if function == "max":
            return [max(bucket) for bucket in bucketed_values]
        if function == "stddev":
            return [pstdev(bucket) if len(bucket) > 1 else 0.0 for bucket in bucketed_values]
        if function == "delta":
            return [bucket[-1] - bucket[0] for bucket in bucketed_values]
        if function == "rate":
            duration_seconds = aggregation.sub_window.total_seconds()
            return [value / duration_seconds for value in _bucketed_delta(bucketed_values)]
        if function == "percentile":
            percentile = aggregation.percentile if aggregation.percentile is not None else 95.0
            return [_percentile(bucket, percentile) for bucket in bucketed_values]
        raise ValueError(f"Unsupported aggregation function: {function}")

    if aggregation.field is not None:
        source_values: NumericSeries = [
            float(getattr(event, aggregation.field)) for event in window.events
        ]
    elif aggregation.input is not None:
        source_values = [float(value) for value in outputs[aggregation.input]]
    else:
        raise ValueError(f"Aggregation {aggregation.agg_id} requires field or input")

    if function == "count":
        return len(source_values)
    if function == "sum":
        return sum(source_values)
    if function == "mean":
        return mean(source_values) if source_values else None
    if function == "min":
        return min(source_values) if source_values else None
    if function == "max":
        return max(source_values) if source_values else None
    if function == "stddev":
        if not source_values:
            return None
        return pstdev(source_values) if len(source_values) > 1 else 0.0
    if function == "delta":
        if not source_values:
            return None
        return source_values[-1] - source_values[0]
    if function == "rate":
        delta = _scalar_delta(source_values)
        if delta is None:
            return None
        duration_seconds = window.duration.total_seconds()
        return delta / duration_seconds if duration_seconds else None
    if function == "percentile":
        if not source_values:
            return None
        percentile = aggregation.percentile if aggregation.percentile is not None else 95.0
        return _percentile(source_values, percentile)
    raise ValueError(f"Unsupported aggregation function: {function}")


def _scalar_delta(values: NumericSeries) -> Optional[float]:
    if not values:
        return None
    return values[-1] - values[0]


def _bucketed_delta(values: BucketedNumericSeries) -> NumericSeries:
    return [bucket[-1] - bucket[0] for bucket in values]


def _compare(left: Any, operator: str, right: Any) -> bool:
    if operator == "eq":
        return bool(left == right)
    if operator == "ne":
        return bool(left != right)
    if operator == "gt":
        return bool(left > right)
    if operator == "gte":
        return bool(left >= right)
    if operator == "lt":
        return bool(left < right)
    if operator == "lte":
        return bool(left <= right)
    raise ValueError(f"Unsupported operator: {operator}")


def _evaluate_operands(
    operator: Optional[str], operands: List[Operand], values: Dict[str, Any]
) -> bool:
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
        if left is None:
            # An aggregation over an empty window yields None, and a rule can
            # reference a metric an event does not carry. Treat a missing value
            # as unsatisfied rather than comparing None, which raised TypeError.
            results.append(False)
            continue
        results.append(_compare(left, operand.operator, operand.value))
    if operator == "OR":
        return any(results)
    return all(results)


class CompiledEngine:
    def __init__(
        self,
        rules: Iterable[CompiledRule],
        config: Optional[EngineConfig] = None,
        sink_registry: Optional[SinkRegistry] = None,
    ):
        self.rules = list(rules)
        self.config = config or EngineConfig()
        self._rule_map = {rule.rule_id: rule for rule in self.rules}
        self._entities: Dict[str, Dict[str, RuleState]] = {}
        self._watermark: Optional[datetime] = (
            _normalize_datetime(self.config.initial_watermark)
            if self.config.initial_watermark is not None
            else None
        )
        self.sink_registry = sink_registry or SinkRegistry()
        if self.config.late_event_policy not in _LATE_EVENT_POLICIES:
            supported = ", ".join(sorted(_LATE_EVENT_POLICIES))
            raise ValueError(
                f"Unsupported late_event_policy '{self.config.late_event_policy}'; "
                f"supported policies: {supported}"
            )
        self._max_allowed_lateness = max(
            (rule.allowed_lateness for rule in self.rules), default=timedelta(0)
        )
        self._late_event_metrics = LateEventMetrics()
        # rule_id -> the previous definition, kept alive while entities drain
        self._draining_rules: Dict[str, CompiledRule] = {}
        self._draining_entities: Dict[str, Set[str]] = {}
        self._pending_reload: Optional[Tuple[List[CompiledRule], str, datetime]] = None
        self._last_reload_report: Optional[ReloadReport] = None

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

    def evaluate(
        self, events: Iterable[SensorEvent], until: Optional[datetime] = None
    ) -> EvaluationResult:
        alerts, delivery_report = self.replay_with_report(events, until=until)
        return EvaluationResult(alerts=alerts, delivery_report=delivery_report)

    def delivery_report(self, alerts: List[EmittedAlert]) -> ReplayDeliveryReport:
        return ReplayDeliveryReport(
            alert_count=len(alerts),
            delivery_metrics=self.sink_registry.metrics(),
            delivery_log=self.sink_registry.delivery_log(),
        )

    def rule_metadata(self) -> List[RuleMetadata]:
        return [rule.metadata() for rule in self.rules]

    @property
    def watermark(self) -> Optional[datetime]:
        """Current event-time watermark, or None before the first event."""
        return self._watermark

    def process_event(self, event: SensorEvent) -> List[EmittedAlert]:
        timestamp = event.timestamp
        if self._watermark is not None and timestamp < self._watermark:
            return self._handle_late_event(event, self._watermark - timestamp)
        emitted = self.advance_to(timestamp)
        self._register_entity(event.entity_id)
        entity_states = self._entities[event.entity_id]

        for rule in self.rules:
            if not rule.applies_to_entity(event.entity_id):
                continue
            rule = self._rule_for(event.entity_id, rule.rule_id)
            state = entity_states[rule.rule_id]
            self._prune_buffer(rule, state, timestamp)
            if rule.trigger_type in {"window", "scheduled"}:
                state.buffered_events.append(event)
            if rule.matches_event(event):
                state.last_seen[event.sensor_type] = timestamp
                if rule.trigger_type == "event":
                    emitted.extend(self._evaluate_event_rule(rule, event))
                elif rule.trigger_type == "absence":
                    # The source is reporting again, so any open episode is over.
                    state.absence_fired = False
                    emitted.extend(self._resolve_episode(rule, event.entity_id, timestamp))
                elif rule.trigger_type == "composite":
                    state.source_absent[event.sensor_type] = False
                    was_active = state.composite_active
                    state.composite_active = self._composite_condition_active(rule, state)
                    if was_active and not state.composite_active:
                        emitted.extend(self._resolve_episode(rule, event.entity_id, timestamp))

        self._watermark = timestamp
        return emitted

    def reload(
        self,
        rules: Iterable[CompiledRule],
        policy: str = "preserve",
        activate_at: Optional[datetime] = None,
    ) -> ReloadReport:
        """Swap the rule set without rebuilding the engine.

        ``policy`` decides what happens to retained state:

        - ``reset`` discards it,
        - ``preserve`` keeps it where the rule's structure is unchanged and
          discards it otherwise,
        - ``drain`` keeps the previous definition running for entities with an
          open alert episode until that episode resolves.

        With ``activate_at`` the swap is staged and applied once the watermark
        reaches that instant, so a change can be lined up ahead of time.
        """
        rules = list(rules)
        if policy not in RELOAD_POLICIES:
            supported = ", ".join(sorted(RELOAD_POLICIES))
            raise ValueError(f"Unsupported reload policy '{policy}'; supported: {supported}")

        if activate_at is not None:
            target = _normalize_datetime(activate_at)
            if self._watermark is None or target > self._watermark:
                self._pending_reload = (rules, policy, target)
                report = ReloadReport(applied=False, activate_at=target.isoformat())
                self._last_reload_report = report
                return report

        return self._apply_reload(rules, policy)

    def last_reload_report(self) -> Optional[ReloadReport]:
        """The most recent reload result, including one applied by a staged activation."""
        return self._last_reload_report

    def _activate_pending_reload(self) -> None:
        if self._pending_reload is None or self._watermark is None:
            return
        rules, policy, target = self._pending_reload
        if self._watermark < target:
            return
        self._pending_reload = None
        self._apply_reload(rules, policy)

    def _apply_reload(self, rules: List[CompiledRule], policy: str) -> ReloadReport:
        new_map = {rule.rule_id: rule for rule in rules}
        old_map = dict(self._rule_map)
        outcomes: List[RuleReloadOutcome] = []

        for rule_id in old_map:
            if rule_id not in new_map:
                self._drop_rule_state(rule_id)
                self._draining_rules.pop(rule_id, None)
                self._draining_entities.pop(rule_id, None)
                outcomes.append(RuleReloadOutcome(rule_id=rule_id, outcome="removed"))

        for rule in rules:
            previous = old_map.get(rule.rule_id)
            if previous is None:
                outcomes.append(RuleReloadOutcome(rule_id=rule.rule_id, outcome="added"))
                continue

            compatible = previous.state_fingerprint() == rule.state_fingerprint()

            if policy == "reset":
                self._drop_rule_state(rule.rule_id)
                outcomes.append(
                    RuleReloadOutcome(
                        rule_id=rule.rule_id, outcome="reset", compatible=compatible
                    )
                )
                continue

            open_episodes = sorted(
                entity_id
                for entity_id, states in self._entities.items()
                if rule.rule_id in states and states[rule.rule_id].episode_started is not None
            )

            if policy == "drain" and open_episodes:
                self._draining_rules[rule.rule_id] = previous
                self._draining_entities[rule.rule_id] = set(open_episodes)
                if not compatible:
                    self._drop_rule_state(rule.rule_id, exclude=set(open_episodes))
                outcomes.append(
                    RuleReloadOutcome(
                        rule_id=rule.rule_id,
                        outcome="draining",
                        compatible=compatible,
                        draining_entities=open_episodes,
                        detail="previous definition stays active until these episodes resolve",
                    )
                )
                continue

            if compatible:
                outcomes.append(
                    RuleReloadOutcome(rule_id=rule.rule_id, outcome="preserved", compatible=True)
                )
            else:
                self._drop_rule_state(rule.rule_id)
                outcomes.append(
                    RuleReloadOutcome(
                        rule_id=rule.rule_id,
                        outcome="reset",
                        compatible=False,
                        detail="state discarded: the rule's structure changed",
                    )
                )

        self.rules = rules
        self._rule_map = new_map
        self._max_allowed_lateness = max(
            (rule.allowed_lateness for rule in rules), default=timedelta(0)
        )
        self._reconcile_entity_states()

        report = ReloadReport(applied=True, outcomes=outcomes)
        self._last_reload_report = report
        return report

    def _drop_rule_state(self, rule_id: str, exclude: Optional[Set[str]] = None) -> None:
        for entity_id, states in self._entities.items():
            if exclude is not None and entity_id in exclude:
                continue
            states.pop(rule_id, None)

    def _reconcile_entity_states(self) -> None:
        """Give existing entities empty state for newly added rules, and forget removed ones."""
        for entity_id, states in self._entities.items():
            for rule in self.rules:
                if rule.applies_to_entity(entity_id) and rule.rule_id not in states:
                    states[rule.rule_id] = RuleState()
            for rule_id in list(states):
                if rule_id not in self._rule_map:
                    states.pop(rule_id)

    def _rule_for(self, entity_id: str, rule_id: str) -> CompiledRule:
        """The definition governing this entity, which may be a draining one."""
        draining = self._draining_rules.get(rule_id)
        if draining is not None and entity_id in self._draining_entities.get(rule_id, set()):
            return draining
        return self._rule_map[rule_id]

    def _rules_for_entity(self, entity_id: str) -> List[CompiledRule]:
        return [self._rule_for(entity_id, rule.rule_id) for rule in self.rules]

    def _finish_drain(self, rule_id: str, entity_id: str) -> None:
        entities = self._draining_entities.get(rule_id)
        if entities is None or entity_id not in entities:
            return
        entities.discard(entity_id)
        if not entities:
            self._draining_entities.pop(rule_id, None)
            self._draining_rules.pop(rule_id, None)

    def draining_rule_ids(self) -> List[str]:
        """Rules with at least one entity still running the previous definition."""
        return sorted(self._draining_rules)

    def snapshot(self) -> EngineSnapshot:
        """Capture watermark, per-entity rule state, and late-event counters."""
        return EngineSnapshot(
            watermark=_iso_or_none(self._watermark),
            entities={
                entity_id: {
                    rule_id: _serialize_rule_state(state) for rule_id, state in states.items()
                }
                for entity_id, states in self._entities.items()
            },
            rule_fingerprints={rule.rule_id: rule.state_fingerprint() for rule in self.rules},
            late_event_metrics=self._late_event_metrics.to_dict(),
        )

    @classmethod
    def restore(
        cls,
        snapshot: EngineSnapshot,
        rules: Iterable[CompiledRule],
        config: Optional[EngineConfig] = None,
        sink_registry: Optional[SinkRegistry] = None,
    ) -> "CompiledEngine":
        """Rebuild an engine from a snapshot.

        Rules present in both the snapshot and ``rules`` must have matching
        state fingerprints. Rules only in the snapshot are dropped (the rule was
        removed) and rules only in ``rules`` start empty (the rule was added).
        The snapshot watermark takes precedence over
        ``EngineConfig.initial_watermark``.
        """
        engine = cls(rules, config=config, sink_registry=sink_registry)
        engine._apply_snapshot(snapshot)
        return engine

    def _apply_snapshot(self, snapshot: EngineSnapshot) -> None:
        for rule in self.rules:
            recorded = snapshot.rule_fingerprints.get(rule.rule_id)
            if recorded is None:
                continue
            current = rule.state_fingerprint()
            if recorded != current:
                raise ValueError(
                    f"Rule {rule.rule_id!r} has changed shape since the snapshot was taken "
                    f"(fingerprint {recorded} -> {current}). Its windows, timers or sources "
                    "differ, so the retained state no longer means the same thing. Restore "
                    "against the original rule, or start a fresh engine for the new one."
                )

        self._watermark = _datetime_or_none(snapshot.watermark)
        self._entities = {}
        known_rule_ids = {rule.rule_id for rule in self.rules}
        for entity_id, states in snapshot.entities.items():
            self._register_entity(entity_id)
            for rule_id, payload in states.items():
                if rule_id not in known_rule_ids:
                    continue
                self._entities[entity_id][rule_id] = _deserialize_rule_state(payload)

        metrics = snapshot.late_event_metrics
        self._late_event_metrics = LateEventMetrics(
            total=metrics.get("total", 0),
            accepted=metrics.get("accepted", 0),
            dropped=metrics.get("dropped", 0),
            rejected=metrics.get("rejected", 0),
            per_rule_accepted=dict(metrics.get("per_rule_accepted", {})),
            per_rule_dropped=dict(metrics.get("per_rule_dropped", {})),
        )

    def late_event_metrics(self) -> LateEventMetrics:
        """Counts of events seen behind the watermark since the engine was built."""
        return self._late_event_metrics

    def _handle_late_event(self, event: SensorEvent, lateness: timedelta) -> List[EmittedAlert]:
        """Route an event that arrived behind the watermark.

        The watermark never moves backward here: timers have already fired up to
        it, so rewinding would produce inconsistent results. A tolerated late
        event is folded into rule state in place instead.
        """
        metrics = self._late_event_metrics
        metrics.total += 1

        if lateness > self._max_allowed_lateness:
            if self.config.late_event_policy == "reject":
                metrics.rejected += 1
                raise ValueError(
                    f"Event for entity {event.entity_id!r} at {event.timestamp.isoformat()} is "
                    f"{lateness} behind the watermark, which exceeds the largest declared "
                    f"allowed_lateness ({self._max_allowed_lateness}). Raise allowed_lateness "
                    "on the rule, set EngineConfig.late_event_policy='drop' to discard such "
                    "events, or use replay(), which sorts a batch for you."
                )
            metrics.dropped += 1
            return []

        self._register_entity(event.entity_id)
        entity_states = self._entities[event.entity_id]
        emitted: List[EmittedAlert] = []
        accepted_by_any = False

        for rule in self.rules:
            if not rule.applies_to_entity(event.entity_id):
                continue
            if lateness > rule.allowed_lateness:
                metrics.per_rule_dropped[rule.rule_id] = (
                    metrics.per_rule_dropped.get(rule.rule_id, 0) + 1
                )
                continue

            accepted_by_any = True
            metrics.per_rule_accepted[rule.rule_id] = (
                metrics.per_rule_accepted.get(rule.rule_id, 0) + 1
            )
            state = entity_states[rule.rule_id]
            if rule.trigger_type in {"window", "scheduled"}:
                _insert_event_in_order(state.buffered_events, event)
            if rule.matches_event(event):
                # last_seen tracks the newest observation, so a late event must
                # never drag it backward.
                previous = state.last_seen.get(event.sensor_type)
                if previous is None or event.timestamp > previous:
                    state.last_seen[event.sensor_type] = event.timestamp
                if rule.trigger_type == "event":
                    emitted.extend(self._evaluate_event_rule(rule, event))

        if accepted_by_any:
            metrics.accepted += 1
        else:
            metrics.dropped += 1
        return emitted

    def advance_to(self, target: datetime) -> List[EmittedAlert]:
        target = _normalize_datetime(target)
        emitted: List[EmittedAlert] = []
        if self._watermark is None:
            self._watermark = target
            return emitted
        if target < self._watermark:
            raise ValueError(
                f"Cannot move the watermark backward from {self._watermark.isoformat()} "
                f"to {target.isoformat()}. Timers have already fired up to the current "
                "watermark, so rewinding it would produce inconsistent results."
            )

        while True:
            next_due = self._next_due_time()
            if next_due is None or next_due > target:
                break
            self._watermark = next_due
            emitted.extend(self._fire_due_timers(next_due))
        self._watermark = target
        self._activate_pending_reload()
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
                now = self._resolve_schedule_start()
                state.next_schedule_fire = _next_cron_fire(now, rule.cron)
            states[rule.rule_id] = state
        self._entities[entity_id] = states

    def _resolve_schedule_start(self) -> datetime:
        if self._watermark is not None:
            return self._watermark
        if self.config.schedule_start is not None:
            return _normalize_datetime(self.config.schedule_start)
        raise ValueError(
            "Scheduled rules require EngineConfig.initial_watermark or "
            "EngineConfig.schedule_start before entity registration"
        )

    def _next_due_time(self) -> Optional[datetime]:
        due_times: List[datetime] = []
        for entity_id, entity_states in self._entities.items():
            for rule_id, state in entity_states.items():
                rule = self._rule_for(entity_id, rule_id)
                if rule.trigger_type == "absence":
                    last_seen = state.last_seen.get(rule.sensor_types[0])
                    if (
                        last_seen is not None
                        and not state.absence_fired
                        and rule.timeout is not None
                    ):
                        due_times.append(last_seen + rule.timeout)
                elif rule.trigger_type == "composite":
                    for sensor_type, timeout in rule.source_timeouts.items():
                        last_seen = state.last_seen.get(sensor_type)
                        if last_seen is not None and not state.source_absent.get(
                            sensor_type, False
                        ):
                            due_times.append(last_seen + timeout)
                elif rule.trigger_type == "window":
                    if state.next_window_end is not None:
                        due_times.append(state.next_window_end)
                elif rule.trigger_type == "scheduled":
                    if state.next_schedule_fire is not None:
                        due_times.append(state.next_schedule_fire)
                if state.next_repeat_fire is not None and state.episode_started is not None:
                    due_times.append(state.next_repeat_fire)
        return min(due_times, default=None)

    def _fire_due_timers(self, fire_time: datetime) -> List[EmittedAlert]:
        emitted: List[EmittedAlert] = []
        for entity_id, entity_states in self._entities.items():
            for rule_id, state in entity_states.items():
                rule = self._rule_for(entity_id, rule_id)
                if (
                    state.episode_started is not None
                    and state.next_repeat_fire is not None
                    and state.next_repeat_fire <= fire_time
                    and rule.repeat_every is not None
                ):
                    # A reminder for an episode that is still open. The episode
                    # is unchanged, so the correlation id stays the same.
                    state.last_emit = fire_time
                    state.next_repeat_fire = fire_time + rule.repeat_every
                    context = RuleContext(
                        entity_id=entity_id,
                        rule_id=rule.rule_id,
                        timestamp=fire_time,
                        duration=fire_time - state.episode_started,
                    )
                    emitted.extend(
                        self._build_alerts(
                            rule,
                            context,
                            {
                                "entity_id": entity_id,
                                "rule_id": rule.rule_id,
                                "episode_duration": str(fire_time - state.episode_started),
                            },
                            lifecycle="repeat",
                        )
                    )
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
                            emitted.extend(self._emit_composite(rule, entity_id, state, fire_time))
                elif rule.trigger_type == "window":
                    if state.next_window_end == fire_time:
                        emitted.extend(self._emit_window(rule, entity_id, state, fire_time))
                        state.next_window_end = fire_time + (rule.slide or timedelta(0))
                elif rule.trigger_type == "scheduled":
                    if state.next_schedule_fire == fire_time:
                        emitted.extend(self._emit_scheduled(rule, entity_id, state, fire_time))
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
            return self._resolve_episode(rule, event.entity_id, event.timestamp)
        label = self._lifecycle_label(rule, event.entity_id, event.timestamp)
        if label is None:
            return []
        context = RuleContext(
            entity_id=event.entity_id,
            rule_id=rule.rule_id,
            timestamp=event.timestamp,
        )
        return self._build_alerts(rule, context, values, lifecycle=label)

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
        label = self._lifecycle_label(rule, entity_id, fire_time)
        if label is None:
            return []
        context = RuleContext(
            entity_id=entity_id,
            rule_id=rule.rule_id,
            timestamp=fire_time,
            duration=duration,
        )
        return self._build_alerts(rule, context, values, lifecycle=label)

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
        label = self._lifecycle_label(rule, entity_id, fire_time)
        if label is None:
            return []
        context = RuleContext(
            entity_id=entity_id,
            rule_id=rule.rule_id,
            timestamp=fire_time,
        )
        return self._build_alerts(rule, context, values, lifecycle=label)

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
            return self._resolve_episode(rule, entity_id, fire_time)
        label = self._lifecycle_label(rule, entity_id, fire_time)
        if label is None:
            return []
        context = RuleContext(
            entity_id=entity_id,
            rule_id=rule.rule_id,
            timestamp=fire_time,
            duration=duration,
        )
        return self._build_alerts(rule, context, values, lifecycle=label)

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
            return self._resolve_episode(rule, entity_id, fire_time)
        label = self._lifecycle_label(rule, entity_id, fire_time)
        if label is None:
            return []
        context = RuleContext(
            entity_id=entity_id,
            rule_id=rule.rule_id,
            timestamp=fire_time,
            duration=fire_time - start,
        )
        return self._build_alerts(rule, context, values, lifecycle=label)

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

    def _correlation_id(self, rule: CompiledRule, entity_id: str) -> str:
        """Stable identifier for one alert episode.

        Repeats and the closing resolution share the firing alert's id, so a
        downstream consumer can correlate them. Rules without an emit block have
        no episodes, so each emission stands alone and is keyed by rule and
        entity only.
        """
        seed = f"{rule.rule_id}|{entity_id}"
        if rule.has_lifecycle:
            state = self._entities.get(entity_id, {}).get(rule.rule_id)
            started = state.episode_started if state is not None else None
            if started is not None:
                seed = f"{seed}|{started.isoformat()}"
        return sha256(seed.encode("utf-8")).hexdigest()[:16]

    def _lifecycle_label(
        self, rule: CompiledRule, entity_id: str, now: datetime
    ) -> Optional[str]:
        """Decide how this emission is labelled, or None to suppress it.

        Rules with no emit block bypass episode tracking entirely and always
        emit, preserving the original behaviour.
        """
        if not rule.has_lifecycle:
            return "firing"

        state = self._entities[entity_id][rule.rule_id]
        if state.episode_started is None:
            state.episode_started = now
            state.last_emit = now
            if rule.repeat_every is not None:
                state.next_repeat_fire = now + rule.repeat_every
            return "firing"

        gap = max(
            rule.cooldown or timedelta(0),
            rule.repeat_every or timedelta(0),
        )
        last_emit = state.last_emit or state.episode_started
        if gap > timedelta(0) and now - last_emit < gap:
            return None

        state.last_emit = now
        if rule.repeat_every is not None:
            state.next_repeat_fire = now + rule.repeat_every
        return "repeat"

    def _resolve_episode(
        self, rule: CompiledRule, entity_id: str, now: datetime
    ) -> List[EmittedAlert]:
        """Close an open episode and emit a resolution, if the rule asks for one."""
        if not rule.resolve:
            return []
        state = self._entities[entity_id][rule.rule_id]
        if state.episode_started is None:
            return []

        started = state.episode_started
        context = RuleContext(
            entity_id=entity_id,
            rule_id=rule.rule_id,
            timestamp=now,
            duration=now - started,
        )
        variables = {
            "entity_id": entity_id,
            "rule_id": rule.rule_id,
            "episode_duration": str(now - started),
        }
        alerts = self._build_alerts(rule, context, variables, lifecycle="resolved")
        self._close_episode(state)
        # The episode this entity was draining is over, so it can take the new
        # definition from here on.
        self._finish_drain(rule.rule_id, entity_id)
        return alerts

    @staticmethod
    def _close_episode(state: "RuleState") -> None:
        state.episode_started = None
        state.last_emit = None
        state.next_repeat_fire = None

    def _build_alerts(
        self,
        rule: CompiledRule,
        context: RuleContext,
        variables: Dict[str, Any],
        lifecycle: str = "firing",
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
                            "lifecycle": lifecycle,
                            "correlation_id": self._correlation_id(rule, context.entity_id),
                        },
                    ),
                )
            )
            emitted[-1].delivery_results.extend(self._deliver_action_sinks(action, emitted[-1]))
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
        state.buffered_events = [
            event for event in state.buffered_events if event.timestamp >= cutoff
        ]
        if (
            rule.trigger_type == "window"
            and state.next_window_end is None
            and rule.slide is not None
        ):
            state.next_window_end = _align_window_end(now, rule.slide)


class DeclarativeEngine(CompiledEngine):
    def __init__(
        self,
        rules: Iterable[DeclarativeRule],
        config: Optional[EngineConfig] = None,
        sink_registry: Optional[SinkRegistry] = None,
    ):
        super().__init__(
            [CompiledRule.from_declarative(rule) for rule in rules],
            config=config,
            sink_registry=sink_registry,
        )
