from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


_SINK_TYPE_ALIASES = {
    "console": "stdout",
    "ndjson": "file",
    "sqs": "queue",
    "object-store": "object_storage",
}
_COMMON_SINK_FIELDS = {"type", "retry"}
_SINK_ALLOWED_FIELDS = {
    "stdout": set(),
    "file": {"path"},
    "webhook": {"url", "timeout_s", "headers", "method"},
    "queue": {"queue", "retryable"},
    "object_storage": {"bucket", "prefix", "extension", "retryable"},
}
_SINK_REQUIRED_FIELDS = {
    "stdout": set(),
    "file": {"path"},
    "webhook": {"url"},
    "queue": {"queue"},
    "object_storage": {"bucket"},
}
_RETRY_ALLOWED_FIELDS = {
    "max_attempts",
    "base_delay_s",
    "multiplier",
    "max_delay_s",
    "sleep",
}


@dataclass
class Trigger:
    type: str
    duration: Optional[str] = None
    slide: Optional[str] = None
    timeout: Optional[str] = None
    cron: Optional[str] = None
    lookback: Optional[str] = None


@dataclass
class Source:
    sensor_type: str
    entity_id: str
    trigger: Optional[Trigger] = None


@dataclass
class Condition:
    operator: Optional[str] = None
    metric: Optional[str] = None
    value: Optional[Any] = None
    operands: List[Any] = field(default_factory=list)


@dataclass
class Action:
    severity: str
    message: str
    sinks: List[Dict[str, Any]]


@dataclass
class DeclarativeRule:
    rule_id: str
    description: str
    trigger: Trigger
    sources: List[Source]
    condition: Condition
    actions: List[Action]
    aggregations: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def functional_primitive(self) -> str:
        if self.trigger.type == "event":
            return "@event_rule"
        if self.trigger.type in {"window", "absence", "composite"}:
            return "@window_rule"
        return "unknown"


def _validate_retry_config(rule_id: str, action_index: int, sink_type: str, value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(
            f"Rule {rule_id} action {action_index} sink '{sink_type}' field 'retry' must be an object"
        )
    unknown = set(value) - _RETRY_ALLOWED_FIELDS
    if unknown:
        unknown_list = ", ".join(sorted(unknown))
        raise ValueError(
            f"Rule {rule_id} action {action_index} sink '{sink_type}' has unsupported retry fields: {unknown_list}"
        )
    return value


def _normalize_sink_config(rule_id: str, action_index: int, sink: Any) -> Dict[str, Any]:
    if not isinstance(sink, dict):
        raise ValueError(f"Rule {rule_id} action {action_index} sink must be an object")

    raw_type = sink.get("type")
    if not raw_type:
        raise ValueError(f"Rule {rule_id} action {action_index} sink is missing required field 'type'")

    sink_type = _SINK_TYPE_ALIASES.get(str(raw_type), str(raw_type))
    if sink_type not in _SINK_ALLOWED_FIELDS:
        supported = ", ".join(sorted(_SINK_ALLOWED_FIELDS))
        raise ValueError(
            f"Rule {rule_id} action {action_index} sink type '{raw_type}' is unsupported; supported sink types: {supported}"
        )

    normalized = dict(sink)
    normalized["type"] = sink_type
    if sink_type == "queue" and "queue" not in normalized and "queue_url" in normalized:
        normalized["queue"] = normalized.pop("queue_url")

    allowed_fields = _COMMON_SINK_FIELDS | _SINK_ALLOWED_FIELDS[sink_type]
    unknown_fields = set(normalized) - allowed_fields
    if unknown_fields:
        unknown_list = ", ".join(sorted(unknown_fields))
        raise ValueError(
            f"Rule {rule_id} action {action_index} sink '{sink_type}' has unsupported fields: {unknown_list}"
        )

    missing_fields = [field for field in sorted(_SINK_REQUIRED_FIELDS[sink_type]) if not normalized.get(field)]
    if missing_fields:
        missing_list = ", ".join(missing_fields)
        raise ValueError(
            f"Rule {rule_id} action {action_index} sink '{sink_type}' is missing required fields: {missing_list}"
        )

    retry_config = normalized.get("retry")
    if retry_config is not None:
        normalized["retry"] = _validate_retry_config(rule_id, action_index, sink_type, retry_config)

    headers = normalized.get("headers")
    if headers is not None and not isinstance(headers, dict):
        raise ValueError(
            f"Rule {rule_id} action {action_index} sink '{sink_type}' field 'headers' must be an object"
        )

    return normalized


def _load_trigger(data: Dict[str, Any]) -> Trigger:
    return Trigger(
        type=data.get("type", ""),
        duration=data.get("duration"),
        slide=data.get("slide"),
        timeout=data.get("timeout"),
        cron=data.get("cron"),
        lookback=data.get("lookback"),
    )


def _load_sources(data: Any) -> List[Source]:
    if data is None:
        return []
    if isinstance(data, dict):
        data = [data]
    return [
        Source(
            sensor_type=source["sensor_type"],
            entity_id=source.get("entity_id", "*"),
            trigger=_load_trigger(source.get("trigger", {}))
            if source.get("trigger")
            else None,
        )
        for source in data
    ]


def _load_condition(data: Dict[str, Any]) -> Condition:
    if data is None:
        return Condition()
    return Condition(
        operator=data.get("operator"),
        metric=data.get("metric"),
        value=data.get("value"),
        operands=data.get("operands", []),
    )


def _load_actions(rule_id: str, data: List[Dict[str, Any]]) -> List[Action]:
    actions: List[Action] = []
    for index, action in enumerate(data, start=1):
        sinks = [_normalize_sink_config(rule_id, index, sink) for sink in action.get("sinks", [])]
        actions.append(
            Action(
                severity=action["severity"],
                message=action["message"],
                sinks=sinks,
            )
        )
    return actions


def _infer_trigger(document: Dict[str, Any]) -> Dict[str, Any]:
    trigger = document.get("trigger")
    if trigger is not None:
        return trigger

    sources = document.get("sources") or document.get("source")
    if sources is None:
        return {}
    if isinstance(sources, dict):
        sources = [sources]

    if len(sources) == 1 and sources[0].get("trigger") is not None:
        return sources[0]["trigger"]

    if len(sources) > 1 and all(
        source.get("trigger", {}).get("type") == "absence" for source in sources
    ):
        return {"type": "composite"}

    return {}


def load_rule_yaml(text: str) -> DeclarativeRule:
    document = yaml.safe_load(text)
    if not isinstance(document, dict):
        raise ValueError("Rule document must be a YAML object")
    trigger_data = _infer_trigger(document)
    return DeclarativeRule(
        rule_id=document["rule_id"],
        description=document.get("description", ""),
        trigger=_load_trigger(trigger_data),
        sources=_load_sources(document.get("sources") or document.get("source")),
        condition=_load_condition(document.get("condition")),
        actions=_load_actions(document["rule_id"], document.get("actions", [])),
        aggregations=document.get("aggregations", []),
    )


def load_rule_file(path: str) -> DeclarativeRule:
    with open(path, "r", encoding="utf-8") as handle:
        return load_rule_yaml(handle.read())
