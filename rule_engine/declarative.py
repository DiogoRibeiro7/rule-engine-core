from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


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


def _load_actions(data: List[Dict[str, Any]]) -> List[Action]:
    return [
        Action(
            severity=action["severity"],
            message=action["message"],
            sinks=action.get("sinks", []),
        )
        for action in data
    ]


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
    trigger_data = _infer_trigger(document)
    return DeclarativeRule(
        rule_id=document["rule_id"],
        description=document.get("description", ""),
        trigger=_load_trigger(trigger_data),
        sources=_load_sources(document.get("sources") or document.get("source")),
        condition=_load_condition(document.get("condition")),
        actions=_load_actions(document.get("actions", [])),
        aggregations=document.get("aggregations", []),
    )


def load_rule_file(path: str) -> DeclarativeRule:
    with open(path, "r", encoding="utf-8") as handle:
        return load_rule_yaml(handle.read())
