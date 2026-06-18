from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .compiler import compile_rule, load_and_compile_rule_files
from .declarative import DeclarativeRule, get_rule_schema, load_rule_file
from .models import EmittedAlert, ReplayDeliveryReport
from .runtime import CompiledEngine, CompiledRule, DeclarativeEngine
from .types import SensorEvent


@dataclass
class RuntimeRule:
    rule_id: str
    description: str
    trigger_type: str
    entity_id_filter: str
    sensor_types: List[str]
    trigger: Dict[str, Any]
    condition: Dict[str, Any]
    actions: List[Dict[str, Any]]

    @classmethod
    def from_declarative(cls, rule: DeclarativeRule) -> "RuntimeRule":
        compiled = compile_rule(rule)
        return cls(
            rule_id=compiled.rule_id,
            description=compiled.description,
            trigger_type=compiled.trigger_type,
            entity_id_filter=compiled.entity_id_filter,
            sensor_types=compiled.sensor_types,
            trigger={
                "type": compiled.trigger_type,
                "duration": str(compiled.duration) if compiled.duration else None,
                "slide": str(compiled.slide) if compiled.slide else None,
                "timeout": str(compiled.timeout) if compiled.timeout else None,
                "cron": compiled.cron,
                "lookback": str(compiled.lookback) if compiled.lookback else None,
                "source_timeouts": {
                    sensor_type: str(timeout)
                    for sensor_type, timeout in compiled.source_timeouts.items()
                },
            },
            condition={
                "operator": compiled.condition_operator,
                "operands": [
                    {
                        "metric": operand.metric,
                        "operator": operand.operator,
                        "value": operand.value,
                        "const": operand.const,
                    }
                    for operand in compiled.operands
                ],
            },
            actions=[
                {
                    "severity": action.severity,
                    "message": action.message,
                    "sinks": action.sinks,
                }
                for action in compiled.actions
            ],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "description": self.description,
            "trigger_type": self.trigger_type,
            "entity_id_filter": self.entity_id_filter,
            "sensor_types": self.sensor_types,
            "trigger": self.trigger,
            "condition": self.condition,
            "actions": self.actions,
        }


def load_rule_documents(paths: Iterable[Path]) -> List[DeclarativeRule]:
    return [load_rule_file(str(path)) for path in paths]


def load_declarative_rules(paths: Iterable[Path]) -> List[RuntimeRule]:
    return [RuntimeRule.from_declarative(rule) for rule in load_rule_documents(paths)]


def load_compiled_rules(paths: Iterable[Path]) -> List[CompiledRule]:
    return load_and_compile_rule_files(paths)


def emit_json_model(rules: List[RuntimeRule]) -> str:
    return json.dumps([rule.to_dict() for rule in rules], indent=2)


def load_ndjson_events(path: Path) -> List[SensorEvent]:
    events: List[SensorEvent] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            events.append(
                SensorEvent(
                    entity_id=payload["entity_id"],
                    sensor_type=payload["sensor_type"],
                    value=float(payload["value"]),
                    timestamp_ms=int(payload["timestamp_ms"]),
                )
            )
    return events


def replay_events(
    rule_paths: List[Path], event_path: Path, until: Optional[datetime] = None
) -> List[EmittedAlert]:
    rules = load_compiled_rules(rule_paths)
    events = load_ndjson_events(event_path)
    engine = CompiledEngine(rules)
    return engine.replay(events, until=until)


def replay_events_with_report(
    rule_paths: List[Path], event_path: Path, until: Optional[datetime] = None
) -> tuple[List[EmittedAlert], ReplayDeliveryReport]:
    rules = load_compiled_rules(rule_paths)
    events = load_ndjson_events(event_path)
    engine = CompiledEngine(rules)
    return engine.replay_with_report(events, until=until)


def format_runtime_rule(rule: RuntimeRule) -> str:
    lines: List[str] = [
        f"rule_id: {rule.rule_id}",
        f"description: {rule.description}",
        f"trigger_type: {rule.trigger_type}",
        f"entity_id_filter: {rule.entity_id_filter}",
        f"sensor_types: {', '.join(rule.sensor_types)}",
        f"trigger: {rule.trigger}",
        f"condition: {rule.condition}",
        f"actions: {len(rule.actions)}",
    ]
    return "\n".join(lines)


def format_alert(alert: EmittedAlert) -> str:
    return (
        f"{alert.timestamp.isoformat()} entity={alert.entity_id} "
        f"rule={alert.rule_id} severity={alert.alert.severity} "
        f"message={alert.alert.message}"
    )


def _alert_to_dict(alert: EmittedAlert) -> Dict[str, Any]:
    return {
        "entity_id": alert.entity_id,
        "rule_id": alert.rule_id,
        "timestamp": alert.timestamp.isoformat(),
        "alert": {
            "severity": alert.alert.severity,
            "message": alert.alert.message,
            "metadata": alert.alert.metadata,
        },
        "delivery_results": [
            {
                "sink_type": result.sink_type,
                "status": result.status,
                "detail": result.detail,
                "retryable": result.retryable,
                "metadata": result.metadata,
            }
            for result in alert.delivery_results
        ],
    }


def _metrics_to_dict(metrics: Any) -> Dict[str, Any]:
    return {
        "total_requests": metrics.total_requests,
        "total_attempts": metrics.total_attempts,
        "delivered": metrics.delivered,
        "failed": metrics.failed,
        "unsupported": metrics.unsupported,
        "retryable_failures": metrics.retryable_failures,
        "retries_attempted": metrics.retries_attempted,
        "dead_letters": metrics.dead_letters,
        "total_latency_ms": metrics.total_latency_ms,
        "max_latency_ms": metrics.max_latency_ms,
        "average_latency_ms": metrics.average_latency_ms,
    }


def emit_replay_report_json(
    alerts: List[EmittedAlert],
    report: ReplayDeliveryReport,
) -> str:
    payload = {
        "alerts": [_alert_to_dict(alert) for alert in alerts],
        "delivery_report": {
            "alert_count": report.alert_count,
            "delivery_metrics": {
                "overall": _metrics_to_dict(report.delivery_metrics.overall),
                "by_sink": {
                    sink_type: _metrics_to_dict(metrics)
                    for sink_type, metrics in report.delivery_metrics.by_sink.items()
                },
            },
            "delivery_log": [
                {
                    "sink_type": entry.sink_type,
                    "rule_id": entry.rule_id,
                    "entity_id": entry.entity_id,
                    "severity": entry.severity,
                    "status": entry.status,
                    "detail": entry.detail,
                    "attempt_count": entry.attempt_count,
                    "retry_count": entry.retry_count,
                    "latency_ms": entry.latency_ms,
                    "dead_lettered": entry.dead_lettered,
                    "retryable": entry.retryable,
                    "metadata": entry.metadata,
                }
                for entry in report.delivery_log
            ],
        },
    }
    return json.dumps(payload, indent=2)


def generate_json_schema() -> Dict[str, Any]:
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "RuntimeRuleList",
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "rule_id": {"type": "string"},
                "description": {"type": "string"},
                "trigger_type": {"type": "string"},
                "entity_id_filter": {"type": "string"},
                "sensor_types": {"type": "array", "items": {"type": "string"}},
                "trigger": {"type": "object"},
                "condition": {"type": "object"},
                "actions": {"type": "array", "items": {"type": "object"}},
            },
            "required": [
                "rule_id",
                "description",
                "trigger_type",
                "entity_id_filter",
                "sensor_types",
                "trigger",
                "condition",
                "actions",
            ],
            "additionalProperties": False,
        },
    }


def generate_rule_json_schema() -> Dict[str, Any]:
    return get_rule_schema()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Load declarative rules and execute them against NDJSON events."
    )
    parser.add_argument("rules", nargs="*", help="Paths to declarative rule YAML files")
    parser.add_argument("--json", action="store_true", help="Emit the compiled runtime model as JSON")
    parser.add_argument("--schema", action="store_true", help="Emit the JSON schema for the runtime model")
    parser.add_argument("--rule-schema", action="store_true", help="Emit the declarative rule schema as JSON")
    parser.add_argument("--events", help="Path to an NDJSON file of sensor events")
    parser.add_argument(
        "--delivery-report-json",
        action="store_true",
        help="Emit replay alerts plus the delivery report as JSON",
    )
    parser.add_argument(
        "--until",
        help="Optional ISO-8601 UTC timestamp to advance timers after the final event",
    )
    args = parser.parse_args(argv)

    if args.schema:
        print(json.dumps(generate_json_schema(), indent=2))
        return 0
    if args.rule_schema:
        print(json.dumps(generate_rule_json_schema(), indent=2))
        return 0
    if not args.rules:
        parser.error("At least one rule path is required unless --schema or --rule-schema is set.")

    rule_paths = [Path(path) for path in args.rules]
    runtime_rules = load_declarative_rules(rule_paths)

    if args.json:
        print(emit_json_model(runtime_rules))
        return 0
    if args.events:
        until = datetime.fromisoformat(args.until) if args.until else None
        if args.delivery_report_json:
            alerts, report = replay_events_with_report(rule_paths, Path(args.events), until=until)
            print(emit_replay_report_json(alerts, report))
            return 0
        alerts = replay_events(rule_paths, Path(args.events), until=until)
        for alert in alerts:
            print(format_alert(alert))
        if not alerts:
            print("No alerts emitted.")
        return 0

    for rule in runtime_rules:
        print(format_runtime_rule(rule))
        print("---")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
