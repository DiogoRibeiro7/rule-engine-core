from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

from .compiler import compile_rule, compile_rules, load_and_compile_rule_files
from .declarative import DeclarativeRule, load_rule_file, load_rule_yaml
from .runtime import CompiledEngine, CompiledRule, EmittedAlert, EngineConfig, ReplayDeliveryReport
from .sinks import SinkRegistry
from .types import SensorEvent


@dataclass
class EmbeddedEngine:
    compiled_rules: List[CompiledRule]
    engine: CompiledEngine

    def replay(
        self,
        events: Iterable[SensorEvent],
        until: Optional[datetime] = None,
    ) -> List[EmittedAlert]:
        return self.engine.replay(events, until=until)

    def replay_with_report(
        self,
        events: Iterable[SensorEvent],
        until: Optional[datetime] = None,
    ) -> tuple[List[EmittedAlert], ReplayDeliveryReport]:
        return self.engine.replay_with_report(events, until=until)


def create_engine(
    compiled_rules: Iterable[CompiledRule],
    *,
    config: Optional[EngineConfig] = None,
    sink_registry: Optional[SinkRegistry] = None,
) -> EmbeddedEngine:
    compiled_rule_list = list(compiled_rules)
    return EmbeddedEngine(
        compiled_rules=compiled_rule_list,
        engine=CompiledEngine(
            compiled_rule_list,
            config=config,
            sink_registry=sink_registry,
        ),
    )


def build_engine(
    rules: Iterable[DeclarativeRule],
    *,
    config: Optional[EngineConfig] = None,
    sink_registry: Optional[SinkRegistry] = None,
) -> EmbeddedEngine:
    return create_engine(
        compile_rules(rules),
        config=config,
        sink_registry=sink_registry,
    )


def build_engine_from_yaml(
    yaml_texts: Iterable[str],
    *,
    config: Optional[EngineConfig] = None,
    sink_registry: Optional[SinkRegistry] = None,
) -> EmbeddedEngine:
    rules = [load_rule_yaml(text) for text in yaml_texts]
    return build_engine(rules, config=config, sink_registry=sink_registry)


def build_engine_from_files(
    paths: Iterable[str | Path],
    *,
    config: Optional[EngineConfig] = None,
    sink_registry: Optional[SinkRegistry] = None,
) -> EmbeddedEngine:
    return create_engine(
        load_and_compile_rule_files(paths),
        config=config,
        sink_registry=sink_registry,
    )


def load_rule(path: str | Path) -> DeclarativeRule:
    return load_rule_file(str(path))


def compile_yaml_rule(yaml_text: str) -> CompiledRule:
    return compile_rule(load_rule_yaml(yaml_text))
