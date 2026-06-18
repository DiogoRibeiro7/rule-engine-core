from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

from .declarative import DeclarativeRule, load_rule_file
from .runtime import CompiledRule


def compile_rule(rule: DeclarativeRule) -> CompiledRule:
    return CompiledRule.from_declarative(rule)


def compile_rules(rules: Iterable[DeclarativeRule]) -> List[CompiledRule]:
    return [compile_rule(rule) for rule in rules]


def load_and_compile_rule_file(path: str | Path) -> CompiledRule:
    return compile_rule(load_rule_file(str(path)))


def load_and_compile_rule_files(paths: Iterable[str | Path]) -> List[CompiledRule]:
    return [load_and_compile_rule_file(path) for path in paths]
