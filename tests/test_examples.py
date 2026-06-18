from pathlib import Path

import pytest

from rule_engine.compiler import compile_rule
from rule_engine.declarative import load_rule_file


@pytest.mark.parametrize(
    "rule_path",
    sorted((Path(__file__).resolve().parents[1] / "sample_rules" / "examples").glob("*.yaml")),
)
def test_example_rules_compile(rule_path: Path) -> None:
    rule = load_rule_file(str(rule_path))
    compiled = compile_rule(rule)

    assert compiled.rule_id == rule.rule_id
