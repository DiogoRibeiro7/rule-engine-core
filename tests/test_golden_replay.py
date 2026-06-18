import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pytest

from rule_engine.runner import emit_replay_report_json, replay_events_with_report


@dataclass(frozen=True)
class ReplayGoldenCase:
    name: str
    rule_paths: list[Path]
    event_path: Path
    expected_path: Path
    until: datetime | None = None

    @classmethod
    def load(cls, case_path: Path) -> "ReplayGoldenCase":
        payload = json.loads(case_path.read_text(encoding="utf-8"))
        fixture_root = case_path.parent
        until_value = payload.get("until")
        return cls(
            name=payload["name"],
            rule_paths=[fixture_root / path for path in payload["rule_paths"]],
            event_path=fixture_root / payload["event_path"],
            expected_path=fixture_root / payload["expected_path"],
            until=datetime.fromisoformat(until_value) if until_value else None,
        )


def _load_cases() -> list[ReplayGoldenCase]:
    fixture_root = Path(__file__).resolve().parent / "fixtures" / "replay"
    return [
        ReplayGoldenCase.load(case_path)
        for case_path in sorted(fixture_root.glob("*.case.json"))
    ]


@pytest.mark.parametrize("case", _load_cases(), ids=lambda case: case.name)
def test_replay_report_matches_golden_fixture(case: ReplayGoldenCase) -> None:
    alerts, report = replay_events_with_report(
        case.rule_paths,
        case.event_path,
        until=case.until,
    )

    actual = json.loads(emit_replay_report_json(alerts, report))
    expected = json.loads(case.expected_path.read_text(encoding="utf-8"))

    assert actual == expected
