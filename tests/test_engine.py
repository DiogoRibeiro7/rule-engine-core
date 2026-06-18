from datetime import datetime, timedelta

from rule_engine import Alert, RuleEngine, SensorEvent, event_rule, window_rule
from rule_engine.types import RuleContext


@event_rule(rule_id="source_primary_spike", sinks=[])
def source_primary_spike(event: SensorEvent, ctx: RuleContext):
    if event.sensor_type != "source_primary":
        return None
    if event.value > 180.0:
        return Alert(severity="critical", message=f"Primary source spike {event.value:.1f}")
    return None


@window_rule(
    rule_id="source_gap",
    duration=timedelta(minutes=30),
    slide=timedelta(minutes=5),
    sinks=[],
)
def source_gap(window, ctx: RuleContext):
    silence = window.silence_duration("source_alpha")
    if silence >= timedelta(minutes=10):
        return Alert(severity="warning", message="No source_alpha for 10m")
    return None


def test_event_rule_fires_on_high_value():
    engine = RuleEngine()
    event = SensorEvent(
        entity_id="entity-1",
        sensor_type="source_primary",
        value=185.0,
        timestamp_ms=1700000000000,
    )
    alerts = engine.process_event(event)
    assert len(alerts) == 1
    assert alerts[0].severity == "critical"


def test_event_rule_ignores_other_sensor_types():
    engine = RuleEngine()
    event = SensorEvent(
        entity_id="entity-1",
        sensor_type="source_alpha",
        value=80.0,
        timestamp_ms=1700000000000,
    )
    alerts = engine.process_event(event)
    assert alerts == []


def test_window_rule_detects_source_gap_when_no_events():
    engine = RuleEngine()
    start = datetime(2024, 1, 1, 0, 0, 0)
    end = datetime(2024, 1, 1, 0, 30, 0)
    events = []
    alerts = engine.process_window("entity-1", start, end, events)
    assert any(a.severity == "warning" for a in alerts)


def test_window_rule_does_not_fire_when_source_present():
    engine = RuleEngine()
    start = datetime(2024, 1, 1, 0, 0, 0)
    end = datetime(2024, 1, 1, 0, 30, 0)
    event_time = end - timedelta(minutes=5)
    events = [
        SensorEvent(
            entity_id="entity-1",
            sensor_type="source_alpha",
            value=95.0,
            timestamp_ms=int(event_time.timestamp() * 1000),
        )
    ]
    alerts = engine.process_window("entity-1", start, end, events)
    assert not any(a.severity == "warning" for a in alerts)
