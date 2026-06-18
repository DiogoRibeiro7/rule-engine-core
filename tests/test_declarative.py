from rule_engine.declarative import load_rule_yaml


def test_load_simple_absence_rule():
    yaml_text = """
rule_id: source_gap_48h
description: Alert if no source_alpha reading received for 48 hours
trigger:
  type: absence
  timeout: 48h
sources:
  - sensor_type: source_alpha
    entity_id: "*"
condition:
  operator: AND
actions:
  - severity: warning
    message: "No source_alpha reading for entity {{entity_id}} in the last 48 hours."
    sinks:
      - type: webhook
        url: "https://hooks.hospital.internal/rule_engine"
"""
    rule = load_rule_yaml(yaml_text)
    assert rule.rule_id == "source_gap_48h"
    assert rule.trigger.type == "absence"
    assert rule.sources[0].sensor_type == "source_alpha"
    assert rule.functional_primitive == "@window_rule"


def test_load_composite_absence_rule():
    yaml_text = """
rule_id: dual_source_gap
description: Alert when source_alpha and source_beta are both silent
trigger:
  type: composite
sources:
  - sensor_type: source_alpha
    entity_id: "*"
    trigger:
      type: absence
      timeout: 5h
  - sensor_type: source_beta
    entity_id: "*"
    trigger:
      type: absence
      timeout: 10h
condition:
  operator: AND
actions:
  - severity: warning
    message: "Dual source inactivity warning for {{entity_id}}."
    sinks:
      - type: sqs
        queue_url: "https://sqs.example.com/queue"
"""
    rule = load_rule_yaml(yaml_text)
    assert rule.trigger.type == "composite"
    assert len(rule.sources) == 2
    assert rule.functional_primitive == "@window_rule"

