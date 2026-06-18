import pytest

from rule_engine.declarative import get_rule_schema, load_rule_yaml


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
      - type: queue
        queue: "alert-events"
"""
    rule = load_rule_yaml(yaml_text)
    assert rule.trigger.type == "composite"
    assert len(rule.sources) == 2
    assert rule.functional_primitive == "@window_rule"
    assert rule.actions[0].sinks[0]["type"] == "queue"
    assert rule.actions[0].sinks[0]["queue"] == "alert-events"


def test_load_rule_normalizes_legacy_queue_alias():
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
        queue_url: "https://example.test/queue"
"""
    rule = load_rule_yaml(yaml_text)

    assert rule.actions[0].sinks[0]["type"] == "queue"
    assert rule.actions[0].sinks[0]["queue"] == "https://example.test/queue"


def test_load_rule_rejects_unknown_sink_type():
    yaml_text = """
rule_id: bad_sink_rule
description: bad sink
trigger:
  type: event
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
actions:
  - severity: warning
    message: "bad"
    sinks:
      - type: email
        address: ops@example.test
"""
    with pytest.raises(ValueError, match="sink type 'email' is unsupported"):
        load_rule_yaml(yaml_text)


def test_load_rule_rejects_missing_required_sink_field():
    yaml_text = """
rule_id: bad_webhook_rule
description: bad sink
trigger:
  type: event
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
actions:
  - severity: warning
    message: "bad"
    sinks:
      - type: webhook
"""
    with pytest.raises(ValueError, match="missing required fields: url"):
        load_rule_yaml(yaml_text)


def test_load_rule_rejects_unsupported_sink_fields():
    yaml_text = """
rule_id: bad_queue_rule
description: bad sink
trigger:
  type: event
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
actions:
  - severity: warning
    message: "bad"
    sinks:
      - type: queue
        queue: alerts
        url: https://example.test/not-allowed
"""
    with pytest.raises(ValueError, match="unsupported fields: url"):
        load_rule_yaml(yaml_text)


def test_load_rule_accepts_webhook_auth_and_signing_fields():
    yaml_text = """
rule_id: webhook_auth_rule
description: signed webhook
trigger:
  type: event
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
actions:
  - severity: warning
    message: "signed"
    sinks:
      - type: webhook
        url: https://example.test/hook
        auth_token: secret-token
        auth_scheme: Token
        signature_secret: signing-secret
        signature_header: X-Test-Signature
"""
    rule = load_rule_yaml(yaml_text)

    sink = rule.actions[0].sinks[0]
    assert sink["auth_token"] == "secret-token"
    assert sink["auth_scheme"] == "Token"
    assert sink["signature_secret"] == "signing-secret"
    assert sink["signature_header"] == "X-Test-Signature"


def test_load_rule_accepts_file_and_object_storage_timeout_fields():
    yaml_text = """
rule_id: timeout_rule
description: sink timeouts
trigger:
  type: event
sources:
  - sensor_type: source_primary
    entity_id: "*"
condition:
  operator: AND
actions:
  - severity: warning
    message: "timed"
    sinks:
      - type: file
        path: output/alerts.ndjson
        timeout_s: 0.5
      - type: object_storage
        bucket: archive
        prefix: alerts
        timeout_s: 1.25
"""
    rule = load_rule_yaml(yaml_text)

    assert rule.actions[0].sinks[0]["timeout_s"] == 0.5
    assert rule.actions[0].sinks[1]["timeout_s"] == 1.25


def test_get_rule_schema_exposes_required_top_level_fields():
    schema = get_rule_schema()

    assert schema["type"] == "object"
    assert "rule_id" in schema["required"]
    assert "actions" in schema["required"]
    assert "sources" in schema["properties"]


def test_load_rule_rejects_invalid_yaml():
    yaml_text = """
rule_id: broken_rule
actions:
  - severity: warning
    message: "oops"
    sinks: [
"""
    with pytest.raises(ValueError, match="Invalid YAML rule document"):
        load_rule_yaml(yaml_text)


def test_load_rule_rejects_missing_required_rule_id():
    yaml_text = """
description: missing id
sources:
  - sensor_type: source_primary
actions:
  - severity: warning
    message: "bad"
"""
    with pytest.raises(ValueError, match="rule is missing required field 'rule_id'"):
        load_rule_yaml(yaml_text)


def test_load_rule_rejects_missing_sources_definition():
    yaml_text = """
rule_id: bad_rule
actions:
  - severity: warning
    message: "bad"
"""
    with pytest.raises(ValueError, match="must define either 'source' or 'sources'"):
        load_rule_yaml(yaml_text)


def test_load_rule_rejects_unknown_top_level_fields():
    yaml_text = """
rule_id: bad_rule
sources:
  - sensor_type: source_primary
actions:
  - severity: warning
    message: "bad"
extra_field: true
"""
    with pytest.raises(ValueError, match="rule has unsupported fields: extra_field"):
        load_rule_yaml(yaml_text)


def test_load_rule_rejects_invalid_trigger_type():
    yaml_text = """
rule_id: bad_rule
trigger:
  type: stream
sources:
  - sensor_type: source_primary
actions:
  - severity: warning
    message: "bad"
"""
    with pytest.raises(ValueError, match="rule.trigger.type must be one of"):
        load_rule_yaml(yaml_text)
