# Examples

This repo is generic on purpose. The examples below show the same runtime used
for different neutral domains without changing the engine itself.

## Facility Temperature Spike

Use an `event` rule when a single reading should emit immediately.

Rule: `sample_rules/examples/facility_temperature_spike.yaml`

```yaml
rule_id: facility_temperature_spike
description: Emit when a facility temperature reading exceeds the threshold
trigger:
  type: event
sources:
  - sensor_type: facility_temperature
    entity_id: "*"
condition:
  operator: AND
  operands:
    - metric: value
      operator: gt
      value: 85
actions:
  - severity: critical
    message: "High facility temperature for {{entity_id}}: {{value}}"
    sinks:
      - type: stdout
```

Example replay:

```bash
python -m rule_engine.runner sample_rules/examples/facility_temperature_spike.yaml --events sample_data/examples/facility_temperature_spike.ndjson
```

## Fleet Heartbeat Gap

Use an `absence` rule when a stream going quiet is the signal.

Rule: `sample_rules/examples/fleet_heartbeat_gap.yaml`

```yaml
rule_id: fleet_heartbeat_gap
description: Emit when a tracked asset stops reporting its heartbeat
trigger:
  type: absence
  timeout: 15m
sources:
  - sensor_type: asset_heartbeat
    entity_id: "*"
condition:
  operator: AND
actions:
  - severity: warning
    message: "No fleet heartbeat for {{entity_id}} in {{duration}}"
    sinks:
      - type: file
        path: output/fleet_alerts.ndjson
```

Example replay:

```bash
python -m rule_engine.runner sample_rules/examples/fleet_heartbeat_gap.yaml --events sample_data/examples/fleet_heartbeat_gap.ndjson --until 2024-01-01T08:20:00+00:00
```

## Daily Usage Review

Use a `scheduled` rule when the engine should evaluate on a wall-clock cadence.

Rule: `sample_rules/examples/daily_usage_review.yaml`

```yaml
rule_id: daily_usage_review
description: Emit a daily summary alert when recent usage volume is high
trigger:
  type: scheduled
  cron: 0 8 * * *
  lookback: 24h
sources:
  - sensor_type: usage_sample
    entity_id: "*"
aggregations:
  - id: sample_count
    function: count
    field: value
condition:
  operator: AND
  operands:
    - metric: sample_count
      operator: gte
      value: 3
actions:
  - severity: info
    message: "Daily usage review for {{entity_id}}: {{sample_count}} samples"
    sinks:
      - type: webhook
        url: https://example.invalid/hooks/usage-review
```

Example replay:

```bash
python -m rule_engine.runner sample_rules/examples/daily_usage_review.yaml --events sample_data/examples/daily_usage_review.ndjson --until 2024-01-02T08:00:00+00:00
```

## Notes

- The examples use only the declarative subset documented in
  `docs/rule-language.md`.
- The sample event files are intentionally small and deterministic so they can
  be used in tests, demos, and documentation.
- The webhook target above is intentionally non-routable documentation-only
  data.
