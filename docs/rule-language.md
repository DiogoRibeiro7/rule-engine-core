# Supported Rule Language

This document defines the currently supported declarative rule-language subset
for `rule-engine-core`. If a field or behavior is not described here, do not
assume it is supported.

## Top-Level Shape

A rule document must be a YAML object with:

- required: `rule_id`, `actions`
- exactly one of: `source` or `sources`
- optional: `description`, `trigger`, `condition`, `aggregations`

Top-level unknown fields are rejected.

## Sources

Supported source fields:

- `sensor_type`: required string
- `entity_id`: optional string, defaults to `*`
- `trigger`: optional per-source trigger object

Per-source trigger support is intentionally narrow:

- only `type: absence` is accepted
- only `timeout` is accepted alongside that type

Rules using `sources` must use the same `entity_id` filter across all sources.

## Trigger Types

Supported top-level trigger types:

- `event`
- `window`
- `absence`
- `composite`
- `scheduled`

### `event`

Supported fields:

- `type`

Rejected fields:

- `duration`
- `slide`
- `timeout`
- `cron`
- `lookback`

### `window`

Supported fields:

- `type`
- `duration`: required positive duration string
- `slide`: optional positive duration string, defaults to `duration`

Validation rules:

- `slide` must be less than or equal to `duration`

Rejected fields:

- `timeout`
- `cron`
- `lookback`

### `absence`

Supported fields:

- `type`
- `timeout`: optional at top level if provided on the single source trigger

Validation rules:

- a timeout must exist either at the top level or on the single source trigger

Rejected fields:

- `duration`
- `slide`
- `cron`
- `lookback`

### `composite`

Supported fields:

- `type`

Validation rules:

- each source must have a per-source `trigger` with `type: absence`
- each source trigger must define `timeout`

Rejected fields:

- `duration`
- `slide`
- `timeout`
- `cron`
- `lookback`

### `scheduled`

Supported fields:

- `type`
- `cron`: required
- `lookback`: optional positive duration string

Validation rules:

- cron must use five fields
- only `minute hour * * *` is supported
- minute must be `0-59`
- hour must be `0-23`

Rejected fields:

- `duration`
- `slide`
- `timeout`

## Duration Format

Supported duration syntax:

- `<integer>s`
- `<integer>m`
- `<integer>h`
- `<integer>d`

Examples:

- `30s`
- `10m`
- `2h`
- `7d`

Validation rules:

- value must be greater than zero
- fractional values are not supported
- mixed-unit expressions like `1h30m` are not supported

## Conditions

Supported condition object fields:

- `operator`
- `metric`
- `value`
- `operands`

Supported condition operators:

- `AND`
- `OR`

Condition evaluation behavior:

- if `operator` is omitted, operand evaluation defaults to `AND`
- if `operands` is empty, the condition evaluates to `False`

## Operands

Supported operand fields:

- `metric`
- `operator`
- `value`
- `const`

Supported comparison operators:

- `eq`
- `ne`
- `gt`
- `gte`
- `lt`
- `lte`

Operand rules:

- an operand with `const` bypasses metric comparison
- otherwise both `metric` and `operator` are required

## Aggregations

Supported aggregation fields:

- `id`: required
- `function`: required
- `field`: optional
- `input`: optional
- `percentile`: optional, only relevant to `percentile`
- `sub_window`: optional positive duration string

An aggregation must provide either:

- `field`
- `input`

Supported aggregation functions:

- `count`
- `sum`
- `mean`
- `min`
- `max`
- `stddev`
- `delta`
- `rate`
- `percentile`

Notes:

- `percentile` defaults to `95.0` if omitted
- `rate` is derived from `delta`
- `sub_window` produces per-bucket lists rather than scalar values

## Actions

An action must contain:

- `severity`
- `message`

Optional:

- `sinks`

`message` uses template substitution with `{{...}}` placeholders. Missing values
are currently left in place rather than raising an error.

## Sink Types

Canonical sink types:

- `stdout`
- `file`
- `webhook`
- `queue`
- `object_storage`

Accepted aliases:

- `console` -> `stdout`
- `ndjson` -> `file`
- `sqs` -> `queue`
- `object-store` -> `object_storage`

### `stdout`

Supported fields:

- `type`
- `retry`

### `file`

Supported fields:

- `type`
- `path`
- `retry`

### `webhook`

Supported fields:

- `type`
- `url`
- `timeout_s`
- `headers`
- `method`
- `retry`

### `queue`

Supported fields:

- `type`
- `queue`
- `retryable`
- `retry`

Legacy normalization:

- `queue_url` is normalized to `queue`

### `object_storage`

Supported fields:

- `type`
- `bucket`
- `prefix`
- `extension`
- `retryable`
- `retry`

### Retry Block

Supported retry fields:

- `max_attempts`
- `base_delay_s`
- `multiplier`
- `max_delay_s`
- `sleep`

## Runtime Output Model

Rule execution can produce:

- emitted alerts with `severity`, rendered `message`, and metadata
- per-sink `DeliveryResult` objects
- replay-level delivery reports with metrics and structured delivery logs

## Explicitly Unsupported Today

Not supported by the current repo surface:

- arbitrary cron syntax beyond `minute hour * * *`
- fractional or compound durations
- custom condition operators
- custom aggregation functions
- sink types beyond the implemented set
- live streaming ingestion
- workflow orchestration or stateful infrastructure integrations

## Source Of Truth

The authoritative implementation lives in:

- `rule_engine/declarative.py`
- `rule_engine/runtime.py`
- `rule_engine/sinks.py`

When the supported language changes, update this file in the same change set.
