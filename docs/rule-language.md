# Supported Rule Language

This document defines the currently supported declarative rule-language subset
for `rule-engine-core`. If a field or behavior is not described here, do not
assume it is supported.

## Top-Level Shape

A rule document must be a YAML object with:

- required: `rule_id`, `actions`
- exactly one of: `source` or `sources`
- optional: `description`, `trigger`, `condition`, `aggregations`, `allowed_lateness`,
  `emit`, `partition_by`

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
- `sequence`
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

### `sequence`

Matches an ordered temporal pattern.

Supported fields:

- `type`
- `within` (required)

Rejected fields: `duration`, `slide`, `timeout`, `cron`, `lookback`.

The pattern itself is declared at the top level:

```yaml
trigger:
  type: sequence
  within: 5m
sources:
  - sensor_type: access_denied
    entity_id: "*"
  - sensor_type: access_granted
    entity_id: "*"
  - sensor_type: credential_reset
    entity_id: "*"
sequence:
  - sensor_type: access_denied
  - sensor_type: access_denied
  - sensor_type: access_granted
without:
  sensor_type: credential_reset
```

`sequence` requires at least two steps. Every `sensor_type` named in `sequence`
or `without` must also be declared in `sources`. `sequence` and `without` are
rejected on any other trigger type.

Matching rules:

- **Ordered.** Steps must occur in the declared order.
- **Bounded.** The whole pattern must complete within `within`, measured from the
  event that matched the first step. The boundary is inclusive. `within` is
  required, because bounding the pattern is what keeps partial-match state
  bounded, and therefore snapshottable.
- **Skip-till-next.** An event that is not the next expected step is ignored
  rather than breaking a partial match, so unrelated traffic between steps is
  harmless.
- **Non-overlapping.** A completed match consumes all partial state for that
  entity, so a burst produces one alert rather than a cascade. A new pattern can
  begin immediately afterwards.
- **Per entity.** Partial matches are tracked per entity and never cross.
- **`without` cancels.** An event of the `without` sensor type discards every
  partial match in flight for that entity. It has no effect when no match is in
  flight.

A sequence rule can carry `condition` operands, which are evaluated against the
event that completed the pattern, and an `emit` block, which behaves as it does
for every other trigger.

The completing alert exposes `sequence_started`, `sequence_duration`, and
`matched_steps` as template variables.

### Not Supported

The grammar is deliberately restricted. There is no repetition quantifier, no
alternation, no nesting, no per-step condition, and no unbounded pattern. Write
repeated steps out explicitly, as the example above does with two denials.

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

## Partitioning

By default a rule keeps independent state per `entity_id`. `partition_by`
replaces that key with one or more fields:

```yaml
partition_by:
  - customer_id
  - device_id
```

The partition key becomes the identity for that rule: its state, its timers, its
alert episodes, and the `entity_id` reported on its alerts. A composite key
joins the values with `|`.

Field values are read from the event's `attributes` map first, then from its
built-in fields (`entity_id`, `sensor_type`, `value`, `timestamp_ms`):

```python
SensorEvent(
    entity_id="device-1",
    sensor_type="source_alpha",
    value=91.0,
    timestamp_ms=1704067260000,
    attributes={"customer_id": "acme", "region": "eu"},
)
```

Rules:

- **Ordering and isolation hold inside a partition.** Two partitions never share
  state, so a cooldown in one cannot suppress another.
- **A rule declaring `partition_by` must use `entity_id: "*"` in every source.**
  A custom key replaces `entity_id` as the identity, so an entity filter
  alongside it would be ambiguous. This is rejected at compile time.
- **An event missing any partition field is skipped by that rule.** It cannot be
  placed in a partition. Other rules still see it, and `explain()` omits the
  rule rather than reporting a misleading result.
- **The partition scheme is part of the rule state fingerprint.** Changing it
  invalidates a checkpoint and is refused on restore, because the retained state
  is keyed by something else.

Omitting `partition_by` is exactly equivalent to `partition_by: [entity_id]`.

### Not A Distribution Mechanism

Partitioning is a state-model change. It defines independent keyed state and is
a clean path toward parallel execution later, but the engine remains
single-process and in-memory. Nothing here makes it distributed.

## Alert Lifecycle

The optional `emit` block turns a rule from fire-on-every-match into one that
tracks alert *episodes*.

```yaml
emit:
  cooldown: 30m
  repeat_every: 2h
  resolve: true
```

Supported fields, all optional:

- `cooldown`: minimum gap between emissions within an episode. Emissions inside
  the gap are suppressed.
- `repeat_every`: re-emit on this interval while the episode is open. This is
  timer-driven, so a reminder fires even when no new events arrive.
- `resolve`: emit a closing alert when the condition clears. Defaults to `false`.

An episode opens on the first qualifying emission and closes when the condition
clears. Emissions are labelled `firing`, `repeat`, or `resolved`, and all
emissions in one episode share a `correlation_id`. Both fields appear in the
delivered payload; see `docs/delivery-contract.md`.

If both `cooldown` and `repeat_every` are set, the next emission is allowed once
the larger of the two has elapsed.

A rule with no `emit` block keeps the original behaviour: every qualifying
evaluation emits, with no episode tracking and no suppression.

Emission policy is not part of the snapshot state fingerprint, so retuning a
cooldown or a repeat interval does not invalidate an existing checkpoint. A
pending reminder scheduled under the old interval fires at its already-scheduled
time and follows the new interval afterwards.

### Not Supported

There is no acknowledgement state. Acknowledging an alert requires an
operator-facing inbound API, which is outside this repo's scope boundary; that
belongs in the alerting system consuming these payloads.

## Late Events

`allowed_lateness` is an optional top-level duration declaring how far behind
the engine watermark an event for this rule may arrive and still be considered.
It defaults to `0s`, and unlike other durations it accepts `0s` explicitly,
because zero tolerance is a meaningful setting rather than a mistake.

```yaml
rule_id: reading_spike
allowed_lateness: 5m
```

An event whose timestamp is behind the watermark is *late*. Lateness is compared
inclusively: an event exactly `allowed_lateness` behind the watermark is still
in range.

- **Within tolerance.** The event is folded into rule state in place. The
  watermark does not move backward, and no timers re-fire. `event` triggers are
  evaluated and may emit; `window` and `scheduled` buffers receive the event in
  timestamp order; `last_seen` is only advanced, never dragged backward.
- **Beyond tolerance.** Governed by `EngineConfig.late_event_policy`, which is
  engine-level rather than per-rule because it decides the fate of an event no
  rule can use. `reject` (the default) raises; `drop` discards and counts it.

Because tolerance is per-rule, one event can be within range for one rule and
beyond it for another. The engine compares against the largest declared
tolerance, then each rule applies its own; `CompiledEngine.late_event_metrics()`
reports both the totals and the per-rule breakdown.

`replay()` sorts a batch before evaluating it, so lateness only arises when
feeding `process_event` directly or replaying a later batch first.

### Not Yet Handled

A tolerated late event does **not** recompute a window that has already closed
and emitted. Reopening a closed window requires retracting the alert it already
produced, which needs the alert lifecycle work tracked in `ROADMAP.md`. Today a
late event only affects windows still open when it arrives.

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
- `timeout_s`
- `retry`

### `webhook`

Supported fields:

- `type`
- `url`
- `timeout_s`
- `headers`
- `method`
- `auth_token`
- `auth_scheme`
- `signature_secret`
- `signature_header`
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
- `timeout_s`
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
- recomputing windows that have already closed when a late event arrives
- per-source or per-entity watermarks; the watermark is engine-wide, so
  partitions share one clock
- parallel or distributed execution across partitions
- alert acknowledgement, which would need an inbound operator API
- sequence quantifiers, alternation, nesting, or per-step conditions

## Source Of Truth

The authoritative implementation lives in:

- `rule_engine/declarative.py`
- `rule_engine/runtime.py`
- `rule_engine/sinks.py`

When the supported language changes, update this file in the same change set.
