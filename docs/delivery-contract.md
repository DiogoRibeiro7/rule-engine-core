# Delivery Contract

This document describes the runtime delivery contract used by the implemented
sink adapters.

## Shared Envelope

Every non-stdout sink receives the same serialized payload envelope. The
runtime builds it from `rule_engine.sinks.DeliveryPayload`.

Fields:

- `contract_version` — current value: `rule-engine-core.v1`
- `sink_type` — sink route selected by the rule action
- `idempotency_key` — deterministic SHA-256 digest of rule id, entity id,
  severity, timestamp, and rendered message
- `entity_id`
- `rule_id`
- `severity`
- `message`
- `timestamp` — ISO-8601 UTC timestamp string
- `payload` — alert metadata/context payload produced by the runtime

Example envelope:

```json
{
  "contract_version": "rule-engine-core.v1",
  "sink_type": "queue",
  "idempotency_key": "8f2fbf9c6208c2786fb365b5b2f5f5c7fd8af2e0b3f8f98b42bc7dd6d4d5f5fd",
  "entity_id": "entity-1",
  "rule_id": "source_primary_spike",
  "severity": "critical",
  "message": "Primary source spike for entity-1: 185.0",
  "timestamp": "2024-01-01T00:00:00+00:00",
  "payload": {
    "rule_id": "source_primary_spike",
    "entity_id": "entity-1",
    "sinks": [
      {
        "type": "queue",
        "queue": "alerts"
      }
    ],
    "variables": {
      "entity_id": "entity-1",
      "rule_id": "source_primary_spike",
      "sensor_type": "source_primary",
      "value": 185.0,
      "timestamp_ms": 1704067200000,
      "timestamp": "2024-01-01T00:00:00+00:00",
      "duration": "0:00:00"
    }
  }
}
```

## Sink Semantics

### `stdout`

- Output shape: human-readable log line, not the JSON envelope
- Timeout handling: none
- Idempotency expectation: best-effort only
- Failure model: always local process output; no retry contract

### `file`

- Output shape: one JSON envelope per line
- Timeout handling: local filesystem write; no retry by default
- Idempotency expectation: caller chooses file path and downstream handling
- Failure model: terminal on invalid path or write error

Example line written to disk:

```json
{
  "contract_version": "rule-engine-core.v1",
  "sink_type": "file",
  "idempotency_key": "f7d28d9baf8b8d462b4d2955447b61cb1a1ca42ef1d4d8f06b8c94ce1c5909ab",
  "entity_id": "entity-1",
  "rule_id": "source_primary_spike",
  "severity": "critical",
  "message": "Primary source spike for entity-1: 185.0",
  "timestamp": "2024-01-01T00:00:00+00:00",
  "payload": {
    "rule_id": "source_primary_spike",
    "entity_id": "entity-1"
  }
}
```

### `webhook`

- Output shape: JSON envelope as HTTP request body
- Timeout handling: `timeout_s` per request
- Optional auth support:
  - `auth_token`
  - `auth_scheme` with default `Bearer`
- Optional request signing support:
  - `signature_secret`
  - `signature_header` with default `X-Signature-256`
- Idempotency expectation: downstream webhook should treat `idempotency_key` as
  the deduplication token if it needs exactly-once behavior
- Failure model:
  - HTTP `5xx`, transport errors, and socket timeouts are retryable
  - HTTP `4xx` is terminal

Success metadata example:

```json
{
  "contract_version": "rule-engine-core.v1",
  "idempotency_key": "8f2fbf9c6208c2786fb365b5b2f5f5c7fd8af2e0b3f8f98b42bc7dd6d4d5f5fd",
  "status_code": 202,
  "url": "https://example.test/hook"
}
```

Header example with auth and signing enabled:

```text
Authorization: Token secret-token
X-Test-Signature: sha256=<hmac of request body>
```

### `queue`

- Output shape: JSON envelope passed to the configured queue transport
- Timeout handling: transport-raised `TimeoutError` is retryable
- Idempotency expectation: downstream queue consumer should use
  `idempotency_key` if duplicate suppression matters
- Failure model:
  - `TimeoutError` is retryable
  - other exceptions are retryable only when `retryable: true` is configured

Success metadata example:

```json
{
  "contract_version": "rule-engine-core.v1",
  "idempotency_key": "8f2fbf9c6208c2786fb365b5b2f5f5c7fd8af2e0b3f8f98b42bc7dd6d4d5f5fd",
  "queue": "alerts",
  "message_id": "1"
}
```

### `object_storage`

- Output shape: one JSON envelope per stored object
- Timeout handling: transport-raised `TimeoutError` is retryable
- Idempotency expectation: object keys are deterministic only up to timestamp
  and rule id; consumers should use `idempotency_key` for semantic deduplication
- Failure model:
  - `TimeoutError` is retryable
  - other exceptions are retryable only when `retryable: true` is configured

Success metadata example:

```json
{
  "contract_version": "rule-engine-core.v1",
  "idempotency_key": "8f2fbf9c6208c2786fb365b5b2f5f5c7fd8af2e0b3f8f98b42bc7dd6d4d5f5fd",
  "bucket": "archive",
  "key": "alerts/source_primary_spike-20240101T000000Z.jsonl",
  "path": ".object_store/archive/alerts/source_primary_spike-20240101T000000Z.jsonl"
}
```

## Delivery Results

Delivery results report:

- `status`
- `detail`
- `retryable`
- `metadata`

For implemented sinks, `metadata` now includes at least:

- `contract_version`
- `idempotency_key`

Concrete sinks may add extra fields such as `path`, `status_code`, `queue`,
`bucket`, `key`, or transport-returned identifiers.

Example failure metadata for a retryable webhook error:

```json
{
  "contract_version": "rule-engine-core.v1",
  "idempotency_key": "8f2fbf9c6208c2786fb365b5b2f5f5c7fd8af2e0b3f8f98b42bc7dd6d4d5f5fd",
  "url": "https://example.test/hook",
  "attempt_latency_ms": 12.4,
  "total_latency_ms": 24.8,
  "attempt": 2,
  "max_attempts": 2,
  "backoff_schedule_s": [
    0.25
  ]
}
```

## Dead-Letter Persistence

Dead-letter recording is intentionally narrow in this repo: it is a local
fallback, not a full operational archive.

Available stores:

- `InMemoryDeadLetterStore` for tests and ephemeral embedding flows
- `FileDeadLetterStore` for newline-delimited JSON persistence on local disk

`FileDeadLetterStore` options:

- `path` — target NDJSON file
- `max_records` — optional retention cap; when set, only the newest records are
  retained after each write
- `fsync` — optional durability hint for callers that want each write flushed to
  disk before returning

Retention guidance:

- Use `max_records` when the dead-letter file is acting as a bounded retry or
  triage buffer rather than a historical archive.
- Keep long-term retention, compaction, shipping, and alerting in downstream
  wrappers or infrastructure outside this core package.
- Treat `fsync=True` as a stronger durability option for lower-throughput local
  fallback paths, not as a substitute for replicated storage.
