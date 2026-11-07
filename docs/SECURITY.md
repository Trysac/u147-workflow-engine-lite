# Security Policy

## Threat Model

- **Data Integrity**: Payload serialization is constrained by a 64 KiB byte limit for JSON representations. Payloads must serialize using `allow_nan=False`.
- **Input Sanitization**: Identifiers (IDs and event types) are validated against the pattern `^[A-Za-z0-9_.:-]{1,200}$`. Event types prefixed with `__` are explicitly forbidden to prevent collision with reserved internal engine events (like `__timeout__`).
- **Clock Security**: The engine validates that the `clock` callable returns timezone-aware datetimes. Naive datetimes are rejected with `invalid_clock` to prevent ambiguity in timeout calculations.
- **Observability**: The `on_event` callback purposefully excludes business data payloads, transmitting only metadata (`instance_id`, `from`, `to`, `trigger`) to avoid leaking sensitive data into logs or monitoring endpoints.
