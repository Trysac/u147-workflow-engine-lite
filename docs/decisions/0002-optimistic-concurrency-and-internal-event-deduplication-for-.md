# Optimistic Concurrency and Internal Event Deduplication for Timeouts

Status: accepted (generated, pending human review)

## Context

When orchestrating workflows across worker processes or distributed ticks, instances must not experience lost updates or multiple timeout firings when clock intervals cross deadline boundaries concurrently.

## Decision

Implement version tracking on Instance records with store-level optimistic concurrency checks (WHERE id = ? AND version = ?). Timeout execution in tick() uses an internal event ID formatted as '__timeout__:<version>', so even concurrent tick workers will safely serialize and only one transition will succeed while the other detects duplicate or concurrency conflict.

## Consequences

Guarantees linear history and reliable timer execution without distributed locking primitives, maintaining compatibility with SQLite and in-memory stores.
