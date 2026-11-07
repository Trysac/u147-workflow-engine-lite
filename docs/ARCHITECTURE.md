# Architecture

## Overview

`trysac-workflow-engine-lite` provides a lightweight, durable state machine execution engine built using Python standard library primitives.

## Core Concepts

1. **WorkflowDef and State**: Declarations of state transitions and timeouts. Validated upon definition creation to guarantee integrity.
2. **Instance**: A snapshot of an active or terminal workflow instance, including its current state, business data, history, version, and seen event IDs.
3. **Store**: Protocol for persisting workflow instances. Supports `MemoryStore` for fast tests/single-process workloads and `SqliteStore` for durable persistence with row-level versioning.
4. **Engine**: Drives workflows, enforces transition rules, deduplicates events, and triggers timeout transitions via `tick()`.

## Concurrency and Timers

- **Optimistic Concurrency**: Each state update checks `expected_version == stored_version`. Stale writes raise `concurrency_conflict`.
- **Idempotency**: `send()` tracks `seen_event_ids` (capped at 1,000 entries). Duplicate event IDs return the instance unchanged with `outcome='duplicate'` without updating the database.
- **Timeouts**: Due instances are queried by `deadline <= now` ordered by `(deadline, instance_id)`. Timeouts trigger an internal `__timeout__` transition with a unique event ID `__timeout__:<version>`, ensuring race safety during distributed tick executions.
