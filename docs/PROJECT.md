# Project Record

## Objective

To provide a lightweight, durable state machine execution engine using only the Python standard library, suitable for long-running business processes that need strict state validation and consistent persistence.

## Scope

- **Persistence**: In-memory and SQLite-backed storage implementation with optimistic concurrency and race-safe timeout handling.
- **Orchestration**: `Engine` class managing transitions, idempotent event delivery, and deterministic timeout triggering via `tick()`.
- **Validation**: Strict validation of definitions, data payloads, and event identifiers.

## Out of Scope

- No support for parallel branches or sub-workflows.
- No built-in scheduler thread or background worker processes (relies on manual `tick()` invocation).
- No support for Postgres or other external RDBMS.

## Factory run u147 (measured from API usage)

| Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|
| generation | gemini-3.8-flash | yes | 4563 | 13789 | 0 | 0 | 0.0276 |
| fix-1 | gemini-3.8-flash | no | 20244 | 392 | 0 | 35230 | 0.0193 |
| fix-2 | gemini-3.8-flash | no | 33164 | 444 | 0 | 54145 | 0.0306 |
| audit | gemini-3.1-flash-lite | no | 56092 | 187 | 0 | 71174 | 0.0161 |
| docs | gemini-3.1-flash-lite | no | 139490 | 1317 | 0 | 0 | 0.0368 |

Total: $0.1304. Prices: Anthropic list prices.
