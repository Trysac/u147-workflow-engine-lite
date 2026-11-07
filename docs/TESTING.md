# Testing Guide

## Coverage

Tests are located in `tests/test_workflow_engine_lite.py`.

- **Core Engine Logic**: Validation of workflow definitions (missing initials, invalid transitions, terminal state rules). 
- **Lifecycle Tests**: `start` and `send` behavior including idempotency checks, transition constraints, and `terminal_state` enforcement.
- **Concurrency Tests**: Verification that `MemoryStore` and `SqliteStore` handle `concurrency_conflict` correctly using `expected_version` checks.
- **Timeouts**: Verification of the `tick()` logic, ensuring expired timeouts fire exactly once and trigger the defined `timeout_target`.
- **Data Constraints**: Verification that data payloads exceeding 64 KiB raise `data_too_large` and that history and seen event IDs are capped at 100 and 1,000, respectively.
