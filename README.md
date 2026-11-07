# trysac-workflow-engine-lite

[![CI](https://github.com/Trysac/u147-workflow-engine-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u147-workflow-engine-lite/actions)

Long-running business processes often end up as status columns, cron scripts, and scattered conditionals—leading to race conditions, double-fired timeouts, and difficult state tracking. `trysac-workflow-engine-lite` provides durable, declarative state machines with validated transition rules, event deduplication, optimistic concurrency, and pluggable persistence.

## Installation

```bash
pip install trysac-workflow-engine-lite
```

## Usage

```python
from datetime import datetime, timezone, timedelta
from workflow_engine_lite import Engine, MemoryStore, State, WorkflowDef

wf = WorkflowDef(
    "order",
    1,
    initial="s1",
    states={"s1": State(transitions={"pay": "s2"}), "s2": State(terminal=True)},
)
engine = Engine(MemoryStore(), [wf], clock=lambda: datetime.now(timezone.utc))
engine.start("order", "o-1")
engine.send("o-1", "pay", event_id="e-1")
```

## API Summary

- **Engine**: Orchestrates instance lifecycle. Methods: `start(def_name, instance_id, data)`, `send(instance_id, event_type, payload, *, event_id)`, `tick(*, limit)`. 
- **WorkflowDef**: Declarative schema for states and transitions. `__post_init__` validates definitions for dangling states, terminal rules, and timeout consistency.
- **Storage**: `MemoryStore` (in-memory) and `SqliteStore` (persistence). Both implement the `Store` protocol with optimistic concurrency using `save(instance, expected_version)`.
- **Errors**: `WorkflowError` with codes: `invalid_definition`, `unknown_definition`, `instance_exists`, `not_found`, `invalid_transition`, `terminal_state`, `concurrency_conflict`, `data_too_large`, `invalid_id`, `invalid_clock`.
- **Limits**: 64 KiB per data payload (JSON), 100 history entries, 1,000 seen event IDs (for deduplication).

## Why not Temporal?

Full engines like Temporal are heavy for these cases. This library runs durable, declarative state machines with timers and exact transition rules on a stdlib store without distributed systems complexity.

See [docs/SECURITY.md](docs/SECURITY.md) for security information.
