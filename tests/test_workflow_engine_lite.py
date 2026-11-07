"""Unit and integration tests for workflow_engine_lite."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from workflow_engine_lite import (
    Engine,
    HistoryEntry,
    Instance,
    MemoryStore,
    SqliteStore,
    State,
    Store,
    WorkflowDef,
    WorkflowError,
)


def fixed_clock(start: datetime) -> tuple[Any, Any]:
    current = [start]

    def clock() -> datetime:
        return current[0]

    def advance(td: timedelta) -> None:
        current[0] = current[0] + td

    return clock, advance


def order_workflow_def() -> WorkflowDef:
    return WorkflowDef(
        "order",
        1,
        initial="awaiting_payment",
        states={
            "awaiting_payment": State(
                transitions={"paid": "paid", "cancel": "cancelled"},
                timeout=timedelta(minutes=30),
                timeout_target="cancelled",
            ),
            "paid": State(transitions={"ship": "shipped"}),
            "shipped": State(terminal=True),
            "cancelled": State(terminal=True),
        },
    )


def test_acceptance_invalid_definition() -> None:
    # Unknown state transition
    with pytest.raises(WorkflowError) as exc_info:
        WorkflowDef(
            "test",
            1,
            initial="s1",
            states={"s1": State(transitions={"go": "s2"})},
        )
    assert exc_info.value.code == "invalid_definition"

    # Missing initial state
    with pytest.raises(WorkflowError) as exc_info:
        WorkflowDef(
            "test",
            1,
            initial="missing",
            states={"s1": State(terminal=True)},
        )
    assert exc_info.value.code == "invalid_definition"

    # Terminal state with transitions
    with pytest.raises(WorkflowError) as exc_info:
        WorkflowDef(
            "test",
            1,
            initial="s1",
            states={"s1": State(terminal=True, transitions={"next": "s1"})},
        )
    assert exc_info.value.code == "invalid_definition"

    # Timeout without timeout_target
    with pytest.raises(WorkflowError) as exc_info:
        WorkflowDef(
            "test",
            1,
            initial="s1",
            states={"s1": State(timeout=timedelta(minutes=10))},
        )
    assert exc_info.value.code == "invalid_definition"

    # Timeout target without timeout
    with pytest.raises(WorkflowError) as exc_info:
        WorkflowDef(
            "test",
            1,
            initial="s1",
            states={"s1": State(timeout_target="s2"), "s2": State(terminal=True)},
        )
    assert exc_info.value.code == "invalid_definition"

    # Terminal state with timeout
    with pytest.raises(WorkflowError) as exc_info:
        WorkflowDef(
            "test",
            1,
            initial="s1",
            states={"s1": State(terminal=True, timeout=timedelta(minutes=5), timeout_target="s1")},
        )
    assert exc_info.value.code == "invalid_definition"

    # Invalid name or version
    with pytest.raises(WorkflowError) as exc_info:
        WorkflowDef("", 1, initial="s1", states={"s1": State(terminal=True)})
    assert exc_info.value.code == "invalid_definition"

    with pytest.raises(WorkflowError) as exc_info:
        WorkflowDef("test", 0, initial="s1", states={"s1": State(terminal=True)})
    assert exc_info.value.code == "invalid_definition"

    # Empty states
    with pytest.raises(WorkflowError) as exc_info:
        WorkflowDef("test", 1, initial="s1", states={})
    assert exc_info.value.code == "invalid_definition"


def test_acceptance_start_and_duplicate() -> None:
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    clock, _ = fixed_clock(now)
    store = MemoryStore()
    engine = Engine(store, [order_workflow_def()], clock=clock)

    inst = engine.start("order", "o-1", {"total": 10})
    assert inst.state == "awaiting_payment"
    assert inst.version == 1
    assert inst.deadline == now + timedelta(minutes=30)
    assert inst.data == {"total": 10}

    with pytest.raises(WorkflowError) as exc_info:
        engine.start("order", "o-1", {"total": 10})
    assert exc_info.value.code == "instance_exists"


def test_acceptance_send_transition_and_duplicate_event_id() -> None:
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    clock, advance = fixed_clock(now)
    store = MemoryStore()
    events: list[dict[str, Any]] = []
    engine = Engine(store, [order_workflow_def()], clock=clock, on_event=events.append)

    engine.start("order", "o-1", {"total": 10})

    advance(timedelta(minutes=5))
    now_at_pay = clock()

    res1 = engine.send("o-1", "paid", {}, event_id="e1")
    assert res1.outcome == "applied"
    assert res1.instance.state == "paid"
    assert res1.instance.version == 2
    assert res1.instance.deadline is None
    assert res1.instance.history == (
        HistoryEntry(
            from_state="awaiting_payment",
            to_state="paid",
            event="paid",
            at=now_at_pay,
        ),
    )

    # Sending event_id 'e1' again returns the unchanged instance and outcome 'duplicate'
    res2 = engine.send("o-1", "paid", {}, event_id="e1")
    assert res2.outcome == "duplicate"
    assert res2.instance.version == 2
    assert res2.instance.state == "paid"

    # Even if the event type differs
    res3 = engine.send("o-1", "cancel", {}, event_id="e1")
    assert res3.outcome == "duplicate"
    assert res3.instance.version == 2
    assert res3.instance.state == "paid"


def test_acceptance_invalid_transition_and_not_found() -> None:
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    clock, _ = fixed_clock(now)
    store = MemoryStore()
    engine = Engine(store, [order_workflow_def()], clock=clock)

    engine.start("order", "o-1", {"total": 10})

    # Invalid transition from 'awaiting_payment' (e.g. 'ship')
    with pytest.raises(WorkflowError) as exc_info:
        engine.send("o-1", "ship", {}, event_id="e-inv")
    assert exc_info.value.code == "invalid_transition"

    loaded = store.load("o-1")
    assert loaded is not None
    assert loaded.version == 1
    assert loaded.state == "awaiting_payment"

    # Unknown instance
    with pytest.raises(WorkflowError) as exc_info:
        engine.send("o-unknown", "paid", {}, event_id="e2")
    assert exc_info.value.code == "not_found"


def test_acceptance_terminal_state_lock() -> None:
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    clock, advance = fixed_clock(now)
    store = MemoryStore()
    engine = Engine(store, [order_workflow_def()], clock=clock)

    engine.start("order", "o-1", {})
    engine.send("o-1", "paid", {}, event_id="e1")
    engine.send("o-1", "ship", {}, event_id="e2")

    shipped_instance = store.load("o-1")
    assert shipped_instance is not None
    assert shipped_instance.state == "shipped"
    assert shipped_instance.deadline is None

    with pytest.raises(WorkflowError) as exc_info:
        engine.send("o-1", "paid", {}, event_id="e9")
    assert exc_info.value.code == "terminal_state"

    advance(timedelta(days=10))
    ticked = engine.tick()
    assert ticked == []
    assert store.load("o-1") == shipped_instance


def test_acceptance_tick_timeouts() -> None:
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    clock, advance = fixed_clock(now)
    store = MemoryStore()
    engine = Engine(store, [order_workflow_def()], clock=clock)

    engine.start("order", "o-1", {})

    # Advance 29 minutes: one minute earlier, tick returns []
    advance(timedelta(minutes=29))
    assert engine.tick() == []
    assert store.load("o-1").state == "awaiting_payment"  # type: ignore

    # Advance 1 more minute (total 30 minutes)
    advance(timedelta(minutes=1))
    fired = engine.tick()
    assert len(fired) == 1
    assert fired[0].id == "o-1"
    assert fired[0].state == "cancelled"
    assert fired[0].history[-1].event == "__timeout__"
    assert fired[0].history[-1].from_state == "awaiting_payment"
    assert fired[0].history[-1].to_state == "cancelled"

    # Running tick twice fires once
    fired_again = engine.tick()
    assert fired_again == []


def test_acceptance_tick_ordering_and_limit() -> None:
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    clock, advance = fixed_clock(now)
    store = MemoryStore()
    engine = Engine(store, [order_workflow_def()], clock=clock)

    # Create multiple instances at different times
    engine.start("order", "o-3", {})
    advance(timedelta(seconds=5))
    engine.start("order", "o-1", {})
    advance(timedelta(seconds=5))
    engine.start("order", "o-2", {})

    advance(timedelta(minutes=31))
    # o-3 deadline was at 12:30:00, o-1 at 12:30:05, o-2 at 12:30:10
    batch1 = engine.tick(limit=2)
    assert len(batch1) == 2
    assert [inst.id for inst in batch1] == ["o-3", "o-1"]

    batch2 = engine.tick(limit=2)
    assert len(batch2) == 1
    assert batch2[0].id == "o-2"


@pytest.mark.parametrize(
    "store_factory", [lambda: MemoryStore(), lambda: SqliteStore(sqlite3.connect(":memory:"))]
)
def test_acceptance_concurrency_conflict_and_contract(store_factory: Any) -> None:
    store: Store = store_factory()
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    inst = Instance(
        id="c-1",
        workflow="order",
        def_version=1,
        state="awaiting_payment",
        data={"k": "v"},
        version=1,
        deadline=now,
        history=(),
        seen_event_ids=(),
    )
    store.create(inst)

    # Successful save
    inst.version = 2
    store.save(inst, expected_version=1)

    # Stale save
    inst.version = 3
    with pytest.raises(WorkflowError) as exc_info:
        store.save(inst, expected_version=1)
    assert exc_info.value.code == "concurrency_conflict"

    # Non-existent load
    assert store.load("non-existent") is None

    # Save non-existent
    missing_inst = Instance(
        id="missing",
        workflow="order",
        def_version=1,
        state="awaiting_payment",
        data={},
        version=1,
        deadline=None,
        history=(),
        seen_event_ids=(),
    )
    with pytest.raises(WorkflowError) as exc_info:
        store.save(missing_inst, expected_version=1)
    assert exc_info.value.code == "not_found"

    # Duplicate create
    with pytest.raises(WorkflowError) as exc_info:
        store.create(inst)
    assert exc_info.value.code == "instance_exists"


def test_acceptance_payload_caps_and_history_limits() -> None:
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    clock, _ = fixed_clock(now)
    store = MemoryStore()
    # Workflow with cyclic transitions to test history and seen_events bounds
    wf = WorkflowDef(
        "cycle",
        1,
        initial="a",
        states={
            "a": State(transitions={"to_b": "b"}),
            "b": State(transitions={"to_a": "a"}),
        },
    )
    engine = Engine(store, [wf], clock=clock)
    engine.start("cycle", "c-1", {})

    # Send 120 transitions
    for i in range(120):
        event = "to_b" if i % 2 == 0 else "to_a"
        engine.send("c-1", event, {"step": i}, event_id=f"ev-{i}")

    inst = store.load("c-1")
    assert inst is not None
    assert len(inst.history) == 100
    assert inst.history[0].event == "to_b"  # oldest 20 (i=0..19) dropped, i=20 is to_b
    assert len(inst.seen_event_ids) == 120

    # Over 65_536 bytes data
    large_payload = {"large": "x" * 70_000}
    with pytest.raises(WorkflowError) as exc_info:
        engine.send("c-1", "to_b", large_payload, event_id="ev-overflow")
    assert exc_info.value.code == "data_too_large"

    with pytest.raises(WorkflowError) as exc_info:
        engine.start("cycle", "c-large", large_payload)
    assert exc_info.value.code == "data_too_large"


def test_chained_timers_and_unicode() -> None:
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    clock, advance = fixed_clock(now)
    store = SqliteStore(sqlite3.connect(":memory:"))
    wf = WorkflowDef(
        "chained",
        1,
        initial="step1",
        states={
            "step1": State(timeout=timedelta(minutes=10), timeout_target="step2"),
            "step2": State(timeout=timedelta(minutes=20), timeout_target="step3"),
            "step3": State(terminal=True),
        },
    )
    engine = Engine(store, [wf], clock=clock)
    engine.start("chained", "ch-1", {"user": "René 😀"})

    advance(timedelta(minutes=10))
    ticked1 = engine.tick()
    assert len(ticked1) == 1
    assert ticked1[0].state == "step2"
    assert ticked1[0].data["user"] == "René 😀"
    assert ticked1[0].deadline == now + timedelta(minutes=30)

    # Chained timer should not fire immediately
    assert engine.tick() == []

    advance(timedelta(minutes=20))
    ticked2 = engine.tick()
    assert len(ticked2) == 1
    assert ticked2[0].state == "step3"
    assert ticked2[0].deadline is None


def test_validation_and_clock_edge_cases() -> None:
    # Naive clock raises 'invalid_clock'
    naive_clock = lambda: datetime(2025, 1, 1, 12, 0)  # noqa: E731
    engine = Engine(MemoryStore(), [order_workflow_def()], clock=naive_clock)
    with pytest.raises(WorkflowError) as exc_info:
        engine.start("order", "inst-1")
    assert exc_info.value.code == "invalid_clock"

    # Invalid IDs
    utc_clock = lambda: datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)  # noqa: E731
    engine_utc = Engine(MemoryStore(), [order_workflow_def()], clock=utc_clock)
    with pytest.raises(WorkflowError) as exc_info:
        engine_utc.start("order", "invalid id spaces!")
    assert exc_info.value.code == "invalid_id"

    with pytest.raises(WorkflowError) as exc_info:
        engine_utc.send("o-1", "bad@event", event_id="e1")
    # event with __ prefix
    with pytest.raises(WorkflowError) as exc_info:
        engine_utc.send("o-1", "__reserved", event_id="e1")
    assert exc_info.value.code == "invalid_id"

    # Unknown definition
    with pytest.raises(WorkflowError) as exc_info:
        engine_utc.start("unknown_wf", "inst-1")
    assert exc_info.value.code == "unknown_definition"


def test_on_event_sentinel_and_error_handling() -> None:
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    clock, _ = fixed_clock(now)
    store = MemoryStore()
    events_received: list[dict[str, Any]] = []

    def failing_callback(payload: dict[str, Any]) -> None:
        events_received.append(payload)
        raise RuntimeError("Callback error")

    engine = Engine(store, [order_workflow_def()], clock=clock, on_event=failing_callback)
    engine.start("order", "o-1", {"secret": "do-not-leak"})
    # Event still succeeds despite callback exception
    res = engine.send("o-1", "paid", {"sensitive": "1234"}, event_id="e1")
    assert res.outcome == "applied"
    assert len(events_received) == 1
    payload = events_received[0]
    assert payload == {
        "event": "transition",
        "instance_id": "o-1",
        "from": "awaiting_payment",
        "to": "paid",
        "trigger": "paid",
    }
    # No secrets or instance data in on_event
    assert "secret" not in payload
    assert "sensitive" not in payload
