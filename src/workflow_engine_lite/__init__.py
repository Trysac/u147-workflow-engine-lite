"""trysac-workflow-engine-lite: Durable workflow instances as validated state machines."""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Protocol, runtime_checkable

__all__ = [
    "Engine",
    "ErrorCode",
    "HistoryEntry",
    "Instance",
    "MemoryStore",
    "SendResult",
    "SqliteStore",
    "State",
    "Store",
    "WorkflowDef",
    "WorkflowError",
    "__version__",
]

__version__ = "0.1.0"

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

ErrorCode = Literal[
    "invalid_definition",
    "unknown_definition",
    "instance_exists",
    "not_found",
    "invalid_transition",
    "terminal_state",
    "concurrency_conflict",
    "data_too_large",
    "invalid_id",
    "invalid_clock",
]

MAX_DATA_BYTES = 65_536
MAX_HISTORY_ENTRIES = 100
MAX_SEEN_EVENT_IDS = 1_000

_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,200}$")


class WorkflowError(Exception):
    """Base exception raised by workflow engine operations."""

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(f"[{code}] {message[:200]}")
        self.code: ErrorCode = code
        self.message: str = message[:200]


def _validate_id(identifier: str, kind: str = "identifier") -> None:
    if not isinstance(identifier, str) or not _ID_PATTERN.fullmatch(identifier):
        raise WorkflowError(
            "invalid_id",
            f"Invalid {kind}: must match [A-Za-z0-9_.:-] with length 1-200",
        )


def _validate_event_type(event_type: str) -> None:
    if not isinstance(event_type, str) or len(event_type) < 1 or len(event_type) > 100:
        raise WorkflowError(
            "invalid_id",
            "Invalid event_type: must be 1-100 characters",
        )
    if event_type.startswith("__"):
        raise WorkflowError(
            "invalid_id",
            "Invalid event_type: '__' prefix is reserved",
        )


def _serialize_and_check_data(data: dict[str, Any]) -> str:
    try:
        serialized = json.dumps(data, allow_nan=False, ensure_ascii=False)
    except (TypeError, ValueError) as err:
        raise WorkflowError("data_too_large", f"Invalid data payload: {err}") from err
    if len(serialized.encode("utf-8")) > MAX_DATA_BYTES:
        raise WorkflowError(
            "data_too_large",
            f"Data serialized size exceeds limit of {MAX_DATA_BYTES} bytes",
        )
    return serialized


@dataclass(frozen=True)
class HistoryEntry:
    """A single transition recorded in an instance's history."""

    from_state: str
    to_state: str
    event: str
    at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "from_state": self.from_state,
            "to_state": self.to_state,
            "event": self.event,
            "at": self.at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HistoryEntry:
        return cls(
            from_state=str(data["from_state"]),
            to_state=str(data["to_state"]),
            event=str(data["event"]),
            at=datetime.fromisoformat(data["at"]),
        )


@dataclass(frozen=True)
class State:
    """A single state definition inside a WorkflowDef."""

    transitions: Mapping[str, str] = field(default_factory=dict)
    timeout: timedelta | None = None
    timeout_target: str | None = None
    terminal: bool = False


@dataclass(frozen=True)
class WorkflowDef:
    """Declarative definition of a workflow state machine."""

    name: str
    version: int
    initial: str
    states: Mapping[str, State]

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise WorkflowError("invalid_definition", "Workflow name must be a non-empty string")
        if not isinstance(self.version, int) or self.version <= 0:
            raise WorkflowError("invalid_definition", "Workflow version must be a positive integer")
        if not self.states:
            raise WorkflowError("invalid_definition", "Workflow states must not be empty")
        if self.initial not in self.states:
            raise WorkflowError(
                "invalid_definition",
                f"Initial state '{self.initial}' not found in states",
            )

        for state_name, state in self.states.items():
            if state.terminal:
                if state.transitions:
                    raise WorkflowError(
                        "invalid_definition",
                        f"Terminal state '{state_name}' must not declare transitions",
                    )
                if state.timeout is not None or state.timeout_target is not None:
                    raise WorkflowError(
                        "invalid_definition",
                        f"Terminal state '{state_name}' must not declare timeouts",
                    )
            else:
                if state.timeout is not None and state.timeout_target is None:
                    raise WorkflowError(
                        "invalid_definition",
                        f"State '{state_name}' has timeout but no timeout_target",
                    )
                if state.timeout is None and state.timeout_target is not None:
                    raise WorkflowError(
                        "invalid_definition",
                        f"State '{state_name}' has timeout_target but no timeout",
                    )
                if state.timeout_target is not None and state.timeout_target not in self.states:
                    raise WorkflowError(
                        "invalid_definition",
                        f"State '{state_name}' has unknown timeout_target '{state.timeout_target}'",
                    )

            for tr_event, tr_target in state.transitions.items():
                if tr_target not in self.states:
                    raise WorkflowError(
                        "invalid_definition",
                        f"State '{state_name}' transitions on '{tr_event}' "
                        f"to unknown state '{tr_target}'",
                    )


@dataclass
class Instance:
    """A running or completed workflow instance."""

    id: str
    workflow: str
    def_version: int
    state: str
    data: dict[str, Any]
    version: int
    deadline: datetime | None
    history: tuple[HistoryEntry, ...]
    seen_event_ids: tuple[str, ...]


@dataclass(frozen=True)
class SendResult:
    """Result of sending an event to an instance."""

    instance: Instance
    outcome: Literal["applied", "duplicate"]


@runtime_checkable
class Store(Protocol):
    """Persistence abstraction for workflow instances."""

    def create(self, instance: Instance) -> None:
        """Save a new instance. Fails if instance.id already exists."""
        ...

    def load(self, instance_id: str) -> Instance | None:
        """Load an instance by ID, or None if not found."""
        ...

    def save(self, instance: Instance, *, expected_version: int) -> None:
        """Save an updated instance using optimistic concurrency."""
        ...

    def due(self, now: datetime, limit: int) -> list[str]:
        """Return IDs of non-terminal instances due on or before `now`."""
        ...


class MemoryStore:
    """In-memory implementation of Store."""

    def __init__(self) -> None:
        self._instances: dict[str, Instance] = {}

    def _clone(self, inst: Instance) -> Instance:
        return Instance(
            id=inst.id,
            workflow=inst.workflow,
            def_version=inst.def_version,
            state=inst.state,
            data=dict(inst.data),
            version=inst.version,
            deadline=inst.deadline,
            history=inst.history,
            seen_event_ids=inst.seen_event_ids,
        )

    def create(self, instance: Instance) -> None:
        if instance.id in self._instances:
            raise WorkflowError("instance_exists", f"Instance '{instance.id}' already exists")
        self._instances[instance.id] = self._clone(instance)

    def load(self, instance_id: str) -> Instance | None:
        inst = self._instances.get(instance_id)
        if inst is None:
            return None
        return self._clone(inst)

    def save(self, instance: Instance, *, expected_version: int) -> None:
        existing = self._instances.get(instance.id)
        if existing is None:
            raise WorkflowError("not_found", f"Instance '{instance.id}' not found")
        if existing.version != expected_version:
            raise WorkflowError(
                "concurrency_conflict",
                f"Conflict saving instance '{instance.id}': "
                f"expected version {expected_version}, got {existing.version}",
            )
        self._instances[instance.id] = self._clone(instance)

    def due(self, now: datetime, limit: int) -> list[str]:
        if limit <= 0:
            return []
        candidates: list[tuple[datetime, str]] = []
        for inst in self._instances.values():
            if inst.deadline is not None and inst.deadline <= now:
                candidates.append((inst.deadline, inst.id))
        candidates.sort(key=lambda item: (item[0], item[1]))
        return [inst_id for _, inst_id in candidates[:limit]]


class SqliteStore:
    """SQLite-backed implementation of Store."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self._init_db()

    def _init_db(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS instances (
                    id TEXT PRIMARY KEY,
                    workflow TEXT NOT NULL,
                    def_version INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    deadline TEXT,
                    body TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_instances_deadline
                ON instances(deadline)
                """
            )

    @staticmethod
    def _deadline_to_iso(dt: datetime | None) -> str | None:
        if dt is None:
            return None
        utc_dt = dt.astimezone(timezone.utc)
        return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    @staticmethod
    def _deadline_from_iso(val: str | None) -> datetime | None:
        if val is None:
            return None
        # Python 3.10 fromisoformat does not accept "Z"; the stored form always ends in "Z".
        return datetime.fromisoformat(val.replace("Z", "+00:00")).astimezone(timezone.utc)

    def create(self, instance: Instance) -> None:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM instances WHERE id = ?", (instance.id,))
            if cursor.fetchone() is not None:
                self.conn.execute("ROLLBACK")
                raise WorkflowError("instance_exists", f"Instance '{instance.id}' already exists")

            body = json.dumps(
                {
                    "data": instance.data,
                    "history": [h.to_dict() for h in instance.history],
                    "seen_event_ids": list(instance.seen_event_ids),
                },
                ensure_ascii=False,
            )
            deadline_str = self._deadline_to_iso(instance.deadline)

            cursor.execute(
                """
                INSERT INTO instances (id, workflow, def_version, state, version, deadline, body)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    instance.id,
                    instance.workflow,
                    instance.def_version,
                    instance.state,
                    instance.version,
                    deadline_str,
                    body,
                ),
            )
            self.conn.execute("COMMIT")
        except WorkflowError:
            raise
        except Exception as e:
            self.conn.execute("ROLLBACK")
            raise e

    def load(self, instance_id: str) -> Instance | None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT id, workflow, def_version, state, version, deadline, body
            FROM instances WHERE id = ?
            """,
            (instance_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        i_id, workflow, def_version, state, version, deadline_str, body_str = row
        body = json.loads(body_str)
        history = tuple(HistoryEntry.from_dict(h) for h in body.get("history", []))
        seen_event_ids = tuple(body.get("seen_event_ids", []))
        data = body.get("data", {})
        deadline = self._deadline_from_iso(deadline_str)
        return Instance(
            id=i_id,
            workflow=workflow,
            def_version=def_version,
            state=state,
            data=data,
            version=version,
            deadline=deadline,
            history=history,
            seen_event_ids=seen_event_ids,
        )

    def save(self, instance: Instance, *, expected_version: int) -> None:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT version FROM instances WHERE id = ?", (instance.id,))
            row = cursor.fetchone()
            if row is None:
                self.conn.execute("ROLLBACK")
                raise WorkflowError("not_found", f"Instance '{instance.id}' not found")
            if row[0] != expected_version:
                self.conn.execute("ROLLBACK")
                raise WorkflowError(
                    "concurrency_conflict",
                    f"Conflict saving instance '{instance.id}': "
                    f"expected {expected_version}, got {row[0]}",
                )

            body = json.dumps(
                {
                    "data": instance.data,
                    "history": [h.to_dict() for h in instance.history],
                    "seen_event_ids": list(instance.seen_event_ids),
                },
                ensure_ascii=False,
            )
            deadline_str = self._deadline_to_iso(instance.deadline)

            cursor.execute(
                """
                UPDATE instances
                SET workflow = ?, def_version = ?, state = ?, version = ?, deadline = ?, body = ?
                WHERE id = ? AND version = ?
                """,
                (
                    instance.workflow,
                    instance.def_version,
                    instance.state,
                    instance.version,
                    deadline_str,
                    body,
                    instance.id,
                    expected_version,
                ),
            )
            if cursor.rowcount != 1:
                self.conn.execute("ROLLBACK")
                raise WorkflowError(
                    "concurrency_conflict",
                    f"Conflict saving instance '{instance.id}': rowcount was {cursor.rowcount}",
                )
            self.conn.execute("COMMIT")
        except WorkflowError:
            raise
        except Exception as e:
            self.conn.execute("ROLLBACK")
            raise e

    def due(self, now: datetime, limit: int) -> list[str]:
        if limit <= 0:
            return []
        now_iso = self._deadline_to_iso(now)
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT id FROM instances
            WHERE deadline IS NOT NULL AND deadline <= ?
            ORDER BY deadline ASC, id ASC
            LIMIT ?
            """,
            (now_iso, limit),
        )
        return [row[0] for row in cursor.fetchall()]


class Engine:
    """Workflow engine orchestrating instance lifecycle, events, and timeouts."""

    def __init__(
        self,
        store: Store,
        definitions: Sequence[WorkflowDef],
        *,
        clock: Callable[[], datetime],
        on_event: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._store = store
        self._clock = clock
        self._on_event = on_event
        self._defs: dict[str, dict[int, WorkflowDef]] = {}
        self._latest_defs: dict[str, int] = {}

        for defn in definitions:
            if defn.name not in self._defs:
                self._defs[defn.name] = {}
            self._defs[defn.name][defn.version] = defn
            curr_latest = self._latest_defs.get(defn.name, -1)
            if defn.version > curr_latest:
                self._latest_defs[defn.name] = defn.version

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.tzinfo.utcoffset(now) is None:
            raise WorkflowError(
                "invalid_clock", "Clock returned naive datetime; timezone-aware required"
            )
        return now

    def _get_definition(self, name: str, version: int) -> WorkflowDef:
        wf_versions = self._defs.get(name)
        if not wf_versions or version not in wf_versions:
            raise WorkflowError(
                "unknown_definition",
                f"Workflow '{name}' version {version} not registered",
            )
        return wf_versions[version]

    def start(
        self,
        def_name: str,
        instance_id: str,
        data: dict[str, Any] | None = None,
    ) -> Instance:
        """Start a new instance using the latest registered version of def_name."""
        _validate_id(instance_id, "instance_id")
        if def_name not in self._latest_defs:
            raise WorkflowError("unknown_definition", f"Unknown workflow definition '{def_name}'")
        latest_v = self._latest_defs[def_name]
        defn = self._defs[def_name][latest_v]

        instance_data = dict(data or {})
        _serialize_and_check_data(instance_data)

        now = self._now()
        initial_state = defn.states[defn.initial]

        deadline: datetime | None = None
        if not initial_state.terminal and initial_state.timeout is not None:
            deadline = now + initial_state.timeout

        inst = Instance(
            id=instance_id,
            workflow=def_name,
            def_version=latest_v,
            state=defn.initial,
            data=instance_data,
            version=1,
            deadline=deadline,
            history=(),
            seen_event_ids=(),
        )
        self._store.create(inst)
        return inst

    def send(
        self,
        instance_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
        *,
        event_id: str,
    ) -> SendResult:
        """Send an event to an instance with idempotency check."""
        _validate_id(instance_id, "instance_id")
        _validate_id(event_id, "event_id")
        _validate_event_type(event_type)

        payload_dict = dict(payload or {})
        _serialize_and_check_data(payload_dict)

        return self._send_internal(
            instance_id=instance_id,
            event_type=event_type,
            payload=payload_dict,
            event_id=event_id,
            is_internal_timeout=False,
        )

    def _send_internal(
        self,
        instance_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        event_id: str,
        is_internal_timeout: bool = False,
    ) -> SendResult:
        now = self._now()
        inst = self._store.load(instance_id)
        if inst is None:
            raise WorkflowError("not_found", f"Instance '{instance_id}' not found")

        # Duplicate event check before state checks
        if event_id in inst.seen_event_ids:
            return SendResult(instance=inst, outcome="duplicate")

        defn = self._get_definition(inst.workflow, inst.def_version)
        current_state = defn.states.get(inst.state)
        if current_state is None:
            raise WorkflowError(
                "invalid_definition",
                f"Instance state '{inst.state}' not in workflow definition",
            )

        if current_state.terminal:
            raise WorkflowError(
                "terminal_state",
                f"Cannot transition from terminal state '{inst.state}'",
            )

        if is_internal_timeout:
            target_state_name = current_state.timeout_target
            if target_state_name is None:
                raise WorkflowError(
                    "invalid_transition",
                    f"State '{inst.state}' has no timeout_target for timeout transition",
                )
        else:
            if event_type not in current_state.transitions:
                raise WorkflowError(
                    "invalid_transition",
                    f"Transition '{event_type}' not allowed from state '{inst.state}'",
                )
            target_state_name = current_state.transitions[event_type]

        target_state = defn.states[target_state_name]

        # Prepare merged data
        new_data = dict(inst.data)
        new_data.update(payload)
        _serialize_and_check_data(new_data)

        # Calculate new deadline
        new_deadline: datetime | None = None
        if not target_state.terminal and target_state.timeout is not None:
            new_deadline = now + target_state.timeout

        # Update history and seen events with cap limits
        new_entry = HistoryEntry(
            from_state=inst.state,
            to_state=target_state_name,
            event=event_type,
            at=now,
        )
        new_history = (*inst.history, new_entry)
        if len(new_history) > MAX_HISTORY_ENTRIES:
            new_history = new_history[-MAX_HISTORY_ENTRIES:]

        new_seen_ids = (*inst.seen_event_ids, event_id)
        if len(new_seen_ids) > MAX_SEEN_EVENT_IDS:
            new_seen_ids = new_seen_ids[-MAX_SEEN_EVENT_IDS:]

        updated_instance = Instance(
            id=inst.id,
            workflow=inst.workflow,
            def_version=inst.def_version,
            state=target_state_name,
            data=new_data,
            version=inst.version + 1,
            deadline=new_deadline,
            history=new_history,
            seen_event_ids=new_seen_ids,
        )

        self._store.save(updated_instance, expected_version=inst.version)

        if self._on_event is not None:
            try:
                self._on_event(
                    {
                        "event": "transition",
                        "instance_id": inst.id,
                        "from": inst.state,
                        "to": target_state_name,
                        "trigger": event_type,
                    }
                )
            except Exception:
                logger.exception("on_event callback raised an exception")

        return SendResult(instance=updated_instance, outcome="applied")

    def tick(self, *, limit: int = 100) -> list[Instance]:
        """Advance expired timeouts for due instances."""
        now = self._now()
        due_ids = self._store.due(now, limit)
        transitioned: list[Instance] = []

        for inst_id in due_ids:
            inst = self._store.load(inst_id)
            if inst is None:
                continue
            if inst.deadline is None or inst.deadline > now:
                continue

            defn = self._defs.get(inst.workflow, {}).get(inst.def_version)
            if defn is None:
                continue
            curr_state = defn.states.get(inst.state)
            if curr_state is None or curr_state.terminal or curr_state.timeout_target is None:
                continue

            event_id = f"__timeout__:{inst.version}"
            try:
                result = self._send_internal(
                    instance_id=inst.id,
                    event_type="__timeout__",
                    payload={},
                    event_id=event_id,
                    is_internal_timeout=True,
                )
                if result.outcome == "applied":
                    transitioned.append(result.instance)
            except WorkflowError as e:
                if e.code in ("concurrency_conflict", "terminal_state", "not_found"):
                    continue
                raise

        return transitioned
