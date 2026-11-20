"""SQLite-backed trace collection and tail-sampling decisions.

Semantics implemented here:

* A trace becomes *active* when its first span is accepted.  Its collection
  deadline is ``first_seen_at + window_seconds`` and never moves.
* At the deadline the trace is decided in one transaction: all spans are kept
  if any accepted span in the window had ERROR status, or if a root span took
  longer than ``slow_root_ms``; otherwise the raw payloads are purged.
* Dropping never loses the ability to audit a chain: each admitted span keeps
  its identity (trace/span/parent id, name, service) and its OTLP links, so a
  lookup can still follow links through dropped traces and verify whether a
  target was admitted.  Only the raw span JSON, timings, status and scope are
  gone.
* Evidence recording is part of the decision transaction.  If it fails, the
  decision is rolled back: the deadline stays in force, raw spans are kept and
  the sweep retries later.
* Spans arriving after the deadline are rejected and can never change a
  decision.  Duplicate ``(trace_id, span_id)`` pairs are stored once.
* All state lives in SQLite, so a restart keeps the original deadlines and the
  retained span identities.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from .config import Settings
from .otlp import ParsedSpan

STATE_COLLECTING = "collecting"
STATE_KEPT = "kept"
STATE_DROPPED = "dropped"
ALL_STATES = (STATE_COLLECTING, STATE_KEPT, STATE_DROPPED)

# Link-lookup outcomes.  The names are stable API values; the UI maps them to
# Chinese labels.
LOOKUP_RAW_AVAILABLE = "raw_available"
LOOKUP_RAW_DISCARDED = "admitted_raw_discarded"
LOOKUP_NOT_ADMITTED = "not_admitted"
LOOKUP_PENDING = "pending"
LOOKUP_UNVERIFIABLE = "unverifiable"
LOOKUP_CYCLE = "cycle"
LOOKUP_STATUSES = (
    LOOKUP_RAW_AVAILABLE,
    LOOKUP_RAW_DISCARDED,
    LOOKUP_NOT_ADMITTED,
    LOOKUP_PENDING,
    LOOKUP_UNVERIFIABLE,
    LOOKUP_CYCLE,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS traces (
    trace_id TEXT PRIMARY KEY,
    first_seen_at REAL NOT NULL,
    deadline_at REAL NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('collecting', 'kept', 'dropped')),
    decided_at REAL,
    decision_reason TEXT,
    has_error INTEGER NOT NULL DEFAULT 0,
    root_duration_ns INTEGER,
    root_span_id TEXT,
    span_count INTEGER NOT NULL DEFAULT 0,
    service TEXT,
    identity_retained INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_traces_state_deadline
    ON traces (state, deadline_at);
CREATE INDEX IF NOT EXISTS idx_traces_first_seen
    ON traces (first_seen_at DESC);

-- ``body`` is NULL once a dropped trace's raw payload has been purged; the
-- remaining columns plus ``links`` are the audit evidence that survives.
CREATE TABLE IF NOT EXISTS spans (
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    name TEXT NOT NULL,
    service TEXT,
    scope TEXT,
    start_ns INTEGER,
    end_ns INTEGER,
    duration_ns INTEGER,
    is_root INTEGER NOT NULL DEFAULT 0,
    is_error INTEGER NOT NULL DEFAULT 0,
    body TEXT,
    links TEXT NOT NULL DEFAULT '[]',
    received_at REAL NOT NULL,
    PRIMARY KEY (trace_id, span_id),
    FOREIGN KEY (trace_id) REFERENCES traces (trace_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans (trace_id);
"""

_TRACE_COLUMNS = (
    "trace_id, state, deadline_at, root_duration_ns, root_span_id, service, "
    "identity_retained"
)


class EvidenceError(RuntimeError):
    """Retained span evidence could not be recorded for a drop decision."""


logger = logging.getLogger("collector.store")


@dataclass
class IngestResult:
    accepted: int = 0
    duplicates: int = 0
    rejected_capacity: int = 0
    rejected_late: int = 0
    invalid: int = 0
    admitted_traces: list[str] = field(default_factory=list)
    decided: list[str] = field(default_factory=list)

    @property
    def rejected(self) -> int:
        return self.rejected_capacity + self.rejected_late + self.invalid

    def rejection_message(self) -> str:
        parts = []
        if self.rejected_capacity:
            parts.append(f"{self.rejected_capacity} span(s) rejected: trace capacity full")
        if self.rejected_late:
            parts.append(
                f"{self.rejected_late} span(s) rejected: collection window closed"
            )
        if self.invalid:
            parts.append(f"{self.invalid} span(s) rejected: invalid trace/span id")
        return "; ".join(parts)


class SqliteStore:
    """All trace state, serialized through one in-process lock.

    The app runs as a single process (one uvicorn worker); the lock together
    with ``BEGIN IMMEDIATE`` makes capacity checks and decisions race-free.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = threading.RLock()
        self._path = Path(settings.db_path)
        # Trace id -> last evidence failure message.  Kept in memory so the
        # operator can see why a due decision is being retried.
        self.settle_failures: dict[str, str] = {}
        if str(self._path) != ":memory:":
            self._path.parent.mkdir(parents=True, exist_ok=True)

    # -- connection helpers -------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._path), timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def init_schema(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(SCHEMA)
                self._migrate(conn)
            finally:
                conn.close()

    def _migrate(self, conn: sqlite3.Connection) -> None:
        """Bring databases created by an older build up to the current schema.

        Old ``spans.body`` was ``NOT NULL`` and old drops deleted every span
        row.  Those traces keep ``identity_retained = 0``, which the link
        lookup reports as unverifiable rather than guessing.
        """
        trace_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(traces)")
        }
        if "identity_retained" not in trace_columns:
            conn.execute(
                "ALTER TABLE traces ADD COLUMN identity_retained "
                "INTEGER NOT NULL DEFAULT 0"
            )
        span_columns = {
            row["name"]: row for row in conn.execute("PRAGMA table_info(spans)")
        }
        if "links" not in span_columns:
            conn.execute(
                "ALTER TABLE spans ADD COLUMN links TEXT NOT NULL DEFAULT '[]'"
            )
        if span_columns.get("body") is not None and span_columns["body"]["notnull"]:
            # body must become nullable so a drop can clear the raw payload
            # while keeping the span identity as evidence.
            conn.execute("PRAGMA foreign_keys=OFF")
            try:
                with self._tx(conn):
                    conn.execute(
                        "CREATE TABLE spans_migrated ("
                        "trace_id TEXT NOT NULL, span_id TEXT NOT NULL, "
                        "parent_span_id TEXT, name TEXT NOT NULL, service TEXT, "
                        "scope TEXT, start_ns INTEGER, end_ns INTEGER, "
                        "duration_ns INTEGER, is_root INTEGER NOT NULL DEFAULT 0, "
                        "is_error INTEGER NOT NULL DEFAULT 0, body TEXT, "
                        "links TEXT NOT NULL DEFAULT '[]', "
                        "received_at REAL NOT NULL, "
                        "PRIMARY KEY (trace_id, span_id), "
                        "FOREIGN KEY (trace_id) REFERENCES traces (trace_id) "
                        "ON DELETE CASCADE)"
                    )
                    conn.execute(
                        "INSERT INTO spans_migrated (trace_id, span_id, "
                        "parent_span_id, name, service, scope, start_ns, end_ns, "
                        "duration_ns, is_root, is_error, body, links, received_at) "
                        "SELECT trace_id, span_id, parent_span_id, name, service, "
                        "scope, start_ns, end_ns, duration_ns, is_root, is_error, "
                        "body, links, received_at FROM spans"
                    )
                    conn.execute("DROP TABLE spans")
                    conn.execute("ALTER TABLE spans_migrated RENAME TO spans")
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_spans_trace "
                        "ON spans (trace_id)"
                    )
            finally:
                conn.execute("PRAGMA foreign_keys=ON")

    @contextmanager
    def _tx(self, conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    # -- decision logic -----------------------------------------------------

    def _decide(self, row: sqlite3.Row) -> tuple[str, str]:
        """Return ``(state, reason)`` for an expiring trace."""
        reasons = []
        if row["has_error"]:
            reasons.append("error")
        root_duration = row["root_duration_ns"]
        if root_duration is not None and root_duration > self.settings.slow_root_ns:
            reasons.append("slow_root")
        if reasons:
            return STATE_KEPT, "+".join(reasons)
        return STATE_DROPPED, "clean"

    def _settle_conn(self, conn: sqlite3.Connection, now: float) -> list[str]:
        """Decide every expired trace inside the caller's transaction.

        A drop decision first writes the audit evidence (span identities and
        links with the raw body cleared).  If that write fails, the exception
        rolls back the whole transaction -- including the state change -- so
        the trace stays ``collecting`` with its raw spans intact and the sweep
        retries after the next interval.
        """
        rows = conn.execute(
            "SELECT trace_id, deadline_at, has_error, root_duration_ns, span_count "
            "FROM traces WHERE state = ? AND deadline_at <= ? "
            "ORDER BY deadline_at, trace_id",
            (STATE_COLLECTING, now),
        ).fetchall()
        decided: list[str] = []
        for row in rows:
            trace_id = row["trace_id"]
            state, reason = self._decide(row)
            # One savepoint per trace: a failed evidence write undoes only
            # this trace's decision, leaving it collecting for a retry.
            conn.execute("SAVEPOINT settle_one")
            try:
                self._settle_one(conn, row, state, reason)
            except (EvidenceError, sqlite3.Error) as exc:
                conn.execute("ROLLBACK TO SAVEPOINT settle_one")
                conn.execute("RELEASE SAVEPOINT settle_one")
                message = str(exc) or exc.__class__.__name__
                self.settle_failures[trace_id] = message
                logger.warning(
                    "decision deferred for %s; raw spans retained: %s",
                    trace_id,
                    message,
                )
                continue
            conn.execute("RELEASE SAVEPOINT settle_one")
            self.settle_failures.pop(trace_id, None)
            decided.append(trace_id)
        return decided

    def _settle_one(
        self, conn: sqlite3.Connection, row: sqlite3.Row, state: str, reason: str
    ) -> None:
        trace_id = row["trace_id"]
        if state == STATE_DROPPED:
            try:
                conn.execute(
                    "UPDATE spans SET body = NULL, start_ns = NULL, end_ns = NULL, "
                    "duration_ns = NULL, scope = NULL, is_error = 0 "
                    "WHERE trace_id = ?",
                    (trace_id,),
                )
            except sqlite3.Error as exc:  # pragma: no cover - defensive
                raise EvidenceError(f"evidence write failed: {exc}") from exc
            # Every admitted span must leave evidence behind; a missing row
            # would make a later lookup claim "never admitted".
            recorded = conn.execute(
                "SELECT COUNT(*) FROM spans WHERE trace_id = ?", (trace_id,)
            ).fetchone()[0]
            if recorded != row["span_count"]:
                raise EvidenceError(
                    "evidence write failed: "
                    f"{recorded} of {row['span_count']} span(s) still recorded"
                )
        updated = conn.execute(
            "UPDATE traces SET state = ?, decision_reason = ?, decided_at = ?, "
            "identity_retained = ? WHERE trace_id = ? AND state = ?",
            (
                state,
                reason,
                row["deadline_at"],
                int(state == STATE_DROPPED),
                trace_id,
                STATE_COLLECTING,
            ),
        ).rowcount
        if not updated:
            raise EvidenceError("decision race: trace is no longer collecting")

    def settle_expired(self, now: float) -> list[str]:
        """Apply every due decision; used by the background sweeper."""
        with self._lock:
            conn = self._connect()
            try:
                with self._tx(conn):
                    return self._settle_conn(conn, now)
            finally:
                conn.close()

    # -- ingestion ----------------------------------------------------------

    def ingest(
        self, spans: Sequence[ParsedSpan], now: float, invalid: int = 0
    ) -> IngestResult:
        result = IngestResult(invalid=invalid)
        with self._lock:
            conn = self._connect()
            try:
                with self._tx(conn):
                    # Settling first frees slots due at this instant and makes
                    # the decision visible before admitting new work.
                    result.decided = self._settle_conn(conn, now)
                    active = self._count_collecting(conn)
                    for span in spans:
                        admitted = self._ingest_one(conn, span, now, active, result)
                        if admitted:
                            active += 1
            finally:
                conn.close()
        return result

    def _count_collecting(self, conn: sqlite3.Connection) -> int:
        return conn.execute(
            "SELECT COUNT(*) FROM traces WHERE state = ?", (STATE_COLLECTING,)
        ).fetchone()[0]

    def _ingest_one(
        self,
        conn: sqlite3.Connection,
        span: ParsedSpan,
        now: float,
        active: int,
        result: IngestResult,
    ) -> bool:
        """Store one span.  Returns True when a new trace was admitted."""
        trace = conn.execute(
            f"SELECT {_TRACE_COLUMNS} FROM traces WHERE trace_id = ?",
            (span.trace_id,),
        ).fetchone()

        admitted = False
        if trace is None:
            if active >= self.settings.max_active_traces:
                result.rejected_capacity += 1
                return False
            conn.execute(
                "INSERT INTO traces (trace_id, first_seen_at, deadline_at, state) "
                "VALUES (?, ?, ?, ?)",
                (
                    span.trace_id,
                    now,
                    now + self.settings.window_seconds,
                    STATE_COLLECTING,
                ),
            )
            trace = conn.execute(
                f"SELECT {_TRACE_COLUMNS} FROM traces WHERE trace_id = ?",
                (span.trace_id,),
            ).fetchone()
            result.admitted_traces.append(span.trace_id)
            admitted = True

        if trace["state"] != STATE_COLLECTING or now >= trace["deadline_at"]:
            # Trace is kept or dropped: its window is closed, the span is late.
            # The deadline check also covers a trace whose decision is deferred
            # because evidence recording failed: the window closed at the
            # deadline, so later spans must not influence the retried decision.
            result.rejected_late += 1
            return admitted

        cursor = conn.execute(
            "INSERT OR IGNORE INTO spans ("
            "trace_id, span_id, parent_span_id, name, service, scope, "
            "start_ns, end_ns, duration_ns, is_root, is_error, body, links, "
            "received_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                span.trace_id,
                span.span_id,
                span.parent_span_id,
                span.name,
                span.service,
                span.scope,
                span.start_ns,
                span.end_ns,
                span.duration_ns,
                int(span.is_root),
                int(span.is_error),
                span.body,
                json.dumps(
                    [{"trace_id": l.trace_id, "span_id": l.span_id} for l in span.links],
                    separators=(",", ":"),
                ),
                now,
            ),
        )
        if cursor.rowcount == 0:
            result.duplicates += 1
            return admitted

        sql = "UPDATE traces SET span_count = span_count + 1"
        params: list[Any] = []
        if span.is_error:
            # Any accepted error span wins for the lifetime of the trace.
            sql += ", has_error = 1"
        if trace["service"] is None and span.service:
            sql += ", service = ?"
            params.append(span.service)
        if span.is_root and span.duration_ns is not None:
            current = trace["root_duration_ns"]
            if current is None or span.duration_ns > current:
                sql += ", root_duration_ns = ?, root_span_id = ?"
                params.extend([span.duration_ns, span.span_id])
        sql += " WHERE trace_id = ?"
        params.append(span.trace_id)
        conn.execute(sql, params)
        result.accepted += 1
        return admitted

    # -- reads --------------------------------------------------------------

    def settle_and_get(self, trace_id: str, now: float) -> dict[str, Any] | None:
        with self._lock:
            conn = self._connect()
            try:
                with self._tx(conn):
                    decided = self._settle_conn(conn, now)
                row = conn.execute(
                    "SELECT * FROM traces WHERE trace_id = ?", (trace_id,)
                ).fetchone()
                if row is None:
                    return None
                spans = conn.execute(
                    "SELECT span_id, parent_span_id, name, service, scope, "
                    "start_ns, end_ns, duration_ns, is_root, is_error, body, "
                    "links, received_at "
                    "FROM spans WHERE trace_id = ? "
                    "ORDER BY (start_ns IS NULL), start_ns, span_id",
                    (trace_id,),
                ).fetchall()
                payload = _trace_dict(row, spans, now, self.settings)
                payload["just_decided"] = trace_id in decided
                failure = self.settle_failures.get(trace_id)
                if failure:
                    payload["settle_failure"] = failure
                return payload
            finally:
                conn.close()

    # -- link lookup --------------------------------------------------------

    def resolve_links(
        self,
        trace_id: str,
        span_id: str,
        now: float,
        max_depth: int | None = None,
        max_nodes: int | None = None,
    ) -> dict[str, Any] | None:
        """Follow OTLP links hop by hop, starting at one retained span.

        Every read happens inside a single ``BEGIN DEFERRED`` transaction so
        the whole walk sees one committed snapshot, even if the background
        sweeper decides traces concurrently.  Due traces are settled first;
        that write is committed before the snapshot begins.

        Returns ``None`` when the starting span is unknown to the collector.
        """
        with self._lock:
            conn = self._connect()
            try:
                with self._tx(conn):
                    self._settle_conn(conn, now)
                # Read snapshot: every hop of this lookup sees the same
                # committed data even if the sweeper decides traces meanwhile.
                conn.execute("BEGIN DEFERRED")
                try:
                    source = self._lookup_node(conn, trace_id, span_id, now)
                    if source is None or source["span"] is None:
                        # The operator must start from a span the collector
                        # actually recorded; anything else is a bad request,
                        # not a link target that may still arrive later.
                        return None
                    return self._walk_links(
                        conn,
                        source,
                        now,
                        max_depth if max_depth is not None
                        else self.settings.link_lookup_max_depth,
                        max_nodes if max_nodes is not None
                        else self.settings.link_lookup_max_nodes,
                    )
                except BaseException:
                    if conn.in_transaction:
                        conn.execute("ROLLBACK")
                    raise
                else:
                    if conn.in_transaction:
                        conn.execute("COMMIT")
            finally:
                conn.close()

    def _lookup_node(
        self, conn: sqlite3.Connection, trace_id: str, span_id: str, now: float
    ) -> dict[str, Any] | None:
        """Resolve one (trace, span) node to its audit status.

        ``None`` means the trace does not exist at all.  The caller decides
        whether that is "pending" (may still be in flight).
        """
        trace = conn.execute(
            "SELECT trace_id, state, decision_reason, deadline_at, "
            "identity_retained FROM traces WHERE trace_id = ?",
            (trace_id,),
        ).fetchone()
        span = conn.execute(
            "SELECT span_id, parent_span_id, name, service, body, links, "
            "received_at FROM spans WHERE trace_id = ? AND span_id = ?",
            (trace_id, span_id),
        ).fetchone()

        if trace is None:
            status = LOOKUP_PENDING
        elif trace["state"] == STATE_COLLECTING:
            status = LOOKUP_PENDING
        elif span is not None:
            status = (
                LOOKUP_RAW_AVAILABLE if span["body"] is not None
                else LOOKUP_RAW_DISCARDED
            )
        elif trace["state"] == STATE_KEPT or trace["identity_retained"]:
            # A kept trace retains every admitted span's raw payload, and a
            # drop with retained identities keeps a row for every admitted
            # span -- so a missing span row is proof it was never admitted.
            status = LOOKUP_NOT_ADMITTED
        else:
            # Dropped before span identities were retained: neither the raw
            # span nor proof of (non-)admission survived, so it cannot be
            # verified either way.
            status = LOOKUP_UNVERIFIABLE

        node: dict[str, Any] = {
            "trace_id": trace_id,
            "span_id": span_id,
            "status": status,
            "trace": (
                {
                    "trace_id": trace["trace_id"],
                    "state": trace["state"],
                    "decision_reason": trace["decision_reason"],
                    "identity_retained": bool(trace["identity_retained"]),
                    "deadline_at": trace["deadline_at"],
                    "remaining_seconds": (
                        max(0.0, trace["deadline_at"] - now)
                        if trace["state"] == STATE_COLLECTING
                        else 0.0
                    ),
                }
                if trace is not None
                else None
            ),
            "span": (
                {
                    "trace_id": trace_id,
                    "span_id": span["span_id"],
                    "parent_span_id": span["parent_span_id"],
                    "name": span["name"],
                    "service": span["service"],
                    "received_at": span["received_at"],
                    "links": _decode_links(span["links"]),
                }
                if span is not None
                else None
            ),
        }
        return node

    def _walk_links(
        self,
        conn: sqlite3.Connection,
        source: dict[str, Any],
        now: float,
        max_depth: int,
        max_nodes: int,
    ) -> dict[str, Any]:
        """Depth-first walk over ``links`` with ancestry-based cycle checks."""
        nodes: dict[tuple[str, str], dict[str, Any]] = {
            (source["trace_id"], source["span_id"]): source
        }
        hops: list[dict[str, Any]] = []
        cycles: list[dict[str, Any]] = []
        expanded: set[tuple[str, str]] = set()
        truncated = False

        # Explicit stack so deep link chains cannot exhaust the Python stack.
        # Each frame is (node_key, depth, ancestor_frozenset).
        stack: list[tuple[tuple[str, str], int, frozenset[tuple[str, str]]]] = [
            ((source["trace_id"], source["span_id"]), 0, frozenset())
        ]
        while stack:
            key, depth, ancestors = stack.pop()
            if key in expanded:
                continue
            expanded.add(key)
            node = nodes[key]
            if node["span"] is None:
                continue  # unknown/late span: no outgoing links known
            if depth >= max_depth:
                truncated = truncated or bool(node["span"]["links"])
                continue
            child_ancestors = ancestors | {key}
            children: list[tuple[str, str]] = []
            for link in node["span"]["links"]:
                child_key = (link["trace_id"], link["span_id"])
                if child_key in nodes:
                    child = nodes[child_key]
                else:
                    if len(nodes) >= max_nodes:
                        truncated = True
                        continue
                    child = self._lookup_node(
                        conn, link["trace_id"], link["span_id"], now
                    )
                    if child is None:
                        child = {
                            "trace_id": link["trace_id"],
                            "span_id": link["span_id"],
                            "status": LOOKUP_PENDING,
                            "trace": None,
                            "span": None,
                        }
                    nodes[child_key] = child
                hop: dict[str, Any] = {
                    "depth": depth + 1,
                    "from": {"trace_id": key[0], "span_id": key[1]},
                    "link": dict(link),
                    "target": child,
                }
                hops.append(hop)
                if child_key in child_ancestors:
                    hop["cycle"] = True
                    hop["cycle_to"] = {
                        "trace_id": child_key[0],
                        "span_id": child_key[1],
                    }
                    cycles.append(hop)
                else:
                    hop["cycle"] = False
                    children.append(child_key)
            # LIFO stack reversed so the first link is expanded first.
            for child_key in reversed(children):
                stack.append((child_key, depth + 1, child_ancestors))

        return {
            "source": source,
            "hops": hops,
            "cycles": cycles,
            "pending": [n for n in nodes.values() if n["status"] == LOOKUP_PENDING],
            "truncated": truncated,
            "now": now,
        }

    def list_traces(
        self, now: float, limit: int = 50, state: str | None = None
    ) -> dict[str, Any]:
        limit = max(1, min(limit, 500))
        if state is not None and state not in ALL_STATES:
            raise ValueError("unknown state filter")
        with self._lock:
            conn = self._connect()
            try:
                with self._tx(conn):
                    self._settle_conn(conn, now)
                sql = (
                    "SELECT t.*, "
                    "(SELECT COUNT(*) FROM spans s WHERE s.trace_id = t.trace_id "
                    "AND s.body IS NOT NULL) AS retained_spans, "
                    "(SELECT COUNT(*) FROM spans s WHERE s.trace_id = t.trace_id) "
                    "AS evidence_spans FROM traces t"
                )
                params: list[Any] = []
                if state is not None:
                    sql += " WHERE t.state = ?"
                    params.append(state)
                sql += " ORDER BY t.first_seen_at DESC, t.trace_id LIMIT ?"
                params.append(limit)
                rows = conn.execute(sql, params).fetchall()
                counts = {
                    row["state"]: row["count"]
                    for row in conn.execute(
                        "SELECT state, COUNT(*) AS count FROM traces GROUP BY state"
                    ).fetchall()
                }
                return {
                    "traces": [
                        _trace_summary_dict(row, now, self.settings) for row in rows
                    ],
                    "counts": {name: counts.get(name, 0) for name in ALL_STATES},
                    "capacity": self.settings.max_active_traces,
                    "now": now,
                }
            finally:
                conn.close()


def _trace_summary_dict(
    row: sqlite3.Row, now: float, settings: Settings
) -> dict[str, Any]:
    remaining = 0.0
    if row["state"] == STATE_COLLECTING:
        remaining = max(0.0, row["deadline_at"] - now)
    root_duration_ns = row["root_duration_ns"]
    keys = row.keys()
    return {
        "trace_id": row["trace_id"],
        "state": row["state"],
        "first_seen_at": row["first_seen_at"],
        "deadline_at": row["deadline_at"],
        "remaining_seconds": remaining,
        "decided_at": row["decided_at"],
        "decision_reason": row["decision_reason"],
        "has_error": bool(row["has_error"]),
        "root_duration_ns": root_duration_ns,
        "root_duration_ms": (
            root_duration_ns / 1_000_000 if root_duration_ns is not None else None
        ),
        "span_count": row["span_count"],
        "retained_spans": (
            row["retained_spans"] if "retained_spans" in keys else None
        ),
        "evidence_spans": (
            row["evidence_spans"] if "evidence_spans" in keys else None
        ),
        "identity_retained": bool(row["identity_retained"]),
        "service": row["service"],
        "window_seconds": settings.window_seconds,
        "slow_root_ms": settings.slow_root_ms,
    }


def _decode_links(raw: Any) -> list[dict[str, str]]:
    try:
        value = json.loads(raw) if raw else []
    except (TypeError, ValueError):
        return []
    if not isinstance(value, list):
        return []
    return [
        {"trace_id": link["trace_id"], "span_id": link["span_id"]}
        for link in value
        if isinstance(link, dict)
        and isinstance(link.get("trace_id"), str)
        and isinstance(link.get("span_id"), str)
    ]


def _trace_dict(
    row: sqlite3.Row,
    spans: Iterable[sqlite3.Row],
    now: float,
    settings: Settings,
) -> dict[str, Any]:
    span_list = list(spans)
    payload = _trace_summary_dict(row, now, settings)
    payload["retained_spans"] = sum(1 for s in span_list if s["body"] is not None)
    payload["evidence_spans"] = len(span_list)
    payload["spans"] = [
        {
            "span_id": span["span_id"],
            "parent_span_id": span["parent_span_id"],
            "name": span["name"],
            "service": span["service"],
            "scope": span["scope"],
            "start_ns": span["start_ns"],
            "end_ns": span["end_ns"],
            "duration_ns": span["duration_ns"],
            "duration_ms": (
                span["duration_ns"] / 1_000_000
                if span["duration_ns"] is not None
                else None
            ),
            "is_root": bool(span["is_root"]),
            "is_error": bool(span["is_error"]),
            "received_at": span["received_at"],
            "body": span["body"],
            "links": _decode_links(span["links"]),
        }
        for span in span_list
    ]
    return payload
