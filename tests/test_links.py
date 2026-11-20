"""Tests for link-based trace lookup and drop-time evidence retention."""

from __future__ import annotations

import json
import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.store import EvidenceError, SqliteStore


class Clock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def clock() -> Clock:
    return Clock()


def make_settings(tmp_path, clock, **overrides) -> Settings:
    values = dict(
        db_path=str(tmp_path / "collector.db"),
        window_seconds=5.0,
        max_active_traces=8,
        slow_root_ms=500.0,
        sweep_interval_seconds=0.05,
        clock=clock,
    )
    values.update(overrides)
    return Settings(**values)


@pytest.fixture()
def app(tmp_path, clock):
    return create_app(make_settings(tmp_path, clock))


@pytest.fixture()
def client(app):
    with TestClient(app) as test_client:
        yield test_client


def tid(n: int) -> str:
    return f"{n:032x}"


def sid(n: int) -> str:
    return f"{n:016x}"


def span(
    trace_id: str,
    span_id: str,
    *,
    parent: str | None = None,
    name: str = "op",
    duration_ms: float = 10.0,
    error: bool = False,
    links: list[tuple[str, str]] | None = None,
) -> dict:
    start = 1_700_000_000_000_000_000
    end = start + int(duration_ms * 1_000_000)
    raw = {
        "traceId": trace_id,
        "spanId": span_id,
        "name": name,
        "kind": 2,
        "startTimeUnixNano": str(start),
        "endTimeUnixNano": str(end),
        "status": {"code": 2 if error else 0},
    }
    if parent:
        raw["parentSpanId"] = parent
    if links:
        raw["links"] = [{"traceId": t, "spanId": s} for t, s in links]
    return raw


def otlp(*spans: dict, service: str = "svc") -> dict:
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": service}}
                    ]
                },
                "scopeSpans": [{"scope": {"name": "test"}, "spans": list(spans)}],
            }
        ]
    }


def post(client, payload):
    response = client.post(
        "/v1/traces",
        content=json.dumps(payload),
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 200, response.text
    return response.json()


def lookup(client, t: str, s: str) -> dict:
    response = client.get(f"/api/traces/{t}/spans/{s}/links")
    assert response.status_code == 200, response.text
    return response.json()


def hop_map(result: dict) -> dict[tuple[str, str], dict]:
    return {
        (h["target"]["trace_id"], h["target"]["span_id"]): h for h in result["hops"]
    }


# --------------------------------------------------------------------------
# intake: links are parsed, deduplicated and normalised
# --------------------------------------------------------------------------


def test_links_are_stored_with_canonical_ids(client, clock):
    upper = tid(1).upper()
    post(
        client,
        otlp(span(tid(1), sid(1), links=[(upper, sid(2).upper()), (tid(2), sid(9))])),
    )
    data = client.get(f"/api/traces/{tid(1)}").json()
    links = data["spans"][0]["links"]
    assert links == [
        {"trace_id": tid(1), "span_id": sid(2)},
        {"trace_id": tid(2), "span_id": sid(9)},
    ]


def test_incomplete_links_are_skipped_not_fatal(client):
    raw = span(tid(1), sid(1))
    raw["links"] = [
        {"traceId": tid(1)},  # no span id
        {"traceId": "zz", "spanId": sid(2)},  # bad hex
        "not-an-object",
        {"traceId": tid(1), "spanId": sid(3)},
    ]
    post(client, otlp(raw))
    links = client.get(f"/api/traces/{tid(1)}").json()["spans"][0]["links"]
    assert links == [{"trace_id": tid(1), "span_id": sid(3)}]


def test_duplicate_span_keeps_first_links(client, clock):
    post(client, otlp(span(tid(1), sid(1), links=[(tid(2), sid(2))])))
    post(client, otlp(span(tid(1), sid(1), links=[(tid(3), sid(3))])))
    data = client.get(f"/api/traces/{tid(1)}").json()
    assert data["span_count"] == 1
    assert data["spans"][0]["links"] == [{"trace_id": tid(2), "span_id": sid(2)}]


# --------------------------------------------------------------------------
# lookup statuses
# --------------------------------------------------------------------------


def test_kept_target_reports_raw_available(client, clock):
    # Target chain is kept (error).
    post(client, otlp(span(tid(2), sid(1), error=True)))
    # Source chain links to the kept span; both settle and are kept via the
    # source's own error so the source span also survives.
    clock.advance(1)
    post(client, otlp(span(tid(1), sid(1), error=True, links=[(tid(2), sid(1))])))
    clock.advance(5.0)

    result = lookup(client, tid(1), sid(1))
    assert result["source"]["status"] == "raw_available"
    hop = hop_map(result)[(tid(2), sid(1))]
    assert hop["target"]["status"] == "raw_available"
    assert hop["target"]["trace"]["state"] == "kept"
    assert result["cycles"] == []
    assert result["pending"] == []


def test_dropped_target_is_admitted_but_raw_gone(client, clock):
    # Target chain is clean -> dropped, raw purged, identity retained.
    post(client, otlp(span(tid(2), sid(1)), span(tid(2), sid(2), parent=sid(1))))
    clock.advance(5.0)
    # Source chain is kept so we can start a lookup from retained data.
    post(client, otlp(span(tid(1), sid(1), error=True, links=[(tid(2), sid(1))])))
    clock.advance(5.0)
    assert client.get(f"/api/traces/{tid(2)}").json()["state"] == "dropped"

    result = lookup(client, tid(1), sid(1))
    target = hop_map(result)[(tid(2), sid(1))]["target"]
    assert target["status"] == "admitted_raw_discarded"
    assert target["trace"]["state"] == "dropped"
    assert target["span"]["name"] == "op"
    assert target["span"]["links"] == []


def test_target_never_admitted_reports_not_admitted(client, clock):
    post(client, otlp(span(tid(2), sid(1), error=True)))
    clock.advance(5.0)  # tid(2) is kept, but span sid(7) never existed
    post(client, otlp(span(tid(1), sid(1), error=True, links=[(tid(2), sid(7))])))
    clock.advance(5.0)

    hop = hop_map(lookup(client, tid(1), sid(1)))[(tid(2), sid(7))]
    assert hop["target"]["status"] == "not_admitted"
    assert hop["target"]["trace"]["state"] == "kept"


def test_unknown_trace_target_is_pending(client, clock):
    post(client, otlp(span(tid(1), sid(1), error=True, links=[(tid(9), sid(1))])))
    clock.advance(5.0)
    result = lookup(client, tid(1), sid(1))
    hop = hop_map(result)[(tid(9), sid(1))]
    assert hop["target"]["status"] == "pending"
    assert hop["target"]["trace"] is None
    assert [(p["trace_id"], p["span_id"]) for p in result["pending"]] == [
        (tid(9), sid(1))
    ]


def test_collecting_target_is_pending_then_resolves(client, clock):
    post(client, otlp(span(tid(2), sid(1))))  # target trace still collecting
    post(client, otlp(span(tid(1), sid(1), error=True, links=[(tid(2), sid(1))])))
    hop = hop_map(lookup(client, tid(1), sid(1)))[(tid(2), sid(1))]
    assert hop["target"]["status"] == "pending"
    assert hop["target"]["trace"]["state"] == "collecting"
    assert hop["target"]["trace"]["remaining_seconds"] == pytest.approx(5.0)

    clock.advance(5.0)
    hop = hop_map(lookup(client, tid(1), sid(1)))[(tid(2), sid(1))]
    assert hop["target"]["status"] == "admitted_raw_discarded"
    assert hop["target"]["trace"]["state"] == "dropped"


def test_cycle_marks_the_span_it_returns_to(client, clock):
    # A.s1 -> B.s1 -> C.s1 -> A.s1 (back to the starting span).
    post(client, otlp(span(tid(2), sid(1), links=[(tid(3), sid(1))])))
    post(client, otlp(span(tid(3), sid(1), links=[(tid(1), sid(1))])))
    post(client, otlp(span(tid(1), sid(1), error=True, links=[(tid(2), sid(1))])))
    clock.advance(5.0)

    result = lookup(client, tid(1), sid(1))
    assert len(result["cycles"]) == 1
    cycle = result["cycles"][0]
    assert cycle["from"] == {"trace_id": tid(3), "span_id": sid(1)}
    assert cycle["cycle_to"] == {"trace_id": tid(1), "span_id": sid(1)}
    assert cycle["target"]["status"] == "raw_available"


def test_cycle_between_siblings_is_detected(client, clock):
    # Self-link and A<->B link only in the walk path, not an actual chain.
    post(
        client,
        otlp(
            span(tid(1), sid(1), error=True, links=[(tid(1), sid(1)), (tid(1), sid(2))]),
            span(tid(1), sid(2), links=[(tid(1), sid(1))]),
        ),
    )
    clock.advance(5.0)
    result = lookup(client, tid(1), sid(1))
    cycles = {
        (h["from"]["span_id"], h["cycle_to"]["span_id"]) for h in result["cycles"]
    }
    assert (sid(1), sid(1)) in cycles  # self link
    assert (sid(2), sid(1)) in cycles  # sibling links back to source


def test_lookup_depth_is_bounded(client, clock):
    spans = [
        span(tid(1), sid(n), links=[(tid(1), sid(n + 1))]) for n in range(1, 6)
    ]
    spans[-1] = span(tid(1), sid(5))
    post(client, otlp(*spans))
    clock.advance(5.0)
    response = client.get(f"/api/traces/{tid(1)}/spans/{sid(1)}/links")
    assert response.status_code == 200
    # Default depth is 10, so all four hops of the chain are followed.
    assert len(response.json()["hops"]) == 4


def test_bounded_depth_marks_truncated(tmp_path, clock):
    settings = make_settings(tmp_path, clock, link_lookup_max_depth=2)
    store = SqliteStore(settings)
    store.init_schema()
    from app.otlp import parse_otlp_json

    payload = otlp(
        *[
            span(tid(1), sid(n), links=[(tid(1), sid(n + 1))]) for n in range(1, 5)
        ][:-1],
        span(tid(1), sid(4)),
    )
    store.ingest(parse_otlp_json(payload).spans, clock())
    clock.advance(5.0)
    result = store.resolve_links(tid(1), sid(1), clock())
    assert len(result["hops"]) == 2
    assert result["truncated"] is True


def test_lookup_unknown_source_is_404(client):
    response = client.get(f"/api/traces/{tid(1)}/spans/{sid(1)}/links")
    assert response.status_code == 404


def test_lookup_rejects_unadmitted_source(client, clock):
    """A span in a never-admitted trace is not a valid starting point."""
    post(client, otlp(span(tid(1), sid(1))))
    response = client.get(f"/api/traces/{tid(1)}/spans/{sid(9)}/links")
    assert response.status_code == 404


# --------------------------------------------------------------------------
# evidence retention and restart
# --------------------------------------------------------------------------


def test_drop_clears_raw_but_keeps_identities_after_restart(tmp_path, clock):
    settings = make_settings(tmp_path, clock)
    app1 = create_app(settings)
    with TestClient(app1) as c1:
        post(
            c1,
            otlp(
                span(tid(1), sid(1), links=[(tid(2), sid(1))]),
                span(tid(1), sid(2), parent=sid(1)),
            ),
        )
        clock.advance(5.0)
        assert c1.get(f"/api/traces/{tid(1)}").json()["state"] == "dropped"

    app2 = create_app(make_settings(tmp_path, clock))
    with TestClient(app2) as c2:
        data = c2.get(f"/api/traces/{tid(1)}").json()
        assert data["state"] == "dropped"
        assert data["identity_retained"] is True
        assert data["retained_spans"] == 0
        assert {s["span_id"] for s in data["spans"]} == {sid(1), sid(2)}
        # The link survives, so the audit trail can still be followed.
        assert data["spans"][0]["links"] == [{"trace_id": tid(2), "span_id": sid(1)}]
        assert data["spans"][0]["parent_span_id"] is None
        assert data["spans"][1]["parent_span_id"] == sid(1)
        assert all(s["body"] is None and s["service"] == "svc" for s in data["spans"])


def test_raw_body_is_really_removed_from_disk(tmp_path, clock):
    settings = make_settings(tmp_path, clock)
    app = create_app(settings)
    with TestClient(app) as c:
        post(c, otlp(span(tid(1), sid(1), name="unique-name-marker")))
        clock.advance(5.0)
        assert c.get(f"/api/traces/{tid(1)}").json()["state"] == "dropped"

    conn = sqlite3.connect(settings.db_path)
    try:
        bodies = [row[0] for row in conn.execute("SELECT body FROM spans")]
        names = [row[0] for row in conn.execute("SELECT name FROM spans")]
    finally:
        conn.close()
    assert bodies == [None]
    assert names == ["unique-name-marker"]


def test_evidence_failure_defers_decision_and_keeps_raw(client, clock, monkeypatch):
    post(client, otlp(span(tid(1), sid(1), error=False)))
    store: SqliteStore = client.app.state.store
    clock.advance(5.0)

    real_connect = store._connect

    def broken_connect():
        conn = real_connect()
        conn.create_function("fail_evidence", 0, _raise_evidence_error)
        return conn

    # Make the evidence UPDATE fail through a trigger, which is rolled back
    # with the savepoint.
    conn = real_connect()
    conn.execute(
        "CREATE TRIGGER fail_drop_evidence BEFORE UPDATE OF body ON spans "
        "BEGIN SELECT RAISE(ABORT, 'disk full'); END"
    )
    conn.close()

    data = client.get(f"/api/traces/{tid(1)}").json()
    assert data["state"] == "collecting"  # decision did not take effect
    assert data["spans"][0]["body"] is not None  # raw payload retained
    assert "disk full" in data["settle_failure"]
    assert store.settle_failures[tid(1)]

    # Once the problem is fixed the next sweep applies the pending decision.
    conn = real_connect()
    conn.execute("DROP TRIGGER fail_drop_evidence")
    conn.close()
    data = client.get(f"/api/traces/{tid(1)}").json()
    assert data["state"] == "dropped"
    assert data["retained_spans"] == 0
    assert "settle_failure" not in data


def _raise_evidence_error() -> None:
    raise EvidenceError("boom")


def test_late_spans_rejected_while_decision_is_deferred(client, clock):
    """A failed evidence write keeps raw data but must not reopen the window."""
    post(client, otlp(span(tid(1), sid(1))))
    store: SqliteStore = client.app.state.store
    clock.advance(5.0)
    conn = store._connect()
    conn.execute(
        "CREATE TRIGGER fail_late BEFORE UPDATE OF body ON spans "
        "BEGIN SELECT RAISE(ABORT, 'locked'); END"
    )
    conn.close()

    data = client.get(f"/api/traces/{tid(1)}").json()
    assert data["state"] == "collecting"
    assert data["settle_failure"]

    # Window is closed even though the decision is pending: a late error span
    # is rejected and cannot flip the retried decision to kept.
    clock.advance(1)
    body = post(client, otlp(span(tid(1), sid(2), error=True)))
    assert body["partialSuccess"]["rejectedSpans"] == 1
    assert "window closed" in body["partialSuccess"]["errorMessage"]

    conn = store._connect()
    conn.execute("DROP TRIGGER fail_late")
    conn.close()
    final = client.get(f"/api/traces/{tid(1)}").json()
    assert final["state"] == "dropped"
    assert final["decision_reason"] == "clean"


def test_lookup_uses_one_snapshot_during_background_decision(tmp_path, clock):
    """Settling happens before the read snapshot; the walk itself is stable."""
    settings = make_settings(tmp_path, clock)
    store = SqliteStore(settings)
    store.init_schema()
    from app.otlp import parse_otlp_json

    store.ingest(
        parse_otlp_json(
            otlp(span(tid(1), sid(1), error=True, links=[(tid(2), sid(1))]))
        ).spans,
        clock(),
    )
    clock.advance(5.0)
    # Target enters its own window later, so it is still collecting at lookup
    # time while the source trace is already kept.
    store.ingest(
        parse_otlp_json(otlp(span(tid(2), sid(1)))).spans, clock()
    )
    clock.advance(1.0)

    calls: list[str] = []
    real_lookup_node = store._lookup_node

    def lookup_then_settle(conn, trace_id, span_id, now):
        # Simulate the sweeper committing a decision in another connection
        # after this lookup has taken its read snapshot.  Every later hop must
        # still see the pre-decision view.
        node = real_lookup_node(conn, trace_id, span_id, now)
        if not calls:
            calls.append(span_id)
            other = store._connect()
            try:
                other.execute("UPDATE traces SET state='dropped' WHERE trace_id=?",
                              (tid(2),))
                other.execute("UPDATE spans SET body=NULL WHERE trace_id=?",
                              (tid(2),))
                other.commit()
            finally:
                other.close()
        return node

    store._lookup_node = lookup_then_settle  # type: ignore[method-assign]
    result = store.resolve_links(tid(1), sid(1), clock())
    target = hop_map(result)[(tid(2), sid(1))]["target"]
    # Despite the concurrent commit, the walk sees the same snapshot: the
    # target trace is still collecting and raw is present.
    assert target["trace"]["state"] == "collecting"
    assert target["span"] is not None


def test_late_and_duplicate_rules_still_apply_to_links(client, clock):
    post(client, otlp(span(tid(1), sid(1), links=[(tid(2), sid(1))])))
    clock.advance(5.0)
    assert client.get(f"/api/traces/{tid(1)}").json()["state"] == "dropped"
    body = post(
        client,
        otlp(span(tid(1), sid(2), links=[(tid(3), sid(1))])),
    )
    assert body["partialSuccess"]["rejectedSpans"] == 1
    data = client.get(f"/api/traces/{tid(1)}").json()
    assert len(data["spans"]) == 1  # late span never stored
    dup = post(client, otlp(span(tid(1), sid(1), links=[(tid(9), sid(9))])))
    assert dup["partialSuccess"]["rejectedSpans"] == 1


# --------------------------------------------------------------------------
# upgrade path: traces dropped before identities were retained
# --------------------------------------------------------------------------


def _create_legacy_db(path: str) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE traces (
                trace_id TEXT PRIMARY KEY, first_seen_at REAL NOT NULL,
                deadline_at REAL NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('collecting','kept','dropped')),
                decided_at REAL, decision_reason TEXT,
                has_error INTEGER NOT NULL DEFAULT 0, root_duration_ns INTEGER,
                root_span_id TEXT, span_count INTEGER NOT NULL DEFAULT 0,
                service TEXT
            );
            CREATE TABLE spans (
                trace_id TEXT NOT NULL, span_id TEXT NOT NULL,
                parent_span_id TEXT, name TEXT NOT NULL, service TEXT, scope TEXT,
                start_ns INTEGER, end_ns INTEGER, duration_ns INTEGER,
                is_root INTEGER NOT NULL DEFAULT 0,
                is_error INTEGER NOT NULL DEFAULT 0,
                body TEXT NOT NULL, received_at REAL NOT NULL,
                PRIMARY KEY (trace_id, span_id)
            );
            """
        )
        # A trace dropped by the old build: raw span rows were deleted.
        conn.execute(
            "INSERT INTO traces (trace_id, first_seen_at, deadline_at, state, "
            "decided_at, decision_reason, span_count) VALUES (?, 1, 6, 'dropped', 6, 'clean', 1)",
            (tid(40),),
        )
        # A kept trace from the old build still has its raw span.
        conn.execute(
            "INSERT INTO traces (trace_id, first_seen_at, deadline_at, state, "
            "decided_at, decision_reason, span_count) VALUES (?, 1, 6, 'kept', 6, 'error', 1)",
            (tid(41),),
        )
        conn.execute(
            "INSERT INTO spans (trace_id, span_id, name, body, received_at) "
            "VALUES (?, ?, 'op', '{\"spanId\":\"0000000000000001\"}', 1)",
            (tid(41), sid(1)),
        )
        conn.commit()
    finally:
        conn.close()


def test_legacy_dropped_trace_is_unverifiable(tmp_path, clock):
    settings = make_settings(tmp_path, clock)
    _create_legacy_db(settings.db_path)
    app = create_app(settings)
    with TestClient(app) as c:
        # Migration left old rows readable.
        kept = c.get(f"/api/traces/{tid(41)}").json()
        assert kept["state"] == "kept"
        assert kept["spans"][0]["body"] is not None
        assert kept["spans"][0]["links"] == []

        # Source span links at the pre-upgrade dropped trace's span.
        post(
            c,
            otlp(span(tid(1), sid(1), error=True, links=[(tid(40), sid(1))])),
        )
        clock.advance(5.0)
        result = lookup(c, tid(1), sid(1))
        target = hop_map(result)[(tid(40), sid(1))]["target"]
        assert target["status"] == "unverifiable"
        assert target["trace"]["state"] == "dropped"
        assert target["trace"]["identity_retained"] is False

        dropped = c.get(f"/api/traces/{tid(40)}").json()
        assert dropped["identity_retained"] is False
        assert dropped["spans"] == []


def test_legacy_schema_accepts_new_span_and_drop(tmp_path, clock):
    settings = make_settings(tmp_path, clock)
    _create_legacy_db(settings.db_path)
    app = create_app(settings)
    with TestClient(app) as c:
        post(
            c,
            otlp(
                span(tid(42), sid(1), links=[(tid(41), sid(1))]),
                span(tid(42), sid(2), parent=sid(1)),
            ),
        )
        clock.advance(5.0)
        data = c.get(f"/api/traces/{tid(42)}").json()
        assert data["state"] == "dropped"
        assert data["identity_retained"] is True
        assert data["retained_spans"] == 0
        assert data["spans"][0]["links"] == [{"trace_id": tid(41), "span_id": sid(1)}]
        # Old kept span was migrated with a nullable body and empty links.
        conn = sqlite3.connect(settings.db_path)
        try:
            names = [
                row[1]
                for row in conn.execute("PRAGMA table_info(spans)")
            ]
        finally:
            conn.close()
        assert "links" in names


# --------------------------------------------------------------------------
# simulator + UI
# --------------------------------------------------------------------------


def test_simulator_carries_links(client, clock):
    target = client.post(
        "/api/simulate", json={"root_error": True, "root_duration_ms": 5}
    ).json()
    target_trace = target["trace_ids"][0]
    source = client.post(
        "/api/simulate",
        json={
            "root_error": True,
            "links": [{"trace_id": target_trace, "span_id": target["root_span_id"]}],
        },
    ).json()
    source_trace = source["trace_ids"][0]
    data = client.get(f"/api/traces/{source_trace}").json()
    root = next(s for s in data["spans"] if s["is_root"])
    assert root["links"] == [
        {"trace_id": target_trace, "span_id": target["root_span_id"]}
    ]

    result = lookup(client, source_trace, source["root_span_id"])
    hop = result["hops"][0]
    assert hop["target"]["trace_id"] == target_trace
    assert hop["target"]["status"] in {"pending", "raw_available"}


def test_simulator_rejects_malformed_links(client):
    response = client.post(
        "/api/simulate", json={"links": [{"trace_id": "abc", "span_id": sid(1)}]}
    )
    assert response.status_code == 400
    assert "hex" in response.json()["error"]


def test_ui_exposes_link_lookup(client):
    text = client.get("/").text
    assert "回查" in text or "links" in text
