"""`common/pg.copy_connection_scope` — reuse a direct-Postgres connection per REQUEST, per THREAD.

⚠ WHY THIS EXISTS: `_run_copy_uncached` opened a fresh `psycopg.connect()` for every COPY.
Measured 2026-08-11 — connect + `SET statement_timeout` + `SELECT 1`:

        local (127.0.0.1)          24.0 ms
        production (eu-west-3)    220.7 ms

The Analyse modal issues 17 COPYs, so it spent **~3.75 s in production purely opening
connections** while a laptop profile reported 0.41 s. ⚠ **This is the class of cost a local
profile structurally cannot show**, which is why it survived several rounds of profiling.

⚠ THE SCOPE IS KEYED PER THREAD, AND THAT IS A CORRECTNESS REQUIREMENT, NOT A REFINEMENT. A
ContextVar is COPIED into a worker by `asyncio.to_thread`, so several workers share one context —
and a psycopg connection is NOT thread-safe. Two COPY streams interleaved on one socket do not
raise; they return the wrong bytes.
"""
from __future__ import annotations

import threading

import common.pg as pg


class _FakeConn:
    """Stands in for a psycopg connection: records use, tracks closure."""
    _n = 0

    def __init__(self):
        _FakeConn._n += 1
        self.id = _FakeConn._n
        self.closed = False
        self.statement_timeout_set = 0

    def cursor(self):
        conn = self

        class _Cur:
            def __enter__(self_inner): return self_inner
            def __exit__(self_inner, *a): return False
            def execute(self_inner, sql, *a):
                if "statement_timeout" in sql:
                    conn.statement_timeout_set += 1
        return _Cur()

    def commit(self): pass

    def close(self): self.closed = True


def _patch(monkeypatch):
    """Make `_scoped_connection` build `_FakeConn`s and count them."""
    made = []

    class _FakePsycopg:
        @staticmethod
        def connect(_url, **_kw):
            c = _FakeConn()
            made.append(c)
            return c

    import sys
    monkeypatch.setitem(sys.modules, "psycopg", _FakePsycopg)
    return made


class TestOneConnectionPerScope:
    def test_repeated_copies_reuse_one_connection(self, monkeypatch):
        made = _patch(monkeypatch)
        with pg.copy_connection_scope():
            conns = [pg._scoped_connection("postgresql://x") for _ in range(17)]
        assert len(made) == 1, f"17 COPYs opened {len(made)} connections"
        assert len({id(c) for c in conns}) == 1
        assert made[0].closed, "the connection must be closed when the scope exits"

    def test_statement_timeout_is_set_once_per_connection(self, monkeypatch):
        """Not once per COPY — that was a round trip on every call."""
        made = _patch(monkeypatch)
        with pg.copy_connection_scope():
            for _ in range(5):
                pg._scoped_connection("postgresql://x")
        assert made[0].statement_timeout_set == 1

    def test_without_a_scope_there_is_no_reuse(self, monkeypatch):
        """A script or scheduler tick keeps today's behaviour: no scope, no shared state."""
        _patch(monkeypatch)
        assert pg._scoped_connection("postgresql://x") is None

    def test_nesting_does_not_close_the_outer_connection(self, monkeypatch):
        """An inner scope must not shut the connection the outer block is still using."""
        made = _patch(monkeypatch)
        with pg.copy_connection_scope():
            outer = pg._scoped_connection("postgresql://x")
            with pg.copy_connection_scope():
                inner = pg._scoped_connection("postgresql://x")
            assert inner is outer
            assert not outer.closed, "the inner exit closed the outer connection"
        assert outer.closed
        assert len(made) == 1


class TestFailureIsolation:
    def test_dropping_after_an_error_forces_a_reconnect(self, monkeypatch):
        """A COPY that errors can leave the session unusable. Dropping it costs one reconnect;
        keeping it would fail every remaining COPY in the request."""
        made = _patch(monkeypatch)
        with pg.copy_connection_scope():
            first = pg._scoped_connection("postgresql://x")
            pg._drop_scoped_connection()
            assert first.closed
            second = pg._scoped_connection("postgresql://x")
            assert second is not first
        assert len(made) == 2

    def test_a_closed_connection_is_replaced(self, monkeypatch):
        made = _patch(monkeypatch)
        with pg.copy_connection_scope():
            first = pg._scoped_connection("postgresql://x")
            first.closed = True                       # e.g. the server hung up
            second = pg._scoped_connection("postgresql://x")
            assert second is not first
        assert len(made) == 2

    def test_drop_outside_a_scope_is_a_noop(self, monkeypatch):
        _patch(monkeypatch)
        pg._drop_scoped_connection()                  # must not raise


class TestThreadsDoNotShareAConnection:
    def test_each_thread_gets_its_own(self, monkeypatch):
        """⚠ THE ONE THAT MATTERS. `to_thread` copies the ContextVar, so workers share the scope
        dict — but a psycopg connection is not thread-safe, and interleaved COPY streams return
        wrong bytes rather than raising."""
        made = _patch(monkeypatch)
        seen: dict[int, object] = {}
        with pg.copy_connection_scope():
            def work():
                seen[threading.get_ident()] = pg._scoped_connection("postgresql://x")
            threads = [threading.Thread(target=work) for _ in range(4)]
            for t in threads: t.start()
            for t in threads: t.join()
        assert len(seen) == 4
        assert len({id(c) for c in seen.values()}) == 4, "threads shared a connection"
        assert len(made) == 4
        assert all(c.closed for c in made), "every thread's connection is closed on scope exit"
