"""A tiny in-memory fake of the supabase-py client, just enough to drive
the DB-shaped functions in `ingest/` and `ingest/phases/` in unit tests —
no Postgres, no network.

Supports the fluent chains the code actually uses:

    fake.table("company").select("...").eq("a", 1).is_("delisted_at", "null")
        .in_("company_id", [1, 2]).order("created_at", desc=True)
        .range(0, 999).limit(5).execute().data

    fake.table("company").update({"x": 1}).eq("company_id", 7).execute()
    fake.table("company").insert({...}).execute()
    fake.table("company").delete().eq("company_id", 7).execute()
    fake.rpc("some_function", {}).range(0, 999).execute().data

Rows are plain dicts; embedded selects (`a:a(b)`) aren't parsed — store the
nested shape the code reads (e.g. {"gurufocus_exchange": {"exchange_code": "HKSE"}})
directly on the row. `select()` ignores its column spec and returns whole
rows; filters / order / range / limit are honored. Mutations are applied
in place and recorded on `.writes` for assertions.
"""
from __future__ import annotations

import re
from typing import Any, Callable


class _Result:
    def __init__(self, data: list[dict]):
        self.data = data


class _NotQuery:
    """Negation shim so `.not_.is_(col, "null")` works on `_Query`."""
    def __init__(self, q: "_Query"):
        self._q = q

    def is_(self, col: str, val: str) -> "_Query":
        self._q._filters.append(lambda r: r.get(col) is not None)
        return self._q


class _Query:
    def __init__(self, store: "FakeSupabase", table: str, rows: list[dict] | None = None):
        self._store = store
        self._table = table
        # `rows` is the live backing list (table mode) or a static snapshot (rpc).
        self._rows = rows if rows is not None else store.tables.setdefault(table, [])
        self._filters: list[Callable[[dict], bool]] = []
        # A LIST, because PostgREST composes `.order(a).order(b)` into a compound sort key and
        # code that pages depends on that key being unique — see `FakeSupabase(max_rows=...)`.
        self._order: list[tuple[str, bool]] = []
        self._range: tuple[int, int] | None = None
        self._limit: int | None = None
        self._mode = "select"
        self._payload: Any = None

    # ── builder verbs ────────────────────────────────────────────
    def select(self, *_a, **_k) -> "_Query":
        self._mode = "select"
        return self

    def insert(self, payload: Any) -> "_Query":
        self._mode, self._payload = "insert", payload
        return self

    def upsert(self, payload: Any, **_kwargs: Any) -> "_Query":
        # Conflict resolution isn't modelled — for the code under test only
        # the returned row count matters, so an upsert behaves like an insert
        # (rows are appended, the batch is echoed back as `.data`). The
        # on_conflict / ignore_duplicates kwargs are accepted and ignored.
        self._mode, self._payload = "insert", payload
        return self

    def update(self, payload: dict) -> "_Query":
        self._mode, self._payload = "update", payload
        return self

    def delete(self) -> "_Query":
        self._mode = "delete"
        return self

    # ── filters ──────────────────────────────────────────────────
    def eq(self, col: str, val: Any) -> "_Query":
        self._filters.append(lambda r: r.get(col) == val)
        return self

    def neq(self, col: str, val: Any) -> "_Query":
        self._filters.append(lambda r: r.get(col) != val)
        return self

    def is_(self, col: str, val: str) -> "_Query":
        # Only the "null" form is used by the code under test.
        self._filters.append(lambda r: r.get(col) is None)
        return self

    @property
    def not_(self) -> "_NotQuery":
        # PostgREST `.not_.is_(col, "null")` → keep rows where col IS NOT NULL.
        return _NotQuery(self)

    # Range filters. Comparison is whatever Python's `>=` does on the stored value, which for
    # the ISO date strings this project stores everywhere is the same ordering Postgres uses.
    def gte(self, col: str, val: Any) -> "_Query":
        self._filters.append(lambda r: r.get(col) is not None and r[col] >= val)
        return self

    def lte(self, col: str, val: Any) -> "_Query":
        self._filters.append(lambda r: r.get(col) is not None and r[col] <= val)
        return self

    def gt(self, col: str, val: Any) -> "_Query":
        self._filters.append(lambda r: r.get(col) is not None and r[col] > val)
        return self

    def lt(self, col: str, val: Any) -> "_Query":
        self._filters.append(lambda r: r.get(col) is not None and r[col] < val)
        return self

    def in_(self, col: str, vals: list) -> "_Query":
        allowed = set(vals)
        self._filters.append(lambda r: r.get(col) in allowed)
        return self

    def like(self, col: str, pattern: str) -> "_Query":
        """PostgREST `.like` — SQL LIKE, with BOTH its wildcards.

        ⚠ `_` MATCHES ANY SINGLE CHARACTER, and every metric code in this project contains one
        (`annuals__Income Statement__Revenue`), while some contain a literal `%` (`ROE %`). A fake
        that treated the pattern as a prefix would pass code that only works by accident here and
        over-matches in production, which is precisely the trap `_page_metrics` is written around.
        """
        rx = re.compile("^" + "".join(
            "." if c == "_" else ".*" if c == "%" else re.escape(c) for c in pattern) + "$")
        self._filters.append(lambda r: isinstance(r.get(col), str) and bool(rx.match(r[col])))
        return self

    def order(self, col: str, desc: bool = False) -> "_Query":
        self._order.append((col, desc))
        return self

    def range(self, start: int, end: int) -> "_Query":
        self._range = (start, end)
        return self

    def limit(self, n: int) -> "_Query":
        self._limit = n
        return self

    # ── terminal ─────────────────────────────────────────────────
    def execute(self) -> _Result:
        matched = [r for r in self._rows if all(f(r) for f in self._filters)]

        if self._mode == "update":
            for r in matched:
                r.update(self._payload)
            self._store.writes.append(("update", self._table, dict(self._payload), len(matched)))
            return _Result([dict(r) for r in matched])

        if self._mode == "delete":
            keep = [r for r in self._rows if r not in matched]
            self._rows[:] = keep
            self._store.writes.append(("delete", self._table, None, len(matched)))
            return _Result([dict(r) for r in matched])

        if self._mode == "insert":
            payloads = self._payload if isinstance(self._payload, list) else [self._payload]
            inserted = [dict(p) for p in payloads]
            self._rows.extend(inserted)
            self._store.writes.append(("insert", self._table, None, len(inserted)))
            return _Result([dict(r) for r in inserted])

        # select
        rows = matched
        # Applied last-key-first so the FIRST `.order()` call is the primary sort, matching
        # PostgREST. A stable sort makes the composition exact.
        for col, desc in reversed(self._order):
            rows = sorted(rows, key=lambda r, c=col: (r.get(c) is None, r.get(c)), reverse=desc)
        if self._range is not None:
            start, end = self._range
            rows = rows[start : end + 1]
        if self._limit is not None:
            rows = rows[: self._limit]
        # ⚠ THE ROW CAP, and it is SILENT — exactly as PostgREST's is. A caller that does not
        # page gets a short answer with nothing to distinguish it from a complete one.
        if self._store.max_rows is not None:
            rows = rows[: self._store.max_rows]
        return _Result([dict(r) for r in rows])


class FakeSupabase:
    def __init__(
        self,
        tables: dict[str, list[dict]] | None = None,
        rpc_results: dict[str, list[dict]] | None = None,
        max_rows: int | None = None,
    ):
        # PostgREST's response cap, off by default. Set it to reproduce the failure mode this
        # project keeps rediscovering: the cap is 1,000 on Supabase cloud and 10,000 locally,
        # a response is truncated SILENTLY, and so a loader that does not page returns a
        # different (and differently WRONG) answer in each environment. A test that wants to
        # prove a reader pages has to be able to cut it off.
        self.max_rows = max_rows
        self.tables: dict[str, list[dict]] = {
            name: [dict(r) for r in rows] for name, rows in (tables or {}).items()
        }
        self.rpc_results: dict[str, list[dict]] = {
            name: [dict(r) for r in rows] for name, rows in (rpc_results or {}).items()
        }
        # ("update"|"insert"|"delete", table, payload-or-None, rows_affected)
        self.writes: list[tuple] = []

    def table(self, name: str) -> _Query:
        return _Query(self, name)

    def rpc(self, name: str, _params: dict | None = None) -> _Query:
        # Static result set; supports .range(...).execute() like the real call.
        return _Query(self, name, rows=list(self.rpc_results.get(name, [])))
