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

from typing import Any, Callable


class _Result:
    def __init__(self, data: list[dict]):
        self.data = data


class _Query:
    def __init__(self, store: "FakeSupabase", table: str, rows: list[dict] | None = None):
        self._store = store
        self._table = table
        # `rows` is the live backing list (table mode) or a static snapshot (rpc).
        self._rows = rows if rows is not None else store.tables.setdefault(table, [])
        self._filters: list[Callable[[dict], bool]] = []
        self._order: tuple[str, bool] | None = None
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

    def in_(self, col: str, vals: list) -> "_Query":
        allowed = set(vals)
        self._filters.append(lambda r: r.get(col) in allowed)
        return self

    def order(self, col: str, desc: bool = False) -> "_Query":
        self._order = (col, desc)
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
        if self._order is not None:
            col, desc = self._order
            rows = sorted(rows, key=lambda r: (r.get(col) is None, r.get(col)), reverse=desc)
        if self._range is not None:
            start, end = self._range
            rows = rows[start : end + 1]
        if self._limit is not None:
            rows = rows[: self._limit]
        return _Result([dict(r) for r in rows])


class FakeSupabase:
    def __init__(
        self,
        tables: dict[str, list[dict]] | None = None,
        rpc_results: dict[str, list[dict]] | None = None,
    ):
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
