"""The per-request read memo: it must remove round trips and change nothing else.

Built 2026-08-11 after profiling one press of Analyse on BUS_Neutraal_FX: **212 database round
trips, 103 of them byte-identical repeats** (`airs_performance` x9, `airs_model_portfolio` x5, the
SP500 universe id x6). No module was at fault — a dozen collaborating loaders each correctly
fetched what it needed, and the duplication existed only in their composition. Measured after:
**109 round trips**, 0 identical repeats, and every payload equal to the uncached one within 1e-9.

⚠ THE THREE TESTS THAT MATTER ARE THE ONES WHERE A BUG WOULD PRODUCE WRONG DATA RATHER THAN SLOW
DATA, because slow is visible and wrong is not:

  1. THE KEY MUST INCLUDE `prefer` AND `range`. PostgREST pages with a `Range` header and asks for
     a count with `Prefer: count=exact` — SAME URL, different question. A URL-only key serves
     page 1 for every page of a paged read, which is how a 30,000-row table quietly becomes its
     first thousand rows (the failure `test_fx_paging` exists for, arriving by a new road).
  2. A CACHED COPY MUST HAND BACK A FRESH `BytesIO`. Returning the cached stream gives the second
     caller a buffer already read to EOF — an empty result that looks exactly like "the database
     has no rows for this".
  3. A WRITE MUST EMPTY THE STORE. A memo that keeps serving reads across an UPDATE lets one
     request contradict itself.
"""
from __future__ import annotations

import io

from common import read_cache
from common.read_cache import copy_bytes, read_cache as memo


class _Resp:
    """Just enough of an httpx.Response for the session wrapper."""

    def __init__(self, body: bytes = b"[]", ok: bool = True) -> None:
        self.content = body
        self.is_success = ok


class _Stub:
    """Stands in for the real httpx client inside `_CachingSession`."""

    def __init__(self, ok: bool = True) -> None:
        self.calls: list[tuple] = []
        self.ok = ok

    def request(self, method, url, **kw):
        self.calls.append((method, str(url), str(kw.get("params") or ""),
                           (kw.get("headers") or {}).get("prefer"),
                           (kw.get("headers") or {}).get("range")))
        return _Resp(f"body-{len(self.calls)}".encode(), self.ok)


def _session(ok: bool = True):
    """A `_CachingSession` whose transport is the stub. Built without httpx: the wrapper only ever
    touches `self._c`, so handing it one directly keeps this a pure unit test."""
    from deps import _CachingSession

    s = _CachingSession.__new__(_CachingSession)
    stub = _Stub(ok)
    object.__setattr__(s, "_c", stub)
    return s, stub


class TestItIsInertUnlessOpenedIn:
    """No global cache, no TTL — outside a block this is the plain client."""

    def test_repeated_gets_are_not_memoized_without_a_block(self):
        s, stub = _session()
        for _ in range(3):
            s.request("GET", "/rest/v1/company", params="select=id")
        assert len(stub.calls) == 3

    def test_active_is_none_outside(self):
        assert read_cache.active() is None


class TestIdenticalReadsAreServedOnce:

    def test_the_second_read_is_the_first_response(self):
        s, stub = _session()
        with memo("t") as st:
            a = s.request("GET", "/rest/v1/company", params="select=id")
            b = s.request("GET", "/rest/v1/company", params="select=id")
        assert len(stub.calls) == 1
        # ⚠ THE SAME RESPONSE OBJECT, DELIBERATELY. postgrest re-parses the bytes into FRESH rows
        # per caller, so sharing the response cannot let one caller's mutation reach another —
        # which is what makes this safe without a deep copy of the payload.
        assert a is b
        assert (st["hits"], st["misses"]) == (1, 1)

    def test_a_different_query_is_a_different_read(self):
        s, stub = _session()
        with memo("t"):
            s.request("GET", "/rest/v1/company", params="select=id")
            s.request("GET", "/rest/v1/company", params="select=name")
        assert len(stub.calls) == 2


class TestTheKeyIncludesTheHeadersPostgrestPagesWith:
    """⚠ THE TRAP THAT WOULD CORRUPT DATA. Same URL, different `Range` = a different page."""

    def test_two_pages_of_one_url_are_two_reads(self):
        s, stub = _session()
        with memo("t"):
            s.request("GET", "/rest/v1/fx_rate", params="select=*",
                      headers={"range": "0-999"})
            s.request("GET", "/rest/v1/fx_rate", params="select=*",
                      headers={"range": "1000-1999"})
        assert len(stub.calls) == 2, "a paged read must not be collapsed into its first page"

    def test_the_count_variant_is_a_different_question(self):
        s, stub = _session()
        with memo("t"):
            s.request("GET", "/rest/v1/company", params="select=id")
            s.request("GET", "/rest/v1/company", params="select=id",
                      headers={"prefer": "count=exact"})
        assert len(stub.calls) == 2


class TestAWriteEmptiesTheStore:

    def test_a_read_after_a_write_goes_back_to_the_database(self):
        s, stub = _session()
        with memo("t") as st:
            s.request("GET", "/rest/v1/company", params="select=id")
            s.request("GET", "/rest/v1/company", params="select=id")     # served from the memo
            s.request("PATCH", "/rest/v1/company", params="id=eq.1")     # invalidates
            s.request("GET", "/rest/v1/company", params="select=id")     # must re-read
        assert [c[0] for c in stub.calls] == ["GET", "PATCH", "GET"]
        assert st["writes"] == 1


class TestOnlySuccessfulReadsAreKept:
    """Caching a 500 turns one flake into a request-long outage."""

    def test_a_failed_read_is_asked_again(self):
        s, stub = _session(ok=False)
        with memo("t"):
            s.request("GET", "/rest/v1/company", params="select=id")
            s.request("GET", "/rest/v1/company", params="select=id")
        assert len(stub.calls) == 2


class TestTheCopyTransport:

    def test_a_repeat_is_served_and_the_stream_is_fresh(self):
        runs = []

        def _run(sql, params):
            runs.append((sql, params))
            return io.BytesIO(b"1,2,3\n4,5,6\n")

        with memo("t") as st:
            a = copy_bytes(("COPY", "sql", "()"), _run, "sql", ())
            first = a.read()                       # drain it, as a real caller would
            b = copy_bytes(("COPY", "sql", "()"), _run, "sql", ())
        assert len(runs) == 1
        # ⚠ A FRESH BUFFER AT POSITION 0. The cached stream would already be at EOF here, and an
        # empty read is indistinguishable from "no rows in the database".
        assert b.read() == first == b"1,2,3\n4,5,6\n"
        assert st["hits"] == 1

    def test_none_is_never_cached(self):
        """`None` means the COPY path is unavailable and the caller must fall back — a fallback is
        not an answer worth remembering, and the connection may be back on the next attempt."""
        calls = []

        def _run(sql, params):
            calls.append(1)
            return None

        with memo("t"):
            assert copy_bytes(("COPY", "s", "()"), _run, "s", ()) is None
            assert copy_bytes(("COPY", "s", "()"), _run, "s", ()) is None
        assert len(calls) == 2

    def test_it_runs_untouched_outside_a_block(self):
        calls = []

        def _run(sql, params):
            calls.append(1)
            return io.BytesIO(b"x")

        copy_bytes(("COPY", "s", "()"), _run, "s", ())
        copy_bytes(("COPY", "s", "()"), _run, "s", ())
        assert len(calls) == 2


class TestNestingKeepsTheOuterStore:
    """An inner block that started its own store would discard everything the outer one had
    already paid for, mid-computation."""

    def test_the_inner_block_reuses_the_outer(self):
        s, stub = _session()
        with memo("outer") as outer:
            s.request("GET", "/rest/v1/company", params="select=id")
            with memo("inner") as inner:
                assert inner is outer
                s.request("GET", "/rest/v1/company", params="select=id")
            s.request("GET", "/rest/v1/company", params="select=id")
        assert len(stub.calls) == 1
        assert outer["hits"] == 2

    def test_the_store_dies_with_the_block(self):
        s, stub = _session()
        with memo("one"):
            s.request("GET", "/rest/v1/company", params="select=id")
        with memo("two"):
            s.request("GET", "/rest/v1/company", params="select=id")
        assert len(stub.calls) == 2, "a memo must not outlive its request"
        assert read_cache.active() is None
