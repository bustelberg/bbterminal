"""`common/parse_cache.py` — reusing a parse must never let one caller see another's edits.

⚠ THE HAZARD IS NOT HYPOTHETICAL. `_benchmark_index._members` runs `r["currency"] = ...` in place
over the rows it reads. If the memo handed out the master list instead of copies, the next caller
would inherit those edits — and ONLY on a cache hit, so the bug would appear under repeated reads
and vanish the moment you looked at a cold one.

⚠ AND A SHALLOW COPY IS ONLY SAFE WHEN EVERY VALUE IS A SCALAR. `dict(row)` shares nested values by
reference; the schema has `jsonb` and array columns (`asset_universe.params`,
`airs_model_portfolio.positions_dates`), so flatness is verified per payload rather than assumed.
"""
from __future__ import annotations

import copy

from common.parse_cache import _copy_rows, _is_flat


class TestFlatnessDetection:
    def test_scalars_only_is_flat(self):
        assert _is_flat([{"a": 1, "b": "x", "c": None, "d": 1.5, "e": True}])

    def test_a_nested_dict_is_not_flat(self):
        assert not _is_flat([{"a": 1, "cfg": {"k": "v"}}])

    def test_a_list_value_is_not_flat(self):
        """`positions_dates` is an array column — a shallow copy would share it."""
        assert not _is_flat([{"a": 1, "positions_dates": ["2026-01-01"]}])

    def test_one_nested_row_among_many_makes_the_payload_unflat(self):
        rows = [{"a": i} for i in range(50)] + [{"a": 1, "cfg": {}}]
        assert not _is_flat(rows), "flatness is a property of the WHOLE payload"

    def test_empty_payload_is_flat(self):
        assert _is_flat([])


class TestCopiesAreIndependent:
    def test_flat_copy_does_not_share_rows(self):
        master = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
        out = _copy_rows(master, flat=True)
        out[0]["name"] = "MUTATED"
        out[0]["injected"] = True
        assert master[0]["name"] == "A", "the master row was mutated"
        assert "injected" not in master[0]

    def test_nested_values_are_deep_copied(self):
        master = [{"id": 1, "cfg": {"a": [1, 2]}}]
        out = _copy_rows(master, flat=_is_flat(master))
        out[0]["cfg"]["a"].append(99)
        out[0]["cfg"]["new"] = True
        assert master[0]["cfg"]["a"] == [1, 2], "a nested list was shared"
        assert "new" not in master[0]["cfg"]

    def test_two_copies_are_independent_of_each_other(self):
        master = [{"id": 1, "cfg": {"a": 1}}]
        a = _copy_rows(master, flat=False)
        b = _copy_rows(master, flat=False)
        a[0]["cfg"]["a"] = 999
        assert b[0]["cfg"]["a"] == 1

    def test_scalar_values_survive_the_nested_path_unchanged(self):
        """The scalar-safe path must not stringify or coerce anything."""
        master = [{"i": 3, "f": 1.5, "s": "x", "n": None, "b": False, "cfg": {"k": 1}}]
        out = _copy_rows(master, flat=False)
        for k in ("i", "f", "s", "n", "b"):
            assert out[0][k] == master[0][k] and type(out[0][k]) is type(master[0][k])

    def test_flat_copy_matches_deepcopy_for_flat_rows(self):
        """The cheap path must produce the same VALUES the expensive one would."""
        master = [{"a": i, "b": f"n{i}", "c": None} for i in range(20)]
        assert _copy_rows(master, flat=True) == copy.deepcopy(master)


class TestInstallIsSafe:
    def test_install_is_idempotent(self):
        """Called from `deps` at import; a second call must not stack a second patch (which
        would copy the rows twice per hit)."""
        from common import parse_cache
        assert parse_cache.install() is True
        assert parse_cache.install() is True

    def test_a_response_without_the_marker_parses_normally(self):
        """Degrades to exactly today's behaviour — the whole fallback story."""
        import httpx
        from postgrest.base_request_builder import APIResponse
        r = httpx.Response(200, content=b'[{"id": 1, "name": "x"}]')
        out = APIResponse.from_http_request_response(r)
        assert out.data == [{"id": 1, "name": "x"}]

    def test_the_second_parse_of_the_same_response_is_equal_but_not_shared(self):
        """The marker is attached to the response object, so a repeated parse of the SAME
        object is served from the memo — and must still hand back an independent copy."""
        import httpx
        from postgrest.base_request_builder import APIResponse
        r = httpx.Response(200, content=b'[{"id": 1, "name": "x"}]')
        first = APIResponse.from_http_request_response(r).data
        first[0]["name"] = "MUTATED"
        second = APIResponse.from_http_request_response(r).data
        assert second == [{"id": 1, "name": "x"}], "a mutation of the first parse leaked"
