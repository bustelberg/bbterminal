"""The allocation POLICY grid — per risk profile, per asset class, a min / default / max share.

⚠ NULL IS NOT ZERO, AND THAT IS THE WHOLE REASON THE COLUMNS ARE NULLABLE. "No policy recorded" and
"hold none of this" are the same claim for a MINIMUM and opposite claims for a DEFAULT and a
MAXIMUM. A grid seeded with zeros — or an editor that saves blanks as 0 — publishes a policy nobody
wrote, one that reads "this profile may hold no equities", and it looks exactly like a policy that
was written.
"""
from __future__ import annotations

import pytest

from routers import _airs_allocation_bands as ab
from routers._airs_portfolio_variant import VARIANTS


class TestTheGridIsAlwaysComplete:
    def test_every_profile_times_every_invested_class(self, monkeypatch):
        # The editor is a fixed grid; rendering only the cells somebody already filled in would
        # make it impossible to fill in the rest.
        monkeypatch.setattr(ab, "supabase", _FakeDb([]))
        out = ab.load_bands()
        assert len(out) == len(VARIANTS) * len(ab.POLICY_BUCKETS)
        assert {(c["variant"], c["bucket"]) for c in out} == {
            (v, b) for v in VARIANTS for b in ab.POLICY_BUCKETS}

    def test_an_unset_cell_is_null_not_zero(self, monkeypatch):
        monkeypatch.setattr(ab, "supabase", _FakeDb([]))
        c = ab.load_bands()[0]
        assert c["min_pct"] is None and c["default_pct"] is None and c["max_pct"] is None

    def test_postgres_numerics_arrive_as_STRINGS_and_are_returned_as_numbers(self, monkeypatch):
        # ⚠ PostgREST serialises `numeric` as a string. Passed through untouched, the editor's
        # inputs go stringly-typed and its defaults-sum CONCATENATES instead of adding — "607040".
        monkeypatch.setattr(ab, "supabase", _FakeDb([
            {"variant": "Offensief", "bucket": "Equity",
             "min_pct": "50", "default_pct": "70.5", "max_pct": "90", "updated_at": "2026-08-04"}]))
        c = next(x for x in ab.load_bands() if x["variant"] == "Offensief" and x["bucket"] == "Equity")
        assert c["min_pct"] == 50.0 and c["default_pct"] == 70.5
        assert all(isinstance(c[f], float) for f in ("min_pct", "default_pct", "max_pct"))

    def test_cash_and_unclassified_are_not_policy_classes(self):
        # Cash is the REMAINDER (which is why the defaults need not sum to 100) and Unclassified is
        # our own inability to see inside a fund. Neither is something a target can be set for.
        assert "Cash" not in ab.POLICY_BUCKETS
        assert "Unclassified" not in ab.POLICY_BUCKETS

    def test_the_class_keys_are_STORED_keys_not_display_labels(self):
        # The reader sees "Stocks"; every join, colour and filter in the app keys off "Equity".
        # A policy table spelling them its own way is a join waiting to break.
        assert "Equity" in ab.POLICY_BUCKETS and "Stocks" not in ab.POLICY_BUCKETS


class TestValidation:
    def test_a_well_ordered_band_passes(self):
        assert ab.validate_band({"min_pct": 10, "default_pct": 20, "max_pct": 30}) is None

    def test_equal_bounds_are_fine(self):
        # A class pinned to exactly one weight is a policy, not an error.
        assert ab.validate_band({"min_pct": 25, "default_pct": 25, "max_pct": 25}) is None

    @pytest.mark.parametrize("cell", [
        {"min_pct": 40, "default_pct": 20, "max_pct": 30},   # min above default
        {"min_pct": 10, "default_pct": 40, "max_pct": 30},   # default above max
        {"min_pct": 40, "max_pct": 30},                      # min above max, no default set
    ])
    def test_an_out_of_order_band_is_refused_with_a_sentence(self, cell):
        msg = ab.validate_band(cell)
        assert msg and "above" in msg

    @pytest.mark.parametrize("cell", [{"default_pct": 140}, {"min_pct": -1}, {"max_pct": 100.01}])
    def test_a_percent_outside_0_100(self, cell):
        assert "between 0 and 100" in (ab.validate_band(cell) or "")

    @pytest.mark.parametrize("cell", [
        {"max_pct": 30}, {"min_pct": 5}, {"default_pct": 20}, {"min_pct": 5, "max_pct": 30}, {},
    ])
    def test_a_HALF_FILLED_row_is_legal(self, cell):
        # ⚠ The grid is filled in over time. Refusing to store a maximum until its minimum exists
        # makes the editor unusable on the way there — only pairs that are BOTH present compare.
        assert ab.validate_band(cell) is None


class TestSaving:
    def _cells(self, **over):
        base = {"variant": "Offensief", "bucket": "Equity",
                "min_pct": 50, "default_pct": 70, "max_pct": 90}
        return [{**base, **over}]

    def test_a_bad_cell_rejects_the_WHOLE_grid_before_anything_is_written(self, monkeypatch):
        # ⚠ A grid save is ONE intent. Landing the first eight cells and refusing the ninth leaves
        # a policy half-updated while the reader believes all of it took.
        db = _FakeDb([])
        monkeypatch.setattr(ab, "supabase", db)
        cells = [*self._cells(), *self._cells(bucket="Bonds", min_pct=90, max_pct=10)]
        with pytest.raises(ValueError, match="above"):
            ab.save_bands(cells)
        assert db.upserted == [] and db.deleted == []

    def test_an_unknown_cell_is_refused_rather_than_stored(self, monkeypatch):
        monkeypatch.setattr(ab, "supabase", _FakeDb([]))
        with pytest.raises(ValueError, match="unknown cell"):
            ab.save_bands(self._cells(variant="Risicodragend"))
        with pytest.raises(ValueError, match="unknown cell"):
            ab.save_bands(self._cells(bucket="Stocks"))     # the LABEL, not the stored key

    def test_an_all_null_cell_is_a_delete_not_an_upsert_of_nulls(self, monkeypatch):
        # Storing three nulls leaves a row whose `updated_at` claims somebody set something.
        db = _FakeDb([])
        monkeypatch.setattr(ab, "supabase", db)
        ab.save_bands([{"variant": "Neutraal", "bucket": "Bonds",
                        "min_pct": None, "default_pct": None, "max_pct": None}])
        assert db.upserted == []
        assert db.deleted == [("Neutraal", "Bonds")]

    def test_a_partly_filled_cell_is_stored_not_deleted(self, monkeypatch):
        db = _FakeDb([])
        monkeypatch.setattr(ab, "supabase", db)
        ab.save_bands([{"variant": "Neutraal", "bucket": "Bonds", "max_pct": 40}])
        assert db.deleted == []
        assert len(db.upserted) == 1 and db.upserted[0]["max_pct"] == 40.0

    def test_the_timestamp_is_a_real_one_not_the_string_now(self, monkeypatch):
        # PostgREST sends JSON, so a SQL expression arrives as six literal characters.
        db = _FakeDb([])
        monkeypatch.setattr(ab, "supabase", db)
        ab.save_bands(self._cells())
        assert db.upserted[0]["updated_at"].startswith("20")


# ── a fake Supabase, only as much as this module touches ──────────────────────────────────────
class _FakeDb:
    def __init__(self, rows):
        self._rows = rows
        self.upserted: list[dict] = []
        self.deleted: list[tuple[str, str]] = []

    def table(self, _name):
        return _FakeTable(self)


class _FakeTable:
    def __init__(self, db):
        self._db = db
        self._eq: dict[str, str] = {}

    def select(self, *_a, **_k):
        return self

    def delete(self):
        self._pending_delete = True
        return self

    def eq(self, col, val):
        self._eq[col] = val
        return self

    def upsert(self, rows, **_k):
        self._db.upserted.extend(rows)
        return self

    def execute(self):
        if getattr(self, "_pending_delete", False):
            self._db.deleted.append((self._eq.get("variant"), self._eq.get("bucket")))
            return _Res([])
        return _Res(self._db._rows)


class _Res:
    def __init__(self, data):
        self.data = data
