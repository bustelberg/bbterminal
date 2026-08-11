"""`routers/_airs_ref.py` must survive PostgREST's silent row cap.

⚠ WHY THIS EXISTS AND WHY IT IS NOT PARANOIA: `airs_model_portfolio_position` held **982 rows**
when this module was written, and PostgREST's cap on Supabase cloud is **1,000**. Eighteen rows of
headroom. The cap truncates SILENTLY, and the LOCAL cap is 10,000 — so the first symptom would be
production quietly losing positions off the end of the table while every local check passed. That
is the identical failure `common/fx_load.py` documents: a cut read makes a fully-priced holding
vanish from its own portfolio and the weights renormalise over what survived, with no error.

⚠ THE SORT KEY MUST BE THE PRIMARY KEY. `(portfolio_id, isin)` is NOT unique in this table — one
model lists the same instrument at two weights (VTopSelectie OFF FX holds CapitaLand at 2% *and*
3%). Postgres makes no promise about tied rows across separate LIMIT/OFFSET queries, so paging on
that pair can serve a row twice or skip it. `_fake_supabase` sorts stably on the ordered key, so
the duplicate-preservation assertion below is what catches a regression to a non-unique key.
"""
from __future__ import annotations

import pytest

from tests._fake_supabase import FakeSupabase


def _positions(n: int = 982) -> list[dict]:
    """`n` position rows, including ONE duplicated (portfolio_id, isin) pair."""
    rows = [{"id": i, "portfolio_id": 100 + (i % 7), "isin": f"XX{i:08d}",
             "fonds": f"Fund {i}", "percentage": 1.0, "datum": "2026-08-01",
             "categorie": "Aandelen"}
            for i in range(1, n + 1)]
    # The CapitaLand case: same (portfolio_id, isin), different row, different weight.
    rows[-1] = {**rows[-1], "portfolio_id": rows[0]["portfolio_id"],
                "isin": rows[0]["isin"], "percentage": 3.0}
    return rows


def _models(n: int = 102) -> list[dict]:
    return [{"id": 1000 + i, "name": f"M{i}", "display_name": f"Model {i}",
             "omschrijving": "", "portfolio_type": "fixed",
             "positions_datum": "2026-08-01", "positions_dates": None,
             "positions_scanned_at": None}
            for i in range(n)]


def _install(monkeypatch, *, max_rows: int | None):
    fake = FakeSupabase(
        {"airs_model_portfolio": _models(),
         "airs_model_portfolio_position": _positions()},
        max_rows=max_rows,
    )
    import routers._airs_ref as ref
    monkeypatch.setattr(ref, "supabase", fake)
    return ref


class TestSurvivesTheServerRowCap:
    # 1000 is production's real cap and the table is at 982 — the case that is one scan from
    # biting. The tighter caps prove the loop is genuinely paging rather than fitting by luck.
    @pytest.mark.parametrize("cap", [None, 10000, 1000, 500, 137, 1])
    def test_every_row_is_returned_whatever_the_cap(self, monkeypatch, cap):
        ref = _install(monkeypatch, max_rows=cap)
        assert len(ref.positions()) == 982, f"positions truncated at cap={cap}"
        assert len(ref.models()) == 102, f"models truncated at cap={cap}"

    def test_the_duplicate_pair_survives_a_page_boundary(self, monkeypatch):
        """Paged on the PK, a non-unique pair is neither doubled nor dropped."""
        ref = _install(monkeypatch, max_rows=137)
        pairs = [(r["portfolio_id"], r["isin"]) for r in ref.positions()]
        assert len(pairs) - len(set(pairs)) == 1, "the duplicate (portfolio_id, isin) pair moved"

    def test_ids_are_unique_and_complete(self, monkeypatch):
        """A page boundary must not serve a row twice — the failure a non-unique sort invites."""
        ref = _install(monkeypatch, max_rows=250)
        ids = [r["id"] for r in ref.positions()]
        assert len(ids) == len(set(ids)) == 982


class TestFiltersInPython:
    """`positions_for` must NOT push a server-side filter — that would be a different request
    shape again and would re-fragment the per-request memo, which is the whole point of the
    module. So it has to return the same rows a filtered query would."""

    def test_positions_for_matches_a_manual_filter(self, monkeypatch):
        ref = _install(monkeypatch, max_rows=1000)
        allrows = ref.positions()
        for pid in {r["portfolio_id"] for r in allrows}:
            assert ref.positions_for(pid) == [r for r in allrows if r["portfolio_id"] == pid]

    def test_model_lookup_matches(self, monkeypatch):
        ref = _install(monkeypatch, max_rows=1000)
        assert ref.model(1000)["name"] == "M0"
        assert ref.model(999_999) is None, "an unknown id must be None, not a KeyError"

    def test_position_counts_ignores_rows_without_an_isin(self, monkeypatch):
        """Cash lines carry no ISIN and are not instruments — the grid counts it that way."""
        ref = _install(monkeypatch, max_rows=1000)
        rows = ref.positions()
        rows_without = [dict(r, isin=None) for r in rows[:10]]
        import routers._airs_ref as mod
        monkeypatch.setattr(mod, "positions", lambda: rows_without + rows[10:])
        assert sum(ref.position_counts().values()) == len(rows) - 10
