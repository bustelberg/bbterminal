"""The checked-in sector facts are valid and reach every deployment without a DB seed."""
from __future__ import annotations

import json
from pathlib import Path

from asset_pipeline.isin_util import is_valid_isin
from asset_pipeline.sector_override import GICS_SECTORS, load_file_sector_overrides
from routers import _airs_portfolio_analysis as analysis
from tests._fake_supabase import FakeSupabase

_FILE = Path(__file__).resolve().parents[1] / "asset_pipeline" / "sector_overrides.json"


def _entries() -> list[dict]:
    return json.loads(_FILE.read_text(encoding="utf-8"))["overrides"]


def test_every_sector_override_has_a_valid_stable_key_and_gics_value():
    for entry in _entries():
        assert is_valid_isin(entry["isin"]), entry
        assert entry["sector"] in GICS_SECTORS, entry


def test_every_override_documents_the_human_decision():
    for entry in _entries():
        assert entry.get("name", "").strip(), entry
        assert len(entry.get("note", "")) > 40, entry


def test_no_isin_is_overridden_twice():
    isins = [entry["isin"].strip().upper() for entry in _entries()]
    assert len(isins) == len(set(isins))


def test_loader_matches_the_checked_in_file():
    expected = {entry["isin"].strip().upper(): entry["sector"].strip()
                for entry in _entries()}
    assert load_file_sector_overrides() == expected


def test_groupe_bruxelles_lambert_is_financials():
    assert load_file_sector_overrides()["BE0003797140"] == "Financials"


def test_checked_in_override_reaches_the_shared_analysis_grid_and_wins(monkeypatch):
    """Composition and attribution both classify through this reader. A conflicting local DB row
    must not make production and development disagree with the reviewed file."""
    monkeypatch.setattr(analysis, "load_rows_via_copy", lambda *_args, **_kwargs: [{
        "isin": "BE0003797140", "company_id": 42, "name": "Groupe Bruxelles Lambert SA",
        "sector": None, "status": "ok",
    }])
    monkeypatch.setattr(analysis, "supabase", FakeSupabase({
        "company_sector_override": [{"company_id": 42, "sector": "Industrials"}],
    }))

    row = analysis._grid_uncached(["BE0003797140"])["BE0003797140"]

    assert row["sector"] == "Financials"
    assert row["sector_default"] is None
    assert row["sector_overridden"] is True


def test_analysis_chunks_the_company_override_lookup(monkeypatch):
    """ACWI sends more ids than fit in a PostgREST GET URL; no request may exceed 200."""
    company_ids = list(range(1, 451))
    monkeypatch.setattr(analysis, "load_rows_via_copy", lambda *_args, **_kwargs: [
        {"isin": f"ISIN{i}", "company_id": i, "name": f"Company {i}",
         "sector": "Industrials", "status": "ok"}
        for i in company_ids
    ])
    monkeypatch.setattr(analysis, "load_file_sector_overrides", lambda: {})

    class Query:
        def __init__(self, owner):
            self.owner = owner
            self.ids = []

        def select(self, *_args):
            return self

        def in_(self, _column, values):
            self.ids = list(values)
            self.owner.chunk_sizes.append(len(self.ids))
            return self

        def execute(self):
            return type("Result", (), {"data": [
                {"company_id": i, "sector": "Financials"} for i in self.ids
            ]})()

    class Supabase:
        def __init__(self):
            self.chunk_sizes = []

        def table(self, name):
            assert name == "company_sector_override"
            return Query(self)

    fake = Supabase()
    monkeypatch.setattr(analysis, "supabase", fake)

    rows = analysis._grid_uncached([f"ISIN{i}" for i in company_ids])

    assert fake.chunk_sizes == [200, 200, 50]
    assert len(rows) == 450
    assert all(row["sector"] == "Financials" for row in rows.values())
