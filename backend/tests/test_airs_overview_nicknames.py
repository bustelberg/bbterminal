"""`_airs_overview._nicknames` must FAIL OPEN.

The hosted databases are migrated by hand (`npx supabase db push`), so deployed code is routinely
ahead of the schema it reads. `airs_account_display_name` is the newest table this endpoint
touches, and reading it unguarded took the whole portfolios overview to a 500 on 2026-07-31.

A nickname is a decoration. Losing it drops the name chain to the model's `display_name` and then
to AIRS's own code — exactly what every row showed before the table existed. Losing the page is a
different order of failure, and the sibling reads (`_airs_accounts._hidden_accounts`,
`_live_accounts`, `_missing_reports`) all already fail open for this reason.
"""
from __future__ import annotations

import pytest

from routers import _airs_overview as ov


class _Table:
    def __init__(self, rows, raises):
        self._rows, self._raises = rows, raises

    def select(self, *_a, **_k):
        return self

    def limit(self, *_a, **_k):
        return self

    def execute(self):
        if self._raises:
            raise self._raises
        return type("R", (), {"data": self._rows})()


class _Supabase:
    def __init__(self, rows=None, raises=None):
        self._rows, self._raises = rows, raises

    def table(self, _name):
        return _Table(self._rows, self._raises)


@pytest.fixture
def stub(monkeypatch):
    def _install(rows=None, raises=None):
        monkeypatch.setattr(ov, "supabase", _Supabase(rows, raises))
    return _install


class TestNicknames:
    def test_rows_are_keyed_lower_and_stripped(self, stub):
        """AIRS's own spelling is stored, but the key must match `portefeuille` however it is cased
        or padded on the account row — the same rule the table's unique index enforces."""
        stub(rows=[{"portefeuille": "  BUS_FTS_OFF_AFS_Dy ", "display_name": "Foundation"}])
        assert ov._nicknames() == {"bus_fts_off_afs_dy": "Foundation"}

    def test_a_blank_name_is_not_a_name(self, stub):
        stub(rows=[{"portefeuille": "A", "display_name": None},
                   {"portefeuille": "B", "display_name": ""}])
        assert ov._nicknames() == {}

    def test_a_missing_table_returns_empty_not_an_exception(self, stub):
        """The measured failure: PostgREST 404s an unmigrated table and postgrest-py raises."""
        stub(raises=Exception(
            "PGRST205: Could not find the table 'public.airs_account_display_name' in the schema cache"
        ))
        assert ov._nicknames() == {}

    def test_null_data_is_tolerated(self, stub):
        stub(rows=None)
        assert ov._nicknames() == {}


class TestOverviewName:
    def test_exact_dynamic_model_nickname_beats_its_fixed_pair(self):
        """Saving `AITopSelectie` on the DYN row must name that overview row."""
        name, custom = ov._overview_name(
            "A custom dynamic account", {},
            {"a custom dynamic account": "Custom name"},
            {"display_name": None},
        )
        assert (name, custom) == ("Custom name", True)

    def test_paired_model_remains_the_fallback_when_the_account_has_no_nickname(self):
        name, custom = ov._overview_name(
            "An unconfigured account", {}, {},
            {"display_name": "Fixed strategy name"},
        )
        assert (name, custom) == ("Fixed strategy name", False)


class TestManagementGroups:
    def test_only_the_named_bustelberg_profiles_are_in_bustelberg(self):
        assert ov._management_group("BUS_Offensief_Dyn") == "bustelberg"
        assert ov._management_group("BUS_Ris_bepOff_Kl_AFS_Dy") == "topselecties"

    def test_only_the_named_toppenberg_profiles_are_in_toppenberg(self):
        assert ov._management_group("TOPS_DEF_BEH_DYN") == "toppenberg"
        assert ov._management_group("TOPS_KM") == "topselecties"

    def test_only_single_variant_building_blocks_lose_a_risk_suffix(self):
        assert ov._management_name("AITopSelectie Offensief", "topselecties", "AITopSelectie OFF DYN") == "AITopSelectie"
        assert ov._management_name("FamilieTopSelectie Beperkt Offensief", "topselecties", "BUS_FTS_BEPOFF_DYN") == "FamilieTopSelectie Beperkt Offensief"

    def test_the_allowlist_supplies_the_topselecties_display_name(self):
        assert ov._management_name("FamilieTopSelectie Offensief", "topselecties", "BUS_FTS_OFF_DYN") == "FamilieTopSelectie"
        assert ov._management_name("MerkenTopSelectie Offensief", "topselecties", "BUS_MTS_OFF_AFS_DYN") == "MerkenTopSelectie"
        assert ov._management_name("TolpoortenSelectie", "test_topselecties", "TolpoortenSelect OFF DYN") == "TolpoortenTopSelectie"
        assert ov._management_group("DealmakersTopSel OFF DYN") == "test_topselecties"

    def test_the_topselecties_tab_is_an_explicit_allowlist(self, monkeypatch, stub):
        stub(rows=[])
        monkeypatch.setattr("routers._airs_accounts.list_accounts", lambda: [
            {"portefeuille": "BUS_FTS_OFF_DYN"},
            {"portefeuille": "BUS_FTS_DEF_DYN"},
        ])
        monkeypatch.setattr("routers._airs_account_links.list_account_links",
                            lambda: {"accounts": []})

        rows = ov.list_overview()

        assert [(r["dynamic_portefeuille"], r["name"]) for r in rows] == [
            ("BUS_FTS_OFF_DYN", "FamilieTopSelectie"),
        ]


class TestOverviewRpcAssembly:
    def test_rpc_payload_includes_configured_test_topselecties(self, monkeypatch):
        """The fast path needs the same test-collection lookup as the legacy path."""
        payload = {
            "accounts": [{
                "portefeuille": "DealmakersTopSel OFF DYN",
                "has_stored_link": False,
                "isins": 0,
            }],
            "models": [],
        }
        monkeypatch.setattr(ov, "_overview_payload", lambda: payload)

        rows = ov.list_overview()

        assert [(r["dynamic_portefeuille"], r["management_group"], r["name"])
                for r in rows] == [
            ("DealmakersTopSel OFF DYN", "test_topselecties", "DealmakersTopSelectie"),
        ]

    def test_rpc_payload_is_the_only_database_input_for_the_fast_path(self, monkeypatch):
        """The SQL RPC owns all relational reads; Python only applies reviewed naming rules."""
        payload = {
            "accounts": [{
                "portefeuille": "BUS_Offensief_Dyn",
                "account_display_name": None,
                "has_stored_link": True,
                "stored_model_portfolio_id": 7,
                "stored_link_note": "reviewed",
                "isins": 4,
                "missing_reports": [],
                "periode": "2026-10-01",
            }],
            "models": [{
                "id": 7,
                "name": "BUS_Offensief_AFS",
                "display_name": "Bustelberg Offensief",
                "omschrijving": "Test model",
                "portfolio_type": "fixed",
                "scanned_at": "2026-10-01T00:00:00+00:00",
                "positions": 4,
            }],
        }
        monkeypatch.setattr(ov, "_overview_payload", lambda: payload)
        monkeypatch.setattr(ov, "_list_overview_legacy",
                            lambda: pytest.fail("the one-RPC path must not fall back to PostgREST"))

        rows = ov.list_overview()

        assert len(rows) == 1
        assert rows[0]["name"] == "Bustelberg Offensief"
        assert rows[0]["link_source"] == "manual"
        assert rows[0]["link_reason"] == "reviewed"
        assert rows[0]["fixed_portfolio_id"] == 7
