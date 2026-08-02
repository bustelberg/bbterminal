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
