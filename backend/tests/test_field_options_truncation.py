"""A dropdown's option list is an AGGREGATE, and building it from `.limit()`-ed rows truncates.

⚠⚠ THE FAILURE THIS CLOSES SHIPPED, IN PRODUCTION ONLY, AND SHOWED NOTHING. `/api/companies/
field-options` built its sector list as `universe_membership.select("sector").limit(10000)` and
reduced it to a set in Python. `.limit()` does not decide how many rows come back — PostgREST's
`db-max-rows` does, and it is **1,000 on the cloud project** against 10,000 locally. The table
holds 8,444 rows carrying 43 distinct sectors, so production derived that list from the first
1,000 rows and offered **40 of the 43**: three filter options that simply did not exist, with no
empty cell and no error anywhere. WHICH three depended on physical row order, so a VACUUM could
change the answer — and the local dataset returned all 43 and could never reproduce any of it.

⚠ SO THE TEST IS ABOUT `max_rows`, NOT ABOUT SPEED. `FakeSupabase(max_rows=…)` is the harness that
makes the cloud's cap reproducible on a laptop (`project_postgrest_max_rows_trap`); a stable local
server hides this bug completely, which is exactly how it shipped.

⚠ The fix is `SELECT DISTINCT` server-side (`common.pg.load_distinct_via_copy`), where the
aggregate runs BEFORE the row limit rather than after it. These tests pin the PostgREST FALLBACK,
because that is the path that runs when the COPY transport is unavailable — i.e. precisely when
nobody is looking — and a fallback that is quietly wrong is worse than no fallback.
"""
from __future__ import annotations

import pytest

from tests._fake_supabase import FakeSupabase

# 8,444 rows over 43 sectors, laid out so the three rarest sit PAST the 1,000-row cap — the real
# table's shape, where the tail sectors belong to universes appended later.
_SECTORS = [f"Sector {i:02d}" for i in range(43)]
_ROWS = (
    [{"sector": _SECTORS[i % 40]} for i in range(8_000)]      # the common 40, all before the cap
    + [{"sector": s} for s in _SECTORS[40:] for _ in range(148)]  # the last 3, only after it
)


def _options(fake, monkeypatch) -> list[str]:
    """`_distinct_options` with the COPY transport unavailable — the fallback path."""
    import common.pg as pg
    import routers.companies as companies

    monkeypatch.setattr(companies, "supabase", fake)
    monkeypatch.setattr(companies, "load_distinct_via_copy", lambda _t, _c: None)
    monkeypatch.setattr(pg, "_db_url", lambda: None)
    return companies._distinct_options("universe_membership", "sector")


def test_the_option_list_survives_the_cloud_row_cap(monkeypatch):
    """1,000-row cap, 8,444 rows: every one of the 43 sectors still reaches the dropdown."""
    fake = FakeSupabase({"universe_membership": list(_ROWS)}, max_rows=1000)
    assert _options(fake, monkeypatch) == sorted(_SECTORS)


def test_the_unpaged_read_this_replaced_would_have_lost_three(monkeypatch):
    """The bug itself, so the test above is known to be measuring something.

    This is what the endpoint used to do. It is green against the local cap and short against the
    cloud one, which is the whole reason it survived review.
    """
    fake = FakeSupabase({"universe_membership": list(_ROWS)}, max_rows=1000)
    rows = fake.table("universe_membership").select("sector").limit(10_000).execute().data
    assert len(rows) == 1000, "the cap is silent — .limit(10000) did not lift it"
    assert len(({r["sector"] for r in rows})) == 40, "three sectors are missing, with no error"


def test_a_local_sized_cap_hides_it(monkeypatch):
    """⚠ Why a laptop could never catch this: at `db-max-rows = 10000` the broken read is CORRECT."""
    fake = FakeSupabase({"universe_membership": list(_ROWS)}, max_rows=10_000)
    rows = fake.table("universe_membership").select("sector").limit(10_000).execute().data
    assert len({r["sector"] for r in rows}) == 43


@pytest.mark.parametrize("cap", [100, 500, 1000, 5000])
def test_the_pager_is_complete_at_every_cap(monkeypatch, cap):
    """The fallback pages on what came back, so no cap can shorten its answer."""
    fake = FakeSupabase({"universe_membership": list(_ROWS)}, max_rows=cap)
    assert _options(fake, monkeypatch) == sorted(_SECTORS)


def test_blanks_and_nulls_are_dropped_once_centrally(monkeypatch):
    """⚠ Filtered in the loader, not per caller — so every option list gets the same answer."""
    rows = [{"sector": "Energy"}, {"sector": None}, {"sector": "   "}, {"sector": "Energy"}]
    fake = FakeSupabase({"universe_membership": rows}, max_rows=1000)
    assert _options(fake, monkeypatch) == ["Energy"]
