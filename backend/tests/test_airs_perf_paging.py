"""The portfolios page showed June's return in production and July's locally. Same code.

MEASURED 2026-08-03. `AITopSelectie OFF DYN` read **+55.20%** in production and **+36.64%**
locally. Neither figure is computed by us — `ytd_pct` is AIRS's own `cumulatief_rendement`, read
straight out of `airs_performance` ("never recomputed here"). Both numbers were in the table:

    periode 2026-06-30   cumulatief_rendement  +55.1994%     <- what production showed
    periode 2026-07-31   cumulatief_rendement  +36.6417%     <- what local showed

July was a −11.96% month, and production simply never read it.

⚠ THE CAP THAT BINDS IS THE SERVER'S, NOT THE `.limit()` YOU WROTE. `_year_perf` asked for
`.order("periode").limit(20000)`. PostgREST's `db-max-rows` is **1,000 on Supabase cloud and
10,000 locally**, and it truncates SILENTLY — no error, no header, no short-read signal. The table
holds 1,334 rows, so locally everything came back and every figure was right; in production the
read stopped at 1,000. And because the order is ASCENDING, the rows dropped were the NEWEST.

⚠ REFRESHING MADE IT WORSE, WHICH IS WHY IT LOOKED LIKE A BROKEN BUTTON. `airs_performance` is
append-only — every daily run writes another row for each month in progress — so each refresh
pushed the newest rows further past the cap. The one action that looks like a fix was feeding it.

This is the third instance of this trap in one codebase (the companies list, the FX loaders, now
this). The `.limit(N)` reads as protection and is not; only paging is.
"""
from __future__ import annotations

import routers._airs_accounts as acc
from tests._fake_supabase import FakeSupabase

# The production cap. Every fixture here uses it, because a test that does not truncate cannot
# tell a paged reader from an unpaged one.
CLOUD_CAP = 1000


def _perf_rows(account: str, periods: list[str], cumulatief: list[float]) -> list[dict]:
    return [{"portefeuille": account, "periode": p, "cumulatief_rendement": c,
             "rendement": 0.0, "beginvermogen": 100.0, "eindvermogen": 100.0,
             "fetched_at": f"2026-08-03T07:49:{i:02d}+00:00"}
            for i, (p, c) in enumerate(zip(periods, cumulatief, strict=True))]


def _filler(n: int) -> list[dict]:
    """Enough OLDER rows to push the interesting ones past a 1,000-row cap — which is exactly
    what a year of daily appends does on its own."""
    return [{"portefeuille": f"FILLER_{i:04d}", "periode": "2026-01-31",
             "cumulatief_rendement": 1.0, "rendement": 0.0, "beginvermogen": 100.0,
             "eindvermogen": 100.0, "fetched_at": "2026-01-31T00:00:00+00:00"}
            for i in range(n)]


class TestTheNewestPeriodSurvivesTheCap:
    def test_it_reads_july_not_june(self, monkeypatch):
        """The reported bug, reproduced: June and July both stored, June ahead of July in the
        ascending order, and 1,000 rows of history in between."""
        rows = _filler(1200) + _perf_rows(
            "AITopSelectie OFF DYN", ["2026-06-30", "2026-07-31"], [55.199438, 36.641677])
        monkeypatch.setattr(acc, "supabase",
                            FakeSupabase({"airs_performance": rows}, max_rows=CLOUD_CAP))

        got = acc._year_perf()["AITopSelectie OFF DYN"]

        assert got["cumulatief_rendement"] == 36.641677, (
            "read June's YTD — the newest rows fell past the server cap, which is exactly what "
            "production did")
        assert got["periode"] == "2026-07-31"

    def test_an_unpaged_read_of_the_same_data_really_would_be_short(self, monkeypatch):
        """Guards the guard: if the fake did not truncate, the test above would pass against the
        broken code too and prove nothing."""
        fake = FakeSupabase({"airs_performance": _filler(1200)}, max_rows=CLOUD_CAP)
        got = (fake.table("airs_performance").select("periode")
               .order("periode").limit(20000).execute().data)

        assert len(got) == CLOUD_CAP < 1200

    def test_every_row_comes_back_however_much_history_accumulates(self, monkeypatch):
        rows = _filler(2500) + _perf_rows("A", ["2026-07-31"], [1.5])
        monkeypatch.setattr(acc, "supabase",
                            FakeSupabase({"airs_performance": rows}, max_rows=CLOUD_CAP))

        out = acc._year_perf()

        # Every filler account plus ours — none silently dropped.
        assert len(out) == 2501


class TestThePagerIsCorrectUnderANYCap:
    """`if len(rows) < page: break` is only right while the server's cap is at least the page
    size — and the cap is the thing we were wrong about. Advancing by what came back and stopping
    on an empty page holds regardless."""

    def test_it_survives_a_cap_below_the_page_size(self):
        fake = FakeSupabase({"airs_performance": _filler(700)}, max_rows=250)

        got = acc._paged(lambda: fake.table("airs_performance").select("periode")
                         .order("periode").order("portefeuille").order("fetched_at"))

        assert len(got) == 700

    def test_it_terminates_on_an_empty_table(self):
        fake = FakeSupabase({"airs_performance": []}, max_rows=CLOUD_CAP)

        assert acc._paged(lambda: fake.table("airs_performance").select("periode")
                          .order("periode")) == []


class TestTheHoldingsSnapshotIsAskedForByDate:
    """`account_holdings` read an account's WHOLE history under `.limit(2000)` — unordered — and
    took `max(as_of_date)` from whatever came back. `airs_holding` keeps one snapshot per account
    per date and grows on every scan (10,084 rows, 704 for the busiest account). Truncated, the
    surviving rows are arbitrary, so `max()` names an OLD snapshot and the panel shows last
    month's positions as today's."""

    def test_it_returns_the_newest_snapshot_not_the_one_that_fit(self, monkeypatch):
        old = [{"portefeuille": "P", "as_of_date": "2026-06-30", "holding_name": f"OLD {i}",
                "quantity": 1, "currency": "EUR", "weight": 1, "start_value_eur": 1,
                "current_value_eur": 1, "ytd_return_eur": 0, "ytd_return_pct": 0,
                "ytd_return_local_pct": 0, "cost_basis_local": 1, "current_price_local": 1,
                "airs_weight": 1, "fund_result_eur": 0, "fx_result_eur": 0, "airs_result_pct": 0}
               for i in range(1500)]
        new = [{**old[0], "as_of_date": "2026-08-01", "holding_name": "NEW A"}]
        monkeypatch.setattr(acc, "supabase",
                            FakeSupabase({"airs_holding": old + new,
                                          "airs_mutatie": [], "airs_model_weight": [],
                                          "airs_performance": []}, max_rows=CLOUD_CAP))

        got = acc.account_holdings("P")

        assert got["as_of"] == "2026-08-01"
        assert [r["holding_name"] for r in got["rows"]] == ["NEW A"]
