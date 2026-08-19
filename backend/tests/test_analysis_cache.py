"""`routers/_analysis_cache.py` — the properties that make a cache safe on this page.

⚠ THE RISK BEING GUARDED IS NOT A SLOW PAGE, IT IS A WRONG NUMBER. /management-dashboard's whole
discipline is that a figure is either current or ABSENT (`n/a` when unpriceable, a refusal under
`MIN_COVERAGE_PCT` rather than a renormalised guess). A cache that serves a stale payload breaks
that silently and looks exactly like a real answer, so every test here is about *when it must
MISS*, not about when it hits.
"""
from __future__ import annotations

import routers._analysis_cache as ac


class TestNoFingerprintMeansNoCache:
    """⚠ THE MOST IMPORTANT PROPERTY. A fingerprint we could not read is NOT evidence that
    nothing changed — e.g. `SUPABASE_DB_URL` unset, or the catalog read failed. Treating that as
    "unchanged" would serve a payload of unknown age forever."""

    def test_get_always_misses_without_a_fingerprint(self):
        ac.invalidate()
        ac.put(("p", 1), "fp-1", {"v": 1})
        assert ac.get(("p", 1), None) is None

    def test_put_without_a_fingerprint_stores_nothing(self):
        ac.invalidate()
        ac.put(("p", 1), None, {"v": 1})
        assert ac.get(("p", 1), "anything") is None
        assert ac.stats()["entries"] == 0

    def test_cached_degrades_to_a_plain_call(self, monkeypatch):
        monkeypatch.setattr(ac, "fingerprint", lambda: None)
        calls = []
        out = [ac.cached(("k",), lambda: (calls.append(1), {"n": len(calls)})[1]) for _ in range(3)]
        assert len(calls) == 3, "with no fingerprint every call must recompute"
        assert [o["n"] for o in out] == [1, 2, 3]


class TestTheFingerprintIsTheKey:
    def test_a_changed_fingerprint_misses(self):
        """This IS invalidation: nothing is evicted, the key simply no longer matches."""
        ac.invalidate()
        ac.put(("p", 1934), "fp-before", {"ytd": 12.3})
        assert ac.get(("p", 1934), "fp-before") == {"ytd": 12.3}
        assert ac.get(("p", 1934), "fp-after") is None

    def test_different_request_keys_do_not_collide(self):
        ac.invalidate()
        ac.put((1934, "SP500", "model"), "fp", {"b": "sp"})
        ac.put((1934, "ACWI", "model"), "fp", {"b": "acwi"})
        assert ac.get((1934, "SP500", "model"), "fp")["b"] == "sp"
        assert ac.get((1934, "ACWI", "model"), "fp")["b"] == "acwi"
        assert ac.get((1934, "SP500", "book"), "fp") is None

    def test_cached_computes_once_per_fingerprint(self, monkeypatch):
        ac.invalidate()
        fp = {"v": "a"}
        monkeypatch.setattr(ac, "fingerprint", lambda: fp["v"])
        calls = []
        def compute():
            calls.append(1)
            return {"n": len(calls)}
        assert ac.cached(("k",), compute)["n"] == 1
        assert ac.cached(("k",), compute)["n"] == 1, "second call must be served from cache"
        fp["v"] = "b"                                    # a write happened
        assert ac.cached(("k",), compute)["n"] == 2, "a new fingerprint must recompute"


class TestFingerprintFailsSafe:
    """A cache must never be the reason a page 500s — an unreachable catalog disables it."""

    def test_a_raising_copy_disables_caching_rather_than_propagating(self, monkeypatch):
        import common.pg as pg
        monkeypatch.setattr(pg, "_db_url", lambda: "postgresql://x/y")
        def boom(*_a, **_k):
            raise RuntimeError("connection refused")
        monkeypatch.setattr(pg, "_run_copy", boom)
        ac.invalidate()
        assert ac._fingerprint_uncached() is None

    def test_no_db_url_means_no_fingerprint(self, monkeypatch):
        import common.pg as pg
        monkeypatch.setattr(pg, "_db_url", lambda: None)
        ac.invalidate()
        assert ac._fingerprint_uncached() is None


class TestWatchedTables:
    """⚠ A TABLE MISSING FROM `_WATCHED` IS A TABLE WHOSE CHANGES THE CACHE CANNOT SEE. The list
    was derived by instrumenting a real call, and these are the ones whose absence would produce a
    confidently wrong figure rather than a merely stale one — the UI-mutable overrides, which a
    user changes and then immediately re-opens the modal to check."""

    def test_the_ui_mutable_override_tables_are_watched(self):
        for t in ("asset_bucket_override", "airs_holding_isin_override",
                  "airs_model_portfolio_link", "airs_account_model_link",
                  "airs_allocation_band", "asset_isin_alias"):
            assert t in ac._WATCHED, f"{t} is UI-mutable and must invalidate the cache"

    def test_the_bulk_data_tables_are_watched(self):
        for t in ("airs_model_portfolio", "airs_model_portfolio_position", "airs_holding",
                  "airs_mutatie", "asset_price", "asset_execution", "fx_rate"):
            assert t in ac._WATCHED

    def test_the_stamp_sql_folds_in_restart_and_reset(self):
        """Tuple counters reset to zero on restart / pg_stat_reset, so without these the
        fingerprint could go BACKWARD and match an entry built from newer data."""
        assert "pg_postmaster_start_time" in ac._STAMP_SQL
        assert "stats_reset" in ac._STAMP_SQL


class TestTheLegStore:
    """The cross-portfolio sub-result cache (2026-08-19).

    ⚠ IT IS A SECOND STORE, NOT A BIGGER FIRST ONE. A leg is one benchmark window or one ISIN's
    three risk numbers; the payload store holds 137KB dicts. They need entry budgets three orders
    of magnitude apart, which is the whole reason for the split — and they share ONE fingerprint,
    so a leg can never outlive the payload cache's notion of "current".
    """

    def test_a_leg_is_computed_once_and_reused(self, monkeypatch):
        monkeypatch.setattr(ac, "fingerprint", lambda: "fp")
        ac.invalidate()
        calls = []
        for _ in range(3):
            ac.leg(("bench_members", "SP500"), lambda: calls.append(1) or "members")
        assert len(calls) == 1

    def test_a_changed_fingerprint_recomputes_it(self, monkeypatch):
        fp = ["fp-1"]
        monkeypatch.setattr(ac, "fingerprint", lambda: fp[0])
        ac.invalidate()
        calls = []
        ac.leg(("bench_members", "SP500"), lambda: calls.append(1) or "a")
        fp[0] = "fp-2"
        ac.leg(("bench_members", "SP500"), lambda: calls.append(1) or "a")
        assert len(calls) == 2, "a write to any watched table must reach the legs too"

    def test_without_a_fingerprint_every_leg_recomputes(self, monkeypatch):
        monkeypatch.setattr(ac, "fingerprint", lambda: None)
        ac.invalidate()
        calls = []
        for _ in range(3):
            ac.leg(("k",), lambda: calls.append(1) or "v")
        assert len(calls) == 3
        assert ac.stats()["leg_entries"] == 0

    def test_the_two_stores_do_not_share_a_budget(self, monkeypatch):
        """⚠ The payload store caps at 48 entries. If legs went in there, ONE portfolio's ~60
        holdings would evict every payload on the page."""
        monkeypatch.setattr(ac, "fingerprint", lambda: "fp")
        ac.invalidate()
        ac.put(("payload", 1934), "fp", {"big": True})
        for i in range(200):
            ac.leg(("holding_risk", f"ISIN{i}"), lambda: {"vol_5y_pct": 1.0})
        assert ac.get(("payload", 1934), "fp") == {"big": True}
        assert ac.stats()["leg_entries"] == 200

    def test_leg_max_entries_holds_the_page_s_working_set(self):
        """~26 books x ~60 holdings, heavily overlapping between the variants of a strategy."""
        assert ac._LEG_MAX_ENTRIES >= 2000


class TestTheBatchedLegSplit:
    """⚠ THE BATCHED FORM EXISTS BECAUSE THE MISS PATH IS BATCHED. `_holding_risk` loads five
    years of daily closes for every holding in ONE `COPY`; a per-ISIN `leg()` loop would serve the
    hits and then run that COPY once per miss, turning the cheapest part of the function into ~60
    round trips. The caller asks "which of these must I still compute", computes exactly those
    together, and files them."""

    def test_it_splits_hits_from_misses(self, monkeypatch):
        monkeypatch.setattr(ac, "fingerprint", lambda: "fp")
        ac.invalidate()
        ac.leg_put_many({("holding_risk", "A"): {"beta_5y": 1.1}})
        hits, misses = ac.leg_get_many([("holding_risk", "A"), ("holding_risk", "B")])
        assert hits == {("holding_risk", "A"): {"beta_5y": 1.1}}
        assert misses == [("holding_risk", "B")]

    def test_an_empty_answer_is_still_an_answer(self, monkeypatch):
        """⚠ A HOLDING WITH TOO LITTLE HISTORY YIELDS `{}` AND THAT IS A RESULT. Treating "no row"
        as "not computed" would make a book of young listings re-run the whole five-year load on
        every single open — the exact case the cache is for."""
        monkeypatch.setattr(ac, "fingerprint", lambda: "fp")
        ac.invalidate()
        ac.leg_put_many({("holding_risk", "YOUNG"): {}})
        hits, misses = ac.leg_get_many([("holding_risk", "YOUNG")])
        assert misses == [], "an empty row must count as cached, not as missing"
        assert hits == {("holding_risk", "YOUNG"): {}}

    def test_without_a_fingerprint_everything_is_a_miss(self, monkeypatch):
        monkeypatch.setattr(ac, "fingerprint", lambda: None)
        ac.invalidate()
        ac.leg_put_many({("holding_risk", "A"): {"beta_5y": 1.1}})
        hits, misses = ac.leg_get_many([("holding_risk", "A")])
        assert hits == {}
        assert misses == [("holding_risk", "A")]

    def test_invalidate_clears_both_stores(self, monkeypatch):
        monkeypatch.setattr(ac, "fingerprint", lambda: "fp")
        ac.invalidate()
        ac.put(("payload", 1), "fp", {"v": 1})
        ac.leg_put_many({("holding_risk", "A"): {"beta_5y": 1.1}})
        assert ac.invalidate() == 2
        assert ac.stats()["entries"] == 0
        assert ac.stats()["leg_entries"] == 0
