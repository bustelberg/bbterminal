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
