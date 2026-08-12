"""Which GuruFocus feeds one press pays for.

⚠⚠ `force` MEANS "IGNORE WHAT WE HOLD", NOT "RUN EVERYTHING", AND THE DIFFERENCE IS THE BILL.
`ingest_company` gates each feed with `if force or c.get(flag, True)` — so passing `force=True`
THERE short-circuits the flags and runs all three feeds whatever they say. The /benchmarks index
fill always expressed force as flags and never passed it down; the per-COMPANY job passed both, so
the drill-down's per-row Refresh (`?force=true&feeds=statements`) spent **3 API calls per company
instead of 1**, on the estimates and indicator feeds no chart on that screen draws. Measured
2026-08-12 on DSM-Firmenich.

`feed_flags` is now the one place that decides, and the rule it encodes is: force sets every flag,
`feeds` narrows afterwards, and narrowing can never be undone by force because it happens last.

Pure — no DB, no network.
"""
from __future__ import annotations

import pytest

from routers._fundamental_backfill import feed_flags

ALL = ("need_fin", "need_est", "need_ind")


class TestForceSetsTheFlagsAndStatementsNarrowsThem:
    def test_force_with_statements_is_ONE_feed(self):
        # ⚠ THE REGRESSION. Three trues here is a tripled bill on data the caller cannot draw.
        assert feed_flags(True, "statements") == {
            "need_fin": True, "need_est": False, "need_ind": False}

    def test_force_with_all_is_three(self):
        assert feed_flags(True, "all") == dict.fromkeys(ALL, True)

    def test_narrowing_is_applied_LAST_so_force_cannot_widen_it(self):
        # Same result whichever way you look at it: `statements` wins over `force`.
        forced = feed_flags(True, "statements")
        probed = feed_flags(False, "statements", dict.fromkeys(ALL, True))
        assert forced == probed


class TestTheProbedPath:
    def test_only_the_missing_feeds_run(self):
        got = feed_flags(False, "all", {"need_fin": False, "need_est": True, "need_ind": False})
        assert got == {"need_fin": False, "need_est": True, "need_ind": False}

    def test_a_company_missing_nothing_runs_nothing(self):
        # ⚠ AND NOT "everything", which is what an ABSENT flag means to `ingest_company`
        # (`c.get(flag, True)`). A missing `needs()` row is "nothing due", not "fetch it all".
        assert feed_flags(False, "all", None) == dict.fromkeys(ALL, False)

    def test_statements_narrows_the_probed_path_too(self):
        got = feed_flags(False, "statements", dict.fromkeys(ALL, True))
        assert got["need_est"] is False
        assert got["need_ind"] is False


class TestTheFlagsAreAlwaysExplicit:
    @pytest.mark.parametrize("force", [True, False])
    @pytest.mark.parametrize("feeds", ["statements", "all"])
    def test_every_flag_is_present_and_boolean(self, force, feeds):
        """⚠ AN ABSENT FLAG MEANS "FETCH IT" to `ingest_company`, so a partial dict is not a
        narrower instruction — it is a wider one. Every path returns all three, explicitly."""
        got = feed_flags(force, feeds, dict.fromkeys(ALL, True))
        assert set(got) == set(ALL)
        assert all(isinstance(v, bool) for v in got.values())
