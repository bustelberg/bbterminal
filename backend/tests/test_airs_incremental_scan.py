"""`accounts_to_scan` — which accounts a fleet Refresh all actually re-downloads.

Refresh all is 44 accounts × 4 reports and takes minutes. Skipping the ones already scanned is
worth real time, and every way of getting the rule wrong is quiet: skip too much and a failed
report is never retried; skip too little and nothing was gained. So the rule is a pure function,
tested here, rather than a condition buried in the scrape loop.
"""
from datetime import datetime, timedelta, timezone

from airs_vermogen import REPORTS, _parse_stamp, accounts_to_scan

NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)
ALL = sorted(REPORTS)


def verdict(hours_ago: float, reports=ALL) -> dict:
    return {"reports_ok": list(reports),
            "reports_at": (NOW - timedelta(hours=hours_ago)).isoformat()}


class TestTheSkipRule:
    def test_a_recent_complete_scan_is_skipped(self):
        todo, current = accounts_to_scan(["A"], {"A": verdict(1)}, NOW, max_age_hours=12)
        assert (todo, current) == ([], ["A"])

    def test_an_old_complete_scan_is_rescanned(self):
        todo, current = accounts_to_scan(["A"], {"A": verdict(13)}, NOW, max_age_hours=12)
        assert (todo, current) == (["A"], [])

    def test_a_partial_scan_is_retried_however_recent(self):
        """A report that failed an hour ago is exactly the one worth asking for again — skipping
        it because 'we looked recently' would make a transient failure permanent."""
        todo, _ = accounts_to_scan(["A"], {"A": verdict(0.1, ["att", "volk", "mut"])}, NOW,
                                   max_age_hours=12)
        assert todo == ["A"]

    def test_an_unknown_account_is_scanned(self):
        """No verdict at all — a brand-new account in AIRS, or one whose rows were just deleted.
        This is what makes the delete-then-Refresh-all test refill precisely that gap."""
        todo, current = accounts_to_scan(["A", "B"], {"B": verdict(1)}, NOW, max_age_hours=12)
        assert (todo, current) == (["A"], ["B"])

    def test_extra_reports_do_not_break_completeness(self):
        """`issubset`, not equality — a report added later must not retire the whole fleet."""
        todo, _ = accounts_to_scan(["A"], {"A": verdict(1, [*ALL, "new"])}, NOW, max_age_hours=12)
        assert todo == []

    def test_force_scans_everything(self):
        todo, current = accounts_to_scan(["A", "B"], {"A": verdict(0), "B": verdict(0)}, NOW,
                                         force=True)
        assert (todo, current) == (["A", "B"], [])

    def test_discovery_order_is_preserved(self):
        names = ["C", "A", "B"]
        todo, _ = accounts_to_scan(names, {}, NOW)
        assert todo == names


class TestTheWaysAStampLies:
    def test_a_future_stamp_is_stale_not_eternally_fresh(self):
        """Clock skew or one bad row would otherwise pin an account in the skip list for ever —
        and the symptom is a refresh that looks impressively fast."""
        todo, _ = accounts_to_scan(["A"], {"A": verdict(-100)}, NOW, max_age_hours=12)
        assert todo == ["A"]

    def test_an_unparseable_stamp_is_stale(self):
        todo, _ = accounts_to_scan(["A"], {"A": {"reports_ok": ALL, "reports_at": "yesterday"}},
                                   NOW, max_age_hours=12)
        assert todo == ["A"]

    def test_a_missing_stamp_is_stale(self):
        todo, _ = accounts_to_scan(["A"], {"A": {"reports_ok": ALL, "reports_at": None}}, NOW)
        assert todo == ["A"]

    def test_a_naive_stamp_is_read_as_utc(self):
        """We write `datetime.now(timezone.utc).isoformat()`, but a stamp that lost its offset
        somewhere must not be reinterpreted as local time — that is an hours-wide error in the
        one comparison this function makes."""
        assert _parse_stamp("2026-07-29T11:00:00") == datetime(2026, 7, 29, 11, tzinfo=timezone.utc)

    def test_a_z_suffix_parses(self):
        assert _parse_stamp("2026-07-29T11:00:00Z") == datetime(2026, 7, 29, 11, tzinfo=timezone.utc)

    def test_empty_verdicts_scan_everything(self):
        """`_roster_verdicts` returns {} when the read fails — failing toward doing the work."""
        todo, current = accounts_to_scan(["A", "B"], {}, NOW)
        assert (todo, current) == (["A", "B"], [])
