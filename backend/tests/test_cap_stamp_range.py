"""When a cap-weighted index was measured — see `_asset_benchmark.cap_stamp_range`.

⚠⚠ THE CLAIM WORTH PINNING IS THAT AN UNSTAMPED CONSTITUENT IS COUNTED RATHER THAN DROPPED. A
plain min/max over the stamped subset produces a tight, recent, confident-looking window that
describes two names out of a thousand — and reads as MORE precise than the honest wide one.
"""
from routers._asset_benchmark import cap_stamp_range


def _m(*stamps):
    return [{"market_cap_checked_at": s} for s in stamps]


class TestTheRangeSpansEveryStamp:
    def test_oldest_and_newest(self):
        rows = _m("2026-08-25T09:00:00Z", "2026-08-22T06:00:00Z", "2026-08-24T06:00:00Z")
        assert cap_stamp_range(rows) == ("2026-08-22T06:00:00Z", "2026-08-25T09:00:00Z", 0)

    def test_one_constituent_is_a_range_of_one(self):
        assert cap_stamp_range(_m("2026-08-25T09:00:00Z")) == (
            "2026-08-25T09:00:00Z", "2026-08-25T09:00:00Z", 0)

    def test_identical_stamps_collapse(self):
        rows = _m("2026-08-25T09:00:00Z", "2026-08-25T09:00:00Z")
        got_from, got_to, _ = cap_stamp_range(rows)
        assert got_from == got_to


class TestAnUnstampedConstituentIsCountedNotHidden:
    def test_the_count_reports_them(self):
        # ⚠ THE RANGE STILL READS 25th-to-25th — which is why the 3 matters. Without it the caller
        # would print one confident date for an index that is three-quarters unmeasured.
        rows = _m("2026-08-25T09:00:00Z", None, None, "")
        assert cap_stamp_range(rows) == ("2026-08-25T09:00:00Z", "2026-08-25T09:00:00Z", 3)

    def test_a_missing_key_counts_the_same_as_an_empty_one(self):
        rows = [{"market_cap_checked_at": "2026-08-25T09:00:00Z"}, {}, {"other": 1}]
        assert cap_stamp_range(rows) == ("2026-08-25T09:00:00Z", "2026-08-25T09:00:00Z", 2)

    def test_nothing_stamped_reports_no_range_and_every_row(self):
        # ⚠ NOT `(None, None, 0)` — the rows exist, none of them is dated, and the caller has to
        # be able to tell that apart from an index with no constituents at all.
        assert cap_stamp_range([{}, {"market_cap_checked_at": None}]) == (None, None, 2)

    def test_no_constituents_at_all(self):
        assert cap_stamp_range([]) == (None, None, 0)
