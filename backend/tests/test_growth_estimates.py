"""The 3–5 year consensus growth rates behind the reverse DCF's comparison column."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from routers._growth_estimates import _STAMP, _is_fresh, extract


class TestExtract:
    def test_reads_the_four_forward_rates(self):
        data = {"Growth": {
            "Future 3-5Y EPS Growth Rate Estimate": "36.66",
            "Future 3-5Y EPS without NRI Growth Rate Estimate": "39.08",
            "Future 3-5Y OCF Per Share Growth Rate Estimate": "30.08",
            "Future 3-5Y Total Revenue Growth Rate Estimate": "24.5",
        }}
        assert extract(data) == {
            "eps_3_5y": 36.66, "eps_nri_3_5y": 39.08,
            "ocf_ps_3_5y": 30.08, "revenue_3_5y": 24.5,
        }

    def test_they_are_strings_on_the_wire(self):
        """⚠ GuruFocus files these as STRINGS. Passed through unparsed they reach the UI as text
        and every comparison against a number silently fails."""
        out = extract({"Growth": {"Future 3-5Y EPS Growth Rate Estimate": "13.14"}})
        assert isinstance(out["eps_3_5y"], float)

    def test_an_uncovered_company_is_null_not_zero(self):
        """⚠ `float("")` RAISES rather than returning a falsy number, and an analyst rate of 0% is
        a real forecast — collapsing "nobody covers this" into "they expect nothing" is a claim."""
        out = extract({"Growth": {
            "Future 3-5Y EPS Growth Rate Estimate": "",
            "Future 3-5Y OCF Per Share Growth Rate Estimate": None,
        }})
        assert out["eps_3_5y"] is None
        assert out["ocf_ps_3_5y"] is None
        assert out["revenue_3_5y"] is None      # absent key

    def test_a_negative_forecast_survives(self):
        """Analysts do forecast shrinking earnings; GuruFocus reports ROE growth of −21% for Apple."""
        assert extract({"Growth": {"Future 3-5Y EPS Growth Rate Estimate": "-8.4"}})["eps_3_5y"] == -8.4

    def test_never_raises_on_a_junk_payload(self):
        for bad in (None, {}, {"Growth": None}, {"Growth": {"Future 3-5Y EPS Growth Rate Estimate": "n/a"}}):
            assert extract(bad)["eps_3_5y"] is None


class TestFreshness:
    """⚠ READ OFF OUR OWN STAMP. `keyratios` carries no date at all — no fiscal period, no as-of —
    so there is nothing in the payload to age against."""

    def _stamped(self, age: timedelta):
        return {"Growth": {}, _STAMP: (datetime.now(timezone.utc) - age).isoformat()}

    def test_a_recent_copy_is_reused(self):
        assert _is_fresh(self._stamped(timedelta(days=2)))

    def test_a_week_old_copy_is_refetched(self):
        assert not _is_fresh(self._stamped(timedelta(days=8)))

    def test_an_unstamped_or_broken_copy_is_refetched(self):
        assert not _is_fresh({"Growth": {}})
        assert not _is_fresh({_STAMP: "not-a-date"})
        assert not _is_fresh(None)
