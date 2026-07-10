"""Dividends-per-share for the /asset-pipeline grid.

Two things are pinned:

1. THE METRIC WHITELIST. `_parse_financials(..., metric_codes=...)` must persist
   ONLY the requested codes. Unrestricted, one GuruFocus `/financials` blob is
   ~36,700 `metric_data` rows; the 1,974 companies the dividends column can reach
   would add ~72.5M rows, 2.8x the whole table. The default (`None`) must stay
   exactly as it was, because the earnings dashboard's refresh depends on it.

2. THE ISIN BRIDGE. The asset universe (`analysis_id`) and the GuruFocus company
   universe (`company_id`) are disjoint; ISIN is the only join key and it reaches
   12.8% of the grid. The endpoint must be honest about that — an unbridged ISIN
   is simply absent from the coverage map, never a silent zero.
"""
from __future__ import annotations

import pytest

from ingest.earnings.financials import _parse_financials
from routers._asset_dividends import (
    ANNUAL_CODE,
    DIVIDEND_METRIC_CODES,
    QUARTERLY_CODE,
)


def _blob() -> dict:
    """A miniature GuruFocus /financials response: two blocks, three fields."""
    return {
        "financials": {
            "annuals": {
                "Fiscal Year": ["2024-12", "2025-12", "TTM"],
                "Per Share Data": {
                    "Dividends per Share": ["1.10", "1.25", "1.30"],
                    "Revenue per Share": ["50.0", "55.0", "56.0"],
                },
                "Income Statement": {"Revenue": ["1000", "1100", "1150"]},
            },
            "quarterly": {
                "Fiscal Quarter": ["2025-06", "2025-09"],
                "Per Share Data": {
                    "Dividends per Share": ["0.30", "0.32"],
                    "Revenue per Share": ["13.0", "14.0"],
                },
            },
        }
    }


class TestMetricWhitelist:
    def test_none_keeps_every_field(self):
        """The earnings dashboard's contract. Do not change this default."""
        rows = _parse_financials(_blob(), company_id=1)
        codes = {r["metric_code"] for r in rows}
        assert codes == {
            "annuals__Per Share Data__Dividends per Share",
            "annuals__Per Share Data__Revenue per Share",
            "annuals__Income Statement__Revenue",
            "quarterly__Per Share Data__Dividends per Share",
            "quarterly__Per Share Data__Revenue per Share",
        }

    def test_whitelist_keeps_only_the_requested_codes(self):
        rows = _parse_financials(_blob(), company_id=1, metric_codes=DIVIDEND_METRIC_CODES)
        assert {r["metric_code"] for r in rows} == {ANNUAL_CODE, QUARTERLY_CODE}

    def test_whitelist_does_not_change_the_values_it_keeps(self):
        """Filtering must be a projection, not a different parse."""
        full = _parse_financials(_blob(), company_id=1)
        narrow = _parse_financials(_blob(), company_id=1, metric_codes=DIVIDEND_METRIC_CODES)
        expected = [r for r in full if r["metric_code"] in DIVIDEND_METRIC_CODES]
        assert narrow == expected

    def test_whitelist_is_the_storage_saving_it_claims(self):
        full = _parse_financials(_blob(), company_id=1)
        narrow = _parse_financials(_blob(), company_id=1, metric_codes=DIVIDEND_METRIC_CODES)
        assert len(narrow) < len(full)
        # 2 of 5 fields survive; TTM columns are dropped by both.
        assert len(narrow) == 4          # 2 annual periods + 2 quarterly
        assert all(r["source_code"] == "gurufocus" for r in narrow)
        assert all(r["is_prediction"] is False for r in narrow)

    def test_ttm_column_never_persisted(self):
        rows = _parse_financials(_blob(), company_id=1, metric_codes={ANNUAL_CODE})
        assert [r["target_date"] for r in rows] == ["2024-12-31", "2025-12-31"]
        assert [r["numeric_value"] for r in rows] == [1.10, 1.25]

    def test_unknown_code_yields_nothing_rather_than_everything(self):
        """A typo in the whitelist must fail closed, not silently store 36k rows."""
        rows = _parse_financials(_blob(), company_id=1, metric_codes={"annuals__Nope__Nope"})
        assert rows == []

    def test_empty_whitelist_is_not_the_same_as_none(self):
        assert _parse_financials(_blob(), company_id=1, metric_codes=set()) == []
        assert _parse_financials(_blob(), company_id=1) != []


class TestDividendCodes:
    def test_codes_match_what_gurufocus_actually_emits(self):
        """These strings are built by `_parse_financials` from the blob's shape:
        `{block}__{section}__{field}`. If GuruFocus renames the section, the
        column goes blank silently — so pin the exact strings."""
        assert ANNUAL_CODE == "annuals__Per Share Data__Dividends per Share"
        assert QUARTERLY_CODE == "quarterly__Per Share Data__Dividends per Share"
        assert DIVIDEND_METRIC_CODES == {ANNUAL_CODE, QUARTERLY_CODE}

    def test_the_blob_fixture_really_produces_those_codes(self):
        """Guards the two assertions above against being a self-fulfilling copy."""
        codes = {r["metric_code"] for r in _parse_financials(_blob(), company_id=1)}
        assert DIVIDEND_METRIC_CODES <= codes


class TestCoverageEntry:
    def test_unsubscribed_flag_uses_the_shared_predicate(self):
        """The badge on /companies and this column must never disagree."""
        from index_universe.acwi.exchange_map import is_gf_subscribed_exchange
        from routers._asset_dividends import DividendCoverageEntry

        # LSE is outside the subscription; XPAR is inside. (Both pinned in
        # tests/test_exchange_map.py.)
        assert not is_gf_subscribed_exchange("LSE")
        assert is_gf_subscribed_exchange("XPAR")

        e = DividendCoverageEntry(company_id=1, exchange="LSE",
                                  gf_unsubscribed=not is_gf_subscribed_exchange("LSE"))
        assert e.gf_unsubscribed is True

    def test_defaults_are_conservative(self):
        from routers._asset_dividends import DividendCoverageEntry
        e = DividendCoverageEntry(company_id=7)
        assert e.gf_unsubscribed is False and e.has_data is False
        assert e.exchange is None and e.gurufocus_ticker is None


class TestEurConversion:
    """`value_eur` is the payment converted at the rate on ITS OWN date — what a
    EUR investor actually received, FX leg included. Not today's rate applied to
    history, and never a fabricated rate for dates we hold no data for."""

    @staticmethod
    def _series():
        import pandas as pd
        idx = pd.date_range("2020-01-01", "2020-12-31", freq="D")
        # USD per 1 EUR, rising through the year.
        return pd.Series([1.10 + i * 0.0005 for i in range(len(idx))], index=idx)

    def _pts(self, *dates):
        from routers._asset_dividends import DividendPoint
        return [DividendPoint(date=d, value=2.0) for d in dates]

    def test_divides_by_the_rate_because_it_is_units_per_eur(self):
        from routers._asset_dividends import _convert
        pts = self._pts("2020-01-01")
        _convert(pts, "USD", self._series(), "2020-01-01")
        assert pts[0].fx_rate == pytest.approx(1.10)
        assert pts[0].value_eur == pytest.approx(2.0 / 1.10, abs=1e-6)

    def test_each_point_uses_its_own_date(self):
        from routers._asset_dividends import _convert
        pts = self._pts("2020-01-01", "2020-12-31")
        _convert(pts, "USD", self._series(), "2020-01-01")
        assert pts[0].fx_rate != pts[1].fx_rate
        assert pts[0].value_eur > pts[1].value_eur   # USD weakened -> fewer EUR

    def test_dates_before_coverage_get_no_eur_rather_than_the_earliest_rate(self):
        """`load_fx_rates` back-fills (`.reindex().ffill().bfill()`), so the series
        HAS a value at 1998. Converting there would invent a euro price two years
        before the euro existed."""
        from routers._asset_dividends import _convert
        pts = self._pts("1998-09-30", "2020-06-01")
        _convert(pts, "USD", self._series(), "2020-01-01")
        assert pts[0].value_eur is None and pts[0].fx_rate is None
        assert pts[1].value_eur is not None

    def test_eur_denominated_dividends_pass_through_at_one(self):
        from routers._asset_dividends import _convert
        pts = self._pts("1990-01-01")
        _convert(pts, "EUR", None, None)
        assert pts[0].value_eur == 2.0 and pts[0].fx_rate == 1.0

    def test_unknown_currency_passes_through_rather_than_dropping_the_value(self):
        from routers._asset_dividends import _convert
        pts = self._pts("2020-06-01")
        _convert(pts, None, None, None)
        assert pts[0].value_eur == 2.0 and pts[0].fx_rate == 1.0

    def test_a_zero_or_negative_rate_is_treated_as_missing(self):
        import pandas as pd
        from routers._asset_dividends import _fx_asof
        s = pd.Series([0.0], index=pd.to_datetime(["2020-01-01"]))
        assert _fx_asof(s, "2020-06-01") is None

    def test_asof_takes_the_last_rate_on_or_before_the_date(self):
        import pandas as pd
        from routers._asset_dividends import _fx_asof
        s = pd.Series([1.0, 2.0], index=pd.to_datetime(["2020-01-01", "2020-06-01"]))
        assert _fx_asof(s, "2020-05-31") == 1.0
        assert _fx_asof(s, "2020-06-01") == 2.0
        assert _fx_asof(s, "2019-12-31") is None


class TestTrailingTwelveMonths:
    """The `payments` cadence exists because the fiscal-year series lags by up to a
    year. NVIDIA raised its quarterly dividend from $0.01 to $0.25 with an ex-date
    of 2026-06-04, inside FY2027 — the annual chart correctly showed $0.04 (FY2026)
    and could not show the hike until 2027."""

    @staticmethod
    def _quarterly(n: int, start_year: int = 2020) -> list[str]:
        """Ex-dates that drift forward ~1 day a year, exactly like Apple's."""
        from datetime import date, timedelta
        d = date(start_year, 2, 9)
        out = []
        for _ in range(n):
            out.append(d.isoformat())
            d += timedelta(days=91)
        return out

    def test_infers_quarterly(self):
        from routers._asset_dividends import _payments_per_year
        assert _payments_per_year(self._quarterly(12)) == 4

    def test_infers_annual_and_semiannual(self):
        from routers._asset_dividends import _payments_per_year
        assert _payments_per_year(["2022-06-01", "2023-06-01", "2024-06-01"]) == 1
        assert _payments_per_year(["2023-01-01", "2023-07-01", "2024-01-01", "2024-07-01"]) == 2

    def test_a_single_payment_cannot_infer_a_frequency(self):
        from routers._asset_dividends import _payments_per_year
        assert _payments_per_year(["2024-01-01"]) == 1

    def test_sums_the_last_k_payments_not_a_365_day_window(self):
        """A strict 365-day window catches FIVE drifting quarterly ex-dates and
        double-counts the anniversary quarter — Apple's real 1.05 reads 1.31."""
        from routers._asset_dividends import _trailing_12m
        dates = self._quarterly(8)
        vals = [0.25] * 7 + [0.26]
        out = _trailing_12m(vals, dates)
        assert out[-1] == pytest.approx(0.25 * 3 + 0.26)   # 1.01, not 1.26
        assert out[-2] == pytest.approx(1.00)

    def test_no_trailing_total_until_a_full_year_of_payments_exists(self):
        from routers._asset_dividends import _trailing_12m
        dates = self._quarterly(6)
        out = _trailing_12m([0.1] * 6, dates)
        assert out[:3] == [None, None, None]
        assert out[3] == pytest.approx(0.4)

    def test_a_hike_shows_up_immediately(self):
        """The whole point: $0.01 x3 then $0.25 -> trailing 0.28, not 0.04."""
        from routers._asset_dividends import _trailing_12m
        dates = self._quarterly(8)
        vals = [0.01] * 7 + [0.25]
        out = _trailing_12m(vals, dates)
        assert out[-1] == pytest.approx(0.28)
        assert out[-2] == pytest.approx(0.04)

    def test_a_missing_eur_value_makes_the_whole_window_unknown(self):
        """A trailing total missing one quarter is not a smaller dividend."""
        from routers._asset_dividends import _trailing_12m
        dates = self._quarterly(6)
        out = _trailing_12m([None, 0.1, 0.1, 0.1, 0.1, 0.1], dates)
        assert out[3] is None       # window still contains the None
        assert out[4] == pytest.approx(0.4)

    def test_empty_input(self):
        from routers._asset_dividends import _trailing_12m
        assert _trailing_12m([], []) == []


class TestFetchGuards:
    @pytest.mark.parametrize("exchange", ["LSE", "NSE", "BOM", "DUB", None])
    def test_unsubscribed_exchanges_are_rejected_before_any_api_call(self, exchange):
        """The endpoint 403s rather than burning a GuruFocus call that would 403
        anyway — and rather than writing an empty series that reads as 'no
        dividend' when it means 'no coverage'."""
        from index_universe.acwi.exchange_map import is_gf_subscribed_exchange
        assert not is_gf_subscribed_exchange(exchange)
