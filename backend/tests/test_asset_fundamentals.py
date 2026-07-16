"""The four soundness charts — price vs fair value, yield, ROIC vs WACC, safety.

Both bugs pinned here SHIPPED and neither raised anything. That is the theme: this module reads a
blob whose sections were renamed and whose values need FX, and BOTH failure modes are silent.
"""
from __future__ import annotations

import inspect

import pytest

from routers import _asset_fundamentals as f
from routers._asset_financials import _SECTION_ALIASES


class TestTheCachedSectionNamesWereMeasuredNotGuessed:
    """⚠ THE OLD NAME IS NOT THE NEW ONE TITLE-CASED, AND GUESSING GAVE AN EMPTY CHART.

    GuruFocus renamed these sections; Storage holds blobs from before and after. Mechanically
    title-casing the snake_case name is right for three of five and WRONG for two:

        common_size_ratios  ->  "Ratios"              NOT "Common Size Ratios"
        gurufocus_rankings  ->  "Gurufocus Rankings"  NOT "GuruFocus Rankings"   (lowercase f)

    Measured on Apple's real Storage blob. With the guessed names, ROIC and WACC returned 0 points
    of 40 — the whole ROIC-vs-WACC chart was blank, with no error anywhere: the section simply is
    not there, so `_series` finds nothing and returns []. An empty chart reads as "this company has
    no ROIC", which for Apple (39.38%) is quite a claim.
    """

    def test_common_size_maps_to_Ratios(self):
        assert _SECTION_ALIASES["common_size"] == ("common_size_ratios", "Ratios")

    def test_rankings_has_a_lowercase_f(self):
        assert _SECTION_ALIASES["rankings"] == ("gurufocus_rankings", "Gurufocus Rankings")

    def test_every_ratio_section_offers_both_spellings(self):
        """One spelling means every blob on the other side of the rename reads as no-data."""
        for key in ("valuation", "quality", "common_size", "rankings", "per_share"):
            assert len(_SECTION_ALIASES[key]) == 2, f"{key} needs the live AND the cached name"


class TestDroppedIsCountedAfterTheConversion:
    """⚠ `dropped` LIED, IN THE FIELD WHOSE ONLY JOB IS NOT LYING.

    `_to_eur` drops any period with no FX rate on or before it, and `fx_rate`'s history is thin —
    Apple's fair values go 40 periods -> 27, losing 1986-1998 outright. Counting drops BEFORE the
    conversion reported `dropped: 0` beside a series that had quietly lost thirteen years.
    """

    def test_it_recounts_from_the_points_as_they_stand(self):
        s = {"period_count": 40, "points": [{"date": "2020-12-31", "value": 1.0}] * 27}
        assert f._dropped(s)["dropped"] == 13

    def test_a_complete_series_drops_nothing(self):
        s = {"period_count": 5, "points": [{"date": "2020-12-31", "value": 1.0}] * 5}
        assert f._dropped(s)["dropped"] == 0

    def test_it_never_goes_negative(self):
        """More points than periods is nonsense, not a negative gap."""
        s = {"period_count": 2, "points": [{"date": "2020-12-31", "value": 1.0}] * 5}
        assert f._dropped(s)["dropped"] == 0

    def test_the_fair_values_are_recounted_after_to_eur(self):
        src = inspect.getsource(f.compute_fundamentals)
        # The recount must come after the conversion, not before it.
        assert 'fair.append(_dropped(s))' in src
        assert src.index('_to_eur(s["points"]') < src.index("fair.append(_dropped(s))")


class TestThePriceIsYfinanceAndNeverGuruFocus:
    """/portfolios prices everything from `asset_price`. A GuruFocus price line here would put a
    second vendor — different adjustment conventions, different FX — on a page whose whole claim is
    that its numbers are comparable."""

    def test_the_price_comes_from_the_asset_helpers(self):
        src = inspect.getsource(f._eur_price)
        assert "_airs_portfolio_perf" in src
        assert "_closes" in src and "_eur_series" in src
        # ⚠ The BODY, not the source. `_eur_price`'s docstring says "Nothing here may reach
        # `metric_data`" — a naive grep for that string fails on the sentence forbidding it, and
        # a guard that fires on its own documentation teaches people to delete the documentation.
        # (Second time this session; the earlier one was `portfolioVariants.ts`.)
        body = src.split('"""', 2)[-1]
        assert "metric_data" not in body

    def test_the_gurufocus_price_is_fetched_only_as_a_crosscheck(self):
        """It IS read — if it and the yfinance line diverge after FX, the ISIN reached two
        different securities and every fair value belongs to the other one. It is never drawn,
        and the payload name says so."""
        src = inspect.getsource(f.compute_fundamentals)
        assert '"price_crosscheck_eur"' in src
        assert f._GF_PRICE[0] == "Month End Stock Price"


class TestBothLegsOfChartOneAreEUR:
    """⚠ GuruFocus FX-converts financials into ITS listing's currency, and its listing comes from
    `pick_listing` — a different id space from `yahoo_symbol`. A USD price line through EUR fair
    values reads the exchange rate as mispricing."""

    def test_the_fair_values_are_converted(self):
        src = inspect.getsource(f.compute_fundamentals)
        assert '_to_eur(s["points"], currency, fx)' in src

    def test_a_point_with_no_rate_is_dropped_not_carried_native(self):
        """Silently leaving a Graham Number in dollars beside a EUR price line IS the bug."""
        fx = {"USD": {"2020-12-31": 1.1}}
        pts = [{"date": "2020-12-31", "value": 110.0}, {"date": "1990-12-31", "value": 5.0}]
        out = f._to_eur(pts, "USD", fx)
        assert len(out) == 1
        assert out[0]["value"] == pytest.approx(100.0)

    def test_eur_passes_through_untouched(self):
        pts = [{"date": "2020-12-31", "value": 42.0}]
        assert f._to_eur(pts, "EUR", {}) == pts
        assert f._to_eur(pts, None, {}) == pts


class TestTheBandIsTheFiveSeries:
    """⚠ NOT the eleven in `summary.chart` — those are SCALARS (today's number, no history) and its
    two DCFs read 0.00, i.e. not computed. A band needs series."""

    def test_the_five(self):
        assert [lab for _, lab in f._FAIR_VALUES] == [
            "Projected FCF", "Median P/S", "Peter Lynch", "Graham Number", "Earnings Power"]

    def test_the_liquidation_floors_are_excluded(self):
        """`Net-Net Working Capital` and `Net Current Asset Value` ARE series in that section, and
        are meaningless for a going concern (-36.34 and -8.49 on a real blob). Including them
        would drag the band's floor below zero on every healthy company."""
        fields = [fld for fld, _ in f._FAIR_VALUES]
        assert "Net-Net Working Capital" not in fields
        assert "Net Current Asset Value" not in fields


class TestChartsTwoToFourAreNeverConverted:
    """A ROIC of 18% is 18% in every currency; a Piotroski score is a count. This is
    `_asset_financials`'s "skip the conversion, do not relabel it" rule — the one its unit system
    already implements for share counts, and the reason its registry bans ratios outright."""

    def test_no_conversion_touches_the_ratio_charts(self):
        src = inspect.getsource(f.compute_fundamentals)
        for key in ('"yields"', '"returns"', '"safety"'):
            line = next(x for x in src.splitlines() if key in x and "_series_out" in x)
            assert "_to_eur" not in line, f"{key} must not be FX-converted"


class TestTheQualityVerdictIsFourNumbersNotOne:
    """⚠ NO COMPOSITE SCORE, EVER. The disagreement between the four IS the finding: Intel reads a
    passable +3.1pp spread while its ROIC fell SEVENTEEN points across the decade. Any single
    0-100 averages the melting moat away — which is exactly what GuruFocus's GF Score does, and
    why it is not used here.

    Measured across 14 large caps on our own blobs (2026-07-16), 10y medians of ROIC-WACC:

        NVDA +65.3pp   AAPL +18.8pp   MSFT +14.9pp   JNJ +9.0pp   KO +6.0pp
        INTC  +3.1pp   IBM   +0.3pp   KHC  -2.0pp    F  -1.9pp    AMD -8.4pp

    That ranking fell out with no tuning.
    """

    def test_the_four_and_only_the_four(self):
        from routers._asset_fundamentals import _quality

        blob = {"financials": {"annuals": {}}}
        assert [m["key"] for m in _quality(blob, "annuals")] == [
            "spread", "trend", "conversion", "gm_sd"]

    def test_no_composite_is_emitted(self):
        """Four metrics and no fifth aggregate.

        Asserted on the OUTPUT. The first version grepped the source for "score" and failed — on
        the comment explaining why there is no score ("a loss-making year would SCORE as excellent
        conversion"). That is the THIRD guard this session to fire on its own documentation
        (`portfolioVariants.ts`, `_eur_price`, this). A source grep tests the prose; the shape of
        the return value is the actual invariant.
        """
        from routers._asset_fundamentals import _quality

        out = _quality({"financials": {"annuals": {}}}, "annuals")
        assert len(out) == 4
        assert not any(m["key"] in ("score", "composite", "overall", "grade") for m in out)
        assert all({"key", "label", "value", "status"} <= set(m) for m in out)


class TestTheTwoVerdictBugsThatShipped:
    """Both produced a WRONG ANSWER on a real company, not an error. A quality card that is
    confidently wrong about NVIDIA is a card nobody uses twice."""

    def test_a_rising_gross_margin_is_not_a_loss_of_pricing_power(self):
        """⚠ σ CANNOT TELL A COLLAPSE FROM AN IMPROVEMENT. NVIDIA's gross-margin σ is 5.9 —
        because it went from ~35% to ~75%. The first version flagged that as "the market sets the
        price", failing the one company in the sample with the most pricing power in it. σ now
        fails only when the margin is high-variance AND NOT improving."""
        src = inspect.getsource(f._quality)
        assert "gm_trend" in src
        assert "not (gm_trend is not None and gm_trend > 0)" in src

    def test_a_banks_cash_conversion_is_inapplicable_not_terrible(self):
        """⚠ JPMorgan scored 0.19x. That is not a company failing to collect its profits — a
        bank's operating cash flow tracks its LOAN BOOK (this repo already documents JPM's OCF at
        -147,782 as information, not a fault). Reported n_a, not as a catastrophic failure."""
        src = inspect.getsource(f._quality)
        assert "conv_ok = has_roic" in src
        assert 'applicable=conv_ok' in src

    def test_a_bank_gets_no_verdict_at_all_rather_than_four_failures(self):
        """All four are built on ROIC / gross margin / cash conversion, and a bank has none of the
        three in a comparable sense. Four red chips would be a confident wrong answer; the strip
        says the card cannot judge it."""
        from routers._asset_fundamentals import _quality

        bank = {"financials": {"annuals": {"Ratios": {"WACC %": ["9"] * 10}}}}
        assert all(m["status"] in ("n_a", "unknown") for m in _quality(bank, "annuals"))


class TestAbsenceIsNotFailure:
    """`n_a` (the line does not exist) and `unknown` (too little history) must never collapse into
    `fail` — that marks every bank a bad business and every young company a suspect one."""

    def test_inapplicable_is_n_a(self):
        m = f._metric("k", "L", "pp", None, 0, False, applicable=False)
        assert m["status"] == "n_a"

    def test_no_value_is_unknown_not_fail(self):
        m = f._metric("k", "L", "pp", None, 3, False)
        assert m["status"] == "unknown"

    def test_a_measured_pass_and_a_measured_fail(self):
        assert f._metric("k", "L", "pp", 18.8, 10, False)["status"] == "ok"
        assert f._metric("k", "L", "pp", -2.0, 10, True)["status"] == "fail"


class TestTheMediansRefuseThinHistory:
    """A 10y median off three points is not a median, and a trend needs a prior 5 to compare
    against. Both refuse rather than compute — the same rule the Sharpe follows at MIN_STAT_DAYS."""

    def test_the_floors(self):
        from routers import _asset_fundamentals as m

        assert m._MIN_MEDIAN_PERIODS >= 6
        assert m._MIN_TREND_PERIODS >= 10          # 5 + 5

    def test_a_loss_year_is_excluded_from_cash_conversion(self):
        """With net income negative the ratio flips sign, and a loss-making year would score as
        excellent conversion."""
        src = inspect.getsource(f._quality)
        assert "if x and x > 0" in src
