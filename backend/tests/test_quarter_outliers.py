"""A corrupt quarter must not be laundered into a confident annual figure by the TTM sum.

⚠⚠ MEASURED 2026-08-13 ON THE ACWI EPS LINE. GuruFocus's quarterly `EPS without NRI` feed carries
occasional garbage, five orders of magnitude out:

    IAG      0.038  0.174  10,987.996  0.265  0.022  0.089  8,748.852  0.184   (annual: ~0.7-1.2)
    Workday  676    2.47   2.32   2.21   2.23   1.92   1.89   1.75            (annual: 9.23)

`_ttm_by_period` summed four consecutive quarters faithfully and produced an LTM of **10,988** for
a company whose annual EPS has never exceeded 1.20. The benchmark line is a cap-weighted index of
REBASED members, so that one 0.03%-weight constituent contributed **+390pp of the index's +411pp
step**: the blended ACWI EPS went 1,015 → **5,186** in a single quarter, with no error anywhere.
After the guard it reads 1,180.7 — a +16.3% trailing year, which is what a TTM should look like.

⚠ THE DAMAGE IS THE LAUNDERING, NOT THE CELL. One absurd value in a chart of raw quarters is
obvious; the same value summed into a "trailing twelve months" and then indexed is a plausible
number on a log axis, and it moves an index built from 1,500 companies.
"""
from __future__ import annotations

from routers.earnings import (
    _QUARTER_OUTLIER_FACTOR, _drop_quarter_outliers, _level_shift, _ttm_by_period,
)

# The real IAG series, as stored.
IAG = {
    "2024-03-31": 0.001, "2024-06-30": 0.184, "2024-09-30": 8748.852, "2024-12-31": 0.089,
    "2025-03-31": 0.022, "2025-06-30": 0.265, "2025-09-30": 10987.996, "2025-12-31": 0.174,
    "2026-03-31": 0.038,
}


class TestTheMeasuredCorruptions:
    def test_IAG_loses_only_the_two_absurd_quarters(self):
        kept = _drop_quarter_outliers(IAG)
        assert set(IAG) - set(kept) == {"2024-09-30", "2025-09-30"}
        assert len(kept) == 7

    def test_workday_loses_only_its_one(self):
        wday = {"2024-04-30": 1.74, "2024-07-31": 1.75, "2024-10-31": 1.89, "2025-01-31": 1.92,
                "2025-04-30": 2.23, "2025-07-31": 2.21, "2025-10-31": 2.32, "2026-01-31": 2.47,
                "2026-04-30": 676.0}
        assert set(wday) - set(_drop_quarter_outliers(wday)) == {"2026-04-30"}

    def test_the_ttm_no_longer_carries_it(self):
        """⚠ THE POINT DISAPPEARS RATHER THAN BEING PATCHED. A dropped quarter leaves the windows
        containing it without `k` consecutive filings, so they produce no TTM at all — a hole, not
        a fabricated year. 10,988 was the number that reached the chart."""
        ttm = _ttm_by_period([{"target_date": d, "numeric_value": v} for d, v in IAG.items()],
                             "sum", key="date")
        assert all(abs(v) < 5 for v in ttm.values()), ttm


class TestItMustNotCLIPREALVOLATILITY:
    """⚠ A GUARD THAT FIRES ON GENUINE DATA WOULD BE WORSE THAN THE BUG — it would quietly smooth
    the volatility the chart exists to show, and nobody would ever see what was removed. The factor
    is deliberately enormous (50x) because the measured corruptions are 300x and 63,000x, while a
    business swinging hard lands an order of magnitude below that."""

    def test_a_loss_quarter_against_profitable_ones_survives(self):
        s = {"a": 2.1, "b": 2.4, "c": -3.8, "d": 2.2, "e": 2.6}
        assert _drop_quarter_outliers(s) == s

    def test_a_tenfold_recovery_quarter_survives(self):
        s = {"a": 0.20, "b": 0.25, "c": 0.18, "d": 2.40, "e": 0.30}
        assert _drop_quarter_outliers(s) == s

    def test_a_series_legitimately_in_the_thousands_survives(self):
        """⚠ "10,988 is too big" is not a fact about a number. A KRW or JPY per-share figure is
        genuinely in the thousands, which is why the bar is the company's OWN median."""
        s = {"a": 4200.0, "b": 5100.0, "c": 3800.0, "d": 6000.0}
        assert _drop_quarter_outliers(s) == s

    def test_a_mostly_negative_series_still_has_a_scale(self):
        """The median is over ABSOLUTE values, so a persistently loss-making company is judged
        against its own magnitudes rather than against zero."""
        s = {"a": -2.0, "b": -2.4, "c": -1.8, "d": -2.2}
        assert _drop_quarter_outliers(s) == s
        assert set(_drop_quarter_outliers({**s, "e": -900.0})) == {"a", "b", "c", "d"}


class TestItRefusesToJudgeWhatItCannotMeasure:
    def test_too_few_quarters_are_left_alone(self):
        """With three points there is no "out of line" to be out of — and refusing to guess beats
        dropping a real filing from a company that has just started reporting."""
        s = {"a": 0.1, "b": 0.2, "c": 900.0}
        assert _drop_quarter_outliers(s) == s

    def test_an_all_zero_series_has_no_scale_and_is_untouched(self):
        """median 0 would make the limit 0 and drop every non-zero value — the guard turning into
        the corruption."""
        s = {"a": 0.0, "b": 0.0, "c": 0.0, "d": 5.0}
        assert _drop_quarter_outliers(s) == s

    def test_a_clean_series_is_returned_unchanged(self):
        s = {"a": 1.0, "b": 1.1, "c": 1.2, "d": 1.3, "e": 1.15}
        assert _drop_quarter_outliers(s) == s

    def test_the_factor_stays_far_above_anything_a_business_does(self):
        assert _QUARTER_OUTLIER_FACTOR >= 20


class TestSizeAloneCannotTellABadCellFromABusinessThatChangedSize:
    """⚠⚠ THE SECOND VERSION OF THIS GUARD WAS DELETING REAL LEVEL SHIFTS — measured 2026-08-14 on
    a local run, on a series with a median of 19.15:

        960.171   1,099.847   1,122.77

    Its three NEWEST quarters, consecutive, each within ~15% of the next, only 50-59x over the bar.
    Nothing about that is garbage — it is an acquisition, a redenomination or a reverse split — and
    dropping it removed the newest year from every card built on the line, silently.

    The discriminator is SHAPE. A corrupt value is ALONE: healthy quarters either side, and the
    series carries on at its own scale. A regime change arrives and STAYS.
    """

    SHIFT = {"2024-06-30": 19.1, "2024-09-30": 19.4, "2024-12-31": 18.8, "2025-03-31": 19.2,
             "2025-06-30": 19.0, "2025-09-30": 960.171, "2025-12-31": 1099.847,
             "2026-03-31": 1122.77}

    def test_the_measured_level_shift_survives_intact(self):
        assert _drop_quarter_outliers(self.SHIFT) == self.SHIFT

    def test_the_two_quarter_variant_survives_too(self):
        """The same series read by an endpoint whose window missed 2025-09 — two adjacent, still a
        run, still kept. Two readers of one company must not disagree about its history."""
        s = {k: v for k, v in self.SHIFT.items() if k != "2025-09-30"}
        s["2025-12-31"] = 972.014
        s["2026-03-31"] = 1018.105
        assert _drop_quarter_outliers(s) == s

    def test_but_a_lone_cell_inside_the_SAME_series_is_still_dropped(self):
        """⚠ THE RULE IS NOT "THIS SERIES IS EXEMPT". A shift and a bad cell can coexist, and the
        one that is alone still goes."""
        s = {**self.SHIFT, "2024-12-31": 90_000.0}
        assert set(self.SHIFT) - set(_drop_quarter_outliers(s)) == {"2024-12-31"}

    def test_a_mid_series_run_that_returns_to_normal_is_DROPPED(self):
        """⚠⚠ "ADJACENT" ALONE KEPT THIS, AND IT WAS WRONG. An excursion that ENDED is the opposite
        of a level shift, however many quarters it spanned — measured 2026-08-14, six of eight kept
        runs were exactly this, all of them finishing in 2015-2017 with normal quarters after."""
        s = {"a": 19.0, "b": 19.4, "c": 2000.0, "d": 2100.0, "e": 19.2, "f": 18.8}
        assert set(s) - set(_drop_quarter_outliers(s)) == {"c", "d"}

    def test_a_LONG_oscillating_run_is_dropped_however_adjacent(self):
        """The measured worst case: `+120 +190 +182 −140 +148 −818 −1700 −120 −2184` against a
        median of 2.11, nine consecutive quarters, preserved by the adjacency-only rule as a "level
        shift". A level does not change sign five times, and this one ended in 2017."""
        spikes = [120.363, 189.831, 181.715, -139.718, 148.38, -817.707, -1700.301, -120.23, -2184.282]
        s = {f"{i:02d}": v for i, v in enumerate(spikes)}
        s.update({f"{i:02d}": 2.113 for i in range(len(spikes), len(spikes) + 12)})
        assert set(s) - set(_drop_quarter_outliers(s)) == {f"{i:02d}" for i in range(len(spikes))}

    def test_once_the_new_level_is_the_MAJORITY_nothing_is_flagged_at_all(self):
        """The median is self-limiting in the same direction — a company two years past its
        acquisition simply has a new scale."""
        s = {"a": 19.0, "b": 19.4, "c": 1000.0, "d": 1100.0, "e": 1050.0, "f": 1120.0}
        assert _drop_quarter_outliers(s) == s


class TestWhatCountsAsALevelShift:
    """A run of flagged quarters reaching the NEWEST filing, and nothing else.

    ⚠ THE SECOND CONDITION FOLLOWS FROM THE MEDIAN, it is not a fitted heuristic: a shift that
    happened and persisted becomes the majority of the series and stops being flagged at all. So a
    run that is STILL flagged can only be recent, and a recent one necessarily reaches the end.
    """

    AXIS = ["2024-06-30", "2024-09-30", "2024-12-31", "2025-03-31",
            "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"]

    def test_a_run_to_the_newest_filing_is_a_shift(self):
        assert _level_shift({"2025-12-31", "2026-03-31"}, self.AXIS) == {"2025-12-31", "2026-03-31"}

    def test_a_run_that_STOPS_MID_HISTORY_is_not(self):
        assert _level_shift({"2024-06-30", "2024-09-30"}, self.AXIS) == set()

    def test_a_run_of_ONE_is_not_a_run_even_at_the_end(self):
        """⚠ WORKDAY'S CONFIRMED BAD CELL WAS THE NEWEST FILING (676 against a median of 2.2), so
        "reaches the end" alone would have kept it. Two points is the minimum at which "it stayed
        there" means anything."""
        assert _level_shift({"2026-03-31"}, self.AXIS) == set()

    def test_IAGs_two_bad_cells_are_not_a_shift(self):
        assert _level_shift({"2024-09-30", "2025-09-30"}, self.AXIS) == set()

    def test_a_lone_newest_cell_beside_an_older_run_is_not_a_shift(self):
        """Measured on 3523's FCF/share: a 3-quarter run in 2024-25 and one more in 2026-03 with a
        healthy quarter between. The run does not reach the end and the end is alone — so ALL of
        them go, which is the coherent answer. The adjacency-only rule kept three and dropped one
        of four anomalies of the same magnitude, in one series."""
        assert _level_shift({"2024-09-30", "2024-12-31", "2026-03-31"}, self.AXIS) == set()

    def test_a_SKIPPED_period_does_not_make_a_phantom_gap(self):
        """A semi-annual filer's consecutive filings are six months apart; they are still
        neighbours, because nothing was filed between them."""
        axis = ["2024-06-30", "2024-12-31", "2025-06-30", "2025-12-31"]
        assert _level_shift({"2025-06-30", "2025-12-31"}, axis) == {"2025-06-30", "2025-12-31"}
