"""The reconstructed cap-weighted index — the two bugs that each looked completely plausible.

Both were live in the first working version, and each on its own produced a number you would
have believed. Measured on the S&P 500, 2026 YTD, against SPY (the real index) at +9.02% USD:

    +21.70%   weights = TODAY's market cap          <- look-ahead bias
     +5.95%   weights right, prices unadjusted      <- one unfixed 9:1 split
     +9.10%   both fixed                            <- 8bp from the real index

The agreement with SPY is the test that matters; these pin the reasons it holds.
"""
from __future__ import annotations

import pytest

from routers._benchmark_index import _at_or_before, _split_adjust


class TestTheWeightIsAsOfTheStart:
    """Weighting a period's return by the market cap at the END of it is look-ahead bias: a
    stock that doubled has ~twice the cap it started with, so today's cap hands the winners a
    share of the index they never actually had.

    This is arithmetic, not opinion — the worked example below is the whole argument."""

    # Two names, equal cap at the start. One doubles, one is flat.
    START_CAP = {"winner": 100.0, "flat": 100.0}
    RETURN = {"winner": 100.0, "flat": 0.0}          # percent
    END_CAP = {"winner": 200.0, "flat": 100.0}       # the cap we actually STORE

    def test_the_truth_is_fifty_percent(self):
        """Equal weights at the start, so the index makes half of the winner's 100%."""
        total = sum(self.START_CAP.values())
        idx = sum(self.START_CAP[k] / total * self.RETURN[k] for k in self.START_CAP)
        assert idx == pytest.approx(50.0)

    def test_todays_cap_overstates_it(self):
        """The winner is now 2/3 of the index BECAUSE it won. Weighting by that gives 66.7%
        — a 17-point overstatement, in the same direction as the return, every time."""
        total = sum(self.END_CAP.values())
        idx = sum(self.END_CAP[k] / total * self.RETURN[k] for k in self.END_CAP)
        assert idx == pytest.approx(66.67, abs=0.01)
        assert idx > 50.0

    def test_rolling_the_cap_back_on_price_recovers_the_start_weight(self):
        """`cap_start = cap_now × (price_start / price_now)` — the share count is what stays
        put, so the price carries the cap. This is what the module does."""
        price = {"winner": (10.0, 20.0), "flat": (10.0, 10.0)}       # (start, now)
        back = {k: self.END_CAP[k] * (p0 / p1) for k, (p0, p1) in price.items()}
        assert back == pytest.approx(self.START_CAP)


class TestSplitAdjust:
    """Our stored closes are NOT split-adjusted and cannot self-heal: `ingest/prices.py` only
    fetches dates newer than what we hold, so when the vendor rewrites history for a split we
    never re-read it. 3 of 493 S&P constituents were affected in 2026 alone."""

    def test_a_real_split_is_rescaled(self):
        """KLA, 2026-06-08: 1929.20 -> 210.81, a ~9:1 split. Uncorrected it reads as -89%."""
        s = [("2026-06-05", 1929.20), ("2026-06-08", 210.81), ("2026-06-09", 215.00)]
        out, factor = _split_adjust(s)
        assert factor is not None
        # The pre-split close is brought onto the post-split basis: ~210.8, not 1929.2.
        assert out[0][1] == pytest.approx(210.81, rel=0.001)
        # And the period return becomes a small real move, not a fake collapse.
        assert (out[-1][1] / out[0][1] - 1) * 100 == pytest.approx(1.98, abs=0.5)

    def test_a_reverse_split_is_rescaled(self):
        """DuPont, 2026-06-23: 48.19 -> 140.01 (~1:3 reverse). The jump goes the OTHER way,
        and a naive 'prices only fall on splits' rule would miss it."""
        s = [("2026-06-22", 48.19), ("2026-06-23", 140.01)]
        out, factor = _split_adjust(s)
        assert factor is not None and factor > 1
        assert out[0][1] == pytest.approx(140.01, rel=0.01)

    def test_a_genuine_crash_is_NOT_adjusted(self):
        """The dangerous direction. A stock CAN fall 45% in a day — a failed trial, a fraud, a
        bid collapsing — and 'correcting' that erases a real loss. Only a ratio near a SMALL
        RATIONAL (what a split is) may be adjusted. -45% gives 1.818, whose nearest simple
        ratio is 2, which is 9% off — outside the 5% tolerance, so it stays a loss."""
        s = [("2026-03-01", 100.0), ("2026-03-02", 55.0)]
        out, factor = _split_adjust(s)
        assert factor is None
        assert out == s                                   # untouched
        assert (out[-1][1] / out[0][1] - 1) * 100 == pytest.approx(-45.0)

    def test_an_ordinary_move_is_untouched(self):
        s = [("2026-03-01", 100.0), ("2026-03-02", 103.5)]
        out, factor = _split_adjust(s)
        assert factor is None and out == s

    def test_a_bad_price_hits_the_index_TWICE(self):
        """Why an unadjusted split is worse than it looks. The start weight is backed out
        through the very same price (`cap_now × price_start/price_now`), so a 9x bogus ratio
        inflates the weight by 9x AND fakes an -89% return — both in the same direction."""
        cap_now, price_start, price_now = 100.0, 1929.20, 210.81   # KLA, uncorrected
        bogus_weight = cap_now * (price_start / price_now)
        assert bogus_weight == pytest.approx(915.1, rel=0.01)      # 9x too big
        assert (price_now / price_start - 1) * 100 < -88            # ...and an -89% "return"


class TestTheOpeningMark:
    def test_the_start_price_is_the_last_close_ON_OR_BEFORE_jan_1(self):
        """Not the first close AFTER it. Jan 1 is never a trading day, so 'first close of the
        year' silently measures from Jan 2 — a day of return already gone."""
        s = [("2025-12-30", 100.0), ("2025-12-31", 101.0), ("2026-01-02", 105.0)]
        assert _at_or_before(s, "2026-01-01") == ("2025-12-31", 101.0)

    def test_it_is_none_when_the_name_had_no_price_yet(self):
        """A company with no opening mark was not in the basket — it must be dropped, not
        given a start price from thin air."""
        s = [("2026-03-01", 50.0)]
        assert _at_or_before(s, "2026-01-01") is None


class TestSplitFactorFromJumpsAlone:
    """The index return reads TWO prices per member, so the panel now loads two marks plus the
    JUMPS between them (`_asset_benchmark.window_marks`) instead of every close — measured, ACWI
    was shipping 264,678 rows to use 3,368 numbers. `split_factor` is what makes that safe: the
    split evidence has to travel with the marks, because our stored closes are not split-adjusted
    and two bare prices cannot tell KLA's 9:1 from an -89% year.

    ⚠ IT TAKES CONSECUTIVE BARS ONLY, AND THAT IS THE ENTIRE SAFETY PROPERTY — see the docstring.
    """

    def test_a_real_split_is_recognised(self):
        from routers._benchmark_index import split_factor

        # KLA, 2026-06-08: 1929.20 -> 210.81, a 9:1.
        f = split_factor([(1929.20, 210.81)])
        assert f is not None and abs(f - 210.81 / 1929.20) < 1e-12

    def test_a_violent_day_is_left_alone(self):
        """A -45% crash is a loss, not a corporate action. Erasing it is the same error, signed."""
        from routers._benchmark_index import split_factor

        assert split_factor([(100.0, 55.0)]) is None

    def test_an_ordinary_move_is_not_even_considered(self):
        from routers._benchmark_index import split_factor

        assert split_factor([(100.0, 103.0)]) is None

    def test_two_splits_in_one_window_compound(self):
        from routers._benchmark_index import split_factor

        f = split_factor([(100.0, 50.0), (80.0, 20.0)])       # 1:2 then 1:4
        assert f is not None and abs(f - 0.125) < 1e-12

    def test_a_stock_that_doubles_over_a_WINDOW_is_not_a_split(self):
        """⚠ THE FAILURE THIS FUNCTION'S CONTRACT EXISTS TO PREVENT. Handing it the window's two
        ENDPOINTS instead of consecutive bars turns the test into a test of the RETURN: a name up
        100% gives exactly 2.0, matches the 1:2 whitelist to 0%, and its gain is 'corrected' away.
        The value below IS a match — which is why only consecutive bars may ever be passed in, and
        why `window_marks` selects them with `lag()` rather than from the marks."""
        from routers._benchmark_index import split_factor

        assert split_factor([(50.0, 100.0)]) is not None   # a 1:2 "split" — as a pair, it matches

    def test_it_is_the_only_copy_of_the_whitelist_test(self):
        """`_split_adjust` (full series) and the marks path must not drift on what a split is."""
        import inspect

        from routers import _benchmark_index as m

        assert "split_factor(" in inspect.getsource(m._split_adjust)
        assert "split_factor(" in inspect.getsource(m._window_rows)
