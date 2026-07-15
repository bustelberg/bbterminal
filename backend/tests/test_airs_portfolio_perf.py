"""Portfolio YTD — the two ways this number lies if you let it.

A model portfolio is a COMPOSITION, not an account. AIRS stores what it should hold, not a
track record, and its snapshot dropdown offers 2-3 dates — not a monthly history. So the only
composition we can have is the CURRENT one, and everything below follows from that.
"""
from __future__ import annotations

import inspect

import pytest

from momentum.diversification import annualized_stats
from routers._airs_portfolio_perf import (
    GOOD_COVERAGE_PCT,
    LOOKTHROUGH_MIN_COVERAGE,
    MIN_CAGR_DAYS,
    MIN_COVERAGE_PCT,
    MIN_STAT_DAYS,
    TRADING_DAYS,
    _daily_returns,
    _eur_return,
    _index,
    _lookthrough_series,
    _mark_at,
    _MAX_INTERP_SPAN_DAYS,
    compute_holding_marks,
    compute_portfolio_performance,
    ytd_anchor_for,
)
from routers.airs import _shape_positions


class TestHindsight:
    """Pricing TODAY's weights back to 1 January backtests a basket that was chosen knowing how
    the year went. When the model predates the year that is harmless — the weights really were
    held throughout. When it does not, the number is not a track record.

    This was not a theoretical worry. Measured 2026-07-13, with the YTD anchored at Jan 1:

        MoTopSelectie_FX    YTD +75.85%    model effective 2026-07-05  (EIGHT DAYS EARLIER)
                            realized over those eight days:  +0.51%

    It was the best-performing portfolio in the list, on weights it had never held. So the YTD
    window now OPENS at the inception instead: `ytd_anchor = max(Jan 1, inception)`.
    """

    def test_the_ytd_window_never_opens_before_the_composition_existed(self):
        """The fix. A model younger than the year is measured from its own inception, so there
        is no stretch of the window in which we price weights it had not chosen yet."""
        assert ytd_anchor_for("2025-04-08", 2026) == "2026-01-01"   # older -> a real YTD
        assert ytd_anchor_for("2026-07-05", 2026) == "2026-07-05"   # younger -> from inception
        assert ytd_anchor_for("2026-01-01", 2026) == "2026-01-01"   # exactly Jan 1 -> Jan 1
        assert ytd_anchor_for(None, 2026) == "2026-01-01"

        src = inspect.getsource(compute_portfolio_performance)
        assert "ytd_anchor = ytd_anchor_for(eff, year)" in src
        assert "_index(legs, ytd_anchor)" in src

    def test_the_expanded_price_marks_share_that_anchor(self):
        """ONE definition of the window, used by the portfolio figure AND by the per-holding
        marks shown when the row is expanded. Two definitions and the entry prices printed under
        a +51.48% would belong to a different window — and reconcile with nothing."""
        src = inspect.getsource(_shape_positions)
        assert "ytd_anchor_for(" in src
        assert "compute_holding_marks(" in src

    def test_a_model_younger_than_the_year_is_still_flagged(self):
        """The flag survives the fix, with a NEW meaning: not "this is a backtest" (it isn't any
        more) but "this is a PARTIAL year". Six days of return and twelve months of it are not
        comparable merely by sharing a column, and the table sorts on that column."""
        src = inspect.getsource(compute_portfolio_performance)
        assert 'eff > jan1' in src
        assert "model_changed_in_period" in src
        assert '"ytd_from": ytd_anchor' in src, "the window it opened must be reported"

    def test_since_model_is_measured_from_the_models_own_date(self):
        """Unchanged, and still the longer window: since-inception spans the model's WHOLE life,
        with no January floor under it. For a model younger than the year the two windows
        coincide — that is the point, not a bug — but for the 29 that predate it, this one
        reaches back years and the YTD does not."""
        src = inspect.getsource(compute_portfolio_performance)
        assert "since_curve, since_w = _index(legs, eff)" in src
        assert "jan1" not in src.split("since_curve, since_w", 1)[1].split("since_pct", 1)[0]


class TestCoverageFloor:
    """25 of 248 held ISINs have no price series at all (Leonteq structured products, in-house
    funds — the zero-bar guard in `store_one` refuses to map them). Renormalising over what
    remains assumes the rest behaved the same. At 95% that is a rounding error. At 1% it is a
    fabrication:

        TOPS_OFF_BEH   "+0.00% YTD"   <- its 1% CASH line, renormalised to 100%,
                                         while 9 structured products (99%) were dropped.

    A precise, confident, entirely invented number. So below the floor we return nothing.
    """

    def test_there_is_a_floor_and_it_is_not_trivial(self):
        assert MIN_COVERAGE_PCT >= 50
        assert GOOD_COVERAGE_PCT > MIN_COVERAGE_PCT

    def test_below_the_floor_no_number_is_returned(self):
        src = inspect.getsource(compute_portfolio_performance)
        assert "enough = covered >= MIN_COVERAGE_PCT" in src
        # The curve is emptied, and every figure read off it therefore goes with it — rather
        # than the number being suppressed at one output site and surviving at another.
        assert "if not enough:\n            ytd_curve = []" in src
        assert '"ytd_pct": ytd_pct' in src
        assert "ytd_pct = (ytd_curve[-1] - 1.0) * 100.0 if ytd_curve else None" in src

    def test_coverage_is_reported_even_when_the_number_is_refused(self):
        """`covered_pct` IS the reason for the refusal — withholding it would leave the reader
        with an unexplained blank."""
        src = inspect.getsource(compute_portfolio_performance)
        after = src.split("enough =", 1)[1]
        assert '"covered_pct": covered' in after

    def test_the_one_percent_case_would_have_been_a_lie(self):
        """What the floor prevents, in numbers: 1% cash at 0%, renormalised, IS '+0.00%'."""
        holdings = [{"w": 1.0, "ret": 0.0}]              # the cash line, alone
        num = sum(h["w"] * h["ret"] for h in holdings)
        den = sum(h["w"] for h in holdings)
        assert num / den == 0.0                          # a confident, precise, invented 0.00%
        assert (den / 100.0) * 100 < MIN_COVERAGE_PCT    # ...and 1% coverage, so: refused


class TestCashIsPricedNotSkipped:
    def test_cash_counts_toward_the_return_at_zero(self):
        """Cash's drag is a FACT, not a gap. Dropping it from the denominator would silently
        scale a 20%-cash portfolio's return up by 25%.

        It enters as a LEG with no price series, which `_index` holds flat at 1.0 — so it is in
        the weight the curve renormalises over, and it contributes zero return to it. (The
        arithmetic of that is pinned in `test_cash_is_a_leg_that_never_moves`.)"""
        src = inspect.getsource(compute_portfolio_performance)
        cash_branch = src.split("Cash. A 0% return is a FACT", 1)[1].split("continue", 1)[0]
        assert "legs.append((w, None))" in cash_branch     # priced, at a flat zero...
        assert "unpriced" not in cash_branch               # ...never dropped as unpriceable

    def test_dropping_cash_would_inflate_the_return(self):
        """80% equities at +10%, 20% cash. Including cash: +8%. Dropping it: +10%."""
        with_cash = (80 * 10.0 + 20 * 0.0) / 100
        without_cash = (80 * 10.0) / 80
        assert with_cash == pytest.approx(8.0)
        assert without_cash == pytest.approx(10.0)


class TestPostgrestPaging:
    def test_the_price_read_pages(self):
        """223 holdings x ~500 trading days (since-inception reaches back to 2024-06) is
        ~118,000 rows. PostgREST caps a response at 1,000 and TRUNCATES SILENTLY — unpaged,
        this computes a confident number off 1% of the data. (I hit exactly this while probing
        coverage: it reported 102 priced holdings when the answer was 221.)"""
        from routers._airs_portfolio_perf import _closes_paged

        src = inspect.getsource(_closes_paged)
        assert ".range(off, off + 999)" in src
        assert "off += 1000" in src

    def test_the_copy_path_falls_back_rather_than_failing(self):
        """COPY is an optimisation, not a dependency: `asset_price` has no PostgREST fallback
        inside `load_series`, so an unconfigured `SUPABASE_DB_URL` raises. This endpoint must
        answer anyway — it did before COPY existed."""
        from routers._airs_portfolio_perf import _closes

        src = inspect.getsource(_closes)
        assert "except SeriesUnavailable:" in src
        assert "return _closes_paged(" in src


class TestSinceInceptionCurve:
    """Sharpe and Sortino need a daily curve, and the curve is where a portfolio return can go
    quietly wrong: holdings sit on different exchange calendars, and they list and delist."""

    def test_the_curve_ends_where_the_weighted_return_does(self):
        """THE identity that lets `since_model_pct` be read off the curve instead of computed a
        second way. A buy-and-hold's final value IS the weighted sum of its holdings' returns —
        so if these two ever disagree, one of them is wrong, and having only one means neither
        can drift from the other on some surface nobody re-checked."""
        a = [("2026-01-01", 100.0), ("2026-01-02", 110.0)]   # +10%
        b = [("2026-01-01", 50.0), ("2026-01-02", 45.0)]     # -10%
        legs = [(60.0, a), (40.0, b)]

        curve, held = _index(legs, "2026-01-01")
        assert held == 100.0
        from_curve = (curve[-1] - 1.0) * 100.0
        weighted = (60.0 * _eur_return(a, "2026-01-01")
                    + 40.0 * _eur_return(b, "2026-01-01")) / 100.0
        assert from_curve == pytest.approx(weighted)
        assert from_curve == pytest.approx(2.0)              # 0.6*10 + 0.4*(-10)

    def test_a_holding_that_did_not_trade_holds_its_price_it_does_not_go_to_zero(self):
        """A Tokyo holiday is a normal Wednesday in Paris. Sampling on the union of both
        calendars, the missing bar means "still held, last price" — not a 0% day and certainly
        not a missing one. Read either other way, the daily vol underneath Sharpe is fiction."""
        paris = [("2026-01-01", 100.0), ("2026-01-02", 100.0), ("2026-01-03", 120.0)]
        tokyo = [("2026-01-01", 100.0), ("2026-01-03", 100.0)]     # no bar on the 2nd
        curve, _ = _index([(50.0, paris), (50.0, tokyo)], "2026-01-01")

        assert len(curve) == 3                                  # anchor + 2 union dates
        assert curve[1] == pytest.approx(1.0)                   # the 2nd: neither leg moved
        assert curve[-1] == pytest.approx(1.10)                 # +20% on half the book

    def test_cash_is_a_leg_that_never_moves(self):
        """Not skipped. A 20%-cash model that returns 10% on its equities made 8%, and dropping
        the cash line would report 10% — scaling the return up by 25% for free."""
        eq = [("2026-01-01", 100.0), ("2026-01-02", 110.0)]
        curve, held = _index([(80.0, eq), (20.0, None)], "2026-01-01")
        assert held == 100.0
        assert (curve[-1] - 1.0) * 100.0 == pytest.approx(8.0)

    def test_a_holding_not_yet_listed_at_inception_comes_OUT_of_the_weight(self):
        """The gate. An ETF that listed in 2025 was NOT in a model whose inception is 2024 — it
        has no opening mark, so it cannot be held from there. `_index` renormalises over what is
        left, which is exactly why the weight it kept has to come back out: without it, a curve
        built from a quarter of the portfolio renders identically to one built from all of it."""
        late = [("2026-06-01", 10.0), ("2026-06-02", 20.0)]     # listed long after the anchor
        early = [("2024-06-01", 100.0), ("2026-06-02", 100.0)]
        _, held = _index([(30.0, early), (70.0, late)], "2024-06-01")
        assert held == 30.0                                     # the 70% could not be held there

    def test_the_since_coverage_floor_is_its_own_not_ytds(self):
        """Coverage at the YTD anchor says nothing about coverage two years earlier, so the
        since-inception figures are gated on the weight THEIR OWN curve could hold."""
        src = inspect.getsource(compute_portfolio_performance)
        assert "since_covered = (since_w / total_w * 100.0)" in src
        assert "if since_covered < MIN_COVERAGE_PCT:" in src


class TestHoldingMarks:
    """The per-holding entry/exit marks shown when a portfolio row is expanded. They are not a
    second calculation of the portfolio's return — they are the SAME one, itemised."""

    def test_weighting_the_marks_reproduces_the_portfolio_return(self):
        """THE property. Weight each holding's `return_pct` by the model's percentages and you
        get the row's YTD back — because both come from the same EUR series and the same anchor.
        (Measured against the live DB: AITopSelectie OFF FX, 51.4812% both ways.)

        If these ever diverge, the expanded rows quietly "explain" a number they do not add up
        to, and a reader has no way to tell which half is wrong."""
        a = [("2026-01-01", 100.0), ("2026-06-01", 125.0)]        # +25%
        b = [("2026-01-01", 200.0), ("2026-06-01", 180.0)]        # -10%
        legs = [(60.0, a), (40.0, b)]

        curve, _ = _index(legs, "2026-01-01")
        portfolio = (curve[-1] - 1.0) * 100.0
        itemised = (60.0 * _eur_return(a, "2026-01-01")
                    + 40.0 * _eur_return(b, "2026-01-01")) / 100.0

        assert portfolio == pytest.approx(itemised)
        assert portfolio == pytest.approx(11.0)                   # 0.6*25 + 0.4*(-10)

    def test_the_marks_are_in_eur_because_the_return_is(self):
        """A EUR return beside NATIVE prices shows two numbers whose ratio is not the third: a
        USD holding can rise in dollars and fall in euros on the same days. The local close is
        carried for the tooltip, never as the arithmetic."""
        from routers._airs_portfolio_perf import compute_holding_marks

        src = inspect.getsource(compute_holding_marks)
        # The return is computed off the EUR series...
        assert '"return_pct": (p1 / p0 - 1.0) * 100.0' in src
        assert "_eur_series(" in src                # the EUR series (per analysis_id) is built here
        assert "eur = eur_by_aid.get(" in src       # ...and the mark is taken off it
        # ...and the native close rides along only as a separate, clearly-named field.
        assert '"start_price_local":' in src
        assert "native.get(d1)" in src

    def test_a_holding_not_yet_listed_gets_no_marks_rather_than_a_zero(self):
        """No close on or before the window opened = it was not held there. A 0% return would be
        a claim; an absence is the truth."""
        from routers._airs_portfolio_perf import compute_holding_marks

        src = inspect.getsource(compute_holding_marks)
        assert "if not mark:" in src
        assert "out[isin] = base" in src        # last_close only — no prices, no return

        # And the primitive itself: nothing before the anchor, nothing to mark it with.
        assert _mark_at([("2026-06-01", 10.0)], "2026-01-01") is None

    def test_last_close_is_returned_even_when_no_marks_can_be(self):
        """The ONLY thing separating "the prices are STALE" from "this holding is broken" — and
        they render identically as a blank row. Meta Platforms is correctly mapped to META with
        3,556 bars, but its last close was 2026-07-02 while BUS_2.0_NEU_FX's window opens
        2026-07-09: no price inside the window, so no return over it can exist. Without
        `last_close` the reader is sent hunting for a mapping bug that does not exist."""
        from routers._airs_portfolio_perf import compute_holding_marks

        src = inspect.getsource(compute_holding_marks)
        assert '"last_close": eur[-1][0]' in src
        # It is in `base`, which is what BOTH no-mark branches return.
        assert src.index('"last_close"') < src.index("if not mark:")


class TestSparseSeriesInterpolation:
    """Some holdings have no price ANYWHERE NEAR the date a window opens.

    Measured 2026-07-14: iShares Euro HY Corp Bd (`IE00B66F4759`) is mapped to `ISHHF`, a US OTC
    line with **54 bars in TEN YEARS**. Its last close before 1 Jan 2026 was 2025-11-03 and its
    next was 2026-03-10 — a 127-day hole straddling the anchor. Marking the position at a close
    two months stale, or dropping it (it is 25% of BUS_MTS_DEF_AFS), are both worse than
    straight-lining between the two real closes and SAYING SO.
    """

    def test_a_normal_close_just_before_the_anchor_is_not_interpolated(self):
        """The 99% case, and the one a careless fix breaks: markets are shut on 1 January, so
        31 December IS the mark. Interpolating there would replace every observed opening price
        in the table with a modelled one."""
        s = [("2025-12-31", 100.0), ("2026-01-02", 110.0)]
        d, p, interp, gap = _mark_at(s, "2026-01-01")
        assert (d, p) == ("2025-12-31", 100.0)
        assert interp is False and gap == 0

    def test_a_hole_around_the_anchor_is_interpolated_and_flagged(self):
        s = [("2025-11-02", 100.0), ("2026-03-02", 200.0)]      # 120 days apart
        d, p, interp, gap = _mark_at(s, "2025-12-02")           # 30 days in: a quarter of the way
        assert interp is True
        assert d == "2025-12-02"                                # the ANCHOR, not a trade date
        assert p == pytest.approx(125.0)
        assert gap == 120                                       # the span, so the UI can state it

    def test_a_bracket_wider_than_a_year_is_REFUSED_not_estimated(self):
        """Straight-lining a price across more than a year is not interpolation, it is invention
        — and it would render exactly like a real price. Same refusal as `_trailing_12m`'s
        450-day span cap."""
        s = [("2024-01-02", 100.0), ("2026-03-02", 200.0)]      # ~790 days
        assert _MAX_INTERP_SPAN_DAYS < 790
        assert _mark_at(s, "2026-01-01") is None

    def test_a_series_that_ends_before_the_window_is_not_extrapolated(self):
        """Nothing after the anchor to bracket with. The honest answer is the last real close —
        never a straight line pushed forward into a window it has no evidence for."""
        s = [("2025-11-03", 100.0)]
        d, p, interp, _ = _mark_at(s, "2026-01-01")
        assert (d, p, interp) == ("2025-11-03", 100.0, False)   # real, stale, and not invented

    def test_the_curve_and_the_expanded_rows_interpolate_THE_SAME_WAY(self):
        """The reconciliation invariant, under interpolation. `_index` builds the portfolio's
        curve and `compute_holding_marks` itemises it — if only ONE of them interpolated, the
        expanded rows would no longer weight to the number above them, and the table would be
        explaining its own figure with different arithmetic."""
        assert "_mark_at(s, anchor)" in inspect.getsource(_index)
        assert "_mark_at(eur, anchor)" in inspect.getsource(compute_holding_marks)

        # ...and it holds numerically: a sparse leg + a dense one, weighted, off the same marks.
        # Nov 2 -> Mar 2 is 120 days and Jan 1 sits 60 of them in, so the sparse leg opens at the
        # midpoint, 150.0 — interpolated. The dense leg opens at its real 31-Dec close.
        sparse = [("2025-11-02", 100.0), ("2026-03-02", 200.0)]
        dense = [("2025-12-31", 50.0), ("2026-03-02", 60.0)]
        assert _mark_at(sparse, "2026-01-01")[1] == pytest.approx(150.0)

        curve, _ = _index([(50.0, sparse), (50.0, dense)], "2026-01-01")
        itemised = (50.0 * (200.0 / 150.0 - 1.0) * 100.0
                    + 50.0 * (60.0 / 50.0 - 1.0) * 100.0) / 100.0
        assert (curve[-1] - 1.0) * 100.0 == pytest.approx(itemised)

    def test_the_opening_bar_is_fetched_however_far_back_it_sits(self):
        """The bug this all started from. The load window is a PERFORMANCE bound; treating it as
        a correctness one meant the expanded row (45-day lookback) could not see an opening bar
        that the portfolio figure (loading from the earliest inception) could — so one showed a
        blank while the other priced 23.6% of the same portfolio, with no error anywhere."""
        src = inspect.getsource(compute_holding_marks)
        assert "_prepend_opening_bars(closes, ids, anchor)" in src
        # ...and FX must reach back to whatever bar that turned up, or `_eur_series` drops it.
        assert "fx_from = min([lookback, *(s[0][0] for s in closes.values() if s)])" in src

    def test_an_interpolated_price_has_no_native_close_behind_it(self):
        """There was no trade that day. Handing back a neighbouring day's local price would dress
        the estimate up as an observation."""
        src = inspect.getsource(compute_holding_marks)
        assert '"start_price_local": None if interpolated else native.get(d0)' in src


class TestCagrIsNotExtrapolated:
    """A CAGR compounds a window's return out to a year. A SHORT window is therefore not merely
    noisy — it is systematically amplified, and the result sits in the same column, same font, as
    a rate earned over two years.

    Measured 2026-07-14:

        AITopSelectie OFF FX   +50.61% over 135 trading days   ->  annualized: +114.8%
        BUS_Risicodragend      +48.26% over 323 trading days   ->  annualized:  +35.97%

    The first is 0.54 years of evidence. Fund reporting does not annualize a sub-year period for
    exactly this reason — it shows the cumulative return, which `since_model_pct` already is.
    """

    def test_the_floor_is_a_full_year(self):
        assert MIN_CAGR_DAYS == TRADING_DAYS == 252

    def test_no_cagr_below_it(self):
        src = inspect.getsource(compute_portfolio_performance)
        assert "len(rets) >= MIN_CAGR_DAYS" in src

    def test_the_arithmetic_it_prevents(self):
        """135 days of +50.61% compounds to a figure nobody earned."""
        short = (1 + 50.61 / 100) ** (252 / 135) - 1
        long_ = (1 + 48.26 / 100) ** (252 / 323) - 1
        assert short * 100 == pytest.approx(114.8, abs=0.5)     # the number we refuse to print...
        assert long_ * 100 == pytest.approx(35.97, abs=0.5)     # ...beside the one we do
        assert short > 3 * long_                                # on a THIRD of the evidence

    def test_a_sharpe_survives_a_short_window_but_a_cagr_does_not(self):
        """Why the two floors differ (20 days vs 252). Sharpe is a RATIO — annualization scales
        both halves, so a short sample is noisy but not biased. A CAGR compounds only the
        numerator, so a short sample is inflated. Same window, different failure."""
        assert MIN_STAT_DAYS < MIN_CAGR_DAYS

    def test_years_running_is_reported_so_the_absence_is_legible(self):
        src = inspect.getsource(compute_portfolio_performance)
        assert '"years_running": (_days_between(today, eff) / 365.25) if eff else None' in src


class TestRatiosNeedASample:
    """27 of 56 models were (re)defined this year; MoTopSelectie_FX was defined 8 days before
    it was measured. Its Sharpe would render in the same column, same font, as one measured
    over two years of trading — and be noise."""

    def test_a_week_old_model_gets_no_ratio_at_all(self):
        assert MIN_STAT_DAYS >= 20
        src = inspect.getsource(compute_portfolio_performance)
        assert "if len(rets) >= MIN_STAT_DAYS else None" in src
        assert '"sharpe": stats.sharpe if stats else None' in src

    def test_the_sample_size_is_returned_so_the_two_can_be_told_apart(self):
        src = inspect.getsource(compute_portfolio_performance)
        assert '"stat_days": len(rets)' in src

    def test_the_ratios_are_annualized_daily_not_monthly(self):
        """The shared helper defaults to 12 — the diversifier's cadence. A daily series
        annualized at sqrt(12) understates vol by ~4.6x and overstates Sharpe by the same."""
        assert TRADING_DAYS == 252
        src = inspect.getsource(compute_portfolio_performance)
        assert "periods_per_year=TRADING_DAYS" in src

    def test_a_flat_curve_has_no_sharpe_rather_than_an_infinite_one(self):
        flat = [1.0] * 30
        rets = _daily_returns(flat)
        assert len(rets) == 29
        st = annualized_stats(rets, periods_per_year=TRADING_DAYS)
        assert st.sharpe is None and st.sortino is None      # 0/0 is undefined, not infinite

    def test_a_never_down_curve_has_no_sortino(self):
        """Sortino's denominator is downside deviation. A series that never fell has none —
        that is 'undefined', and rendering it as a very large number would be a lie about a
        portfolio's risk."""
        rising = [1.0 + 0.001 * i for i in range(40)]
        st = annualized_stats(_daily_returns(rising), periods_per_year=TRADING_DAYS)
        assert st.sortino is None
        assert st.sharpe is not None                          # vol is real; downside isn't


class TestLookThrough:
    """A certificate wrapping another model (CH1381833321 "Star Selection Index" IS
    StarTopSelectie OFF FX) has no Yahoo price of its own, so it used to sit in the table as a
    dead row — no Start, no End, no Return — and its weight fell out of the coverage denominator.
    Look-through prices it from the model it wraps instead.
    """

    # `ex` maps ISIN -> execution row; `eur` maps analysis_id -> its EUR close series. This is the
    # shape both `compute_portfolio_performance` and `compute_holding_marks` hand to the builder.
    EX = {"A": {"analysis_id": 1}, "B": {"analysis_id": 2}}
    EUR = {
        1: [("2026-01-01", 100.0), ("2026-02-01", 110.0)],   # +10%
        2: [("2026-01-01", 50.0), ("2026-02-01", 45.0)],     # -10%
    }

    def test_the_basket_is_a_weighted_index_of_the_model_it_wraps(self):
        """60/40 of +10% and -10% is +2%, and the level is indexed to 100 at the base."""
        rows = [{"isin": "A", "percentage": 60}, {"isin": "B", "percentage": 40}]
        s = _lookthrough_series(rows, self.EX, self.EUR)
        assert s[0] == ("2026-01-01", pytest.approx(100.0))
        assert s[-1] == ("2026-02-01", pytest.approx(102.0))   # 0.6*110/100 + 0.4*45/50 = 1.02

    def test_the_certificate_reconciles_into_the_parent_exactly_like_a_stock(self):
        """The whole point of an anchor-INDEPENDENT level: fed to `_index` as the certificate's
        leg, `level(t)/level(anchor)` is the wrapped model's return, so weighting the row's return
        reproduces the parent's figure — the same invariant a real holding obeys."""
        rows = [{"isin": "A", "percentage": 60}, {"isin": "B", "percentage": 40}]
        s = _lookthrough_series(rows, self.EX, self.EUR)

        # A parent holding this certificate at 100% earns exactly the basket's +2%.
        curve, _ = _index([(100.0, s)], "2026-01-01")
        assert (curve[-1] - 1.0) * 100.0 == pytest.approx(2.0)

        # And the per-holding mark the expanded row shows is that same +2%.
        d0, p0, _i, _g = _mark_at(s, "2026-01-01")
        d1, p1 = s[-1]
        assert (p1 / p0 - 1.0) * 100.0 == pytest.approx(2.0)

    def test_cash_inside_the_wrapped_model_is_a_flat_leg(self):
        """A 50% cash sleeve holds its value — so 50% of +10% and 50% flat is +5%."""
        rows = [{"isin": "A", "percentage": 50}, {"isin": None, "percentage": 50}]
        s = _lookthrough_series(rows, self.EX, self.EUR)
        assert s[-1][1] == pytest.approx(105.0)

    def test_too_little_of_the_wrapped_model_priceable_looks_through_to_nothing(self):
        """The same coverage floor a portfolio's own figure obeys, applied to the model behind a
        certificate: renormalising a return over a sliver of the basket is a fabrication, so below
        the floor it returns nothing and the row stays dead rather than confidently wrong."""
        assert LOOKTHROUGH_MIN_COVERAGE == MIN_COVERAGE_PCT / 100.0
        # 40% priceable (A), 60% an ISIN we cannot price (not in `ex`) — under the 60% floor.
        rows = [{"isin": "A", "percentage": 40}, {"isin": "Z", "percentage": 60}]
        assert _lookthrough_series(rows, self.EX, self.EUR) == []

    def test_an_all_cash_basket_has_no_price_path(self):
        """Cash is priceable (flat), but a basket that is ONLY cash has no series to track — it is
        not a return we can chart, and inventing a flat line would imply we looked through to
        something."""
        rows = [{"isin": None, "percentage": 100}]
        assert _lookthrough_series(rows, self.EX, self.EUR) == []

    def test_it_is_one_level_deep_no_recursion_into_nested_certificates(self):
        """A certificate held INSIDE the wrapped model has no execution row of its own, so it is
        just an unpriceable leg here — renormalised out, never recursed into. That is also what
        makes a link cycle impossible to loop on."""
        # C is a nested certificate: no entry in `ex`, so it drops out and A carries the basket.
        rows = [{"isin": "A", "percentage": 100}, {"isin": "C", "percentage": 0}]
        s = _lookthrough_series(rows, self.EX, self.EUR)
        assert s[-1][1] == pytest.approx(110.0)     # 100% A, +10%

    def test_the_portfolio_figure_and_the_row_marks_both_look_through(self):
        """Source invariant: a certificate is priced the SAME way in the headline figure and in
        the itemised marks — one builder, one anchor — or the two would disagree about the row."""
        assert "_lookthrough_series(" in inspect.getsource(compute_portfolio_performance)
        assert "_lookthrough_series(" in inspect.getsource(compute_holding_marks)
        # ...and a direct listing, if it ever existed, wins over the look-through.
        assert 'if out.get(isin, {}).get("return_pct") is not None:' in \
            inspect.getsource(compute_holding_marks)
