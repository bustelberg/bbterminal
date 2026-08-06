"""The benchmark fundamentals grid — the parts that decide whether a number may be shown at all.

No DB: every function here is pure. What is pinned is the set of refusals, because each one is a
place where the wrong answer is a well-formed number rather than an error.
"""
from __future__ import annotations

from routers._benchmark_fundamental_grid import (
    _AGG_BY_UNIT, MIN_COVERAGE_PCT, _period_label, _period_summary, _unit,
)


def _row(period: str, **vals: float) -> dict:
    return {"v": {period: dict(vals)}, "n": {}, "fx": {}}


class TestTheUnitDecidesWhetherFxIsAppliedAtAll:
    """⚠ Two of the nineteen lines are not currency, and dividing them by an FX rate produces a
    plausible wrong number rather than an error — NVIDIA's ~24,514M diluted shares came back as
    20,902M before this map existed, and a share count is exactly what a reader would use to
    sanity-check a market cap."""

    def test_a_share_count_and_a_percent_are_not_currency(self):
        assert _unit("shares") == "shares"
        assert _unit("roic") == "percent"

    def test_per_share_amounts_are_currency_at_a_different_scale(self):
        assert _unit("div_ps") == "per_share"
        assert _unit("price_ps") == "per_share"

    def test_everything_else_defaults_to_millions(self):
        for k in ("revenue", "net_income", "market_cap", "total_equity", "capex"):
            assert _unit(k) == "millions", k


class TestWhatTheIndexRowMayDoWithAColumn:
    """⚠ DERIVED FROM THE UNIT, NOT FROM THE TTM ROLL-UP RULE. The TTM rule aggregates ONE company
    OVER TIME; this aggregates MANY companies AT ONE TIME. They agree on most lines and part
    company exactly where it matters: `shares` is `mean` over time (a share count IS an average of
    four quarters) and was therefore handed a cap-weighted mean ACROSS companies — a number whose
    referent, "the S&P 500's share count", does not exist."""

    def test_a_currency_amount_sums(self):
        assert _AGG_BY_UNIT["millions"] == "sum"

    def test_a_rate_is_cap_weighted_never_summed(self):
        # Summed across 500 constituents ROIC reads ~5,000%, which the cell prints with a % sign.
        assert _AGG_BY_UNIT["percent"] == "weighted_mean"

    def test_a_count_and_a_per_share_amount_have_no_index_total_at_all(self):
        assert _AGG_BY_UNIT["shares"] == "none"
        assert _AGG_BY_UNIT["per_share"] == "none"


class TestThePeriodLabelComesFromTheRealMonth:
    """⚠ A fiscal quarter need not end on 03-31/06-30/09-30/12-31, and synthesising the calendar
    quarter would move every point of an off-calendar filer into a quarter it does not belong to."""

    def test_a_calendar_quarter_end(self):
        assert _period_label("2025-09-30") == "2025-Q3"
        assert _period_label("2024-12-31") == "2024-Q4"

    def test_an_off_calendar_filer_keeps_its_own_quarter(self):
        # NVIDIA's fiscal year ends in late January — that is its Q1, not the prior Q4.
        assert _period_label("2026-01-25") == "2026-Q1"
        assert _period_label("2025-02-01") == "2025-Q1"


class TestCoverageGatesTheWeights:
    """⚠ THE DENOMINATOR IS THE INDEX, NOT THE COVERED SET, so `covered_pct` falls as you scrub
    back — which is the finding. Dividing by the rows that happen to have data would pin it at
    100% in every period and describe nothing."""

    def test_covered_pct_is_over_the_whole_index(self):
        s = _period_summary([_row("2021", market_cap=100.0)], "2021", members=4)
        assert s["covered_pct"] == 25.0
        assert s["cap_covered_pct"] == 25.0

    def test_a_row_with_figures_but_no_cap_counts_as_covered_and_not_as_capped(self):
        # It can be read; it cannot be weighted. Two different facts, two different counters.
        rows = [_row("2021", revenue=5.0), _row("2021", market_cap=100.0)]
        s = _period_summary(rows, "2021", members=2)
        assert (s["covered"], s["with_market_cap"]) == (2, 1)
        assert s["total_market_cap_eur"] == 100.0

    def test_weights_are_withheld_under_the_floor(self):
        rows = [_row("2021", market_cap=100.0)]
        assert _period_summary(rows, "2021", members=10)["weights_usable"] is False

    def test_and_allowed_above_it(self):
        rows = [_row("2021", market_cap=100.0) for _ in range(9)]
        s = _period_summary(rows, "2021", members=10)
        assert s["cap_covered_pct"] >= MIN_COVERAGE_PCT
        assert s["weights_usable"] is True

    def test_a_period_with_no_caps_at_all_has_no_total_and_no_weights(self):
        s = _period_summary([_row("2021", revenue=5.0)], "2021", members=1)
        assert s["total_market_cap_eur"] is None
        assert s["weights_usable"] is False


class TestACappedIndexGetsNoWeightsAtAnyCoverage:
    """⚠⚠ THE AEX. Euronext caps a constituent at 15% at each review, precisely because ASML would
    otherwise swallow a 25-name index: uncapped it is 37.53% against the real index's 15.00%. So
    `cap / Σcap` is not that index's weighting AT ANY COVERAGE LEVEL — full data makes an uncapped
    weight more precisely wrong, not less.

    This grid refuses rather than capping, and the reason is in `INDEX_CAP_PCT`'s own comment: the
    weight formula had already leaked into four inline copies, `index_weights` was made the one
    place a weight is formed, and a cap applied to three of four would be worse than no cap at all.
    A second capping implementation here would be that fourth copy."""

    def test_full_coverage_does_not_rescue_it(self):
        rows = [_row("2025", market_cap=100.0) for _ in range(10)]
        uncapped = _period_summary(rows, "2025", members=10, capped=False)
        capped = _period_summary(rows, "2025", members=10, capped=True)
        assert uncapped["cap_covered_pct"] == capped["cap_covered_pct"] == 100.0
        assert uncapped["weights_usable"] is True
        assert capped["weights_usable"] is False

    def test_the_coverage_figures_themselves_are_untouched(self):
        # The per-company rows still render, and the counts still describe them honestly — it is
        # only the WEIGHT that is refused. Blanking the coverage too would say we hold less than
        # we do.
        rows = [_row("2025", market_cap=100.0, revenue=7.0)]
        s = _period_summary(rows, "2025", members=1, capped=True)
        assert (s["covered"], s["with_market_cap"]) == (1, 1)
        assert s["total_market_cap_eur"] == 100.0

    def test_the_cap_is_read_from_the_one_declaration_not_restated_here(self):
        # A second list of which indices cap is a second thing to forget to update, and the failure
        # is silent because an uncapped weight is a perfectly well-formed percentage.
        from routers._benchmark_index import INDEX_CAP_PCT

        assert INDEX_CAP_PCT.get("AEX") == 15.0
        assert INDEX_CAP_PCT.get("SP500") is None
