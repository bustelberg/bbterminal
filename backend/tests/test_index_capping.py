"""A CAP IS A PROPERTY OF THE INDEX, NOT OF THE ARITHMETIC.

The S&P 500 and ACWI are uncapped — no constituent sits anywhere near a binding cap, so raw cap
weights ARE their weights. The AEX is a different animal: 25 names, and Euronext caps a
constituent at 15% at each review precisely BECAUSE ASML would otherwise swallow it.

Measured on our own data (2026-07-16, after the minor-unit market-cap repair):

    ASML   uncapped 37.53%   ->  capped 15.00%    (the real AEX says 15.00%)
    Shell  uncapped 12.52%   ->  capped 15.00%

Shipping the uncapped number would not be an approximation of the AEX. It would be an ASML
tracker wearing the AEX's name — and it would look entirely plausible doing it.
"""
from __future__ import annotations

import pytest

from routers._benchmark_index import INDEX_CAP_PCT, index_weights


def _rows(*caps: float) -> list[dict]:
    """Members carrying only what `index_weights` reads: their start-of-window cap."""
    return [{"start_cap_eur": c} for c in caps]


class TestAnUncappedIndexIsUntouched:
    """The four call sites each computed `start_cap_eur / total` inline before this existed.
    SP500 and ACWI must come out BIT-IDENTICAL, or this refactor moved a number that nobody
    asked it to move."""

    def test_sp500_is_raw_cap_weight(self):
        rows = _rows(50e9, 30e9, 20e9)
        assert index_weights(rows, "SP500") == pytest.approx([50.0, 30.0, 20.0])

    def test_acwi_is_raw_cap_weight(self):
        rows = _rows(90e9, 10e9)
        assert index_weights(rows, "ACWI") == pytest.approx([90.0, 10.0])

    def test_an_unknown_label_is_uncapped(self):
        """Absence of a rule means no cap — a label we have never heard of must not silently
        acquire one."""
        assert index_weights(_rows(80e9, 20e9), "NOT_AN_INDEX") == pytest.approx([80.0, 20.0])

    def test_the_inline_formula_is_reproduced_exactly(self):
        rows = _rows(1.234e9, 5.678e9, 9.1011e9, 2e9)
        total = sum(r["start_cap_eur"] for r in rows)
        expected = [r["start_cap_eur"] / total * 100.0 for r in rows]
        assert index_weights(rows, "SP500") == pytest.approx(expected, abs=1e-12)


class TestTheAEXCapBinds:
    def test_the_rule_exists_and_is_15pct(self):
        assert INDEX_CAP_PCT["AEX"] == 15.0

    def test_a_dominant_constituent_is_capped(self):
        """ASML's real shape: one name at ~37% of a 25-name index."""
        rows = _rows(37.53e9, *([2.6e9] * 24))
        w = index_weights(rows, "AEX")
        assert w[0] == pytest.approx(15.0)

    def test_the_weights_still_sum_to_100(self):
        """Capping REDISTRIBUTES; it never destroys weight. An index summing to 85% understates
        every return it produces."""
        w = index_weights(_rows(40e9, 30e9, 15e9, 10e9, 5e9, 1e9, 1e9, 1e9), "AEX")
        assert sum(w) == pytest.approx(100.0)

    def test_the_excess_goes_to_the_others_pro_rata(self):
        """Members under the cap keep their RATIO to each other — redistribution is pro rata, not
        equal, or a cap would quietly RE-RANK the index below the capped names.

        (25 members, so the cap is feasible: an AEX-shaped book where only the top name binds.)
        """
        rows = _rows(20e9, *([6e9] * 12), *([2e9] * 12))
        w = index_weights(rows, "AEX")
        assert w[0] == pytest.approx(15.0)            # the only one over
        assert w[1] / w[13] == pytest.approx(3.0)     # 6:2 preserved through the spill
        assert max(w[1:]) < 15.0                      # nobody else was lifted over
        assert sum(w) == pytest.approx(100.0)

    def test_nobody_ends_above_the_cap(self):
        """THE ONE-PASS BUG. Redistributing ASML's excess LIFTS the others — and can push one of
        them over the cap in the process. A single pass leaves it there, above a cap the index
        says is impossible. Here Shell is under 15% before the spill and over it after."""
        rows = _rows(37.5e9, 12.5e9, *([2e9] * 25))
        w = index_weights(rows, "AEX")
        assert max(w) <= 15.0 + 1e-9
        assert sum(w) == pytest.approx(100.0)

    def test_two_names_can_both_cap(self):
        """The measured AEX case: ASML AND Shell both land on 15.00%."""
        rows = _rows(37.53e9, 12.52e9, *([2.1e9] * 23))
        w = index_weights(rows, "AEX")
        assert w[0] == pytest.approx(15.0)
        assert w[1] == pytest.approx(15.0)
        assert sum(w) == pytest.approx(100.0)

    def test_an_already_compliant_index_is_untouched(self):
        """No member over the cap -> the cap is a no-op, not a reshuffle."""
        rows = _rows(*([4e9] * 25))
        assert index_weights(rows, "AEX") == pytest.approx([4.0] * 25)


class TestAnImpossibleCapIsRefusedNotFudged:
    """With n members a cap of c% holds at most n*c% of weight. Under 100% there IS no valid
    redistribution — the weights cannot sum to the index. Returning 75% anyway would understate
    every return by a quarter, silently. For a real AEX (25 x 15% = 375%) this cannot fire; it
    fires when the universe has collapsed to a handful of priced names, which is a fact worth
    raising rather than smoothing over."""

    def test_too_few_members_to_reach_100pct_raises(self):
        with pytest.raises(ValueError, match="cannot sum to 100"):
            index_weights(_rows(40e9, 30e9, 30e9), "AEX")     # 3 x 15% = 45%

    def test_the_message_names_the_real_cause(self):
        """The universe collapsed, the cap did not tighten — a reader debugging this must not go
        looking for a bad cap."""
        with pytest.raises(ValueError, match="universe is too thin to cap"):
            index_weights(_rows(*([1e9] * 6)), "AEX")         # 6 x 15% = 90% < 100

    def test_the_feasibility_boundary_is_exact(self):
        """100/15 = 6.67, so 7 members is the first feasible book. Six is not — and `n*cap == 100`
        exactly would pin EVERY member at the cap with nothing left to absorb a rounding error,
        which is a degenerate weighting, not a weighting."""
        with pytest.raises(ValueError):
            index_weights(_rows(*([1e9] * 6)), "AEX")         # 6 x 15% =  90% -> refused
        w = index_weights(_rows(*([1e9] * 7)), "AEX")         # 7 x 15% = 105% -> fine
        assert sum(w) == pytest.approx(100.0)


class TestDegenerateInput:
    def test_no_rows(self):
        assert index_weights([], "AEX") == []

    def test_zero_total_cap_does_not_divide_by_zero(self):
        assert index_weights(_rows(0.0, 0.0), "SP500") == [0.0, 0.0]


class TestOneWeightingForTheWholeIndex:
    """`index_rows` (the constituents Brinson decomposes) and `index_returns` (the headline) must
    weight identically — an attribution that reconciles against a DIFFERENT weighting reconciles
    against nothing. Both now call `index_weights`; nothing may re-derive it inline."""

    def test_no_call_site_forms_a_weight_by_hand(self):
        import inspect

        from routers import _asset_benchmark, _benchmark_index

        for mod in (_asset_benchmark, _benchmark_index):
            src = inspect.getsource(mod)
            body = src.split("def index_weights", 1)
            # Outside `index_weights` itself, the inline formula must not reappear.
            rest = body[0] + (body[1].split("\ndef ", 1)[1] if len(body) > 1 else "")
            assert 'r["start_cap_eur"] / total * r[' not in rest
            assert 'r["start_cap_eur"] / total_start' not in rest


class TestTheBenchmarksPanelIsPricedInThePortfolioWorld:
    """⚠ THE PANEL'S OWN SUBTITLE IS THE SPEC: "same basis as a portfolio, so the numbers are
    comparable". The portfolios on that page are priced from `asset_price` (yfinance). Until
    2026-07-16 the panel was priced from GuruFocus — two vendors, two adjustment conventions, two
    FX sources, and the difference between them reads as alpha.

    It was also structurally unable to price two of the three indices. GuruFocus is blind to
    31.96% of the AEX (Shell, Unilever and RELX are LSE rows with no GuruFocus market cap) and to
    ~7.8% of ACWI, and a cap-weighted rebuild REDISTRIBUTES that weight rather than losing it:

        GuruFocus AEX   22 members   +14.80%   <- Prosus capped at 15%, and it is really 10.46%
        asset AEX       25 members   +12.12%

    Nothing about the first looks wrong — the weights sum to 100%, ASML sits dutifully on the cap,
    and the cap firing on the WRONG name makes it look more correct rather than less.

    The measured cost, kept honest: against SPY's +9.02% USD the GuruFocus rebuild was +9.05% and
    the asset one is +9.23%, so on the ONE index GuruFocus fully covers it was ~0.2pp closer. That
    trade was made deliberately.
    """

    def test_the_route_uses_the_asset_path(self):
        import inspect

        from routers import benchmarks

        src = inspect.getsource(benchmarks.benchmark_reconstructed_index)
        assert "routers._asset_benchmark import compute_index_async" in src
        assert "_benchmark_index import compute_index_async" not in src

    def test_the_gurufocus_path_survives_as_the_spy_crosscheck(self):
        """`_benchmark_index.compute_index` is not dead code — it validates the METHOD against a
        real ETF. Deleting it would remove the only independent check that the weighting is right
        at all."""
        from routers._benchmark_index import compute_index

        assert callable(compute_index)

    def test_both_paths_share_one_weighting(self):
        """The two `compute_index`es must not become two weightings. Both call `index_weights`,
        so a cap (or a look-ahead fix) cannot land on one and miss the other."""
        import inspect

        from routers import _asset_benchmark, _benchmark_index

        for mod in (_asset_benchmark, _benchmark_index):
            assert "index_weights(rows, label)" in inspect.getsource(mod.compute_index)
