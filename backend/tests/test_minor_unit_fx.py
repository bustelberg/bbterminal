"""`GBp` is PENCE, not a currency — and `fx_rate` has never had a row for it.

Yahoo quotes every London listing in pence, so `asset_execution.currency` says "GBp" for **343**
of our rows. That string then travels into FX lookups that ask `fx_rate` for a currency called
"GBp", get nothing back, and hand the caller a missing rate — which every caller reads as "we
cannot price this holding".

    GB0032398678  Judges Scientific plc   JDG.L   GBp   5,930 bars back to 2003-02-17

...and it read as UNPRICED in all 13 AIRS portfolios holding a pence-quoted name, with every one
of those bars sitting in the table. `DealmakersTopSel OFF FX` showed a blank row for it while
counting it 5% of the model.

⚠ THE BUG IS SILENT IN BOTH DIRECTIONS, WHICH IS WHY THE MAP IS SHARED AND NOT RE-DERIVED:

    forget to normalise the CODE     -> no rate  -> the holding disappears
    forget to apply the DIVISOR      -> a rate   -> £46.75 prices as £4,675

The second is worse, because it still looks like a number. `_rate` therefore scales the RATE by
the divisor rather than leaving each caller to divide the price: `eur = pence / (100 * gbp_rate)`
is the same arithmetic, in one place, and a caller cannot forget half of it.
"""
from __future__ import annotations

import pytest

from asset_pipeline.fx import SUBUNIT
from routers._benchmark_index import _rate


class TestPenceIsNotACurrency:
    FX = {"GBP": {"2026-01-02": 0.8730}}      # GBP per EUR, as `fx_rate` stores it

    def test_the_map_knows_pence(self):
        assert SUBUNIT["GBp"] == ("GBP", 100.0)
        assert SUBUNIT["GBX"] == ("GBP", 100.0)

    def test_a_pence_quote_resolves_to_the_pound_rate(self):
        """The code is normalised: `fx_rate` is asked for GBP, which exists."""
        assert _rate(self.FX, "GBp", "2026-01-02") is not None

    def test_and_it_carries_the_divisor(self):
        """The rate comes back 100x the pound rate, so `eur = pence / rate` lands on the right
        side of a hundredfold error."""
        r = _rate(self.FX, "GBp", "2026-01-02")
        assert r == pytest.approx(87.30)                     # 0.8730 * 100

        # Judges Scientific's real close, 2026-07-03: 4,675 pence = £46.75.
        eur = 4675.0 / r
        assert eur == pytest.approx(53.55, abs=0.1)          # ~EUR 54, NOT ~EUR 5,355
        assert eur < 100                                     # the hundredfold error, pinned

    def test_a_major_currency_is_untouched(self):
        assert _rate({"USD": {"2026-01-02": 1.05}}, "USD", "2026-01-02") == pytest.approx(1.05)
        assert _rate({}, "EUR", "2026-01-02") == 1.0

    def test_an_unknown_currency_still_returns_None(self):
        """The fix must not turn 'we have no rate' into a silent 1.0 — that would price a
        foreign holding as though it were already in euros."""
        assert _rate({"USD": {"2026-01-02": 1.05}}, "ZWL", "2026-01-02") is None


class TestTheLoadersAskForTheBaseCurrency:
    """`_rate` cannot resolve GBp -> GBP if the FX dict never had GBP in it. Both loaders build
    that dict from `asset_execution.currency`, so both must normalise before querying — a fix in
    `_rate` alone leaves the table empty and the holding just as unpriced."""

    def test_the_portfolio_loader_normalises(self):
        import inspect

        from routers._airs_portfolio_perf import _fx

        src = inspect.getsource(_fx)
        assert "SUBUNIT.get(c, (c, 1.0))[0]" in src

    def test_the_benchmark_loader_normalises(self):
        import inspect

        from routers._benchmark_index import _fx_to_eur

        src = inspect.getsource(_fx_to_eur)
        assert "SUBUNIT.get(c, (c, 1.0))[0]" in src
