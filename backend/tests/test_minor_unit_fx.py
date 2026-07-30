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

        assert "SUBUNIT" in inspect.getsource(_fx)

    def test_the_benchmark_loader_normalises(self):
        import inspect

        from routers._benchmark_index import _fx_to_eur

        assert "SUBUNIT" in inspect.getsource(_fx_to_eur)


class TestAMarketCapIsNotAPrice:
    """⚠ ONE `currency` FIELD, TWO DIFFERENT UNITS — and the divisor belongs to only one of them.

    Yahoo quotes a London listing's PRICE in pence and, in the SAME v7 payload, reports its
    `marketCap` in POUNDS. Both are labelled `"GBp"`. So the rule the price path lives by — always
    apply the divisor — is exactly wrong for a market cap, which never had it:

        SHEL.L   native 166.43bn GBP  ->  stored EUR   1.95bn   (a EUR 195bn company)
        HSBA.L          251.26bn GBP  ->  stored EUR   2.94bn
        AZN.L           223.92bn GBP  ->  stored EUR   2.62bn

    100x too small and still a plausible number, which is why it sat there. Measured across ACWI:
    the 36 minor-unit members carried **0.02%** of index weight where they should carry **~1.93%**
    — ingested (UK 0 -> 44), then weighted to nothing. `covered_pct` could not see it: it counts
    members PRICED, and every one of those counted as covered.

    This is the OPPOSITE half of the module docstring's symmetry. The price bug is "forget the
    divisor and £46.75 prices at £4,675". This is "apply the divisor where it does not belong" —
    and being 100x SMALL is quieter than being 100x large, because a small weight just looks like
    a small company.
    """

    def test_a_cap_currency_is_always_the_major_unit(self):
        from scripts.asset_backfill_marketcap import _cap_currency

        assert _cap_currency("GBp") == "GBP"
        assert _cap_currency("GBX") == "GBP"
        assert _cap_currency("ZAc") == "ZAR"
        assert _cap_currency("ILA") == "ILS"

    def test_a_major_currency_passes_through_untouched(self):
        from scripts.asset_backfill_marketcap import _cap_currency

        assert _cap_currency("USD") == "USD"
        assert _cap_currency("EUR") == "EUR"
        assert _cap_currency(None) is None

    def test_the_cap_writer_normalises_before_converting(self):
        """The whole bug in one line: `fx_to_eur(q["currency"])` on a cap. If the raw quote
        currency ever reaches the conversion again, Shell is a EUR 1.95bn company again."""
        import inspect

        from scripts import asset_backfill_marketcap as m

        # The cap writer routes the quote currency through `_cap_currency` before converting.
        assert "_cap_currency(" in inspect.getsource(m.main)

    def test_shell_reconstructs_to_the_right_order_of_magnitude(self):
        """The arithmetic, pinned end to end against the measured figures. Yahoo hands back
        166.43bn for SHEL.L; at ~0.8555 GBP/EUR that is a ~EUR 195bn company, not EUR 1.95bn."""
        from scripts.asset_backfill_marketcap import _cap_currency

        native, quote_ccy = 166.43e9, "GBp"
        gbp_per_eur = 0.8555
        rate_major = 1.0 / gbp_per_eur                      # EUR per 1 GBP

        broken = native * (rate_major / 100.0)              # what shipped: the pence divisor
        fixed = native * rate_major if _cap_currency(quote_ccy) == "GBP" else None

        assert 150e9 < fixed < 250e9                        # a EUR ~195bn company
        assert broken < 5e9                                 # ...that read as EUR ~1.9bn
        assert fixed == pytest.approx(broken * 100.0)

    def test_fx_to_eur_knows_every_minor_unit_not_just_pence(self):
        """`fx_to_eur` special-cased "GBp" inline, so ZAc/ILA asked Yahoo for a nonexistent
        "ZAcEUR=X" and got None. A cap of NULL drops the company from a cap-weighted index
        ENTIRELY — a louder failure than being under-weighted, and it hit Johannesburg's
        `med_adv_eur` too, where a zero loses the liquidity ranking in `resolve()`."""
        import inspect

        from asset_pipeline import yahoo

        src = inspect.getsource(yahoo.fx_to_eur)
        assert "SUBUNIT" in src
        assert '"GBp"' not in src.split('"""')[-1]      # no inline special-case left in the body


class TestUpperCasingIsNotNormalisation:
    """⚠ THE THIRD WAY TO LOSE THE DIVISOR, and the quietest: `.upper()`.

    Forgetting to normalise gives no rate and the holding disappears (loud). Forgetting the divisor
    gives £4,675 for a £46.75 share (wrong, but at least the code was `GBp` all along). Upper-casing
    is worse than both, because it *manufactures a valid code*: `GBp` -> `GBP`, which `fx_rate` HAS.
    `SUBUNIT` no longer matches, so `_rate` returns the pound rate with divisor 1.0, and every
    conversion lands 100x high with no missing lookup anywhere to notice.

    Measured on the /portfolios price chart, 2026-07-29: CHRT.L's close of 1,424p rendered as
    EUR 1,661.22 on the EUR panel — beside a NATIVE panel that was perfectly correct, which is how
    it survived. It is EUR 16.61.
    """

    def test_a_minor_unit_survives_normalisation(self):
        from routers._asset_financials import _norm_ccy

        assert _norm_ccy("GBp") == "GBp"          # NOT "GBP"
        assert _norm_ccy("GBX") == "GBX"
        assert _norm_ccy("ZAc") == "ZAc"
        assert _norm_ccy("ILA") == "ILA"

    def test_everything_else_is_tidied(self):
        from routers._asset_financials import _norm_ccy

        assert _norm_ccy("usd") == "USD"
        assert _norm_ccy(" eur ") == "EUR"
        assert _norm_ccy("") is None
        assert _norm_ccy(None) is None

    def test_the_pence_close_converts_to_the_right_order_of_magnitude(self):
        """End to end through the shared `_rate`: the CHRT.L figure, pinned."""
        from routers._asset_financials import _norm_ccy

        fx = {"GBP": {"2026-07-03": 0.8572}}
        eur = 1424.0 / _rate(fx, _norm_ccy("GBp"), "2026-07-03")
        assert eur == pytest.approx(16.61, abs=0.05)
        assert eur < 100                          # not EUR 1,661

    def test_the_yahoo_price_readers_route_through_it(self):
        """All three readers of `asset_execution.currency` in this module. `.upper()` here is the
        bug; a new one added without `_norm_ccy` re-opens it for London, Johannesburg and Tel Aviv."""
        import inspect

        from routers import _asset_financials as m

        for fn in (m._price_series_for_isin, m._performance_for_isin, m._latest_close_for_isin):
            # Comments talk ABOUT `.upper()` on purpose — it is the trap being warned against.
            code = [ln for ln in inspect.getsource(fn).splitlines()
                    if not ln.lstrip().startswith("#")]
            src = "\n".join(code)
            assert "_norm_ccy(" in src, fn.__name__
            assert ".upper()" not in src, fn.__name__
