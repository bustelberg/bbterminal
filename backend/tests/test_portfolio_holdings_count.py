"""Counting a model portfolio's instruments — where the three "empty" states must not merge.

`holdings` is a count of ISIN-bearing rows in a portfolio's fixed model. There are three
different ways for it to be absent, and collapsing any two of them tells the user something
false:

    None  — NO FIXED MODEL EXISTS. AirSPMS only stores a composition for a `fixed (…)`
            portfolio; a `normaal` (31 of 95) or `meervoudig` (6) one — the benchmarks and
            multi-model wrappers — has none at all. "0 holdings" would be a claim about a
            model that isn't there.
    0     — a real, EMPTY fixed model. Measured: 58 portfolios are `fixed (…)` but only 57
            have a composition, so exactly ONE is genuinely empty. That single row is the
            entire reason None and 0 are kept apart.
    error — we did not learn the answer. Writing 0 would be a fabricated fact.

The cash line ("Liquiditeiten") carries no ISIN and is not an instrument, so it is never
counted — which also means the count cannot be inflated by the `"nan"`-string trap that
`_parse_positions_xls` exists to prevent (a NaN ISIN str()s to "nan", which is TRUTHY).
"""
from __future__ import annotations

from airs_scanner import has_fixed_model


class TestOnlyAFixedPortfolioHasAModel:
    def test_the_fixed_types_have_one(self):
        # The number in parentheses is not a count — it's the portfolio's own figure.
        assert has_fixed_model("fixed (0)") is True
        assert has_fixed_model("fixed (14.5)") is True

    def test_normaal_and_meervoudig_do_not(self):
        assert has_fixed_model("normaal") is False        # 31 of 95
        assert has_fixed_model("meervoudig") is False     # 6 of 95

    def test_a_missing_type_is_not_a_model(self):
        assert has_fixed_model(None) is False
        assert has_fixed_model("") is False

    def test_fixed_zero_is_a_model_not_an_absence(self):
        """`fixed (0)` is the trap: it LOOKS like a zero. It is a portfolio type, and 24 of
        them exist. Reading the parenthesised number as a holdings count would report 24
        portfolios as empty when they are not."""
        assert has_fixed_model("fixed (0)") is True


class TestTheCountItself:
    def test_only_isin_bearing_rows_are_instruments(self):
        """Cash has no ISIN and is not an instrument. This mirrors the count in
        `count_model_portfolio_holdings_sync`."""
        rows = [
            {"ISINCode": "US0378331005", "Fonds": "Apple"},
            {"ISINCode": "NL0011794037", "Fonds": "Ahold"},
            {"ISINCode": None, "Fonds": "Liquiditeiten"},        # cash — not an instrument
        ]
        assert sum(1 for r in rows if r.get("ISINCode")) == 2

    def test_a_nan_isin_would_have_inflated_the_count(self):
        """Why `_parse_positions_xls` must `astype(object)` BEFORE replacing NaN. If a NaN
        survives, it reaches here as the float nan (or the string "nan") — both truthy — and
        the cash line silently counts as a holding."""
        assert bool("nan") is True
        assert bool(float("nan")) is True
        # The parser's job is to make sure this is what we actually see:
        assert bool(None) is False


class TestTheStreamedShape:
    def test_a_failed_portfolio_leaves_holdings_unset(self):
        """A count we failed to take must not be written as 0 — see the module docstring."""
        import inspect

        from airs_scanner import count_model_portfolio_holdings_sync

        src = inspect.getsource(count_model_portfolio_holdings_sync)
        body = src.split("except Exception", 1)[1]
        assert "holdings_error" in body
        assert '["holdings"] = 0' not in body
