"""`parse_airs_excel` against the real Vermogensoverzicht (VOLK) header.

The sheet's thirteen columns, verbatim from AIRS:

    Fondsomschrijving · Aantal · Kostprijs lopend jaar · Beginwaarde lopend jaar ·
    Beginwaarde lopend jaar EUR · Huidige koers · Huidige waarde · Huidige waarde  EUR ·
    Weging · Fondsresultaat · Valutaresultaat · Resultaat in % · Valuta

Note `Huidige waarde  EUR` — TWO spaces. That is not a typo here; it is what AIRS ships.
"""
from __future__ import annotations

from io import BytesIO

import pandas as pd
import pytest

from portfolio import parse_airs_excel

# One USD holding, priced so every derived figure is checkable by hand:
#   400 shares, opened the year at EUR 47,911.52 and now worth EUR 55,072.77
#   -> EUR return = 55072.77/47911.52 - 1 = +14.95%
#   -> local (USD) return = 137.68/119.78 ... via the local value columns = +12.40%
_ROW = {
    "Fondsomschrijving": "Amphenol",
    "Aantal": 400,
    "Kostprijs lopend jaar": 41000.00,
    "Beginwaarde lopend jaar": 50000.00,      # local (USD)
    "Beginwaarde lopend jaar EUR": 47911.52,
    "Huidige koers": 140.20,                  # local price per share
    "Huidige waarde": 56200.00,               # local (USD)
    "Huidige waarde  EUR": 55072.77,          # <- two spaces, as AIRS ships it
    "Weging": 5.00,                           # AIRS's own weight (a percent)
    "Fondsresultaat": 6200.00,
    "Valutaresultaat": 961.25,
    "Resultaat in %": 14.95,                  # AIRS's own return (a percent)
    "Valuta": "USD",
}


def _xls(rows: list[dict]) -> bytes:
    buf = BytesIO()
    pd.DataFrame(rows).to_excel(buf, index=False)
    return buf.getvalue()


def _one(overrides: dict | None = None):
    row = dict(_ROW)
    if overrides:
        row.update(overrides)
    return parse_airs_excel(_xls([row]))[0]


class TestAirsOwnColumnsAreCarried:
    """The six columns we used to drop on the floor."""

    def test_every_airs_column_is_parsed(self):
        h = _one()
        assert h.cost_basis_local == 41000.00
        assert h.current_price_local == 140.20
        assert h.airs_weight == 5.00
        assert h.fund_result_eur == 6200.00
        assert h.fx_result_eur == 961.25
        assert h.airs_result_pct == 14.95

    def test_airs_figures_are_passed_through_never_rescaled(self):
        """`Resultaat in %` is stored AS REPORTED.

        Our `ytd_return_pct` is a FRACTION; AIRS's `Resultaat in %` is a PERCENT, and they
        are the same quantity — the EUR return. Measured on a real download
        (BUS_MTS_OFF_AFS_DYN, row `Visa`): AIRS 11.41 against our own 0.1141. Neither is
        rescaled into the other; they are carried side by side as the cross-check.
        """
        h = _one()
        assert h.airs_result_pct == 14.95           # AIRS's, untouched
        assert h.ytd_return_pct == pytest.approx(0.1495, abs=1e-4)   # ours, a fraction
        # The two describe the same holding and differ by exactly 100x. That is the trap.
        assert h.airs_result_pct == pytest.approx(h.ytd_return_pct * 100, rel=1e-3)

    def test_our_figures_are_unchanged_by_the_new_columns(self):
        h = _one()
        assert h.holding_name == "Amphenol"
        assert h.quantity == 400
        assert h.currency == "USD"
        assert h.start_value_eur == 47911.52
        assert h.current_value_eur == 55072.77
        assert h.ytd_return_eur == pytest.approx(7161.25, abs=0.01)
        assert h.ytd_return_local_pct == pytest.approx(0.1240, abs=1e-4)

    def test_the_performance_and_fx_legs_are_kept_apart(self):
        """`Fondsresultaat`/`Valutaresultaat` are the split we cannot derive.

        Summing them into one "result" throws away the only statement of how much of a
        holding's EUR return was the asset and how much was the euro moving.
        """
        h = _one()
        assert h.fund_result_eur == 6200.00
        assert h.fx_result_eur == 961.25


class TestTheHeaderIsMatchedByShapeNotByLuck:
    def test_the_double_space_in_huidige_waarde_eur_is_not_load_bearing(self):
        """AIRS ships `Huidige waarde  EUR`; a single-space export must parse identically.

        The parser used to compare literal header strings, so this was one AIRS tweak away
        from `Excel missing columns` on every portfolio at once.
        """
        row = {("Huidige waarde EUR" if k == "Huidige waarde  EUR" else k): v
               for k, v in _ROW.items()}
        h = parse_airs_excel(_xls([row]))[0]
        assert h.current_value_eur == 55072.77

    def test_headers_are_case_and_whitespace_insensitive(self):
        row = {f"  {k.upper()}  ": v for k, v in _ROW.items()}
        h = parse_airs_excel(_xls([row]))[0]
        assert h.current_value_eur == 55072.77
        assert h.airs_result_pct == 14.95

    def test_huidige_waarde_never_resolves_to_the_eur_column(self):
        """⚠ EXACT match, never a prefix.

        `Huidige waarde` (local) is a PREFIX of `Huidige waarde  EUR`. Resolve the local
        lookup with `startswith` and every USD holding's "local" value is silently its EUR
        one — the local return collapses to the EUR return and the FX leg vanishes.
        """
        h = _one()
        # Local and EUR genuinely differ here, so a prefix hit would show up as equality.
        assert h.ytd_return_local_pct != pytest.approx(h.ytd_return_pct, abs=1e-4)


class TestAnOlderExportStillParses:
    def test_the_airs_columns_are_optional(self):
        """An export predating these columns must parse, with the new fields None — not 0.

        A 0 there would read as "AIRS says this holding made nothing".
        """
        row = {k: v for k, v in _ROW.items()
               if k not in {"Kostprijs lopend jaar", "Huidige koers", "Weging",
                            "Fondsresultaat", "Valutaresultaat", "Resultaat in %"}}
        h = parse_airs_excel(_xls([row]))[0]
        assert h.current_value_eur == 55072.77      # ours still work
        assert h.cost_basis_local is None
        assert h.airs_result_pct is None
        assert h.fund_result_eur is None

    def test_a_missing_required_column_still_raises(self):
        row = {k: v for k, v in _ROW.items() if k != "Huidige waarde  EUR"}
        with pytest.raises(ValueError, match="missing columns"):
            parse_airs_excel(_xls([row]))

    def test_a_zero_airs_result_is_a_value_not_a_gap(self):
        h = _one({"Resultaat in %": 0, "Fondsresultaat": 0})
        assert h.airs_result_pct == 0.0
        assert h.fund_result_eur == 0.0
