"""The `MODEL` report — a dynamic portfolio's own model weights, and the end of the pairing.

Every fixture is a real row from `rapport_types=MODEL` on BUS_Neutraal_Dyn (42 rows, model
percentage summing to exactly 100.000).
"""
from __future__ import annotations

from io import BytesIO

import pandas as pd
import pytest

from airs_model import NAME_ALIASES, attach_model, model_total_pct, parse_model

COLS = ["Fondsomschrijving", "Model percentage", "Werkelijk percentage", "Afwijking percentage",
        "Afwijking in euro", "Kopen", "Verkopen", "Waarde volgens model",
        "Koers in locale valuta", "Geschat orderbedrag"]


def _row(naam, model, werk, afw, afw_eur, kopen, verkopen, waarde, koers, order):
    return dict(zip(COLS, [naam, model, werk, afw, afw_eur, kopen, verkopen, waarde, koers, order]))


REAL = [
    _row("ASML Holding", 3.250, 5.275297, -2.025297, -23800.950875, 0, 15, 38193.449125, 1589.60, 23844.0),
    _row("MasterCard", 1.300, 2.539570, -1.239570, -14567.220350, 0, 31, 15277.379650, 531.98, 14455.978259),
    _row("Effectenrekening Liquiditeiten", 1.358, 2.501220, -1.143220, -13434.924181, 0, 0, 15958.985819, 0.0, 0.0),
    # ⚠ In the model and NOT held — the strategy says buy it and the book has not.
    _row("iShares Global Select Dividend 100", 2.000, 0.0, 2.000, 23000.0, 12, 0, 23000.0, 25.5, 23000.0),
]


def _xls(rows: list[dict]) -> bytes:
    buf = BytesIO()
    pd.DataFrame(rows, columns=COLS).to_excel(buf, index=False)
    return buf.getvalue()


class TestTheWeightsAreReadAsReported:
    def test_model_percentage_is_a_percent_not_a_fraction(self):
        m = {r.fonds: r for r in parse_model(_xls(REAL))}
        assert m["ASML Holding"].model_pct == pytest.approx(3.250)
        assert m["ASML Holding"].actual_pct == pytest.approx(5.275297)
        assert m["ASML Holding"].drift_pct == pytest.approx(-2.025297)

    def test_the_rebalance_orders_ride_along(self):
        m = {r.fonds: r for r in parse_model(_xls(REAL))}
        assert m["ASML Holding"].sell == 15 and m["ASML Holding"].buy == 0
        assert m["iShares Global Select Dividend 100"].buy == 12

    def test_the_model_sums_to_100(self):
        """⚠ Measured at EXACTLY 100.000 on every book. Far from it means a PARTIAL download, and
        a partial model understates every weight without looking wrong."""
        full = [*REAL, _row("Filler", 92.092, 0, 0, 0, 0, 0, 0, 0, 0)]
        assert model_total_pct(parse_model(_xls(full))) == pytest.approx(100.0, abs=1e-3)


class TestTheCashLineIsAliasedNotFuzzyMatched:
    """⚠ THE ONE SYSTEMATIC RENAME. This sheet says `Effectenrekening Liquiditeiten`; the
    Vermogensoverzicht says `Effectenrekening`. One row, both books, every time — so it is an
    explicit alias. Fuzzy matching the whole join to absorb it would re-import the failure mode
    that put a cash line on Hermes' ISIN this morning."""

    def test_the_cash_line_is_renamed_to_the_holdings_spelling(self):
        assert NAME_ALIASES["Effectenrekening Liquiditeiten"] == "Effectenrekening"
        assert any(r.fonds == "Effectenrekening" for r in parse_model(_xls(REAL)))

    def test_nothing_else_is_renamed(self):
        names = {r.fonds for r in parse_model(_xls(REAL))}
        assert "ASML Holding" in names and "MasterCard" in names
        assert len(NAME_ALIASES) == 1, "a growing alias map is fuzzy matching by another name"


class TestAttachingToTheBook:
    HOLD = {"ASML Holding", "MasterCard", "Effectenrekening", "Nvidia"}

    def test_a_model_line_lands_on_its_holding(self):
        by_name, _ = attach_model(parse_model(_xls(REAL)), self.HOLD)
        assert set(by_name) == {"ASML Holding", "MasterCard", "Effectenrekening"}
        assert by_name["Effectenrekening"].model_pct == pytest.approx(1.358)

    def test_a_model_line_the_book_does_not_hold_is_reported_as_drift(self):
        """⚠ Measured: `iShares Global Select Dividend 100` is in BUS_Neutraal_Dyn's model and not
        held. Dropping it would hide the one thing a model-vs-book view is for."""
        _, unheld = attach_model(parse_model(_xls(REAL)), self.HOLD)
        assert [r.fonds for r in unheld] == ["iShares Global Select Dividend 100"]
        assert unheld[0].model_pct == pytest.approx(2.0)

    def test_a_holding_with_no_model_line_simply_has_none(self):
        by_name, _ = attach_model(parse_model(_xls(REAL)), self.HOLD)
        assert "Nvidia" not in by_name          # held, but the model does not name it

    def test_the_match_is_exact(self):
        by_name, unheld = attach_model(parse_model(_xls(REAL)), {"ASML"})
        assert by_name == {} and len(unheld) == 4


class TestTheSheetShapeIsChecked:
    def test_a_missing_required_column_raises(self):
        """⚠ A silently empty model reads as "this book has no strategy", which is never true."""
        rows = [{k: v for k, v in r.items() if k != "Model percentage"} for r in REAL]
        buf = BytesIO()
        pd.DataFrame(rows).to_excel(buf, index=False)
        with pytest.raises(ValueError, match="missing columns"):
            parse_model(buf.getvalue())

    def test_headers_are_matched_case_and_whitespace_insensitively(self):
        rows = [{f"  {k.upper()}  ": v for k, v in r.items()} for r in REAL]
        buf = BytesIO()
        pd.DataFrame(rows).to_excel(buf, index=False)
        assert len(parse_model(buf.getvalue())) == 4

    def test_a_blank_name_is_skipped_not_stored_as_nan(self):
        rows = [*REAL, _row(None, 1.0, 0, 0, 0, 0, 0, 0, 0, 0)]
        assert "nan" not in {r.fonds for r in parse_model(_xls(rows))}
        assert len(parse_model(_xls(rows))) == 4
