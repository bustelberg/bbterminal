"""The consensus FREE cash flow, out of GuruFocus's undocumented `keyratios` endpoint.

⚠⚠ THE FIELD WAS RULED OUT AND IT EXISTS. `analyst_estimate`'s annual block genuinely has no
free-cash-flow key, and every stored spelling of one came back with zero rows — so this app
concluded GuruFocus published it only through its Excel add-in. Both observations were true and the
conclusion did not follow: `stock/{sym}/keyratios` → `Fundamental` carries
`Estimated Free Cash Flow for Next FY1/FY2 End (M)`, and `gurufocus_api.json` had listed that
endpoint as **real** the whole time. Nobody had opened its 264-key section.

⚠ IT MATTERS BECAUSE THE DERIVATION IT REPLACES NETS THE WRONG CAPEX. `OCF_est − trailing capex`
reads 45,005 for Meta FY2026 where the vendor's own forecast is 5,412 — the 39.6bn gap is capex the
company has guided to and not yet spent.

⚠⚠ AND THE PAYLOAD CARRIES NO DATES: "Next FY1 End" is an ORDINAL. The pairing with real fiscal
year-ends is the one thing here that can be silently wrong, so it is what these tests are about.

Unit-only: `_parse_key_ratios` is pure and the dates are handed in.
"""
from __future__ import annotations

from datetime import date

from ingest.earnings import _parse_key_ratios

FCF = "annual_fcf_estimate"
#: AAPL's real payload shape, trimmed to what is read.
PAYLOAD = {
    "Basic": {"Company": "Apple Inc"},
    "Fundamental": {
        "Estimated Free Cash Flow for Next FY1 End (M)": 137187.366,
        "Estimated Free Cash Flow for Next FY2 End (M)": 145707.389,
        "Estimated Operating Cash Flow for Next FY1 End (M)": 148323.411,
        "Estimated EPS for Next FY1 End": 8.760,
    },
}
FY = [date(2026, 9, 30), date(2027, 9, 30), date(2028, 9, 30)]


def _by_date(rows: list[dict]) -> dict[str, float]:
    return {r["target_date"]: r["numeric_value"] for r in rows if r["metric_code"] == FCF}


class TestTheOrdinalIsPairedWithARealFiscalYearEnd:
    def test_fy1_and_fy2_land_on_the_dates_they_were_forecast_for(self):
        rows = _parse_key_ratios(PAYLOAD, 1, FY)
        assert _by_date(rows) == {"2026-09-30": 137187.366, "2027-09-30": 145707.389}

    def test_a_missing_date_skips_that_ordinal_rather_than_shifting_the_rest_up(self):
        """⚠ THE SILENT FAILURE. `fy_dates` shorter than the ordinals published is ordinary — the
        endpoint carries FY3, the estimate block may not — and sliding FY2's figure onto FY1's date
        files a forecast against a year it was never made for."""
        rows = _parse_key_ratios(PAYLOAD, 1, FY[:1])
        assert _by_date(rows) == {"2026-09-30": 137187.366}

    def test_no_dates_at_all_stores_nothing(self):
        # ⚠ NOT A GUESSED FISCAL YEAR END. The panel falls back to deriving the base instead.
        assert _parse_key_ratios(PAYLOAD, 1, []) == []


class TestItStoresOnlyWhatIsNotAlreadyIngested:
    def test_the_operating_cash_flow_and_eps_estimates_are_left_alone(self):
        """⚠ `analyst_estimate` ALREADY WRITES THOSE as `annual_*_estimate`. Storing them from here
        too would be two writers for one code, disagreeing in the last decimal for ever — the
        payloads carry different precision (148323.41 there, 148323.411 here)."""
        codes = {r["metric_code"] for r in _parse_key_ratios(PAYLOAD, 1, FY)}
        assert codes == {FCF}

    def test_every_row_is_flagged_as_a_prediction(self):
        # ⚠ BOTH HALVES ARE WHAT PUT IT IN THE PANEL'S PAYLOAD: `load_company_metric_rows` reads
        # forward rows with `is_prediction=True AND metric_code LIKE 'annual_%'`.
        rows = _parse_key_ratios(PAYLOAD, 7, FY)
        assert rows and all(r["is_prediction"] and r["company_id"] == 7 for r in rows)
        assert all(r["metric_code"].startswith("annual_") for r in rows)


class TestAnAbsentFigureIsNotAZero:
    def test_a_company_with_no_forward_fcf_yields_no_rows(self):
        assert _parse_key_ratios({"Fundamental": {"Estimated EPS for Next FY1 End": 3.0}},
                                 1, FY) == []

    def test_a_payload_with_no_fundamental_section_is_refused_rather_than_raising(self):
        for blob in ({}, {"Fundamental": None}, {"Fundamental": []}, None):
            assert _parse_key_ratios(blob, 1, FY) == []
