"""The iShares venue is often NOT the venue we hold — and matching on the bare ticker is unsafe.

⚠⚠ THE MOTIVATING REPORT (2026-09-01): "Shopify is in our ACWI excel sheet but somehow still not
recognised as an ACWI member." iShares books Shopify on the **Toronto** Stock Exchange, so the file
resolves to `SHOP.TO`; we hold the same company as `SHOP` on NasdaqGS under `CA82509L1076`. The
exact-symbol join missed it. Measured on that file, 111 constituents were in the same state.

⚠⚠ AND THE OBVIOUS FIX IS THE DANGEROUS ONE. Matching the bare ticker on any venue, over those same
111 rows, pairs Target with **11 88 0 Solutions AG**, National Grid with **NovaGold**, Hermès with
**Ramelius Resources**, L'Oréal with **OR Royalties** and BAE Systems with **Boeing**. Every one
would appear in the index, priced, looking entirely ordinary. So the fallback is gated on the
ISIN's own country against the country the file names — the rule `close_company_bridge` already
proved for this exact "same ticker, different venue" question.

These are the cases that gate has to get right, taken from the live file. Unit-only: pure
functions over dicts, no database and no network.
"""
from __future__ import annotations

from index_universe.acwi.asset_membership import _base, _by_base_ticker, _interlisted

#: `{ISIN prefix: country}` — the slice of the `country` table these cases need.
CODES = {
    "CA": "Canada", "US": "United States", "GB": "United Kingdom", "DE": "Germany",
    "AU": "Australia", "SE": "Sweden", "NL": "Netherlands", "ZA": "South Africa",
    "TH": "Thailand", "AE": "United Arab Emirates",
}


def ex(isin: str, symbol: str, name: str, aid: int) -> dict:
    return {"isin": isin, "yahoo_symbol": symbol, "name": name, "analysis_id": aid}


class TestTheShopifyCase:
    """The report this was written for."""

    def test_a_company_we_hold_on_another_venue_is_found(self):
        cands = [ex("CA82509L1076", "SHOP", "Shopify Inc.", 3029)]
        hit = _interlisted(cands, "Canada", "SHOPIFY SUBORDINATE VOTING INC CLA", CODES)
        assert hit is not None and hit["analysis_id"] == 3029

    def test_and_the_name_need_not_agree_for_it(self):
        """⚠ THE COUNTRY IS THE TEST; THE NAME IS ONLY A TIE-BREAKER. Requiring both would cost
        seven correct members on the live file: GSK is stored as `GSK plc` against the file's
        `GLAXOSMITHKLINE`, five Thai NVDR lines carry `… NON-VOTING DR PCL`, and First Abu Dhabi
        Bank's stored name is simply WRONG (`First Trust Multi Cap Value Al`) over a correct
        `AEN000101016`. A single unambiguous candidate from the right country is the answer."""
        hit = _interlisted([ex("AEN000101016", "FAB", "First Trust Multi Cap Value Alph", 91)],
                           "United Arab Emirates", "FIRST ABU DHABI BANK", CODES)
        assert hit is not None and hit["analysis_id"] == 91


class TestTheCollisionsItMustRefuse:
    """⚠⚠ EVERY ONE OF THESE IS REAL, off the 15-Apr-2026 file. They are what a bare-ticker match
    would have put in the index."""

    def test_target_is_not_a_german_telecoms_reseller(self):
        assert _interlisted([ex("DE0005118806", "TGT.DU", "11 88 0 Solutions AG", 1)],
                            "United States", "TARGET CORP", CODES) is None

    def test_national_grid_is_not_novagold(self):
        assert _interlisted([ex("CA66987E2069", "NG", "NovaGold Resources Inc.", 2)],
                            "United Kingdom", "NATIONAL GRID PLC", CODES) is None

    def test_bae_systems_is_not_boeing(self):
        assert _interlisted([ex("US0970231058", "BA", "The Boeing Company", 3)],
                            "United Kingdom", "BAE SYSTEMS PLC", CODES) is None

    def test_eqt_corp_is_not_eqt_ab(self):
        """⚠ THE NAME GATE ALONE WOULD ACCEPT THIS ONE — both are "EQT" once corporate forms are
        stripped — which is why the country is the test and not the name."""
        assert _interlisted([ex("SE0012853455", "EQT.ST", "EQT AB (publ)", 4)],
                            "United States", "EQT CORP", CODES) is None


class TestItPicksTheRightLineWhereSeveralExist:
    def test_rio_tinto_plc_takes_the_london_ordinary(self):
        """⚠ THREE CANDIDATES, ONE ANSWER. The file files this row under the United Kingdom, so
        the Australian company and the US ADR are both wrong even though all three are named
        `Rio Tinto Group` in our store."""
        cands = [ex("AU000000RIO1", "RIO.AX", "Rio Tinto Group", 403),
                 ex("GB0007188757", "RIO", "Rio Tinto Group", 3808),
                 ex("US7672041008", "RIO", "Rio Tinto Group", 3808)]
        hit = _interlisted(cands, "United Kingdom", "RIO TINTO PLC", CODES)
        assert hit is not None and hit["isin"] == "GB0007188757"

    def test_newmont_does_not_take_the_australian_cdi(self):
        """⚠ THE SAME ISSUER ON A DIFFERENT LINE IS STILL THE WRONG ANSWER — this is the pairing
        `measure_acwi_asset_gap`'s name matcher made, and the reason that script never shipped."""
        assert _interlisted([ex("AU0000297962", "NEM.AX", "Newmont Corporation", 5)],
                            "United States", "NEWMONT", CODES) is None


class TestItRefusesRatherThanGuesses:
    def test_two_candidates_from_one_country_are_ambiguous(self):
        """⚠ BOTH BOMBARDIER CLASSES PASS THE COUNTRY TEST AND THE NAME TEST. Picking either is a
        coin toss between two different securities, so it takes neither."""
        cands = [ex("CA0977512007", "BBD-A.TO", "Bombardier Inc.", 6),
                 ex("CA0977517274", "BBD-B.TO", "Bombardier Inc.", 7)]
        assert _interlisted(cands, "Canada", "BOMBARDIER INC CLASS B", CODES) is None

    def test_an_unmappable_country_is_refused_not_assumed(self):
        """⚠ IT ERRS SAFE. A false refusal costs one member this run; a false accept puts a
        different company in the index for ever."""
        assert _interlisted([ex("CA82509L1076", "SHOP", "Shopify Inc.", 3029)],
                            "Ruritania", "SHOPIFY", CODES) is None
        assert _interlisted([ex("CA82509L1076", "SHOP", "Shopify Inc.", 3029)],
                            "", "SHOPIFY", CODES) is None

    def test_an_isin_whose_prefix_is_not_in_the_table_is_refused(self):
        assert _interlisted([ex("XX1234567890", "ZZZ", "Somewhere Ltd", 8)],
                            "Canada", "SOMEWHERE", CODES) is None

    def test_no_candidates_at_all(self):
        assert _interlisted([], "Canada", "SHOPIFY", CODES) is None


class TestTheBaseKey:
    """⚠⚠ IT MUST STRIP THE CLASS MARKER AS WELL AS THE VENUE, and that is not cosmetic. Once
    `yahoo_map` renders a class marker as a hyphen on every venue, the Thai NVDR `DELTA.R` becomes
    `DELTA-R.BK` while the ordinary we hold is `DELTA.BK`. Splitting on the dot alone leaves
    `DELTA-R`, which matches nothing — and all twelve Thai constituents would have stopped
    resolving as a side effect of fixing a DIFFERENT venue's spelling."""

    def test_it_strips_the_venue_and_the_class_marker(self):
        assert _base("SHOP.TO") == "SHOP"
        assert _base("DELTA-R.BK") == "DELTA"
        assert _base("NOVO-B.CO") == "NOVO"
        assert _base("BRK-B") == "BRK"
        assert _base("SHOP") == "SHOP"

    def test_the_index_groups_the_lines_of_one_ticker_together(self):
        rows = [ex("CA82509L1076", "SHOP", "Shopify Inc.", 3029),
                ex("TH0528A10Z06", "DELTA.BK", "Delta Electronics", 10),
                ex("US00206R1023", "T", "AT&T Inc.", 11)]
        idx = _by_base_ticker(rows)
        assert [r["isin"] for r in idx["SHOP"]] == ["CA82509L1076"]
        assert [r["isin"] for r in idx["DELTA"]] == ["TH0528A10Z06"]
        assert "T" in idx

    def test_a_row_with_no_symbol_or_no_asset_is_left_out(self):
        rows = [ex("CA82509L1076", "", "Shopify Inc.", 3029),
                {"isin": "X", "yahoo_symbol": "AAA", "name": "A", "analysis_id": None}]
        assert _by_base_ticker(rows) == {}
