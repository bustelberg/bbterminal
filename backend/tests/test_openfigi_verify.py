"""Unit tests for the pure OpenFIGI classifier (`classify_openfigi`).

Mirrors the real OpenFIGI `/v3/mapping` `data` shape: a list of listings, each
with `ticker`, `exchCode`, `name`, `securityType`. No network — pure logic.
"""
from __future__ import annotations

from index_universe.openfigi_verify import classify_openfigi, _names_match


def _rec(ticker, exch, name, sectype="Common Stock"):
    return {"ticker": ticker, "exchCode": exch, "name": name, "securityType": sectype}


class TestListingTier:
    def test_exact_listing_verifies(self):
        # Our HKSE:01071 — OpenFIGI returns "1071" on HK; canonical pads to 01071.
        data = [_rec("1071", "HK", "HUADIAN POWER INTL CORP-H")]
        status, name = classify_openfigi("CNE1000002Z3", "Huadian Power", "01071", "HKSE", data)
        assert status == "verified"

    def test_us_ticker_on_mapped_exchange(self):
        data = [_rec("AAPL", "UW", "APPLE INC")]
        status, _ = classify_openfigi("US0378331005", "Apple Inc", "AAPL", "NASDAQ", data)
        assert status == "verified"


class TestNameTier:
    def test_suffix_tolerant_name_verifies(self):
        # Nestlé: our exchange listing not in the (truncated) data, but name matches.
        data = [_rec("NESN", "SW", "NESTLE SA-REG")]
        status, name = classify_openfigi("CH0038863350", "NESTLE SA", "NESN", "XOTHER", data)
        assert status == "verified"
        assert name == "NESTLE SA-REG"

    def test_wrong_isin_is_mismatch(self):
        # The HAL trap: stored ISIN resolves to HAL TRUST, not Hindustan Aeronautics.
        data = [_rec("HAL", "NA", "HAL TRUST")]
        status, name = classify_openfigi("BMG455841020", "Hindustan Aeronautics Ltd", "HAL", "NSE", data)
        assert status == "mismatch"
        assert name == "HAL TRUST"

    def test_abbreviated_shared_root_verifies(self):
        # Lindt: OpenFIGI abbreviates + appends a share-class suffix; the full
        # legal name isn't a strict prefix but shares a long root.
        data = [_rec("LISP", "SW", "CHOCOLADEFABRIKEN LINDT-PC")]
        status, _ = classify_openfigi(
            "CH0010570767", "Chocoladefabriken Lindt & Spruengli AG", "LISN", "XSWX", data)
        assert status == "verified"

    def test_gdr_shared_root_verifies(self):
        # Samsung GDR ISIN: "SAMSUNG ELECTR-GDR REG S" vs "Samsung Electronics Co Ltd".
        data = [_rec("SSU", "GR", "SAMSUNG ELECTR-GDR REG S", "GDR")]
        status, _ = classify_openfigi(
            "US7960508882", "Samsung Electronics Co Ltd", "005930", "XKRX", data)
        assert status == "verified"

    def test_name_match_checks_all_records(self):
        # EDP: best_match returns the abbreviated "EDP SA"; the full name lives in
        # a later record and must still verify.
        data = [
            _rec("EDP", "GR", "EDP SA"),
            _rec("EDP", "EO", "EDP-ENERGIAS DE PORTUGAL SA"),
        ]
        status, _ = classify_openfigi(
            "PTEDP0AM0009", "EDP ENERGIAS DE PORTUGAL SA", "EDP", "XLIS", data)
        assert status == "verified"

    def test_space_insensitive_listing_verifies(self):
        # H&M: GuruFocus "HM B" / OpenFIGI "HMB" on the same Stockholm listing.
        data = [_rec("HMB", "SS", "HENNES & MAURITZ AB-B SHS")]
        status, _ = classify_openfigi(
            "SE0000106270", "H & M Hennes & Mauritz AB", "HM B", "OSTO", data)
        assert status == "verified"

    def test_genuinely_wrong_isin_still_mismatch(self):
        # Air Liquide's stored ISIN was C3.ai's — different company, no shared root.
        data = [_rec("AI", "US", "C3.AI INC-A")]
        status, name = classify_openfigi(
            "US12468P1049", "Air Liquide SA", "AI", "XPAR", data)
        assert status == "mismatch"
        assert name == "C3.AI INC-A"


class TestEdgeCases:
    def test_no_isin(self):
        assert classify_openfigi(None, "X", "X", "NYSE", None) == ("no_isin", None)
        assert classify_openfigi("  ", "X", "X", "NYSE", [])[0] == "no_isin"

    def test_not_found(self):
        assert classify_openfigi("US0000000000", "X", "X", "NYSE", None) == ("not_found", None)
        assert classify_openfigi("US0000000000", "X", "X", "NYSE", [])[0] == "not_found"


class TestNamesMatch:
    def test_equal_and_punctuation(self):
        assert _names_match("Apple Inc", "APPLE INC.")
        assert _names_match("Nestle SA", "NESTLE SA-REG")

    def test_different_companies(self):
        assert not _names_match("Hindustan Aeronautics Ltd", "HAL TRUST")

    def test_too_short_no_overmatch(self):
        # 3-char shared prefix shouldn't verify unrelated names.
        assert not _names_match("ABC Corp", "ABZ Holdings")

    def test_ampersand_equals_and(self):
        # GuruFocus spells out "AND", OpenFIGI uses "&" — same company.
        assert _names_match("SMITH AND NEPHEW PLC", "SMITH & NEPHEW PLC")
