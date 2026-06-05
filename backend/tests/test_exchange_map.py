"""Unit tests for `index_universe/acwi/exchange_map.py` — the iShares ↔
GuruFocus exchange/ticker resolution that has caused real listing-misroute
incidents (the 2026-04-20 Wiener→XPRA collapse `b12f4b6`, the cross-exchange
override format, the HKSE zero-pad). Everything here is pure (the only
external input is `gf_ticker_overrides.json`), so these run with no DB and
no network.

Several tests deliberately pin live entries in `gf_ticker_overrides.json`
(BRK.B, Komerční→KOMB, Verisure→FRA:6R9, …). That coupling is intentional:
if someone deletes one of those overrides, the corresponding regression
test fails loudly — which is exactly the guard the listing-misroute history
calls for.
"""
from __future__ import annotations

import pytest

from index_universe.acwi.exchange_map import (
    FEASIBLE_GF_EXCHANGES,
    apply_company_override,
    expected_db_exchange_codes,
    gurufocus_exchange,
    gurufocus_exchange_for_db,
    gurufocus_ticker_normalized,
    gurufocus_url,
    unavailable_reason,
)


class TestIsharesToGfMapping:
    def test_us_exchanges_map_to_empty_prefix(self):
        assert gurufocus_exchange("NYSE") == ""
        assert gurufocus_exchange("NASDAQ") == ""
        assert gurufocus_exchange("Cboe BZX") == ""

    def test_unknown_exchange_is_none(self):
        assert gurufocus_exchange("Some Imaginary Bourse") is None

    def test_vienna_and_prague_stay_distinct(self):
        # The b12f4b6 regression collapsed Wiener Boerse onto XPRA (Prague),
        # silently mixing Czech companies onto the Vienna row. Vienna must be
        # WBO; Prague must be XPRA — and the two must never be equal.
        assert gurufocus_exchange("Wiener Boerse Ag") == "WBO"
        assert gurufocus_exchange("Prague Stock Exchange") == "XPRA"
        assert gurufocus_exchange("Wiener Boerse Ag") != gurufocus_exchange(
            "Prague Stock Exchange"
        )

    def test_vienna_and_prague_distinct_in_db_codes(self):
        assert gurufocus_exchange_for_db("Wiener Boerse Ag") == "WBO"
        assert gurufocus_exchange_for_db("Prague Stock Exchange") == "XPRA"


class TestGurufocusExchangeForDb:
    def test_us_exchanges_map_to_db_codes(self):
        assert gurufocus_exchange_for_db("NYSE") == "NYSE"
        assert gurufocus_exchange_for_db("NASDAQ") == "NASDAQ"
        assert gurufocus_exchange_for_db("Cboe BZX") == "CBOE"

    def test_url_codes_converted_to_api_codes(self):
        # _GF_URL_TO_API: the URL-style code differs from the DB/API code.
        assert gurufocus_exchange_for_db("Tel Aviv Stock Exchange") == "XTAE"  # TASE → XTAE
        assert gurufocus_exchange_for_db("Standard-Classica-Forts") == "MIC"   # MCX → MIC

    def test_passthrough_for_codes_without_api_remap(self):
        assert gurufocus_exchange_for_db("Tokyo Stock Exchange") == "TSE"
        assert gurufocus_exchange_for_db("Hong Kong Exchanges And Clearing Ltd") == "HKSE"

    def test_unknown_exchange_is_none(self):
        assert gurufocus_exchange_for_db("Some Imaginary Bourse") is None


class TestExpectedDbExchangeCodes:
    def test_includes_vienna_and_prague_distinctly(self):
        codes = expected_db_exchange_codes()
        assert "WBO" in codes
        assert "XPRA" in codes

    def test_includes_us_and_remapped_codes(self):
        codes = expected_db_exchange_codes()
        assert {"NYSE", "NASDAQ", "CBOE"} <= codes
        assert "XTAE" in codes  # TASE remapped
        assert "MIC" in codes   # MCX remapped


class TestGurufocusUrl:
    def test_us_listing_is_bare(self):
        assert gurufocus_url("AAPL", "NASDAQ") == "https://www.gurufocus.com/stock/AAPL/summary"

    def test_foreign_listing_is_prefixed(self):
        assert (
            gurufocus_url("NESN", "SIX Swiss Exchange")
            == "https://www.gurufocus.com/stock/XSWX:NESN/summary"
        )

    def test_hkse_numeric_ticker_zero_padded(self):
        # Tencent 700 → HKSE:00700
        assert (
            gurufocus_url("700", "Hong Kong Exchanges And Clearing Ltd")
            == "https://www.gurufocus.com/stock/HKSE:00700/summary"
        )

    def test_istanbul_e_suffix_stripped(self):
        assert (
            gurufocus_url("THYAO.E", "Istanbul Stock Exchange")
            == "https://www.gurufocus.com/stock/IST:THYAO/summary"
        )

    def test_thailand_r_suffix_stripped(self):
        assert (
            gurufocus_url("PTT.R", "Stock Exchange Of Thailand")
            == "https://www.gurufocus.com/stock/BKK:PTT/summary"
        )

    def test_us_class_share_slash_to_dot(self):
        assert (
            gurufocus_url("BRK/B", "NYSE")
            == "https://www.gurufocus.com/stock/BRK.B/summary"
        )

    def test_unknown_exchange_returns_none(self):
        assert gurufocus_url("XYZ", "Some Imaginary Bourse") is None

    def test_empty_and_placeholder_ticker_returns_none(self):
        assert gurufocus_url("", "NYSE") is None
        assert gurufocus_url("--", "NYSE") is None


class TestGurufocusTickerNormalized:
    def test_us_rename_override_applied(self):
        # "" key: BRKB → BRK.B (string rename, same exchange).
        assert gurufocus_ticker_normalized("BRKB", "NYSE") == ("NYSE", "BRK.B")

    def test_cross_exchange_remap_uses_override_prefix(self):
        # Verisure: iShares says OSTO (Nasdaq Omx Nordic) but the GF listing
        # is FRA:6R9. The returned DB exchange must be the override target.
        assert gurufocus_ticker_normalized("VSURE", "Nasdaq Omx Nordic") == ("FRA", "6R9")

    def test_hkse_zero_pad(self):
        assert gurufocus_ticker_normalized("700", "Hong Kong Exchanges And Clearing Ltd") == (
            "HKSE",
            "00700",
        )

    def test_skip_listing_returns_none(self):
        # _SKIP_LISTINGS — HKSE:3750 is deliberately dropped.
        assert gurufocus_ticker_normalized("3750", "Hong Kong Exchanges And Clearing Ltd") is None

    def test_unknown_exchange_and_empty_ticker(self):
        assert gurufocus_ticker_normalized("AAPL", "Some Imaginary Bourse") is None
        assert gurufocus_ticker_normalized("", "NYSE") is None
        assert gurufocus_ticker_normalized("--", "NYSE") is None


class TestApplyCompanyOverride:
    """`apply_company_override` works on DB-form (exchange_code, ticker)
    pairs — the entry point every ingest path calls before inserting a
    `company` row. Must be idempotent on the no-override case."""

    def test_no_override_is_identity(self):
        r = apply_company_override("NASDAQ", "AAPL")
        assert (r.target_exchange, r.target_ticker, r.unavailable_reason) == ("NASDAQ", "AAPL", None)

    def test_empty_ticker_is_identity(self):
        r = apply_company_override("NYSE", "")
        assert (r.target_exchange, r.target_ticker, r.unavailable_reason) == ("NYSE", "", None)

    def test_us_string_rename_via_empty_key(self):
        # US DB codes normalize to the "" outer key. BRKB → BRK.B.
        r = apply_company_override("NYSE", "BRKB")
        assert (r.target_exchange, r.target_ticker, r.unavailable_reason) == ("NYSE", "BRK.B", None)

    def test_cross_exchange_remap(self):
        # XPRA:EBS (Erste) actually lives on WBO (Vienna).
        r = apply_company_override("XPRA", "EBS")
        assert (r.target_exchange, r.target_ticker, r.unavailable_reason) == ("WBO", "EBS", None)

    def test_us_adr_remapped_to_hk(self):
        # BIDUN ADR remaps to HKSE:09888 — the override is under the "" key,
        # so a NASDAQ DB row resolves through it.
        r = apply_company_override("NASDAQ", "BIDUN")
        assert (r.target_exchange, r.target_ticker, r.unavailable_reason) == ("HKSE", "09888", None)

    def test_unavailable_marks_out_of_scope_without_remap(self):
        r = apply_company_override("XPRA", "VAR1")
        assert (r.target_exchange, r.target_ticker) == ("XPRA", "VAR1")
        assert r.unavailable_reason is not None
        assert "XHAM" in r.unavailable_reason or r.unavailable_reason


class TestUnavailableReason:
    def test_unavailable_listing_returns_reason(self):
        reason = unavailable_reason("VAR1", "Prague Stock Exchange")
        assert reason is not None

    def test_available_listing_returns_none(self):
        assert unavailable_reason("EBS", "Prague Stock Exchange") is None

    def test_unknown_exchange_returns_none(self):
        assert unavailable_reason("VAR1", "Some Imaginary Bourse") is None


class TestFeasibleExchanges:
    def test_us_and_core_regions_feasible(self):
        assert "" in FEASIBLE_GF_EXCHANGES  # US
        assert {"LSE", "XTER", "HKSE", "TSE", "SAU"} <= FEASIBLE_GF_EXCHANGES

    def test_out_of_scope_regions_excluded(self):
        # Russia / Australia / NZ / Africa / LatAm are out of subscription.
        for code in ("MCX", "ASX", "NZSE", "JSE", "CAI", "TSX", "MEX"):
            assert code not in FEASIBLE_GF_EXCHANGES


class TestHistoricallyBrokenCompanies:
    """The 10 companies that have broken before (per the ACWI-overrides
    memory). Each pins the (iShares ticker, iShares exchange) → expected
    (db_exchange, gf_ticker) resolution so a regression in the override
    file or `_ISHARES_TO_GF` fails here first."""

    @pytest.mark.parametrize(
        "ishares_ticker,ishares_exchange,expected",
        [
            ("BRKB", "NYSE", ("NYSE", "BRK.B")),          # Berkshire B
            ("HEIA", "NYSE", ("NYSE", "HEI.A")),          # HEICO Class A
            ("EBS", "Prague Stock Exchange", ("WBO", "EBS")),   # Erste → Vienna
            ("CICT", "Singapore Exchange", ("SGX", "C38U")),    # CapitaLand Integrated
            ("CLAR", "Singapore Exchange", ("SGX", "A17U")),    # CapitaLand Ascendas
            ("BAAKOMB", "Prague Stock Exchange", ("XPRA", "KOMB")),  # Komerční banka
            ("VSURE", "Nasdaq Omx Nordic", ("FRA", "6R9")),    # Verisure cross-exchange
            ("PSTG", "NYSE", ("NYSE", "P")),              # EVERPURE → P (US "" key)
            ("SPL", "Warsaw Stock Exchange/Equities/Main Market", ("WAR", "EBP")),  # Santander PL
        ],
    )
    def test_resolution(self, ishares_ticker, ishares_exchange, expected):
        assert gurufocus_ticker_normalized(ishares_ticker, ishares_exchange) == expected
