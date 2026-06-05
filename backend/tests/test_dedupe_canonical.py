"""Unit tests for the pure dedupe helpers in `ingest/dedupe.py` —
`canonical_ticker`, `canonical_name`, `exchange_priority`, and
`pick_winner`. These are the rules that stop the `company` table from
accumulating two rows for the same issuer (Tencent `700`/`00700`, an
H-share vs its A-share or US ADR, …). No DB — `find_canonical_match` /
`merge_existing_duplicates` (which hit Supabase) are covered at the
phase/HTTP level.
"""
from __future__ import annotations


from ingest.dedupe import (
    CompanyRow,
    canonical_name,
    canonical_ticker,
    exchange_priority,
    pick_winner,
)


class TestCanonicalTicker:
    def test_strip_and_uppercase(self):
        assert canonical_ticker("  aapl ", "NASDAQ") == "AAPL"

    def test_empty_ticker(self):
        assert canonical_ticker("", "NASDAQ") == ""
        assert canonical_ticker(None, "NASDAQ") == ""

    def test_hkse_numeric_zero_padded(self):
        # 700 / 0700 / 00700 all collapse to one canonical form.
        assert canonical_ticker("700", "HKSE") == "00700"
        assert canonical_ticker("0700", "HKSE") == "00700"
        assert canonical_ticker("00700", "HKSE") == "00700"

    def test_hkse_non_numeric_untouched(self):
        assert canonical_ticker("ABC", "HKSE") == "ABC"

    def test_nordic_class_delimiters_normalized_to_space(self):
        for raw in ("NOVO.B", "NOVO-B", "NOVO B"):
            assert canonical_ticker(raw, "OCSE") == "NOVO B"

    def test_us_class_share_slash_to_dot(self):
        assert canonical_ticker("BRK/B", "NYSE") == "BRK.B"
        assert canonical_ticker("BF/B", "") == "BF.B"

    def test_non_us_slash_preserved(self):
        # The slash→dot rule is scoped to US/TSX exchanges only.
        assert canonical_ticker("FOO/B", "XSWX") == "FOO/B"


class TestCanonicalName:
    def test_lowercase_and_collapse_whitespace(self):
        assert canonical_name("  AIA   Group   Ltd ") == "aia group ltd"

    def test_case_insensitive_match(self):
        assert canonical_name("AIA GROUP LTD") == canonical_name("AIA Group Ltd")

    def test_empty(self):
        assert canonical_name("") == ""
        assert canonical_name(None) == ""

    def test_suffixes_preserved(self):
        # Different legal entities sharing a root must stay distinct.
        assert canonical_name("BYD Co Ltd") != canonical_name("BYD Electronic")


class TestExchangePriority:
    def test_hkse_is_highest_priority(self):
        assert exchange_priority("HKSE") == 0

    def test_china_a_shares_yield_to_hkse(self):
        assert exchange_priority("SHSE") > exchange_priority("HKSE")
        assert exchange_priority("SZSE") > exchange_priority("HKSE")

    def test_us_adrs_low_priority(self):
        assert exchange_priority("NYSE") == 10
        assert exchange_priority("NASDAQ") == 10

    def test_unknown_and_empty_default_to_99(self):
        assert exchange_priority("ZZZ") == 99
        assert exchange_priority("") == 99
        assert exchange_priority(None) == 99

    def test_case_insensitive(self):
        assert exchange_priority("hkse") == 0


def _row(cid: int, ticker: str, exch: str, *, lookup_failed=None, oos=None) -> CompanyRow:
    return CompanyRow(
        company_id=cid,
        company_name="Same Issuer",
        gurufocus_ticker=ticker,
        exchange_code=exch,
        exchange_id=None,
        gurufocus_lookup_failed_at=lookup_failed,
        out_of_scope_at=oos,
    )


class TestPickWinner:
    def test_hkse_beats_a_share(self):
        winner = pick_winner([_row(2, "600519", "SHSE"), _row(5, "00700", "HKSE")])
        assert winner.company_id == 5  # HKSE wins despite the higher company_id

    def test_hkse_beats_us_adr(self):
        winner = pick_winner([_row(1, "BABA", "NYSE"), _row(9, "09988", "HKSE")])
        assert winner.exchange_code == "HKSE"

    def test_padded_hkse_form_preferred_on_tie(self):
        # Two HKSE rows for the same issuer: the canonical 5-digit form wins.
        winner = pick_winner([_row(3, "700", "HKSE"), _row(8, "00700", "HKSE")])
        assert winner.gurufocus_ticker == "00700"

    def test_lowest_company_id_breaks_full_tie(self):
        winner = pick_winner([_row(7, "NESN", "XSWX"), _row(4, "NESN", "XSWX")])
        assert winner.company_id == 4

    def test_single_candidate(self):
        winner = pick_winner([_row(1, "AAPL", "NASDAQ")])
        assert winner.company_id == 1


class TestPickWinnerViability:
    """Viability beats EXCHANGE_PRIORITY: a phantom listing (lookup-failed /
    out-of-scope / off-coverage exchange) never wins over a real one."""

    def test_real_us_listing_beats_lookup_failed_out_of_scope_phantom(self):
        # The ALTM incident: a BMV (Mexican, off-GF-coverage) phantom that
        # failed GuruFocus lookup must NOT outrank the real NASDAQ listing —
        # even though BMV (EXCHANGE_PRIORITY 2) ranks above NASDAQ (10).
        nasdaq = _row(6016, "ALTM", "NASDAQ")
        bmv = _row(5611, "ALTM", "BMV", lookup_failed="2026-06-01T00:00:00Z")
        assert pick_winner([bmv, nasdaq]).company_id == 6016

    def test_off_coverage_exchange_loses_even_without_flags(self):
        # BMV is outside FEASIBLE_GF_EXCHANGES → non-viable on feasibility
        # alone, so the US row wins despite BMV's higher EXCHANGE_PRIORITY.
        assert pick_winner([_row(2, "ALTM", "BMV"), _row(9, "ALTM", "NASDAQ")]).company_id == 9

    def test_out_of_scope_flag_down_ranks(self):
        viable = _row(3, "NESN", "XSWX")
        oos = _row(1, "NESN", "XSWX", oos="2026-01-01T00:00:00Z")
        assert pick_winner([oos, viable]).company_id == 3  # viable wins despite higher id

    def test_two_phantoms_fall_back_to_priority_then_id(self):
        # Both non-viable → tiebreak by EXCHANGE_PRIORITY (BMV 2 < unmapped 99).
        a = _row(5, "X", "BMV", lookup_failed="t")
        b = _row(2, "X", "ZZZ", lookup_failed="t")
        assert pick_winner([a, b]).company_id == 5
