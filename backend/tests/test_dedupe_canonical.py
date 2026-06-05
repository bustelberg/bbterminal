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


def _row(cid: int, ticker: str, exch: str) -> CompanyRow:
    return CompanyRow(
        company_id=cid,
        company_name="Same Issuer",
        gurufocus_ticker=ticker,
        exchange_code=exch,
        exchange_id=None,
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
