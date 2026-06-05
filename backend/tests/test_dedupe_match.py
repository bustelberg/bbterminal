"""DB-backed dedupe test: `ingest.dedupe.find_canonical_match` — the
pre-insert duplicate detector every ingest path consults. Two buckets:
(1) same exchange + canonical ticker (catches HKSE `700` vs `00700`), and
(2) same canonical company name across ANY exchange (catches the H-share /
ADR / GDR cross-listings the user wants to reject). Run against the
in-memory `FakeSupabase`.
"""
from __future__ import annotations

from ingest.dedupe import find_canonical_match

from tests._fake_supabase import FakeSupabase


def _co(cid, name, ticker, exch, exch_id):
    return {
        "company_id": cid,
        "company_name": name,
        "gurufocus_ticker": ticker,
        "gurufocus_exchange": {"exchange_code": exch, "exchange_id": exch_id},
    }


def _fake():
    return FakeSupabase(tables={"company": [
        _co(1, "Tencent Holdings", "00700", "HKSE", 1),
        _co(2, "AIA Group Ltd", "01299", "HKSE", 1),
        _co(3, "aia group ltd", "AAGIY", "NYSE", 2),  # ADR, same canonical name as #2
    ]})


class TestFindCanonicalMatch:
    def test_ticker_bucket_matches_zero_padded_hkse(self):
        # Punching in "700" must surface the stored HKSE:00700 row.
        matches = find_canonical_match(_fake(), name=None, ticker="700", exchange_code="HKSE")
        assert [m.company_id for m in matches] == [1]

    def test_name_bucket_matches_across_exchanges(self):
        # The HK primary + its US ADR share a canonical name → both surface.
        matches = find_canonical_match(_fake(), name="AIA Group Ltd", ticker=None, exchange_code=None)
        assert [m.company_id for m in matches] == [2, 3]

    def test_no_match_returns_empty(self):
        matches = find_canonical_match(_fake(), name="Nonexistent Co", ticker="ZZZ", exchange_code="NASDAQ")
        assert matches == []

    def test_row_in_both_buckets_deduped(self):
        # Tencent matches on ticker (bucket 1) and name (bucket 2) — once.
        matches = find_canonical_match(_fake(), name="Tencent Holdings", ticker="700", exchange_code="HKSE")
        assert [m.company_id for m in matches] == [1]

    def test_ticker_bucket_scoped_to_exchange(self):
        # Same canonical ticker on a different exchange must NOT match via
        # bucket 1 (different listing); only an exact-name hit would.
        matches = find_canonical_match(_fake(), name=None, ticker="01299", exchange_code="NYSE")
        assert matches == []
