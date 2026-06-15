"""Unit tests for the ISIN-based dedupe helpers (same security = same ISIN).

Pins the two subtle pure functions behind `dedupe_by_isin`:
  - `_name_root`: same-company matching that ignores ADR/ADS + punctuation, so
    share-class / ADR siblings collapse but unrelated issuers don't.
  - `_pick_winner_isin`: the survivor preference — Leonteq holding first, then a
    USA/EU listing, then viability / exchange priority / lowest id.
"""
from __future__ import annotations

from ingest.dedupe import CompanyRow, _name_root, _pick_winner_isin


class TestNameRoot:
    def test_adr_suffix_ignored(self):
        assert _name_root("KE Holdings Inc - ADR") == _name_root("KE HOLDINGS INC")

    def test_punctuation_and_case_ignored(self):
        assert _name_root("Chocoladefabriken Lindt & Spruengli AG") == \
               _name_root("chocoladefabriken lindt  spruengli ag")

    def test_unrelated_issuers_differ(self):
        assert _name_root("Apple Inc") != _name_root("Apple Hospitality REIT")


def _row(cid, ticker, exch):
    return CompanyRow(company_id=cid, company_name="X", gurufocus_ticker=ticker,
                      exchange_code=exch, exchange_id=None)


class TestPickWinnerIsin:
    def test_leonteq_holding_wins(self):
        nyse = _row(86, "LIN", "NYSE")      # LongEquity only
        nasdaq = _row(3248, "LIN", "NASDAQ")  # Leonteq
        winner = _pick_winner_isin([nyse, nasdaq], leonteq_ids={3248})
        assert winner.company_id == 3248

    def test_usa_eu_beats_other_when_no_leonteq(self):
        hk = _row(4075, "02423", "HKSE")
        nyse = _row(5302, "BEKE", "NYSE")
        winner = _pick_winner_isin([hk, nyse], leonteq_ids=set())
        assert winner.company_id == 5302  # NYSE (USA) over HKSE

    def test_deterministic_lowest_id_on_full_tie(self):
        a = _row(2111, "PTC", "NYSE")
        b = _row(3862, "PTC", "NASDAQ")
        winner = _pick_winner_isin([a, b], leonteq_ids=set())
        assert winner.company_id == 2111  # both USA + viable → lowest id
