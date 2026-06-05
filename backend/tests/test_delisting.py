"""Stale-price delisting: the `trading_days_behind` staleness measure + the
`sweep_delisted_companies` DB sweep (against the in-memory fake Supabase)."""
from __future__ import annotations

from datetime import date

import ingest.delisting as delisting_mod
from ingest.delisting import sweep_delisted_companies
from ingest.staleness import trading_days_behind, trading_days_between

from tests._fake_supabase import FakeSupabase


class TestTradingDaysBehind:
    def test_current_close_is_zero(self):
        # Fri 2026-06-05 → most recent expected close is Thu 2026-06-04.
        assert trading_days_behind(date(2026, 6, 4), today=date(2026, 6, 5)) == 0

    def test_counts_weekdays_only(self):
        # Tue 2026-06-02 vs expected Thu 06-04 → Wed + Thu = 2 trading days.
        assert trading_days_behind(date(2026, 6, 2), today=date(2026, 6, 5)) == 2
        # Over a weekend: Fri 05-29 vs expected Thu 06-04 → Mon..Thu = 4.
        assert trading_days_behind(date(2026, 5, 29), today=date(2026, 6, 5)) == 4

    def test_long_stale_exceeds_threshold(self):
        assert trading_days_behind(date(2026, 4, 1), today=date(2026, 6, 5)) > 15


class TestTradingDaysBetween:
    def test_zero_when_end_not_after_start(self):
        assert trading_days_between(date(2026, 6, 4), date(2026, 6, 4)) == 0
        assert trading_days_between(date(2026, 6, 4), date(2026, 6, 1)) == 0

    def test_counts_weekdays_in_half_open_range(self):
        # (Tue 06-02, Thu 06-04] → Wed + Thu = 2.
        assert trading_days_between(date(2026, 6, 2), date(2026, 6, 4)) == 2
        # Across a weekend stays a trading-day count.
        assert trading_days_between(date(2026, 4, 1), date(2026, 6, 2)) > 15


def _co(cid, ticker, exch, delisted=None, oos=None):
    return {
        "company_id": cid,
        "gurufocus_ticker": ticker,
        "delisted_at": delisted,
        "out_of_scope_at": oos,
        "gurufocus_exchange": {"exchange_code": exch},
    }


class TestSweepDelistedCompanies:
    def test_marks_only_stale_companies_with_data(self, monkeypatch):
        company = [
            _co(1, "FRESH", "NASDAQ"),                       # fresh → keep
            _co(2, "STALE", "NASDAQ"),                       # stale > 15 td → delist
            _co(3, "NODATA", "BMV"),                         # no close data → skip
            _co(4, "GONE", "NYSE", delisted="2026-01-01"),   # already delisted → excluded
            _co(5, "OOS", "MEX", oos="2026-01-01"),          # out of scope → excluded
        ]
        fake = FakeSupabase(tables={"company": company})
        # DB-only latest-close dates (cid 3 absent → no data).
        monkeypatch.setattr(
            delisting_mod, "_default_supabase", fake, raising=False,
        )
        # Global freshest close = 2026-06-02; row 2 is ~44 td behind it.
        monkeypatch.setattr(
            "momentum.data._pg.load_all_latest_close_dates_via_copy",
            lambda: {1: "2026-06-02", 2: "2026-04-01"},
        )

        res = sweep_delisted_companies(fake, threshold_trading_days=15)

        assert res.skipped_no_pg is False
        assert res.checked == 3          # rows 1,2,3 (4 + 5 filtered out)
        assert res.with_data == 2        # rows 1,2 have a latest close
        assert res.newly_delisted == 1   # only the stale row 2
        # Row 2 now carries delisted_at; the others don't.
        by_id = {r["company_id"]: r for r in fake.tables["company"]}
        assert by_id[2]["delisted_at"] is not None
        assert by_id[1]["delisted_at"] is None
        assert by_id[3]["delisted_at"] is None

    def test_outage_no_false_positives(self, monkeypatch):
        # GuruFocus is blocked: EVERY company's close stalls together (all old
        # relative to the calendar, but within 1 td of each other). Anchoring
        # to the global freshest close → nothing is marked delisted.
        company = [_co(1, "A", "NASDAQ"), _co(2, "B", "LSE")]
        fake = FakeSupabase(tables={"company": company})
        monkeypatch.setattr(
            "momentum.data._pg.load_all_latest_close_dates_via_copy",
            lambda: {1: "2026-04-01", 2: "2026-04-02"},
        )
        res = sweep_delisted_companies(fake, threshold_trading_days=15)
        assert res.with_data == 2
        assert res.newly_delisted == 0

    def test_skips_when_no_direct_postgres(self, monkeypatch):
        fake = FakeSupabase(tables={"company": [_co(1, "X", "NASDAQ")]})
        monkeypatch.setattr(
            "momentum.data._pg.load_all_latest_close_dates_via_copy", lambda: None,
        )
        res = sweep_delisted_companies(fake)
        assert res.skipped_no_pg is True
        assert res.newly_delisted == 0
