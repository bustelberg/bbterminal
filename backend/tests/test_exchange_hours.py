"""Unit tests for `index_universe/exchange_hours.py` — the per-exchange
trading-hours reference + Amsterdam conversion behind the /timezone page."""
from __future__ import annotations

from index_universe.exchange_hours import (
    EXCHANGE_HOURS,
    exchange_amsterdam_hours,
    known_exchange_codes,
)


class TestAmsterdamConversion:
    def test_unknown_code_returns_none(self):
        assert exchange_amsterdam_hours("NOPE") is None

    def test_us_close_is_2200_amsterdam_both_seasons(self):
        # NYSE closes 16:00 ET. The US and the EU both observe DST (on slightly
        # different dates), so on the mid-Jan / mid-Jul reference dates the
        # Amsterdam-relative time is a steady 22:00 in both seasons.
        h = exchange_amsterdam_hours("NYSE")
        assert h["amsterdam_winter"]["close"] == {"time": "22:00", "day_offset": 0}
        assert h["amsterdam_summer"]["close"] == {"time": "22:00", "day_offset": 0}
        assert h["observes_dst"] is True

    def test_tokyo_close_shifts_with_amsterdam_dst(self):
        # Japan has no DST (UTC+9 year-round); Amsterdam is UTC+1 in winter and
        # UTC+2 in summer, so Tokyo's 15:30 close lands an hour later in Ams summer.
        h = exchange_amsterdam_hours("TSE")
        assert h["observes_dst"] is False
        assert h["amsterdam_winter"]["close"]["time"] == "07:30"
        assert h["amsterdam_summer"]["close"]["time"] == "08:30"
        # No calendar rollover for the afternoon close.
        assert h["amsterdam_winter"]["close"]["day_offset"] == 0

    def test_far_east_open_can_fall_on_previous_amsterdam_day(self):
        # New Zealand opens 10:00 local. In Amsterdam winter (Jan) NZ is on DST
        # (UTC+13) while Amsterdam is UTC+1 — a 12h gap — so the open is 22:00
        # the *previous* Amsterdam day.
        h = exchange_amsterdam_hours("NZSE")
        assert h["amsterdam_winter"]["open"]["day_offset"] == -1
        assert h["amsterdam_winter"]["open"]["time"] == "22:00"

    def test_lunch_and_trading_week_passthrough(self):
        hk = exchange_amsterdam_hours("HKSE")
        assert hk["lunch_start"] == "12:00" and hk["lunch_end"] == "13:00"
        sau = exchange_amsterdam_hours("SAU")
        assert sau["trading_week"] == "Sun–Thu"


class TestCoverage:
    def test_core_universe_exchanges_have_hours(self):
        # A representative spread across regions must be covered so the
        # /timezone table never silently drops a major exchange.
        for code in ["NYSE", "XAMS", "XSWX", "LSE", "TSE", "HKSE", "ASX", "XKRX", "SAU", "XTAE"]:
            assert exchange_amsterdam_hours(code) is not None, code

    def test_all_sessions_have_valid_timezone(self):
        # Every entry must carry an open<close pair and a resolvable tz (the
        # conversion constructs ZoneInfo from it, so a typo would raise here).
        assert len(known_exchange_codes()) == len(EXCHANGE_HOURS)
        for code in known_exchange_codes():
            h = exchange_amsterdam_hours(code)
            assert h["timezone"]
            assert h["local_open"] < h["local_close"]
