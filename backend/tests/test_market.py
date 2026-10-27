"""Market highs and current quotes must use the same unadjusted index basis."""
from datetime import datetime

import pytest

from routers import market


def stamp(day):
    return datetime.fromisoformat(f"{day}T20:00:00+00:00").timestamp()


def history():
    return {"timestamp": [stamp("2026-01-02"), stamp("2026-01-05")],
            "indicators": {"quote": [{"close": [95, 90], "high": [100, 96]}]},
            "meta": {"regularMarketPrice": 92, "regularMarketTime": stamp("2026-01-05") + 60}}


def test_intraday_peak_and_latest_quote():
    result = market.build_snapshot(history(), stamp("2026-01-06"))
    assert result.peak == 100
    assert result.peak_date == "2026-01-02"
    assert result.latest == 92
    assert result.from_peak_pct == pytest.approx(-8)
    assert result.points[-1].close == 92


def test_older_quote_does_not_replace_newer_close():
    raw = history()
    raw["meta"]["regularMarketTime"] = stamp("2026-01-02")
    result = market.build_snapshot(raw, stamp("2026-01-06"))
    assert result.latest == 90


def test_latest_quote_can_establish_new_peak():
    raw = history()
    raw["meta"]["regularMarketPrice"] = 105
    result = market.build_snapshot(raw, stamp("2026-01-06"))
    assert result.peak == 105
    assert result.peak_date == "2026-01-05"
    assert result.from_peak_pct == 0


def test_missing_data_is_not_zero_drawdown():
    with pytest.raises(ValueError):
        market.build_snapshot({}, stamp("2026-01-06"))


def test_cache_avoids_repeat_vendor_reads(monkeypatch):
    monkeypatch.setattr(market, "_cached", {})
    monkeypatch.setattr(market.time, "time", lambda: stamp("2026-01-06"))
    calls = []
    monkeypatch.setattr(market, "chart_window", lambda *args: calls.append(args) or history())
    first = market.nasdaq()
    assert market.nasdaq() is first
    assert len(calls) == 1


def test_acwi_has_its_own_symbol_and_cache(monkeypatch):
    monkeypatch.setattr(market, "_cached", {})
    monkeypatch.setattr(market.time, "time", lambda: stamp("2026-01-06"))
    calls = []
    monkeypatch.setattr(market, "chart_window", lambda *args: calls.append(args) or history())
    nasdaq = market.nasdaq()
    acwi = market.acwi()
    assert nasdaq.symbol == "^IXIC"
    assert acwi.symbol == "ACWI"
    assert acwi.name == "iShares MSCI ACWI ETF"
    assert market.acwi() is acwi
    assert [args[0] for args in calls] == ["^IXIC", "ACWI"]
