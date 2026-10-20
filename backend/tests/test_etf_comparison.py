from datetime import date, datetime, timezone

import pytest

from routers._etf_comparison import adjusted_closes, compare


def history(days, prices, currency="EUR"):
    return {"meta": {"currency": currency},
            "timestamp": [int(datetime.fromisoformat(d).replace(tzinfo=timezone.utc).timestamp()) for d in days],
            "indicators": {"adjclose": [{"adjclose": prices}], "quote": [{"close": [999] * len(days)}]}}


def test_independent_start_date_and_shared_calendar():
    etf = history(["2020-01-02", "2020-01-03", "2020-01-06", "2020-01-07"], [50, 100, 110, 120])
    bench = history(["2020-01-02", "2020-01-03", "2020-01-07"], [100, 200, 220])
    result = compare("SPY", "ACWI", date(2020, 1, 3), etf, bench)
    assert result.start == "2020-01-03"
    assert result.end == "2020-01-07"
    assert len(result.points) == 2
    assert result.points[0].etf_pct == result.points[0].benchmark_pct == 0
    assert result.total_return_pct == pytest.approx(20)
    assert result.benchmark_return_pct == pytest.approx(10)
    assert result.excess_pp == pytest.approx(10)
    older = compare("SPY", "ACWI", date(2020, 1, 2), etf, bench)
    assert older.total_return_pct == pytest.approx(140)
    assert older.benchmark_return_pct == pytest.approx(120)


def test_start_before_listing_moves_both_baselines_to_first_shared_close():
    result = compare("ETF", "ACWI", date(2019, 12, 1),
                     history(["2020-01-06", "2020-01-07"], [10, 11], "USD"),
                     history(["2020-01-03", "2020-01-06", "2020-01-07"], [10, 20, 22], "EUR"),
                     {"USD": {"2020-01-03": 1.2}})
    assert result.start == "2020-01-06"
    assert result.benchmark_return_pct == pytest.approx(10)
    assert result.currency == "EUR"
    assert result.benchmark_currency == "EUR"


def test_invalid_prices_are_omitted_and_raw_close_is_not_a_fallback():
    payload = history(["2020-01-02", "2020-01-03", "2020-01-06", "2020-01-07"], [0, None, float("nan"), 10])
    assert adjusted_closes(payload) == {"2020-01-07": 10}
    payload["indicators"].pop("adjclose")
    assert adjusted_closes(payload) == {}


def test_refuses_insufficient_overlap():
    with pytest.raises(ValueError, match="shared"):
        compare("ETF", "ACWI", date(2020, 1, 1), history(["2020-01-02"], [10]), history(["2020-01-02"], [10]))


def test_live_day_is_excluded(monkeypatch):
    from asset_pipeline import yahoo
    monkeypatch.setattr(yahoo, "is_closed_bar", lambda timestamp: yahoo.utc_dt(timestamp).date() < date(2020, 1, 3))
    assert adjusted_closes(history(["2020-01-02", "2020-01-03"], [10, 99])) == {"2020-01-02": 10}


def test_fx_changes_both_return_paths_before_rebasing():
    days = ["2020-01-02", "2020-01-03", "2020-01-06"]
    result = compare("ETF", "ACWI", date(2020, 1, 2),
                     history(days, [100, 100, 120], "USD"),
                     history(days, [200, 220, 240], "GBP"),
                     {"USD": {days[0]: 2, days[1]: 2.5, days[2]: 3},
                      "GBP": {days[0]: 1, days[1]: 1, days[2]: 2}})
    assert result.currency == result.benchmark_currency == "EUR"
    assert result.points[0].etf_pct == result.points[0].benchmark_pct == 0
    assert result.points[1].etf_pct == pytest.approx(-20)
    assert result.total_return_pct == pytest.approx(-20)  # EUR 50 -> 40, not USD +20%
    assert result.benchmark_return_pct == pytest.approx(-40)  # EUR 200 -> 120
    assert result.excess_pp == pytest.approx(20)


@pytest.mark.parametrize("currency", ["GBp", "GBX"])
def test_minor_quote_units_and_previous_only_fx(currency):
    days = ["2020-01-02", "2020-01-03"]
    result = compare("ETF", "ACWI", date(2020, 1, 2), history(days, [10000, 11000], currency),
                     history(days, [100, 110]), {"GBP": {"2020-01-01": 1, "2020-01-03": 2}})
    assert result.total_return_pct == pytest.approx(-45)
    assert result.benchmark_return_pct == pytest.approx(10)


@pytest.mark.parametrize("rates", [{}, {"USD": {"2020-01-06": 2}}, {"USD": {"2020-01-01": 0}}])
def test_missing_invalid_or_future_only_fx_never_falls_back_to_native(rates):
    days = ["2020-01-02", "2020-01-03"]
    with pytest.raises(ValueError, match="historical"):
        compare("ETF", "ACWI", date(2020, 1, 2), history(days, [100, 110], "USD"),
                history(days, [100, 110]), rates)


def test_unknown_quote_currency_is_not_assumed_to_be_eur():
    days = ["2020-01-02", "2020-01-03"]
    with pytest.raises(ValueError, match="quote currency"):
        compare("ETF", "ACWI", date(2020, 1, 2), history(days, [100, 110], None),
                history(days, [100, 110]))
