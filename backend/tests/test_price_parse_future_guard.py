"""Regression tests for `_parse_price_series`'s future-date guard.

A close price can never be dated after today. GuruFocus occasionally returns a
stray/corrupt tick with a future date; storing it poisoned the momentum
engine's `latest_data_date` anchor (pinning a rebalance to an upcoming grid
date) and the ETF overlay's entry price — the SPMO +277% incident. The parser
is the single choke point every ingest path flows through, so it drops
future-dated rows for all of them.
"""
from __future__ import annotations

from datetime import date, timedelta

from ingest.prices import _parse_price_series


def test_drops_future_dated_rows():
    today = date.today()
    future = (today + timedelta(days=5)).isoformat()
    yesterday = (today - timedelta(days=1)).isoformat()
    data = [
        [yesterday, 150.83],
        [today.isoformat(), 151.0],
        [future, 40.18],  # corrupt future tick — must be dropped
    ]
    parsed = _parse_price_series(data)
    dates = [d for d, _ in parsed]
    assert all(d <= today for d in dates)
    assert date.fromisoformat(future) not in dates
    # The real (past + today) rows survive.
    assert (date.fromisoformat(yesterday), 150.83) in parsed
    assert (today, 151.0) in parsed


def test_today_is_kept():
    today = date.today()
    parsed = _parse_price_series([[today.isoformat(), 100.0]])
    assert parsed == [(today, 100.0)]


def test_normal_history_unaffected():
    data = [["2020-01-02", 10.0], ["2021-06-15", 20.0], ["2023-12-29", 30.0]]
    parsed = _parse_price_series(data)
    assert parsed == [
        (date(2020, 1, 2), 10.0),
        (date(2021, 6, 15), 20.0),
        (date(2023, 12, 29), 30.0),
    ]
