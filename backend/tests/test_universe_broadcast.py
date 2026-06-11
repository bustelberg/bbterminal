"""`broadcast_constant` — single-snapshot universes are constant through time.

A frozen "fixed basket" stores its constituents under one month; the loader
broadcasts that one snapshot across the backtest window so every rebalance
sees the set. Multi-month universes (live ACWI reconstruction) keep their
point-in-time membership untouched.
"""
from __future__ import annotations

from datetime import date

from routers.momentum.backtest_stream.universe_loader import broadcast_constant


def test_single_month_broadcasts_across_range():
    me = {"2020-03": {1: "Tech", 2: "Health"}}
    out = broadcast_constant(me, date(2019, 1, 1), date(2019, 4, 15))
    assert sorted(out.keys()) == ["2019-01", "2019-02", "2019-03", "2019-04"]
    assert all(out[k] == {1: "Tech", 2: "Health"} for k in out)
    # Same inner dict aliased across months (read-only downstream).
    assert out["2019-01"] is out["2019-04"]


def test_multi_month_returned_unchanged():
    me = {"2020-01": {1: "Tech"}, "2020-02": {2: "Health"}}
    assert broadcast_constant(me, date(2019, 1, 1), date(2021, 1, 1)) is me


def test_none_empty_and_single_empty_are_noops():
    assert broadcast_constant(None, date(2019, 1, 1), date(2020, 1, 1)) is None
    empty: dict = {}
    assert broadcast_constant(empty, date(2019, 1, 1), date(2020, 1, 1)) is empty
    single_empty = {"2020-03": {}}
    assert broadcast_constant(single_empty, date(2019, 1, 1), date(2020, 1, 1)) is single_empty


def test_spans_multiple_years():
    me = {"2020-03": {1: "Tech"}}
    out = broadcast_constant(me, date(2019, 11, 1), date(2020, 2, 1))
    assert sorted(out.keys()) == ["2019-11", "2019-12", "2020-01", "2020-02"]
