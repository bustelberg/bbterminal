"""The bulk daily loaders must key their TTM series by DATE, not by fiscal-quarter label.

⚠⚠ THIS IS THE "SILENTLY SHORTER CHART" BUG, caught before it shipped. The two yield cards' daily
branches read `close_price` plus the quarterly numerator codes ONCE PER COMPANY PER CODE — 116
PostgREST round trips and 4.58s for a 19-holding book, directly below a comment reading "⚠ ONE BULK
READ PER METRIC, NOT ONE PER COMPANY". Replacing that with a bulk read looked like a one-liner,
because `metrics_by_company_bulk` already rolls a quarterly TTM for every company in one query.

It is the WRONG bulk twin. It calls `_ttm_by_period` WITHOUT `key="date"`, so its keys are period
LABELS (`2015-Q4`) where `_step_onto_dates` matches ISO dates (`2015-10-31`). Measured on the real
book: the SAME 43 periods with IDENTICAL values, and the daily series still came out 42 trading
days shorter for one holding and one day shorter for most of the rest.

⚠⚠ AND IT DOES NOT FAIL LOUDLY — IT FAILS LATE, WITH THE WRONG VALUE. `_step_onto_dates` compares
its keys as STRINGS, and `"2015-Q4"` sorts after `"2015-12-31"` and before `"2016-01-04"`. So a
label-keyed series does not carry nothing; it carries each window onto the days after the label
happens to sort, which is why the chart lost exactly the November-December stretch and then resumed
with a stale numerator. Nothing raised, no cell was empty, no coverage figure moved — a dividend
yield that begins in January instead of the previous November is not something a reader can catch
by looking.

So the bulk part is the READ; the arithmetic stays per-company and keyed by date. These tests pin
both halves of that.

Pure — no DB, no network.
"""
from __future__ import annotations

from routers.earnings import _daily_metrics_bulk, _step_onto_dates, _ttm_by_period

# Four quarters of a dividend, filed on their true period-end dates.
ROWS = [
    {"company_id": 1, "metric_code": "q__div", "target_date": "2015-01-31", "numeric_value": 0.10},
    {"company_id": 1, "metric_code": "q__div", "target_date": "2015-04-30", "numeric_value": 0.10},
    {"company_id": 1, "metric_code": "q__div", "target_date": "2015-07-31", "numeric_value": 0.12},
    {"company_id": 1, "metric_code": "q__div", "target_date": "2015-10-31", "numeric_value": 0.12},
    {"company_id": 1, "metric_code": "q__div", "target_date": "2016-01-31", "numeric_value": 0.15},
]
TRADING_DAYS = ["2015-10-30", "2015-11-02", "2015-12-31", "2016-01-04", "2016-02-01"]


def test_date_keyed_ttm_reaches_the_trading_days_between_period_ends():
    """The whole point of a daily yield: the numerator is flat between fiscal period ends."""
    ttm = _ttm_by_period(ROWS, "sum", key="date")
    assert set(ttm) == {"2015-10-31", "2016-01-31"}, "TTM needs four quarters before it reports"

    stepped = _step_onto_dates(ttm, TRADING_DAYS)
    # ⚠ 2015-10-30 is BEFORE the first complete TTM window — no value, not a zero.
    assert "2015-10-30" not in stepped
    # The November and December days carry the 2015-10-31 window, flat.
    assert stepped["2015-11-02"] == stepped["2015-12-31"] == 0.44
    # And they step up only once the next period end is reached.
    assert stepped["2016-02-01"] == 0.49


def test_label_keyed_ttm_starts_late_and_then_reports_a_stale_window():
    """The same values under period LABELS produce a plausible, wrong series.

    ⚠ THIS IS WHY THE SWAP LOOKED SAFE: the two dicts have the same length and the same numbers.
    Only the keys differ — and `_step_onto_dates` matches on keys, as strings.
    """
    by_label = _ttm_by_period(ROWS, "sum")
    by_date = _ttm_by_period(ROWS, "sum", key="date")
    assert len(by_label) == len(by_date)
    assert sorted(by_label.values()) == sorted(by_date.values())
    assert set(by_label) & set(by_date) == set(), "labels are not dates — that is the trap"

    wrong = _step_onto_dates(by_label, TRADING_DAYS)
    # ⚠ NOT EMPTY, WHICH IS THE WHOLE DANGER. "2015-Q4" string-sorts after "2015-12-31", so the
    # November and December trading days are dropped...
    assert "2015-11-02" not in wrong and "2015-12-31" not in wrong
    # ...and February then reports the PRIOR window, because "2016-Q1" sorts after "2016-02-01".
    assert wrong["2016-02-01"] == 0.44
    assert _step_onto_dates(by_date, TRADING_DAYS)["2016-02-01"] == 0.49


def test_bulk_steps_each_company_onto_its_own_trading_days(monkeypatch):
    """A holiday-closed listing must not inherit the other's calendar.

    ⚠ A SHARED DATE AXIS WOULD CARRY A STALE VALUE ONTO A DAY THE LISTING DID NOT TRADE — a price
    that never printed, under a yield that looks perfectly ordinary.
    """
    monkeypatch.setattr("routers.earnings.rows_by_metric",
                        lambda ids, metrics, cadence: {"div_ps": {1: ROWS, 2: ROWS}})
    closes = {
        1: {"2015-11-02": 10.0, "2015-12-31": 11.0},
        2: {"2015-12-31": 20.0},                      # closed on 2 Nov
    }
    out = _daily_metrics_bulk([1, 2], ("div_ps",), closes)
    assert set(out["div_ps"][1]) == {"2015-11-02", "2015-12-31"}
    assert set(out["div_ps"][2]) == {"2015-12-31"}


def test_bulk_skips_companies_with_no_closes_rather_than_inventing_an_axis(monkeypatch):
    """No price series means no daily yield — not a numerator hanging on nothing."""
    monkeypatch.setattr("routers.earnings.rows_by_metric",
                        lambda ids, metrics, cadence: {"div_ps": {1: ROWS, 2: ROWS}})
    out = _daily_metrics_bulk([1, 2], ("div_ps",), {1: {"2015-12-31": 10.0}})
    assert set(out["div_ps"]) == {1}


def test_unknown_metric_yields_an_empty_map_not_a_crash(monkeypatch):
    """A metric with no TTM rule has no daily numerator; the card draws nothing for it."""
    monkeypatch.setattr("routers.earnings.rows_by_metric",
                        lambda ids, metrics, cadence: {})
    assert _daily_metrics_bulk([1], ("not_a_metric",), {1: {"2015-12-31": 1.0}}) == {
        "not_a_metric": {}}
