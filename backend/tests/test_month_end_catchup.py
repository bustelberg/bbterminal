"""Unit tests for the end-of-month full-refresh window
(`scheduler._fire_month_end_refresh`).

The full price refresh runs on a daily 12:00-UTC tick but only during the last
`_MONTH_END_WINDOW_DAYS`+1 days of the month, and only until the window has a
successful run — so a single missed/failed day retries the next day instead of
losing the whole month, without any startup / every-deploy auto-repricing. These
tests pin the fire/no-op decision without touching APScheduler or the DB (the
ingest_run guard lookup + the `_fire_job` dispatch are stubbed).
"""
from __future__ import annotations

import calendar
from datetime import date, datetime, timezone

import deps
import scheduler


class _FakeQuery:
    def __init__(self, data: list[dict]):
        self._data = data

    def select(self, *a, **k): return self
    def in_(self, *a, **k): return self
    def eq(self, *a, **k): return self
    def gte(self, *a, **k): return self
    def order(self, *a, **k): return self
    def limit(self, *a, **k): return self

    def execute(self):
        return type("R", (), {"data": self._data})()


class _FakeSupabase:
    def __init__(self, data: list[dict]):
        self._data = data

    def table(self, _name: str) -> _FakeQuery:
        return _FakeQuery(self._data)


def _freeze_today(monkeypatch, d: date) -> None:
    class _DT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(d.year, d.month, d.day, 12, 0, tzinfo=tz or timezone.utc)
    monkeypatch.setattr(scheduler, "datetime", _DT)


def _setup(monkeypatch, *, today: date, already_ran: bool, window_days: int = 2) -> list[str]:
    fired: list[str] = []
    _freeze_today(monkeypatch, today)
    monkeypatch.setattr(scheduler, "_MONTH_END_WINDOW_DAYS", window_days)
    monkeypatch.setattr(deps, "supabase", _FakeSupabase([{"run_id": 1}] if already_ran else []))
    monkeypatch.setattr(scheduler, "_fire_job", lambda job: fired.append(job))
    return fired


# July 2026 has 31 days → default window (2) = the 29th–31st.
def test_fires_on_first_window_day(monkeypatch):
    fired = _setup(monkeypatch, today=date(2026, 7, 29), already_ran=False)
    scheduler._fire_month_end_refresh()
    assert fired == ["full_price_refresh"]


def test_noop_before_window(monkeypatch):
    # 28th is one day before the window opens.
    fired = _setup(monkeypatch, today=date(2026, 7, 28), already_ran=False)
    scheduler._fire_month_end_refresh()
    assert fired == []


def test_retries_next_window_day_when_not_yet_run(monkeypatch):
    # 30th, still no successful run this window → retry fires.
    fired = _setup(monkeypatch, today=date(2026, 7, 30), already_ran=False)
    scheduler._fire_month_end_refresh()
    assert fired == ["full_price_refresh"]


def test_noop_when_already_ran_this_window(monkeypatch):
    # 30th but the 29th already succeeded → no second run.
    fired = _setup(monkeypatch, today=date(2026, 7, 30), already_ran=True)
    scheduler._fire_month_end_refresh()
    assert fired == []


def test_fires_on_last_day(monkeypatch):
    fired = _setup(monkeypatch, today=date(2026, 7, 31), already_ran=False)
    scheduler._fire_month_end_refresh()
    assert fired == ["full_price_refresh"]


def test_window_start_handles_short_month(monkeypatch):
    # February 2026 has 28 days → window (2) opens on the 26th.
    assert calendar.monthrange(2026, 2)[1] == 28
    fired = _setup(monkeypatch, today=date(2026, 2, 26), already_ran=False)
    scheduler._fire_month_end_refresh()
    assert fired == ["full_price_refresh"]
    fired2 = _setup(monkeypatch, today=date(2026, 2, 25), already_ran=False)
    scheduler._fire_month_end_refresh()
    assert fired2 == []
