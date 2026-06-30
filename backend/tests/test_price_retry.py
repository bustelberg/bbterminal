"""Unit tests for the stale-held-price 3h retry scheduling
(`scheduler.maybe_schedule_price_retry`).

The retry re-runs the held-price refresh a few hours after a price-update
that left GuruFocus prices behind (publish lag), bounded per UTC day so a
genuinely unpublishable name can't loop forever. These tests pin the
schedule/no-op decision + the daily budget cap without touching APScheduler
or the DB (a fake scheduler records `add_job` ids; `_held_prices_stale` is
stubbed).
"""
from __future__ import annotations

import ingest.phases.prices as prices
import scheduler


class _FakeSched:
    def __init__(self) -> None:
        self.jobs: list[str] = []

    def add_job(self, _fn, _trigger, **kw) -> None:
        self.jobs.append(kw.get("id"))


def _setup(monkeypatch, *, stale: bool, max_per_day: int = 3) -> _FakeSched:
    fake = _FakeSched()
    monkeypatch.setattr(scheduler, "_scheduler", fake)
    # `maybe_schedule_price_retry` lazy-imports this from ingest.phases.prices,
    # so patch it at the source module.
    monkeypatch.setattr(prices, "held_prices_lagging", lambda: stale)
    monkeypatch.setattr(scheduler, "_price_retry_counts", {})
    monkeypatch.setattr(scheduler, "_PRICE_RETRY_MAX_PER_DAY", max_per_day)
    return fake


def test_schedules_retry_when_held_prices_stale(monkeypatch):
    fake = _setup(monkeypatch, stale=True)
    scheduler.maybe_schedule_price_retry(reason="test")
    assert fake.jobs == ["price_update_retry"]


def test_noop_when_prices_fresh(monkeypatch):
    fake = _setup(monkeypatch, stale=False)
    scheduler.maybe_schedule_price_retry()
    assert fake.jobs == []


def test_budget_caps_retries_per_day(monkeypatch):
    fake = _setup(monkeypatch, stale=True, max_per_day=3)
    for _ in range(6):
        scheduler.maybe_schedule_price_retry()
    # Only 3 scheduled despite 6 stale completions — the rest wait for the
    # next daily tick.
    assert len(fake.jobs) == 3


def test_disabled_budget_never_schedules(monkeypatch):
    fake = _setup(monkeypatch, stale=True, max_per_day=0)
    scheduler.maybe_schedule_price_retry()
    assert fake.jobs == []


def test_noop_and_safe_when_scheduler_disabled(monkeypatch):
    monkeypatch.setattr(scheduler, "_scheduler", None)
    monkeypatch.setattr(prices, "held_prices_lagging", lambda: True)
    # Must not raise even with no scheduler running (CI / DISABLE_SCHEDULER).
    scheduler.maybe_schedule_price_retry()


def test_lag_probe_failure_is_swallowed(monkeypatch):
    fake = _FakeSched()
    monkeypatch.setattr(scheduler, "_scheduler", fake)

    def _boom() -> bool:
        raise RuntimeError("DB down")

    monkeypatch.setattr(prices, "held_prices_lagging", _boom)
    monkeypatch.setattr(scheduler, "_price_retry_counts", {})
    # Best-effort: a probe failure neither raises nor schedules.
    scheduler.maybe_schedule_price_retry()
    assert fake.jobs == []


# ── held_prices_lagging — the publish-lag staleness logic ───────────────
# 2026-06-29 = Monday, 2026-06-26 = the prior Friday.
_MON = "2026-06-29"
_FRI = "2026-06-26"


def _stub_lag(monkeypatch, *, held, latest, excluded=frozenset()):
    monkeypatch.setattr(prices, "_collect_held_company_ids", lambda: set(held))
    monkeypatch.setattr(prices, "_latest_close_dates_all", lambda: dict(latest))
    monkeypatch.setattr(prices, "_price_status_excluded_ids", lambda _cids: set(excluded))


def test_lagging_when_a_held_name_is_behind_the_pack(monkeypatch):
    # Pack's freshest close is Monday; held cid 3 is still at Friday → lagging.
    _stub_lag(monkeypatch, held={1, 2, 3},
              latest={1: _MON, 2: _MON, 3: _FRI, 99: _MON})
    assert prices.held_prices_lagging() is True


def test_not_lagging_when_all_held_at_global_latest(monkeypatch):
    # Every held name has the freshest close — the normal post-refresh state.
    _stub_lag(monkeypatch, held={1, 2}, latest={1: _MON, 2: _MON, 99: _MON})
    assert prices.held_prices_lagging() is False


def test_excluded_held_name_does_not_count_as_lagging(monkeypatch):
    # cid 3 is behind but marked illiquid/delisted/out-of-scope → ignored.
    _stub_lag(monkeypatch, held={1, 2, 3},
              latest={1: _MON, 2: _MON, 3: _FRI}, excluded={3})
    assert prices.held_prices_lagging() is False


def test_active_held_name_with_no_price_is_lagging(monkeypatch):
    # cid 3 active but has no close data at all → must refetch.
    _stub_lag(monkeypatch, held={1, 2, 3}, latest={1: _MON, 2: _MON})
    assert prices.held_prices_lagging() is True


def test_no_held_companies_is_not_lagging(monkeypatch):
    _stub_lag(monkeypatch, held=set(), latest={99: _MON})
    assert prices.held_prices_lagging() is False
