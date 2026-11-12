"""Daily fundamentals must use outcomes, not parse prose or count selected IDs as filled."""
import pytest

import dashboard_daily as daily
import scheduler
from scheduler_jobs import market as market_jobs
from jobs import UserFacingJobError


@pytest.fixture
def scheduled_fill(monkeypatch):
    from ingest import api_usage
    from routers import _fundamental_backfill as backfill
    monkeypatch.setattr(api_usage, "remaining_budget", lambda _: {"usa": market_jobs._FUNDAMENTALS_REGION_FLOOR + 100})
    monkeypatch.setattr(daily, "held_company_ids", lambda: [])
    monkeypatch.setattr(market_jobs, "_FUNDAMENTAL_INDICES", ("ACWI",))
    monkeypatch.setattr(scheduler, "_fundamental_company_ids", lambda _: [(1, "NYSE"), (2, "NYSE")])
    monkeypatch.setattr(backfill, "smart_flags_bulk", lambda ids: {cid: {"need_fin": True} for cid in ids})


def test_unavailable_receipt_does_not_fail_daily_job(scheduled_fill, monkeypatch):
    from routers import _fundamental_fill as fill
    def run(ctx, label, ids, *, feeds, receipt, best_effort):
        assert best_effort
        receipt.update(loaded=0, failed=0, unavailable=2)
        return "ACWI — unavailable: outside our GuruFocus subscription"
    monkeypatch.setattr(fill, "fill_company_ids", run)
    _, summary = scheduler._body_benchmark_fundamentals()
    assert summary["filled"] == summary["failed"] == 0
    assert summary["unavailable"] == 2


@pytest.mark.parametrize("raise_error", [False, True])
def test_vendor_failures_keep_actual_counts_and_cause(scheduled_fill, monkeypatch, raise_error):
    from routers import _fundamental_fill as fill
    def run(ctx, label, ids, *, feeds, receipt, best_effort):
        receipt.update(loaded=0 if raise_error else 1, failed=1, unavailable=0,
                       failure_samples=["Company: missing statements"])
        if raise_error:
            raise UserFacingJobError("GuruFocus did not provide financial statements")
        return "1 loaded, 1 failed: missing statements"
    monkeypatch.setattr(fill, "fill_company_ids", run)
    with pytest.raises(daily.IncompleteRefresh) as caught:
        scheduler._body_benchmark_fundamentals()
    summary = caught.value.summary
    assert summary["filled"] == (0 if raise_error else 1)
    assert summary["failed"] == 1
    assert summary["results"]["ACWI"]["failure_samples"] == ["Company: missing statements"]
    assert "statements" in summary["errors"][0]


def test_unsubscribed_listings_do_not_use_quota_reserve(scheduled_fill, monkeypatch):
    from ingest import api_usage
    from routers import _fundamental_fill as fill
    monkeypatch.setattr(api_usage, "remaining_budget", lambda _: {"europe": market_jobs._FUNDAMENTALS_REGION_FLOOR + 2})
    monkeypatch.setattr(scheduler, "_fundamental_company_ids", lambda _: [(1, "LSE"), (2, "XAMS")])
    def run(ctx, label, ids, *, feeds, receipt, best_effort):
        assert ids == [1, 2]
        receipt.update(loaded=1, failed=0, unavailable=1)
        return "1 loaded, 1 unavailable"
    monkeypatch.setattr(fill, "fill_company_ids", run)
    _, summary = scheduler._body_benchmark_fundamentals()
    assert summary["deferred"] == 0
    assert summary["filled"] == 1
