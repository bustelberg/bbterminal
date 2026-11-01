"""Manual scheduling returns a usable job for both launches and attachments."""
import asyncio

import pytest

import jobs
import scheduler
from routers import admin


@pytest.mark.parametrize("reused", [False, True])
def test_manual_endpoint_returns_registry_job(monkeypatch, reused):
    job = jobs.Job(id="manual-test", kind="scheduled.dashboard_market", label="Market")
    calls = []

    def start(kind, label, work):
        calls.append((kind, label))
        return job, reused

    monkeypatch.setattr(jobs, "start", start)
    monkeypatch.setattr(admin, "_require_admin", lambda _authorization: None)

    result = asyncio.run(admin.admin_run_scheduled_job("dashboard_market"))

    assert result == {"job_id": job.id, "label": job.label, "kind": job.kind}
    assert calls == [(job.kind, scheduler.BY_ID["dashboard_market"].label)]


def test_unknown_manual_job_returns_404(monkeypatch):
    monkeypatch.setattr(admin, "_require_admin", lambda _authorization: None)
    monkeypatch.setattr(jobs, "start", lambda *_args: pytest.fail("must not launch"))
    with pytest.raises(admin.HTTPException) as caught:
        asyncio.run(admin.admin_run_scheduled_job("unknown-job"))
    assert caught.value.status_code == 404
