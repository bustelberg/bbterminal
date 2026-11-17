"""Saved valuation reads and selected-portfolio database-only preparation."""
import asyncio

from fastapi import APIRouter, Path
from pydantic import BaseModel, Field
from pydantic import ConfigDict
from typing import Literal

from services import portfolio_valuation as service

router = APIRouter(tags=['airs'])


class ValuationPortfolio(BaseModel):
    id: int | None
    name: str
    account: str | None = None
    management_group: str | None = None
    unavailable_reason: str | None = None
    composition_date: str | None = None


class ValuationRow(BaseModel):
    isin: str | None = None
    name: str
    weight_pct: float
    status: str
    reason: str | None = None
    price: float | None = None
    price_date: str | None = None
    currency: str | None = None
    eps_ttm: float | None = None
    eps_date: str | None = None
    eps_metric_code: str | None = None
    current_pe: float | None = None
    median_pe: float | None = None
    estimated_value: float | None = None
    discount_pct: float | None = None
    observations: int = 0
    history_from: str | None = None
    history_to: str | None = None


class ValuationBundle(BaseModel):
    portfolio_id: int
    ready: bool
    stale: bool = False
    prepared_at: str | None = None
    calculation_date: str | None = None
    detail: str | None = None
    method: dict
    workbook: dict | None = None
    evidence: dict = Field(default_factory=dict)
    rows: list[ValuationRow]


class WorkbookParameters(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)
    years: int = Field(default=3, ge=1, le=30)
    safety_margin: float = Field(default=.2, ge=0, lt=1)
    bear_growth_delta: float = Field(default=-.05, ge=-2, le=2)
    bear_multiple_delta: float = Field(default=-.25, gt=-1, le=10)
    bull_growth_delta: float = Field(default=.05, ge=-2, le=2)
    bull_multiple_delta: float = Field(default=.25, gt=-1, le=10)


class WorkbookCompanyAssumption(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)
    growth: float | None = Field(default=None, gt=-1, le=10)
    fair_multiple: float | None = Field(default=None, gt=0)
    exit_multiple: float | None = Field(default=None, gt=0)
    risk_note: str = Field(default='', max_length=2000)
    review_required: bool = False
    basis_kind: Literal['forward_eps', 'manual'] = 'forward_eps'
    basis_label: str = Field(default='', max_length=200)
    manual_basis: float | None = None
    basis_currency: str | None = Field(default=None, max_length=10)
    basis_date: str | None = Field(default=None, max_length=100)
    basis_source: str | None = Field(default=None, max_length=1000)
    basis_fx: float | None = Field(default=None, gt=0)
    months_fy1: float | None = Field(default=None, ge=0, le=12)


class WorkbookAssumptions(BaseModel):
    model_config = ConfigDict(extra='forbid')
    parameters: WorkbookParameters
    companies: dict[str, WorkbookCompanyAssumption] = Field(default_factory=dict, max_length=500)


class WorkbookSaveResult(WorkbookAssumptions):
    portfolio_id: int
    updated_at: str


@router.get('/api/airs/valuation/portfolios', response_model=list[ValuationPortfolio])
async def valuation_portfolios():
    return await asyncio.to_thread(service.portfolios)


@router.get('/api/airs/valuation/portfolios/{portfolio_id}', response_model=ValuationBundle)
async def valuation_bundle(portfolio_id: int = Path(ge=1)):
    return await asyncio.to_thread(service.read, portfolio_id)


@router.post('/api/airs/valuation/portfolios/{portfolio_id}/refresh', response_model=ValuationBundle)
async def refresh_valuation(portfolio_id: int = Path(ge=1), force: bool = False):
    return await asyncio.to_thread(service.refresh, portfolio_id, force=force)


@router.get('/api/airs/valuation/portfolios/{portfolio_id}/workbook')
async def valuation_workbook(portfolio_id: int = Path(ge=1)):
    from services import valuation_workbook
    return await asyncio.to_thread(valuation_workbook.read, portfolio_id, include_membership=False)


@router.put('/api/airs/valuation/portfolios/{portfolio_id}/workbook', response_model=WorkbookSaveResult)
async def save_valuation_workbook(body: WorkbookAssumptions, portfolio_id: int = Path(ge=1)):
    from services import valuation_dashboard as dashboard
    return await asyncio.to_thread(dashboard.save_settings, portfolio_id, body.model_dump())


@router.post('/api/airs/valuation/portfolios/{portfolio_id}/rows/{isin}/refresh', response_model=ValuationBundle)
async def refresh_valuation_row(isin: str, portfolio_id: int = Path(ge=1)):
    from services import valuation_workbook
    def refresh():
        bundle = service.refresh_row(portfolio_id, isin)
        return {**bundle, 'workbook': valuation_workbook.read(portfolio_id, only_isin=isin, prepared_bundle=bundle)}
    return await asyncio.to_thread(refresh)


@router.get('/api/airs/valuation/portfolios/{portfolio_id}/workbook/memberships')
async def valuation_workbook_memberships(portfolio_id: int = Path(ge=1)):
    from services.portfolio_membership import read
    def memberships():
        service.read(portfolio_id)  # Enforce the same dashboard scope as the table.
        return read()
    return await asyncio.to_thread(memberships)


@router.post('/api/airs/valuation/holdings/refresh/job')
async def refresh_valuation_holdings(source: Literal['all', 'airs', 'leonteq'] = 'all'):
    import jobs
    from services.portfolio_membership import refresh_holdings
    job, reused = jobs.start('valuation.holdings.refresh', source, lambda ctx: refresh_holdings(ctx, source=source))
    return {'job_id': job.id, 'label': source, 'already_running': reused}


@router.get('/api/airs/valuation/portfolios/{portfolio_id}/dashboard', response_model=ValuationBundle)
async def valuation_dashboard(portfolio_id: int = Path(ge=1)):
    from services import valuation_dashboard as dashboard
    return await asyncio.to_thread(dashboard.read, portfolio_id)


@router.post('/api/airs/valuation/portfolios/{portfolio_id}/dashboard/refresh')
async def refresh_valuation_dashboard(portfolio_id: int = Path(ge=1)):
    from services import valuation_dashboard as dashboard
    return _valuation_stream(lambda emit, check: dashboard.refresh(portfolio_id, emit, check))


@router.post('/api/airs/valuation/prepare/refresh')
async def prepare_valuation_dashboard():
    from services import valuation_dashboard as dashboard
    from scheduler_runtime.context import LogContext

    def prepare(emit, check):
        class Context(LogContext):
            def check(self):
                check()

            @property
            def cancelled(self):
                from jobs import JobCancelled
                try:
                    check()
                    return False
                except JobCancelled:
                    return True

        context = Context(step=lambda done, total, message: emit({
            'type': 'progress', 'done': done, 'total': total, 'message': message}))
        summary = dashboard.prepare_all(context)
        emit({'type': 'complete', 'portfolios': dashboard.V.portfolios(),
              'failures': [str(reason) for reason in summary['errors'].values()]})
    return _valuation_stream(prepare)


def _valuation_stream(operation):
    import queue
    import threading
    from fastapi import HTTPException
    from fastapi.responses import StreamingResponse
    from jobs import JobCancelled
    from routers._sse import sse_event, sse_keepalive

    events = queue.Queue()
    cancelled = threading.Event()
    def check():
        if cancelled.is_set():
            raise JobCancelled()
    def run():
        try:
            from leonteq.progress import observe
            with observe(lambda english, _dutch: events.put({'type': 'progress', 'done': 0, 'total': 0, 'message': english})):
                operation(events.put, check)
        except JobCancelled:
            events.put({'type': 'cancelled'})
        except Exception as exc:
            message = exc.detail if isinstance(exc, HTTPException) and isinstance(exc.detail, str) else 'Portfolio refresh failed. Completed data was saved.'
            events.put({'type': 'error', 'message': message})
        finally:
            events.put(None)
    async def stream():
        threading.Thread(target=run, daemon=True).start()
        try:
            while True:
                try:
                    event = await asyncio.to_thread(events.get, True, 15)
                except queue.Empty:
                    yield sse_keepalive()
                    continue
                if event is None:
                    break
                yield sse_event(event)
        finally:
            cancelled.set()
    return StreamingResponse(stream(), media_type='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})
