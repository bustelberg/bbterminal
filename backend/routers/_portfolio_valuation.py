"""Saved valuation reads and selected-portfolio database-only preparation."""
import asyncio

from fastapi import APIRouter, Path
from pydantic import BaseModel, Field

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
    evidence: dict = Field(default_factory=dict)
    rows: list[ValuationRow]


@router.get('/api/airs/valuation/portfolios', response_model=list[ValuationPortfolio])
async def valuation_portfolios():
    return await asyncio.to_thread(service.portfolios)


@router.get('/api/airs/valuation/portfolios/{portfolio_id}', response_model=ValuationBundle)
async def valuation_bundle(portfolio_id: int = Path(ge=1)):
    return await asyncio.to_thread(service.read, portfolio_id)


@router.post('/api/airs/valuation/portfolios/{portfolio_id}/refresh', response_model=ValuationBundle)
async def refresh_valuation(portfolio_id: int = Path(ge=1), force: bool = False):
    return await asyncio.to_thread(service.refresh, portfolio_id, force=force)
