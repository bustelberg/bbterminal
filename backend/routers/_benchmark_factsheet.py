"""Factsheet HTTP contracts. Provider work only runs on explicit preparation POSTs."""
import asyncio
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services import benchmark_factsheet as service

router = APIRouter(tags=['benchmarks'])


class FactsheetStatistics(BaseModel):
    start: str
    end: str
    years: float
    observations: int
    total_return_pct: float
    cagr_pct: float
    volatility_pct: float
    cvar_95_pct: float | None = None


class FactsheetRow(BaseModel):
    ticker: str
    name: str
    currency: str
    isin: str | None = None
    source_url: str | None = None
    requested_start: str
    end: str
    period: FactsheetStatistics | None = None
    available_history: FactsheetStatistics | None = None
    reason: str | None = None


class FactsheetCorrelations(BaseModel):
    tickers: list[str]
    start: str
    end: str
    observations: int
    matrix: list[list[float | None]]


class FactsheetDownside(BaseModel):
    key: str
    start: str
    end: str
    observations: int
    correlation: float | None
    acwi_mean_pct: float | None
    hdg_mean_pct: float | None
    hdg_positive: int


class FactsheetAllocationRow(BaseModel):
    weights: tuple[int, int, int]
    cagr_pct: float
    sharpe: float | None
    volatility_pct: float | None = None
    cvar_95_pct: float
    drawdowns_pct: list[float]
    lower_quartile_pct: float | None
    worst_pct: float | None


class FactsheetAllocations(BaseModel):
    start: str
    end: str
    windows: int
    rows: list[FactsheetAllocationRow]


class FactsheetResponse(BaseModel):
    ready: bool
    period_years: int = 15
    stale: bool = False
    currency: str
    prepared_at: str | None = None
    calculation_date: str | None = None
    method: str | None = None
    rows: list[FactsheetRow]
    correlations: FactsheetCorrelations | None = None
    downside: list[FactsheetDownside] | None = None
    allocations: FactsheetAllocations | None = None


@router.get('/api/benchmarks/factsheet', response_model=FactsheetResponse)
async def factsheet():
    return await asyncio.to_thread(service.read)


@router.post('/api/benchmarks/factsheet/refresh', response_model=FactsheetResponse)
async def refresh_factsheet(force: bool = False):
    try:
        return await asyncio.to_thread(service.prepare, force=force)
    except HTTPException:
        raise
    except Exception as exc:
        logging.getLogger(__name__).exception('Factsheet preparation failed; saved data retained')
        raise HTTPException(503, f'Factsheet could not be updated: {exc}. Saved results are retained.') from exc
