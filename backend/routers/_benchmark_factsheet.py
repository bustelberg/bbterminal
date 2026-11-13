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


class FactsheetRow(BaseModel):
    ticker: str
    name: str
    currency: str
    requested_start: str
    end: str
    period: FactsheetStatistics | None = None
    available_history: FactsheetStatistics | None = None
    reason: str | None = None


class FactsheetResponse(BaseModel):
    ready: bool
    period_years: int = 15
    stale: bool = False
    currency: str
    prepared_at: str | None = None
    calculation_date: str | None = None
    method: str | None = None
    rows: list[FactsheetRow]


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
