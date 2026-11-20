"""Read saved macro indicators and fetch individual series on demand."""
import asyncio
import csv
import io
from calendar import monthrange
from datetime import date, datetime, timezone
from math import isfinite
from zoneinfo import ZoneInfo

import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from deps import supabase

router = APIRouter(prefix='/api/macro-dashboard', tags=['macro-dashboard'])
OECD_CLI = 'https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,4.1/{area}.M.LI...AA...H'
OECD_AREAS = {'oecd-cli': 'USA', 'europe-oecd-cli': 'G4E'}
# Reserve year 0 for full histories so old YTD snapshots cannot appear as complete.
FULL_HISTORY = 0

SERIES = {
    'oecd-cli': ('OECD Leading Indicator, U.S. (index)', None, 'Monthly'),
    'europe-oecd-cli': ('OECD Leading Indicator, four major European economies (index)', None, 'Monthly'),
    'us-unemployment': ('U.S. unemployment rate (%)', 'UNRATE', 'Monthly'),
    'core-cpi': ('U.S. core inflation (%, YoY)', 'CPILFESL', 'Monthly'),
    'ea-core-hicp': ('Euro-area core HICP (%, YoY)', '00XEFDEZCCM086NEST', 'Monthly'),
    'us-10y': ('U.S. 10-year Treasury yield (%)', 'DGS10', 'Daily'),
    'de-10y': ('German 10-year government bond yield (%)', 'IRLTLT01DEM156N', 'Monthly'),
    'high-yield': ('US High Yield spread (bp)', 'BAMLH0A0HYM2', 'Daily'),
    'nfci': ('Chicago Fed National Financial Conditions Index', 'NFCI', 'Weekly'),
    'vix': ('VIX (index)', 'VIXCLS', 'Daily'),
    'buffett': ('Buffett Indicator (× GDP)', 'BOGZ1FL883164115Q', 'Quarterly'),
    'eur-usd': ('EUR/USD (USD per EUR)', 'DEXUSEU', 'Daily'),
}


class Indicator(BaseModel):
    key: str
    name: str
    frequency: str
    sources: list[str]
    method: str | None = None


class Point(BaseModel):
    date: date
    value: float


class Snapshot(BaseModel):
    points: list[Point]
    fetched_at: datetime | None = None


class OverviewReading(BaseModel):
    key: str
    latest: Point
    comparison: Point | None = None
    trend: list[Point]


@router.get('/indicators/overview', response_model=list[OverviewReading])
async def indicator_overview():
    def read():
        rows = (supabase.table('macro_indicator_snapshot').select('key,points')
                .eq('year', FULL_HISTORY).execute().data or [])
        readings = []
        for row in rows:
            key = row['key']
            points = [Point.model_validate(point) for point in row['points']]
            if key not in SERIES or not points:
                continue
            latest = points[-1]
            month_index = latest.date.year * 12 + latest.date.month - 2
            year, month = divmod(month_index, 12)
            cutoff = date(year, month + 1, min(latest.date.day, monthrange(year, month + 1)[1]))
            frequency = SERIES[key][2]
            comparison = None if frequency == 'Quarterly' else next(
                (point for point in reversed(points) if point.date <= cutoff), None)
            if (frequency == 'Monthly' and comparison is not None
                    and (comparison.date.year, comparison.date.month) != (cutoff.year, cutoff.month)):
                comparison = None
            trend = [point for point in points if (latest.date - point.date).days <= 366]
            # Sample only the overview sparkline. The modal reads every stored observation.
            if len(trend) > 52:
                trend = [trend[round(index * (len(trend) - 1) / 51)] for index in range(52)]
            readings.append(OverviewReading(key=key, latest=latest, comparison=comparison, trend=trend))
        return readings
    return await asyncio.to_thread(read)


def today_amsterdam():
    return datetime.now(ZoneInfo('Europe/Amsterdam')).date()


def fred_csv(series, end):
    response = requests.get('https://fred.stlouisfed.org/graph/fredgraph.csv',
                            params={'id': series, 'coed': end.isoformat()},
                            timeout=15)
    response.raise_for_status()
    return csv_points(response.text, ('observation_date', 'DATE'), series, end)


def csv_points(content, date_columns, value_column, end):
    reader = csv.DictReader(io.StringIO(content.lstrip('\ufeff')))
    if not reader.fieldnames or value_column not in reader.fieldnames:
        raise ValueError('Invalid source CSV response')
    values = {}
    for row in reader:
        try:
            raw_date = next((row[column] for column in date_columns if row.get(column)), '')
            day = date.fromisoformat(raw_date + '-01' if len(raw_date) == 7 else raw_date)
            value = float(row[value_column])
        except (KeyError, TypeError, ValueError):
            continue
        if day <= end and isfinite(value):
            values[day] = value
    return values


def fetch_points(key, today):
    series = SERIES[key][1]
    if key in OECD_AREAS:
        response = requests.get(OECD_CLI.format(area=OECD_AREAS[key]), headers={'Accept': 'text/csv'}, timeout=15)
        response.raise_for_status()
        values = csv_points(response.text, ('TIME_PERIOD',), 'OBS_VALUE', today)
    else:
        values = fred_csv(series, today)
    if key in ('core-cpi', 'ea-core-hicp'):
        by_month = {(day.year, day.month): value for day, value in values.items()}
        values = {day: 100 * (value / prior - 1) for day, value in values.items()
                  if (prior := by_month.get((day.year - 1, day.month), 0)) > 0}
    elif key == 'high-yield':
        values = {day: value * 100 for day, value in values.items()}
    elif key == 'buffett':
        gdp = fred_csv('GDP', today)
        values = {day: value / (gdp[day] * 1000) for day, value in values.items() if gdp.get(day, 0) > 0}
    return [{'date': day.isoformat(), 'value': value} for day, value in sorted(values.items())]


@router.get('/indicators', response_model=list[Indicator])
async def indicators():
    return [
        Indicator(key=key, name=name, frequency=frequency,
                  sources=[f'{OECD_CLI.format(area=OECD_AREAS[key])}?format=csvfile'] if code is None else [f'https://fred.stlouisfed.org/series/{code}', *(
                      ['https://fred.stlouisfed.org/series/GDP'] if key == 'buffett' else [])],
                  method={
                      'buffett': 'U.S. public equity market value divided by annualized nominal GDP for the same quarter.',
                      'europe-oecd-cli': 'Regional proxy covering France, Germany, Italy and the United Kingdom.',
                  }.get(key))
        for key, (name, code, frequency) in SERIES.items()
    ]


def check_key(key):
    if key not in SERIES:
        raise HTTPException(404, 'Unknown indicator.')


@router.get('/indicators/{key}', response_model=Snapshot)
async def open_indicator(key: str):
    check_key(key)
    def read():
        rows = (supabase.table('macro_indicator_snapshot').select('points,fetched_at')
                .eq('key', key).eq('year', FULL_HISTORY).limit(1).execute().data or [])
        return rows[0] if rows else {'points': []}
    return await asyncio.to_thread(read)


@router.post('/indicators/{key}/fetch', response_model=Snapshot)
async def fetch_indicator(key: str):
    check_key(key)
    try:
        return await asyncio.to_thread(refresh_snapshot, key)
    except (requests.RequestException, ValueError) as exc:
        raise HTTPException(502, 'The source could not provide the series. Try again later.') from exc


def refresh_snapshot(key):
    points = fetch_points(key, today_amsterdam())
    if not points:
        raise ValueError('The source returned no historical observations.')
    snapshot = {'points': points, 'fetched_at': datetime.now(timezone.utc).isoformat()}
    supabase.table('macro_indicator_snapshot').upsert(
        {'key': key, 'year': FULL_HISTORY, **snapshot}, on_conflict='key,year').execute()
    return snapshot
