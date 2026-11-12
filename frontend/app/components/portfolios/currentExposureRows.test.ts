import { expect, it } from 'vitest';
import type { ModelPortfolioAnalysis, EtfSectorAllocationResponse, EtfRegionAllocationResponse } from '../../../lib/types/api';
import { currentExposureRows } from './currentExposureRows';

type Holding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];
const holdings = [
  { isin: 'STOCK', bucket: 'Equity', is_fund: false, sector: 'Technology', region: 'North America', weight_now_pct: 20,
    sources: [{ label: null, weight_now_pct: 18 }, { label: 'Certificate', weight_now_pct: 2 }] },
  { isin: 'UNKNOWN', bucket: 'Equity', is_fund: false, weight_now_pct: 3 },
  { isin: 'FUND', bucket: 'Equity', is_fund: true, sector_allocation_available: true, weight_now_pct: 10 },
  { isin: 'UNSUPPORTED', bucket: 'Equity', is_fund: true, sector_allocation_available: false, weight_now_pct: 5 },
  { isin: 'BOND', bucket: 'Bonds', weight_now_pct: 60 },
  { bucket: 'Cash', weight_now_pct: 2 },
] as Holding[];
const sectors = { FUND: { sectors: [{ sector: 'Financials', weight_pct: 100 }] } } as unknown as Record<string, EtfSectorAllocationResponse>;
const regions = { FUND: { regions: [{ region: 'Europe', weight_pct: 70 }, { region: 'Unclassified', weight_pct: 30 }] } } as unknown as Record<string, EtfRegionAllocationResponse>;
const sum = (rows: ReturnType<typeof currentExposureRows>) => Math.round(rows.reduce((total, row) => total + (row.portfolio_pct ?? 0), 0) * 100) / 100;

it('includes identical positions, certificate routes, unsupported funds and unknown stocks in both axes', () => {
  const sector = currentExposureRows([], holdings, 'sector', true, false, sectors, regions);
  const region = currentExposureRows([], holdings, 'region', true, false, sectors, regions);
  expect(sum(sector)).toBe(38);
  expect(sum(region)).toBe(38);
  expect(sector.find(row => row.bucket === 'Technology')?.portfolio_pct).toBe(20);
  expect(sector.find(row => row.bucket === 'Unclassified')?.portfolio_pct).toBe(8);
  expect(region.find(row => row.bucket === 'Unclassified')?.portfolio_pct).toBe(11);
  expect(region.find(row => row.bucket === 'Europe')?.portfolio_pct).toBe(7);
});

it('moves loading or failed fund exposure out of Unclassified without changing totals or ACWI', () => {
  const base = [{ bucket: 'Technology', portfolio_pct: 999, benchmark_pct: 17.68, diff_pct: 0, holdings: [] }];
  const pending = currentExposureRows(base, holdings, 'sector', true, false, {}, {});
  const loaded = currentExposureRows(base, holdings, 'sector', true, false, sectors, regions);
  expect(sum(pending)).toBe(sum(loaded));
  expect(pending.find(row => row.bucket === 'Unclassified')?.portfolio_pct).toBe(18);
  expect(loaded.find(row => row.bucket === 'Technology')?.benchmark_pct).toBe(17.68);
  expect(base[0].portfolio_pct).toBe(999);
});

it.each([false, true])('uses the same fund-off membership with certificate expansion %s', lookThrough => {
  const sector = currentExposureRows([], holdings, 'sector', false, lookThrough, sectors, regions);
  const region = currentExposureRows([], holdings, 'region', false, lookThrough, sectors, regions);
  expect(sum(sector)).toBe(lookThrough ? 23 : 21);
  expect(sum(region)).toBe(sum(sector));
});

it('does not inflate partial fund breakdowns and preserves missing classification', () => {
  const partial = { FUND: { sectors: [{ sector: 'Technology', weight_pct: 40 }] } } as unknown as typeof sectors;
  const rows = currentExposureRows([], [holdings[2]], 'sector', true, false, partial, {});
  expect(rows.find(row => row.bucket === 'Technology')?.portfolio_pct).toBe(4);
  expect(rows.find(row => row.bucket === 'Unclassified')?.portfolio_pct).toBe(6);
});

it('retains unknown classification labels and invalid fund data as Unclassified', () => {
  const unknown = [{ ...holdings[0], sector: 'Not a sector', region: 'Not a region' }, holdings[2]] as Holding[];
  const invalid = { FUND: { sectors: [{ sector: 'Technology', weight_pct: 150 }] } } as unknown as typeof sectors;
  for (const axis of ['sector', 'region'] as const) {
    const result = currentExposureRows([], unknown, axis, true, false, invalid, {});
    expect(sum(result)).toBe(30);
    expect(result.find(row => row.bucket === 'Unclassified')?.portfolio_pct).toBe(30);
  }
});

it('keeps displayed totals equal despite different bucket rounding and preserves cached regions', () => {
  const tiny = [
    { isin: 'A', bucket: 'Equity', sector: 'Technology', weight_now_pct: 1.004 },
    { isin: 'B', bucket: 'Equity', sector: 'Financials', region: 'Europe', weight_now_pct: 1.004 },
  ] as Holding[];
  const oldRegion = [{ bucket: 'Europe', portfolio_pct: 1.004, benchmark_pct: 60, diff_pct: 0, holdings: [{ isin: 'A', weight_pct: 1.004 }] }];
  const a = currentExposureRows([], tiny, 'sector', true, false, {}, {});
  const b = currentExposureRows(oldRegion, tiny, 'region', true, false, {}, {});
  expect(sum(a)).toBe(2.01);
  expect(sum(b)).toBe(2.01);
  expect(b.find(row => row.bucket === 'Europe')?.portfolio_pct).toBe(2.01);
});
