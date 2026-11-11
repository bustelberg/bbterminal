import { expect, it } from 'vitest';
import type { EtfRegionAllocationResponse, ModelPortfolioAnalysis } from '../../../lib/types/api';
import { addEtfRegionWeights } from './etfRegionLookThrough';

type Holding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];
const holding = { isin: 'ETF', is_fund: true, sector_allocation_available: true, weight_now_pct: 3.91, bucket: 'Equity' } as Holding;
const allocation: EtfRegionAllocationResponse = { isin: 'ETF', name: 'Fund', source: 'justETF', source_url: 'source', as_of: '2026-09-28', regions: [
  { region: 'North America', weight_pct: 45.14, provider_weights: [{ country: 'United States', weight_pct: 38.97 }, { country: 'Canada', weight_pct: 6.17 }] },
  { region: 'Europe', weight_pct: 28.84, provider_weights: [{ country: 'European countries', weight_pct: 28.84 }] },
  { region: 'Pacific', weight_pct: 20.71, provider_weights: [{ country: 'Pacific countries', weight_pct: 20.71 }] },
  { region: 'Unclassified', weight_pct: 5.31, provider_weights: [{ country: 'Other', weight_pct: 5.31 }] },
] };

it('adds fund regions to existing bars using whole-portfolio weights and preserves ACWI', () => {
  const base = [{ bucket: 'North America', portfolio_pct: 30, benchmark_pct: 65, diff_pct: -35, holdings: [] }];
  const result = addEtfRegionWeights(base, [holding], { ETF: allocation });
  expect(result.find(row => row.bucket === 'North America')?.portfolio_pct).toBeCloseTo(31.764974);
  expect(result.find(row => row.bucket === 'North America')?.benchmark_pct).toBe(65);
  expect(result.find(row => row.bucket === 'North America')?.diff_pct).toBeCloseTo(-33.235026);
  expect(result.find(row => row.bucket === 'Unclassified')?.portfolio_pct).toBeCloseTo(0.207621);
  expect(result.reduce((sum, row) => sum + (row.portfolio_pct ?? 0), 0)).toBeCloseTo(33.91);
  expect(base[0].portfolio_pct).toBe(30); // toggling off uses the original bars
});

it('does not add unsupported funds or missing allocations', () => {
  expect(addEtfRegionWeights([], [holding], {})).toEqual([]);
  expect(addEtfRegionWeights([], [{ ...holding, sector_allocation_available: false }], { ETF: allocation })).toEqual([]);
});

it('normalizes publication rounding and counts separate held portions without rebasing the book', () => {
  const rounded = { ...allocation, regions: allocation.regions.map((row, index) => ({ ...row, weight_pct: row.weight_pct + (index === 0 ? 0.01 : 0) })) };
  const result = addEtfRegionWeights([], [holding, { ...holding, weight_now_pct: 2 }], { ETF: rounded });
  expect(result.reduce((sum, row) => sum + (row.portfolio_pct ?? 0), 0)).toBeCloseTo(5.91);
});
