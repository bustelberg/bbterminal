import { describe, expect, it } from 'vitest';
import type { EtfSectorAllocationResponse, ModelPortfolioAnalysis } from '../../../lib/types/api';
import {
  addCertificateSectorWeights, addEtfSectorWeights, certificateSectorLookThroughCount,
  collapseEtfSectors, etfSectorPortfolioContribution, normalizedEtfSectorWeight,
  portfolioSectorBucket,
} from './etfSectorLookThrough';

type Holding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];

const allocation = (isin: string, sectors: Array<[string, number]>): EtfSectorAllocationResponse => ({
  isin,
  name: 'ETF',
  source: 'test',
  source_url: 'https://example.test',
  sectors: sectors.map(([sector, weight_pct]) => ({
    sector, weight_pct, provider_sectors: [sector],
    provider_weights: [{ sector, weight_pct }],
  })),
});

describe('addEtfSectorWeights', () => {
  it('maps every ETF/GICS alias into the established portfolio chart buckets', () => {
    expect([
      'Information Technology', 'Health Care', 'Consumer Discretionary', 'Consumer Staples',
      'Financial Services', 'Basic Materials', 'Cash',
    ].map(portfolioSectorBucket)).toEqual([
      'Technology', 'Healthcare', 'Consumer Cyclical', 'Consumer Defensive',
      'Financials', 'Materials', 'Unclassified',
    ]);
  });

  it('squishes aliases into one internal sector before displaying or weighting them', () => {
    const sectors = allocation('ETF1', [
      ['Health Care', 8], ['Healthcare', 2], ['Cash', 0.5], ['Unclassified', 0.5],
    ]).sectors;

    expect(collapseEtfSectors(sectors).map((row) => [row.sector, row.weight_pct])).toEqual([
      ['Healthcare', 10], ['Unclassified', 1],
    ]);
  });

  it('exposes the same normalized ETF and portfolio weights shown in the comparison modal', () => {
    expect(normalizedEtfSectorWeight(33.34, 100.01)).toBeCloseTo(33.3367, 4);
    expect(etfSectorPortfolioContribution(15, 33.34, 100.01)).toBeCloseTo(5.0005, 4);
  });

  it('adds each ETF sector share using the ETF current portfolio weight', () => {
    const result = addEtfSectorWeights(
      [{ bucket: 'Technology', portfolio_pct: 10, benchmark_pct: 8, diff_pct: 2,
        holdings: [] }],
      [{ isin: 'ETF1', is_fund: true, sector_allocation_available: true,
        weight_now_pct: 20, bucket: 'Equity' } as Holding],
      { ETF1: allocation('ETF1', [['Information Technology', 60], ['Financials', 40]]) },
    );

    expect(result.find((row) => row.bucket === 'Technology')?.portfolio_pct).toBe(22);
    expect(result.find((row) => row.bucket === 'Technology')?.diff_pct).toBe(14);
    expect(result.find((row) => row.bucket === 'Financials')?.portfolio_pct).toBe(8);
  });

  it('normalizes rounded provider totals so the ETF contributes exactly its owned weight', () => {
    const result = addEtfSectorWeights(
      [],
      [{ isin: 'ETF1', is_fund: true, sector_allocation_available: true,
        weight_now_pct: 15, bucket: 'Equity' } as Holding],
      { ETF1: allocation('ETF1', [['Industrials', 66.67], ['Energy', 33.34]]) },
    );

    expect(result.reduce((sum, row) => sum + (row.portfolio_pct ?? 0), 0)).toBeCloseTo(15, 12);
  });

  it('never creates Health Care beside an existing Healthcare chart row', () => {
    const result = addEtfSectorWeights(
      [{ bucket: 'Healthcare', portfolio_pct: 2.11, benchmark_pct: 8.39,
        diff_pct: -6.28, holdings: [] }],
      [{ isin: 'ETF1', is_fund: true, sector_allocation_available: true,
        weight_now_pct: 10, bucket: 'Equity' } as Holding],
      { ETF1: allocation('ETF1', [['Health Care', 25], ['Industrials', 75]]) },
    );

    const healthcare = result.filter(
      (row) => row.bucket.toLowerCase().replace(' ', '') === 'healthcare',
    );
    expect(healthcare).toHaveLength(1);
    expect(healthcare[0].bucket).toBe('Healthcare');
    expect(healthcare[0].portfolio_pct).toBeCloseTo(4.61, 12);
    expect(result.some((row) => row.bucket === 'Health Care')).toBe(false);
  });

  it('does not infer sectors for unsupported funds or allocations that failed to load', () => {
    const holdings = [
      { isin: 'INTERNAL', is_fund: true, sector_allocation_available: false,
        weight_now_pct: 30, bucket: 'Equity' },
      { isin: 'MISSING', is_fund: true, sector_allocation_available: true,
        weight_now_pct: 20, bucket: 'Equity' },
    ] as Holding[];

    expect(addEtfSectorWeights([], holdings, {})).toEqual([]);
  });

  it('adds only the certificate routes of expanded TopSelectie constituents', () => {
    const holdings = [
      {
        name: 'Mastercard', bucket: 'Equity', sector: 'Financial Services', is_fund: false,
        weight_now_pct: 8,
        sources: [
          { label: null, value_eur: 600, weight_now_pct: 6 },
          { label: 'StarTopSelectie', value_eur: 200, weight_now_pct: 2 },
        ],
      },
      {
        name: 'NVIDIA', bucket: 'Equity', sector: 'Technology', is_fund: false,
        weight_now_pct: 3,
        sources: [
          { label: 'StarTopSelectie', value_eur: 300, weight_now_pct: 3 },
        ],
      },
    ] as Holding[];

    const result = addCertificateSectorWeights([
      { bucket: 'Financials', portfolio_pct: 6, benchmark_pct: 10, diff_pct: -4,
        holdings: [] },
    ], holdings);

    expect(result.find((row) => row.bucket === 'Financials')?.portfolio_pct).toBe(8);
    expect(result.find((row) => row.bucket === 'Technology')?.portfolio_pct).toBe(3);
    expect(result.reduce((sum, row) => sum + (row.portfolio_pct ?? 0), 0)).toBe(11);
    expect(certificateSectorLookThroughCount(holdings)).toBe(1);
  });

  it('ignores non-equity and fund rows inside certificate payloads', () => {
    const holdings = [
      { bucket: 'Cash', sector: 'Cash', is_fund: false,
        sources: [{ label: 'StarTopSelectie', value_eur: 2, weight_now_pct: 2 }] },
      { bucket: 'Equity', sector: 'Unclassified', is_fund: true,
        sources: [{ label: 'StarTopSelectie', value_eur: 4, weight_now_pct: 4 }] },
    ] as Holding[];

    expect(addCertificateSectorWeights([], holdings)).toEqual([]);
    expect(certificateSectorLookThroughCount(holdings)).toBe(0);
  });
});
