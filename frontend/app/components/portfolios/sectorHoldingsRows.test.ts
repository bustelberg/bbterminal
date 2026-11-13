import { expect, it } from 'vitest';
import type { ModelPortfolioAnalysis } from '../../../lib/types/api';
import { currentExposureRows } from './currentExposureRows';
import { sectorHoldingsCompanies } from './sectorHoldingsRows';
import type { SectorChartRow } from './etfSectorLookThrough';

type Holding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];

it('preserves production overlap through exposure rebuilding and joins vendor name variants by issuer', () => {
  const base: SectorChartRow[] = [{ bucket: 'Technology', portfolio_pct: 2, benchmark_pct: 5, diff_pct: -3,
    holdings: [{ name: 'Nvidia', isin: 'PORT-NVDA', weight_pct: 2, in_both: true, issuer_key: 'nvidia' }],
    benchmark_holdings: [
      { name: 'NVIDIA Corporation', isin: 'BENCH-NVDA', issuer_key: 'nvidia', in_both: true, weight_pct: 2.62 },
      { name: 'MICROSOFT CORP', isin: null, issuer_key: 'microsoft', in_both: true, weight_pct: 2.29 },
    ] }];
  const holdings = [
    { name: 'Nvidia', isin: 'PORT-NVDA', bucket: 'Equity', sector: 'Technology', weight_now_pct: 2.68 },
    // Certificate-only constituents were not in the direct-only base chart.
    { name: 'Microsoft', isin: 'PORT-MSFT', bucket: 'Equity', sector: 'Technology', weight_now_pct: 1.92,
      in_both: true, issuer_key: 'microsoft', via_names: ['StarTopSelectie'] },
  ] as Holding[];
  const rows = currentExposureRows(base, holdings, 'sector', true, false, {}, {});
  const companies = sectorHoldingsCompanies(rows.find(row => row.bucket === 'Technology')!);
  expect(companies).toHaveLength(2);
  const nvda = companies.find(([key]) => key === 'issuer:nvidia')![1];
  expect(nvda.name).toBe('Nvidia');
  expect(nvda.inBoth).toBe(true);
  expect(nvda.portfolio).toBe(2.68);
  expect(nvda.benchmark).toBe(2.62);
  const microsoft = companies.find(([key]) => key === 'issuer:microsoft')![1];
  expect(microsoft.name).toBe('Microsoft');
  expect(microsoft.inBoth).toBe(true);
  expect(microsoft.benchmark).toBe(2.29);
  expect([...microsoft.via]).toEqual(['StarTopSelectie']);
  expect(base[0].portfolio_pct).toBe(2);
});

it('never uses display-name equality to invent membership and supports old ISIN-only payloads', () => {
  const rows = sectorHoldingsCompanies({ bucket: 'Technology', portfolio_pct: 1, benchmark_pct: 2, diff_pct: -1,
    holdings: [{ name: 'Apple', isin: 'US-AAPL', weight_pct: 1 }],
    benchmark_holdings: [{ name: 'Apple Inc.', isin: 'US-AAPL', weight_pct: 2 },
      { name: 'Apple', isin: null, weight_pct: 0.1 }] });
  expect(rows).toHaveLength(2);
  expect(rows[0][1].portfolio).toBe(1);
  expect(rows[0][1].benchmark).toBe(2);
  expect(rows[1][1].portfolio).toBeUndefined();
  expect(rows[1][1].inBoth).toBe(false);
});

it('bridges issuer spelling variants through exact ISIN without losing either side or weight', () => {
  const rows = sectorHoldingsCompanies({ bucket: 'Technology', portfolio_pct: 1.28, benchmark_pct: 0.2, diff_pct: 1.08,
    holdings: [{ name: 'Adobe Systems', isin: 'US00724F1012', issuer_key: 'adobe systems', weight_pct: 1.28 }],
    benchmark_holdings: [{ name: 'ADOBE INC', isin: 'US00724F1012', issuer_key: 'adobe', weight_pct: 0.2 }] });
  expect(rows).toHaveLength(1);
  expect(rows[0][1].name).toBe('Adobe Systems');
  expect(rows[0][1].portfolio).toBe(1.28);
  expect(rows[0][1].benchmark).toBe(0.2);
});

it('marks certificate-held Constellation in North America even with an unresolved benchmark listing', () => {
  const base: SectorChartRow[] = [{ bucket: 'North America', portfolio_pct: 0, benchmark_pct: 0.03, diff_pct: -0.03,
    holdings: [], benchmark_holdings: [{ name: 'CONSTELLATION SOFTWARE', isin: null,
      issuer_key: 'constellation software', in_both: true, weight_pct: 0.03 }] }];
  const holdings = [{ name: 'Constellation Software', isin: 'CA21037X1006', bucket: 'Equity', region: 'North America',
    weight_now_pct: 1.93, via_names: ['StarTopSelectie'], in_both: true, issuer_key: 'constellation software' }] as Holding[];
  const rows = currentExposureRows(base, holdings, 'region', true, false, {}, {});
  const companies = sectorHoldingsCompanies(rows[0]);
  expect(companies).toHaveLength(1);
  expect(companies[0][1].inBoth).toBe(true);
  expect(companies[0][1].name).toBe('Constellation Software');
  expect(companies[0][1].portfolio).toBe(1.93);
  expect(companies[0][1].benchmark).toBe(0.03);
});
