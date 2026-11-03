import { expect, it, vi } from 'vitest';
import fixture from '../../../../backend/tests/fixtures/benchmark_snapshot_parity.json';
import { savedBenchmarkSeries, readBenchmarkSnapshot } from './benchmarkSnapshot';
import { marginByYear } from './marginData';
import { debtRatioByYear } from './debtRatioData';
import { seriesByYear } from './cashReturnData';
import { investedCapitalIndexByYear } from './investedCapitalData';
import { interestBurdenByYear } from './interestBurdenData';
import { sbcOcfByYear } from './sbcOcfData';
import { capexMarginByYear } from './capexMarginData';
import { grossMarginByYear } from './grossMarginData';
import { cashConversionByYear } from './cashConversionData';
import { fcfSbcYieldByYear } from './fcfSbcYieldData';
import { dividendYieldByYear } from './dividendYieldData';

const mocks = vi.hoisted(() => ({ fetch: vi.fn() }));
vi.mock('../../../lib/apiFetch', () => ({ apiFetch: mocks.fetch }));
vi.mock('../../../lib/apiUrl', () => ({ API_URL: 'http://test' }));

it.each([fixture, fixture.quarterly])('saved Python series match the existing chart calculations for every metric and toggle (%#)', (fixture) => {
  type FixtureRow = { isin: string; name: string; weight_pct: number; status: 'ok'; currency: string;
    ticker: string; exchange: string; market_cap_by_period: Record<string, number> }
    & Record<'revenue' | 'fcf' | 'sbc' | 'total_assets' | 'goodwill' | 'long_term_debt' |
      'noncurrent_liabilities' | 'total_equity' | 'roic' | 'operating_income' | 'interest_expense' |
      'ocf' | 'capex' | 'gross_profit' | 'net_income' | 'market_cap' | 'div_ps' | 'price_ps', Record<string, number | null>>;
  const rows = fixture.rows.map(r => ({ ...r, market_cap_by_period: fixture.caps[r.isin as keyof typeof fixture.caps] })) as unknown as FixtureRow[];
  // The fixture deliberately has missing optional fields and negative/zero observations.
  const typed = rows as unknown as Parameters<typeof seriesByYear>[0];
  const checks: [string, string, Map<number, number>][] = [
    ['margin-inputs', 'reported', marginByYear(rows, false)], ['margin-inputs', 'corrected', marginByYear(rows, true)],
    ['debt-ratio-inputs', 'default', debtRatioByYear(rows)],
    ['cash-return-inputs', 'croic_reported', seriesByYear(typed, 'croic', false)],
    ['cash-return-inputs', 'croic_corrected', seriesByYear(typed, 'croic', true)],
    ['cash-return-inputs', 'roic', seriesByYear(typed, 'roic')],
    ['cash-return-inputs', 'invested_capital', investedCapitalIndexByYear(typed)],
    ['interest-burden-inputs', 'default', interestBurdenByYear(rows)],
    ['sbc-ocf-inputs', 'default', sbcOcfByYear(rows)],
    ['capex-margin-inputs', 'default', capexMarginByYear(rows)],
    ['gross-margin-inputs', 'default', grossMarginByYear(rows)],
    ['cash-conversion-inputs', 'reported', cashConversionByYear(rows, false)],
    ['cash-conversion-inputs', 'corrected', cashConversionByYear(rows, true)],
    ['fcf-sbc-yield-inputs', 'reported', fcfSbcYieldByYear(rows, false)],
    ['fcf-sbc-yield-inputs', 'corrected', fcfSbcYieldByYear(rows, true)],
    ['dividend-yield-inputs', 'default', dividendYieldByYear(rows)],
  ];
  const series = fixture.snapshot.series as Record<string, Record<string, { points: number[][] }>>;
  for (const [path, mode, expected] of checks) {
    const saved = series[path][mode].points;
    expect(saved.map(p => p[0]), `${path}/${mode} periods`).toEqual([...expected.keys()]);
    for (const [x, value] of saved) expect(value, `${path}/${mode}/${x}`).toBeCloseTo(expected.get(x)!, 5);
  }
});

it('uses a saved empty line without falling back to constituent calculations', () => {
  const fallback = vi.fn();
  expect(savedBenchmarkSeries({ saved_series: { default: { points: [] } } }, 'default', fallback).size).toBe(0);
  expect(fallback).not.toHaveBeenCalled();
});

it('reads a single chart snapshot rather than posting company inputs', async () => {
  mocks.fetch.mockResolvedValue({ ok: true, json: async () => fixture.snapshot });
  expect(await readBenchmarkSnapshot('ACWI', 'annual')).toEqual(fixture.snapshot);
  expect(mocks.fetch).toHaveBeenCalledWith('http://test/api/benchmarks/index/ACWI/chart-snapshot?cadence=annual');
});
