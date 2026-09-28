import { describe, expect, it } from 'vitest';
import {
  epsActualForYear, epsActualToEstimateCagr2025To2027, epsEstimateForYear,
} from './PortfolioFundamentalModal';

const estimate = (year: number, value: number | null, code = 'annual_per_share_eps_estimate') => ({
  metric_code: code,
  target_date: `${year}-12-31`,
  numeric_value: value,
  recorded_at: '2026-09-28T08:00:00',
});
const actual = (year: number, value: number | null) =>
  estimate(year, value, 'annuals__Per Share Data__EPS without NRI');

describe('portfolio EPS estimate view', () => {
  it('selects the requested fiscal-year consensus rather than another EPS series', () => {
    const rows = [estimate(2025, 4), estimate(2026, 5), estimate(2025, 99, 'annual_eps_nri_estimate')];
    expect(epsEstimateForYear(rows, 2025)?.numeric_value).toBe(4);
    expect(epsEstimateForYear(rows, 2027)).toBeNull();
  });

  it('selects reported EPS without NRI as the 2025 actual', () => {
    expect(epsActualForYear([actual(2025, 4), estimate(2025, 99)], 2025)?.numeric_value)
      .toBe(4);
  });

  it('compounds the 2025 actual-to-2027 estimate change over two years', () => {
    expect(epsActualToEstimateCagr2025To2027([actual(2025, 4), estimate(2027, 9)]))
      .toBeCloseTo(0.5);
  });

  it('does not publish a CAGR from a missing or non-positive endpoint', () => {
    expect(epsActualToEstimateCagr2025To2027([actual(2025, 4)])).toBeNull();
    expect(epsActualToEstimateCagr2025To2027([actual(2025, -1), estimate(2027, 9)]))
      .toBeNull();
  });
});
