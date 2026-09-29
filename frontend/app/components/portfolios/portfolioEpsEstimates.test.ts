import { describe, expect, it } from 'vitest';
import {
  dcfGrowthCellLabel,
  epsActualForYear, epsActualToEstimateCagr2025To2027, epsEstimateForYear,
  medianPeCalculation, priceToEpsMultiple,
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

  it('calculates each P/E from the current stock price and the selected EPS', () => {
    expect(priceToEpsMultiple(150, 6)).toBe(25);
    expect(priceToEpsMultiple(150, null)).toBeNull();
    expect(priceToEpsMultiple(150, -2)).toBeNull();
  });
});

describe('portfolio Reverse DCF refusal labels', () => {
  it('names a non-positive normalised base instead of looking like missing data', () => {
    expect(dcfGrowthCellLabel(null, -1043.28)).toBe('No +FCF');
    expect(dcfGrowthCellLabel(null, 0)).toBe('No +FCF');
  });

  it('keeps missing inputs distinct and still formats a solved rate', () => {
    expect(dcfGrowthCellLabel(null, null)).toBe('—');
    expect(dcfGrowthCellLabel(0.1236, -1043.28)).toBe('12.4%');
  });
});

describe('portfolio Exit P/E working', () => {
  it('shows the sorted observations and the actual odd-count median', () => {
    expect(medianPeCalculation({
      rows: [
        { year: 2023, price: 90, eps: 3, pe: 30, used: true },
        { year: 2024, price: 100, eps: 5, pe: 20, used: true },
        { year: 2025, price: 100, eps: 4, pe: 25, used: true },
      ],
      median: 25,
    })).toBe('Sorted fiscal-year P/Es: 20×, 25×, 30×. Median = middle value 25×.');
  });

  it('shows the averaging step for an even count and omits an excluded loss year', () => {
    const text = medianPeCalculation({
      rows: [
        { year: 2022, price: 100, eps: -5, pe: null, used: false },
        { year: 2023, price: 100, eps: 5, pe: 20, used: true },
        { year: 2024, price: 120, eps: 4, pe: 30, used: true },
      ],
      median: 25,
    });
    expect(text).toBe('Sorted fiscal-year P/Es: 20×, 30×. Median = (20 + 30) ÷ 2 = 25×.');
  });
});
