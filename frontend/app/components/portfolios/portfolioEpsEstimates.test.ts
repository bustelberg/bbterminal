import { describe, expect, it } from 'vitest';
import {
  dcfGrowthCellLabel,
  displayedWeightPct,
  epsActualForYear, epsActualToEstimateCagr2025To2027, epsEstimateForYear, epsInput,
  epsObservationForYear,
  medianPeCalculation, peDeltaFromHistoricalMedian, priceToEpsMultiple,
  sourceInput,
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

  it('uses a reported result when available and otherwise falls back to consensus', () => {
    const rows = [actual(2026, 6), estimate(2026, 5), estimate(2027, 7)];
    expect(epsObservationForYear(rows, 2026)).toMatchObject({
      kind: 'actual', metric: { numeric_value: 6 },
    });
    expect(epsObservationForYear(rows, 2027)).toMatchObject({
      kind: 'estimate', metric: { numeric_value: 7 },
    });
    expect(epsObservationForYear(rows, 2025)).toBeNull();
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

  it('distinguishes a checked estimate feed from an estimate GuruFocus actually supplied', () => {
    expect(epsInput(null, 'EPS estimate for FY2026', 'USD', '2026-09-29T12:47:56Z'))
      .toEqual([{
        label: 'EPS estimate for FY2026',
        value: 'not supplied by GuruFocus',
        retrieved: '2026-09-29T12:47:56Z',
        applies: null,
        retrievedText: undefined,
        appliesText: 'No matching fiscal period was returned',
      }]);
  });

  it('uses the estimate row own retrieval time and fiscal period when it exists', () => {
    expect(epsInput(estimate(2026, 5), 'EPS estimate for FY2026', 'USD',
      '2026-09-29T12:47:56Z')).toEqual([{
      label: 'EPS estimate for FY2026',
      value: '5 USD/share',
      retrieved: '2026-09-28T08:00:00',
      applies: '2026-12-31',
    }]);
  });

  it('calculates each P/E from the current stock price and the selected EPS', () => {
    expect(priceToEpsMultiple(150, 6)).toBe(25);
    expect(priceToEpsMultiple(150, null)).toBeNull();
    expect(priceToEpsMultiple(150, -2)).toBeNull();
  });

  it('measures each fiscal-year P/E against the ten-year historical median', () => {
    expect(peDeltaFromHistoricalMedian(24, 20)).toBeCloseTo(0.2);
    expect(peDeltaFromHistoricalMedian(15, 20)).toBeCloseTo(-0.25);
    expect(peDeltaFromHistoricalMedian(24, null)).toBeNull();
    expect(peDeltaFromHistoricalMedian(null, 20)).toBeNull();
  });
});

describe('portfolio Weight sorting value', () => {
  it('uses the displayed AIRS book percentage instead of the hidden model weight', () => {
    expect(displayedWeightPct(12, {
      current_value_eur: 50_600,
      total_current_value_eur: 1_000_000,
    })).toBeCloseTo(5.06);
    expect(displayedWeightPct(7.62, null)).toBe(7.62);
  });
});

describe('portfolio Reverse DCF refusal labels', () => {
  it('names a non-positive normalised base instead of looking like missing data', () => {
    expect(dcfGrowthCellLabel(null, -1043.28)).toBe('No +FCF');
    expect(dcfGrowthCellLabel(null, 0)).toBe('No +FCF');
  });

  it('keeps missing inputs distinct and still formats a solved rate', () => {
    expect(dcfGrowthCellLabel(null, null)).toBe('Missing inputs');
    expect(dcfGrowthCellLabel(null, 100, 1_000)).toBe('No solution');
    expect(dcfGrowthCellLabel(0.1236, -1043.28)).toBe('12.4%');
  });

  it('keeps a missing source operand visible in the info card', () => {
    expect(sourceInput([], 'Diluted shares outstanding', {
      raw: null, used: null, date: null, code: null,
    }, 'm shares')).toEqual({
      label: 'Diluted shares outstanding',
      value: 'not available',
      retrieved: null,
      applies: null,
      retrievedText: 'not available',
      appliesText: 'not available',
    });
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
