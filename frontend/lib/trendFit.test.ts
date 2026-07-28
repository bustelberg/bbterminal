import { describe, it, expect } from 'vitest';
import { logLinearFit, trendValueAt } from './trendFit';

const series = (from: number, base: number, rate: number, n: number) =>
  Array.from({ length: n }, (_, i) => ({ year: from + i, value: base * Math.pow(1 + rate, i) }));

describe('logLinearFit', () => {
  it('recovers a clean exponential exactly', () => {
    const fit = logLinearFit(series(2016, 10, 0.12, 10));
    expect(fit.cagr).toBeCloseTo(0.12, 9);
    expect(fit.r2).toBeCloseTo(1, 9);
    expect(fit.n).toBe(10);
  });

  it('⚠ drops a cash-burn year rather than failing — a loss has no logarithm', () => {
    const fit = logLinearFit([...series(2016, 10, 0.12, 5), { year: 2021, value: -3 }]);
    expect(fit.dropped).toBe(1);
    expect(fit.n).toBe(5);
    expect(fit.r2).toBeCloseTo(1, 9);
  });

  it('has no line to project from with fewer than two usable points', () => {
    for (const pts of [[], [{ year: 2024, value: 10 }], [{ year: 2024, value: -1 }]]) {
      const fit = logLinearFit(pts);
      expect(fit.slope).toBeNull();
      expect(fit.intercept).toBeNull();
      expect(trendValueAt(fit, 2026)).toBeNull();
    }
  });
});

describe('trendValueAt', () => {
  it('continues the fitted exponential past the data', () => {
    // 12%/yr from 10 at 2016; two years past the last point (2025) is 10·1.12^11.
    const fit = logLinearFit(series(2016, 10, 0.12, 10));
    expect(trendValueAt(fit, 2027)).toBeCloseTo(10 * Math.pow(1.12, 11), 6);
  });

  it('⚠ is invariant to the base the caller indexed on', () => {
    // The chart fits the INDEX, not the raw amounts: ln(k·v) = ln k + ln v shifts the intercept and
    // leaves slope and R² alone. So R² describes the cash flow, not the base year chosen.
    const raw = logLinearFit(series(2016, 7.3, 0.09, 8));
    const indexed = logLinearFit(series(2016, 100, 0.09, 8));
    expect(indexed.cagr).toBeCloseTo(raw.cagr as number, 12);
    expect(indexed.r2).toBeCloseTo(raw.r2 as number, 12);
    expect(indexed.slope).toBeCloseTo(raw.slope as number, 12);
  });

  it('never returns a non-finite value', () => {
    const fit = logLinearFit(series(2016, 10, 5.0, 5));   // 500%/yr
    const v = trendValueAt(fit, 2500);
    expect(v === null || Number.isFinite(v)).toBe(true);
  });
});
