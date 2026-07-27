import { describe, expect, it } from 'vitest';
import { logLinearFit } from './trendFit';

describe('logLinearFit', () => {
  it('fits perfect exponential growth: R²=1 and CAGR = the rate', () => {
    // 100 growing at exactly 10%/yr.
    const pts = Array.from({ length: 6 }, (_, i) => ({ year: 2020 + i, value: 100 * 1.1 ** i }));
    const f = logLinearFit(pts);
    expect(f.r2).toBeCloseTo(1, 6);
    expect(f.cagr).toBeCloseTo(0.10, 6);
    expect(f.n).toBe(6);
    // The trend overlays the points exactly.
    expect(f.trend[0].value).toBeCloseTo(100, 4);
    expect(f.trend[5].value).toBeCloseTo(100 * 1.1 ** 5, 2);
  });

  it('a lumpy series fits worse (lower R²) than a steady one at the same endpoints', () => {
    const steady = logLinearFit([
      { year: 2020, value: 100 }, { year: 2021, value: 110 },
      { year: 2022, value: 121 }, { year: 2023, value: 133.1 },
    ]);
    const lumpy = logLinearFit([
      { year: 2020, value: 100 }, { year: 2021, value: 180 },
      { year: 2022, value: 95 }, { year: 2023, value: 133.1 },
    ]);
    expect(steady.r2!).toBeGreaterThan(0.99);
    expect(lumpy.r2!).toBeLessThan(steady.r2!);
  });

  it('drops non-positive years (no log) and reports the count', () => {
    const f = logLinearFit([
      { year: 2020, value: -5 }, { year: 2021, value: 100 },
      { year: 2022, value: 110 }, { year: 2023, value: 0 },
    ]);
    expect(f.dropped).toBe(2);
    expect(f.n).toBe(2);
  });

  it('returns nulls when fewer than two usable points', () => {
    const f = logLinearFit([{ year: 2020, value: 100 }]);
    expect(f).toMatchObject({ cagr: null, r2: null, n: 1, trend: [] });
  });

  it('a flat series is its own trend: R²=1, CAGR 0', () => {
    const f = logLinearFit([
      { year: 2020, value: 50 }, { year: 2021, value: 50 }, { year: 2022, value: 50 },
    ]);
    expect(f.r2).toBe(1);
    expect(f.cagr).toBeCloseTo(0, 9);
  });

  it('handles a decline: negative CAGR, still a good fit', () => {
    const pts = Array.from({ length: 5 }, (_, i) => ({ year: 2020 + i, value: 100 * 0.9 ** i }));
    const f = logLinearFit(pts);
    expect(f.cagr).toBeCloseTo(-0.10, 6);
    expect(f.r2).toBeCloseTo(1, 6);
  });
});
