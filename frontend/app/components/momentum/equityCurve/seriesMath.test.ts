/**
 * Tests for the pure-function helpers in `seriesMath.ts`. Focus is on
 * `computeYearlySubplots` (the per-year cumulative + alpha grid we
 * added this session) and the small date/series builders feeding it.
 */
import { describe, expect, it } from 'vitest';
import {
  alignSeries,
  computeYearlySubplots,
  endOfMonth,
  seriesFromMonthly,
  seriesFromPrices,
  type ResolvedSeries,
} from './seriesMath';

// ─── endOfMonth ─────────────────────────────────────────────────────

describe('endOfMonth', () => {
  it('promotes YYYY-MM to the last day of that month', () => {
    expect(endOfMonth('2024-01')).toBe('2024-01-31');
    expect(endOfMonth('2024-02')).toBe('2024-02-29'); // leap year
    expect(endOfMonth('2023-02')).toBe('2023-02-28');
    expect(endOfMonth('2024-04')).toBe('2024-04-30');
  });

  it('passes through full YYYY-MM-DD strings unchanged', () => {
    expect(endOfMonth('2024-01-15')).toBe('2024-01-15');
    expect(endOfMonth('2024-12-31')).toBe('2024-12-31');
  });
});

// ─── seriesFromMonthly ──────────────────────────────────────────────

describe('seriesFromMonthly', () => {
  it('converts cumulative_return_pct to a growth factor map keyed by EOM dates', () => {
    const monthly = [
      { date: '2024-01', holdings: [], portfolio_return_pct: 0, cumulative_return_pct: 0 },
      { date: '2024-02', holdings: [], portfolio_return_pct: 10, cumulative_return_pct: 10 },
      { date: '2024-03', holdings: [], portfolio_return_pct: 5, cumulative_return_pct: 15.5 },
    ];
    const { map, months } = seriesFromMonthly(monthly);
    expect(months).toEqual(['2024-01-31', '2024-02-29', '2024-03-31']);
    expect(map.get('2024-01-31')).toBeCloseTo(1.0);
    expect(map.get('2024-02-29')).toBeCloseTo(1.1);
    expect(map.get('2024-03-31')).toBeCloseTo(1.155, 4);
  });
});

// ─── seriesFromPrices ───────────────────────────────────────────────

describe('seriesFromPrices', () => {
  it('rebases to 1.0 on the first day and tracks proportional growth', () => {
    const prices = [
      { target_date: '2024-01-02', price: 100 },
      { target_date: '2024-01-03', price: 110 },
      { target_date: '2024-01-04', price: 90 },
    ];
    const { map } = seriesFromPrices(prices);
    expect(map.get('2024-01-02')).toBeCloseTo(1.0);
    expect(map.get('2024-01-03')).toBeCloseTo(1.1);
    expect(map.get('2024-01-04')).toBeCloseTo(0.9);
  });

  it('returns empty maps when the first price is non-positive', () => {
    const { map, months } = seriesFromPrices([
      { target_date: '2024-01-01', price: 0 },
      { target_date: '2024-01-02', price: 10 },
    ]);
    expect(map.size).toBe(0);
    expect(months).toEqual([]);
  });
});

// ─── computeYearlySubplots ──────────────────────────────────────────

function mkSeries(
  id: string,
  kind: 'active' | 'benchmark',
  points: { date: string; value: number }[],
): ResolvedSeries {
  const map = new Map<string, number>();
  for (const p of points) map.set(p.date, 1 + p.value / 100);
  return {
    id,
    label: id,
    color: '#000',
    kind,
    removable: false,
    factorByMonth: map,
    months: points.map((p) => p.date),
  };
}

describe('computeYearlySubplots', () => {
  it('returns [] when there is no universe baseline series', () => {
    // Only the active series — no `id === "universe"` companion.
    const active = mkSeries('active', 'active', [
      { date: '2023-12-31', value: 0 },
      { date: '2024-06-30', value: 5 },
    ]);
    const aligned = alignSeries([active]);
    expect(computeYearlySubplots(aligned)).toEqual([]);
  });

  it('buckets points by calendar year and rebases each year to 0% at year-start', () => {
    // 3 years of data. `alignSeries` rebases the cumulative return
    // series so the COMMON start date (the earliest of all series'
    // first dates) is 0%; subsequent points are relative to that.
    // Then `computeYearlySubplots` rebases AGAIN per year so each
    // panel starts at 0% on its own Jan 1 (using the prior year's
    // end as the baseline).
    const active = mkSeries('active', 'active', [
      { date: '2022-12-31', value: 0 },   // common start (rebased to 0)
      { date: '2023-12-31', value: 10 },  // year 1 end → +10%
      { date: '2024-06-30', value: 16.5 },
      { date: '2024-12-31', value: 32 },
    ]);
    const universe = mkSeries('universe', 'benchmark', [
      { date: '2022-12-31', value: 0 },
      { date: '2023-12-31', value: 5 },
      { date: '2024-06-30', value: 8 },
      { date: '2024-12-31', value: 12 },
    ]);
    const aligned = alignSeries([active, universe]);
    const subplots = computeYearlySubplots(aligned);
    expect(subplots.map((s) => s.year)).toEqual(['2022', '2023', '2024']);

    // Year 2023: baseline = end-of-2022 = 0% (rebased), last point
    // = +10% (strategy), +5% (universe).
    const y2023 = subplots[1];
    const last2023 = y2023.points[y2023.points.length - 1];
    expect(last2023.strategyCum).toBeCloseTo(10);
    expect(last2023.universeCum).toBeCloseTo(5);
    expect(last2023.alpha).toBeCloseTo(5);

    // Year 2024: baseline = +10% (strategy) / +5% (universe).
    // last strategy cum=32 → (1.32/1.10 - 1)*100 = 20%
    // last universe cum=12 → (1.12/1.05 - 1)*100 ≈ 6.67%
    const y2024 = subplots[2];
    const last2024 = y2024.points[y2024.points.length - 1];
    expect(last2024.strategyCum).toBeCloseTo(20, 1);
    expect(last2024.universeCum).toBeCloseTo(6.67, 1);
    expect(last2024.alpha).toBeCloseTo(13.33, 1);
  });
});
