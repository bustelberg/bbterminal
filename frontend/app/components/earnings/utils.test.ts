/**
 * Unit tests for the pure-function math helpers in `utils.ts`. These
 * pin down the regressions we hit during the comparison-feature
 * session: the leap-year off-by-one in `trailingYearsWindow`, the
 * semi-annual-vs-quarterly cadence guard, the FCF positive-run salvage
 * thresholds, and the various "null on non-positive endpoint" guards
 * on CAGR / R² / SD.
 */
import { describe, expect, it } from 'vitest';
import {
  computeCAGR,
  logLinearR2,
  stdDev,
  trailingPositiveRun,
  trailingYearsWindow,
  ttmSeries,
  flowSeriesPreferQuarterlyTTM,
  yoyGrowthRates,
  ttmYoYGrowthRates,
  annualSeries,
} from './utils';
import type { MetricRow } from './types';

// ─── Test helpers ──────────────────────────────────────────────────

const pt = (date: string, value: number) => ({ date, value });

function mkRow(metric_code: string, target_date: string, numeric_value: number | null): MetricRow {
  return { metric_code, target_date, numeric_value, is_prediction: false };
}

// ─── trailingYearsWindow ───────────────────────────────────────────

describe('trailingYearsWindow', () => {
  it('returns the empty array for an empty input', () => {
    expect(trailingYearsWindow([], 5)).toEqual([]);
  });

  it('includes the boundary entry exactly 5 calendar years before the anchor (leap-year regression)', () => {
    // The bug that hid 2GB's Revenue 5Y CAGR: 5 calendar years from
    // 2024-12-31 back to 2019-12-31 spans TWO leap years (2020, 2024),
    // so the actual gap is 1827 days — but `N * 365.25` is 1826.25, so
    // the old code excluded the 2019-12-31 entry. Calendar-year
    // arithmetic + inclusive >= comparison must include it.
    const series = [
      pt('2019-12-31', 100),
      pt('2020-12-31', 110),
      pt('2021-12-31', 120),
      pt('2022-12-31', 130),
      pt('2023-12-31', 140),
      pt('2024-12-31', 150),
    ];
    const window = trailingYearsWindow(series, 5);
    expect(window).toHaveLength(6);
    expect(window[0].date).toBe('2019-12-31');
    expect(window[window.length - 1].date).toBe('2024-12-31');
  });

  it('drops entries older than N calendar years', () => {
    const series = [
      pt('2015-12-31', 50),
      pt('2019-12-31', 100),
      pt('2024-12-31', 200),
    ];
    const window = trailingYearsWindow(series, 5);
    expect(window.map((p) => p.date)).toEqual(['2019-12-31', '2024-12-31']);
  });

  it('returns [] when fewer than 2 points survive the cutoff', () => {
    const series = [pt('2024-12-31', 100)];
    expect(trailingYearsWindow(series, 5)).toEqual([]);
  });

  it('returns [] when the span is too short for the requested window', () => {
    // Only 1 year of data, asking for 5Y → too narrow (span < years-0.5).
    const series = [pt('2024-01-01', 100), pt('2024-12-31', 200)];
    expect(trailingYearsWindow(series, 5)).toEqual([]);
  });
});

// ─── trailingPositiveRun ───────────────────────────────────────────

describe('trailingPositiveRun', () => {
  it('returns the trailing contiguous run of strictly-positive values', () => {
    const series = [
      pt('2020', -10),
      pt('2021', 5),
      pt('2022', -3),
      pt('2023', 7),
      pt('2024', 12),
    ];
    const out = trailingPositiveRun(series);
    expect(out.trimmed.map((p) => p.value)).toEqual([7, 12]);
  });

  it('stops walking back at the first non-positive value (including zero)', () => {
    // 0 counts as non-positive (must be strictly > 0), so the trailing
    // run is just the single 2022 point — below the 2-point minimum,
    // so the function returns empty per the length < 2 guard.
    const series = [pt('2020', 5), pt('2021', 0), pt('2022', 10)];
    const out = trailingPositiveRun(series);
    expect(out.trimmed).toEqual([]);
    expect(out.spanYears).toBe(0);
  });

  it('returns a 2-point run when the trailing two are positive but earlier values are not', () => {
    const series = [pt('2020', 5), pt('2021', -2), pt('2022', 8), pt('2023', 10)];
    const out = trailingPositiveRun(series);
    expect(out.trimmed.map((p) => p.value)).toEqual([8, 10]);
  });

  it("computes span as the date-distance of the trimmed window's endpoints", () => {
    const series = [pt('2022-06-30', -5), pt('2023-06-30', 10), pt('2024-06-30', 20)];
    const out = trailingPositiveRun(series);
    expect(out.trimmed.map((p) => p.date)).toEqual(['2023-06-30', '2024-06-30']);
    expect(out.spanYears).toBeCloseTo(1.0, 1);
  });

  it('returns empty when no positive run exists at the tail', () => {
    expect(trailingPositiveRun([pt('2020', -1), pt('2021', -2)]).trimmed).toEqual([]);
    expect(trailingPositiveRun([]).trimmed).toEqual([]);
  });
});

// ─── computeCAGR ───────────────────────────────────────────────────

describe('computeCAGR', () => {
  it('computes a positive CAGR for a clean doubling over 5 years', () => {
    // 2019-12-31 → 2024-12-31 is 1827 days = ~5.0013y (two leap years
    // in the window), so the function returns slightly below the
    // exact 5-year compound rate. Loose precision (2 decimals).
    const r = computeCAGR([pt('2019-12-31', 100), pt('2024-12-31', 200)]);
    expect(r).not.toBeNull();
    expect(r!).toBeCloseTo(Math.pow(2, 1 / 5) - 1, 2); // ~14.87% ±1bps
    expect(r!).toBeGreaterThan(0.14);
    expect(r!).toBeLessThan(0.15);
  });

  it('returns null when start ≤ 0 (the regime-change guard)', () => {
    expect(computeCAGR([pt('2019', -10), pt('2024', 100)])).toBeNull();
    expect(computeCAGR([pt('2019', 0), pt('2024', 100)])).toBeNull();
  });

  it('returns null when end ≤ 0', () => {
    expect(computeCAGR([pt('2019', 100), pt('2024', -10)])).toBeNull();
  });

  it('returns null on < 2 points or too-short window', () => {
    expect(computeCAGR([])).toBeNull();
    expect(computeCAGR([pt('2024', 100)])).toBeNull();
    // < 0.5 years span:
    expect(computeCAGR([pt('2024-01-01', 100), pt('2024-03-01', 200)])).toBeNull();
  });

  it('allows non-positive endpoints when requirePositive=false', () => {
    const r = computeCAGR([pt('2019', -10), pt('2024', -5)], false);
    expect(r).not.toBeNull();
  });
});

// ─── logLinearR2 ───────────────────────────────────────────────────

describe('logLinearR2', () => {
  it('returns ~0.96 for the smooth 2GB-style revenue line', () => {
    // 2019: 236 → 2024: 376, ~steady ~10% per year growth
    const r = logLinearR2([
      pt('2019-12-31', 236.396),
      pt('2020-12-31', 246.729),
      pt('2021-12-31', 266.348),
      pt('2022-12-31', 312.627),
      pt('2023-12-31', 365.065),
      pt('2024-12-31', 375.608),
    ]);
    expect(r).not.toBeNull();
    expect(r!).toBeGreaterThan(0.95);
    expect(r!).toBeLessThanOrEqual(1);
  });

  it('returns null when ANY value is ≤ 0 (log domain)', () => {
    const series = [pt('2019', 100), pt('2020', -10), pt('2021', 200), pt('2022', 300)];
    expect(logLinearR2(series)).toBeNull();
  });

  it('returns null with < 3 data points', () => {
    expect(logLinearR2([pt('2024', 100)])).toBeNull();
    expect(logLinearR2([pt('2023', 100), pt('2024', 200)])).toBeNull();
  });
});

// ─── stdDev ─────────────────────────────────────────────────────────

describe('stdDev', () => {
  it('returns null on < 2 values (FCF Growth SD blanks correctly)', () => {
    expect(stdDev([])).toBeNull();
    expect(stdDev([0.1])).toBeNull();
  });

  it('computes sample standard deviation', () => {
    // [-0.5, 0.5] has sample SD = 0.7071
    expect(stdDev([-0.5, 0.5])!).toBeCloseTo(0.7071, 3);
  });
});

// ─── isQuarterlyCadence (via flowSeriesPreferQuarterlyTTM) ──────────

describe('flowSeriesPreferQuarterlyTTM', () => {
  it('falls back to annuals when the quarterly twin is semi-annual (2GB regression)', () => {
    // 2GB Energy publishes semi-annually — quarterly__... has Jun-30
    // + Dec-31 entries spaced ~180 days apart, not 90. The cadence
    // guard must skip the TTM path and fall back to annuals so we
    // don't compute "trailing 24 months" mislabeled as TTM.
    const rows: MetricRow[] = [];
    for (let y = 2018; y <= 2024; y++) {
      rows.push(mkRow('quarterly__Cashflow Statement__Free Cash Flow', `${y}-06-30`, 5));
      rows.push(mkRow('quarterly__Cashflow Statement__Free Cash Flow', `${y}-12-31`, 10));
      rows.push(mkRow('annuals__Cashflow Statement__Free Cash Flow', `${y}-12-31`, 15));
    }
    const out = flowSeriesPreferQuarterlyTTM(rows, 'annuals__Cashflow Statement__Free Cash Flow');
    // Annual fallback → 7 points (2018-2024), each at year-end with value 15.
    expect(out).toHaveLength(7);
    expect(out.every((p) => p.value === 15)).toBe(true);
  });

  it('uses quarterly TTM when the quarterly twin is genuinely quarterly', () => {
    const rows: MetricRow[] = [];
    // 8 quarters of equal value 1 → TTM should be 4 at every point.
    const dates = [
      '2023-03-31', '2023-06-30', '2023-09-30', '2023-12-31',
      '2024-03-31', '2024-06-30', '2024-09-30', '2024-12-31',
    ];
    for (const d of dates) {
      rows.push(mkRow('quarterly__Income Statement__Revenue', d, 1));
    }
    const out = flowSeriesPreferQuarterlyTTM(rows, 'annuals__Income Statement__Revenue');
    expect(out.length).toBeGreaterThanOrEqual(5);
    expect(out.every((p) => p.value === 4)).toBe(true);
  });
});

// ─── ttmSeries ──────────────────────────────────────────────────────

describe('ttmSeries', () => {
  it('returns the sum of the trailing 4 values at each step starting from index 3', () => {
    const q = [pt('2023-Q1', 1), pt('2023-Q2', 2), pt('2023-Q3', 3), pt('2023-Q4', 4), pt('2024-Q1', 5)];
    const out = ttmSeries(q);
    expect(out).toHaveLength(2);
    expect(out[0]).toEqual({ date: '2023-Q4', value: 10 }); // 1+2+3+4
    expect(out[1]).toEqual({ date: '2024-Q1', value: 14 }); // 2+3+4+5
  });

  it('returns [] when fewer than 4 quarters are provided', () => {
    expect(ttmSeries([pt('a', 1), pt('b', 2), pt('c', 3)])).toEqual([]);
  });
});

// ─── annualSeries (collapse to one point per year) ──────────────────

describe('annualSeries', () => {
  it('collapses multi-entry years to the latest date in that year', () => {
    const rows: MetricRow[] = [
      mkRow('x', '2023-06-30', 100),
      mkRow('x', '2023-12-31', 200),
      mkRow('x', '2024-12-31', 300),
    ];
    const out = annualSeries(rows, 'x');
    expect(out).toEqual([
      pt('2023-12-31', 200),
      pt('2024-12-31', 300),
    ]);
  });

  it('returns [] when no rows match the code', () => {
    expect(annualSeries([mkRow('x', '2024-01-01', 1)], 'y')).toEqual([]);
  });
});

// ─── ttmYoYGrowthRates ──────────────────────────────────────────────

describe('ttmYoYGrowthRates', () => {
  it('compares quarter t to quarter t-4 only when both are positive', () => {
    const ttm = [
      pt('q1', 100),  // i=0
      pt('q2', 110),  // i=1
      pt('q3', 120),  // i=2
      pt('q4', 130),  // i=3
      pt('q5', 200),  // i=4: 200/100 - 1 = 1.0
      pt('q6', -50),  // i=5: skipped (curr ≤ 0)
      pt('q7', 240),  // i=6: 240/120 - 1 = 1.0
      pt('q8', 195),  // i=7: 195/130 - 1 = 0.5
    ];
    expect(ttmYoYGrowthRates(ttm)).toEqual([1.0, 1.0, 0.5]);
  });
});

// ─── yoyGrowthRates ─────────────────────────────────────────────────

describe('yoyGrowthRates', () => {
  it('compares year t to year t-1 only when both positive', () => {
    const series = [pt('2020', 100), pt('2021', 150), pt('2022', -10), pt('2023', 60)];
    // 150/100 = 1.5, 150→-10 skipped, -10→60 skipped → just [0.5]
    expect(yoyGrowthRates(series)).toEqual([0.5]);
  });
});
