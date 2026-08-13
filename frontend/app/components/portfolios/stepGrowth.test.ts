import { describe, expect, it } from 'vitest';
import { MIN_STEP_BASE_FRACTION, memberScale, stepGrowth } from './stepGrowth';

/**
 * The client twin of `backend/tests/test_blend_step_growth.py`, over the same measured cases —
 * because the drill-down's "Weighted (= the line)" row is supposed to BE the plotted line, and a
 * table that explains a number the chart does not show is worse than no table.
 *
 * The failure, in one sentence: the chain guarded its growth ratio with `prev > 0`, so a divisor
 * that was positive and NEAR zero (Prosus 0.0090 a share against a 0.1485 median) produced −2,700%
 * growth at a 26% weight, drove the AEX FCF/share index to −1,456, and a LOG axis then dropped
 * every point after the crossing without a word — 6 of 10 drawn annually, 26 of 32 quarterly.
 */

describe('stepGrowth', () => {
  it('is the plain ratio for a normal step', () => {
    expect(stepGrowth(100, 150, 100)).toBeCloseTo(0.5);
  });

  it('returns null — not 0 — when the member cannot span the interval', () => {
    // A 0 would dilute the step as though it had stood still, which is a different claim.
    expect(stepGrowth(null, 150, 100)).toBeNull();
    expect(stepGrowth(100, null, 100)).toBeNull();
  });

  it('refuses a non-positive anchor', () => {
    expect(stepGrowth(0, 5, 10)).toBeNull();
    expect(stepGrowth(-3, 5, 10)).toBeNull();
  });

  it('refuses an anchor that is immaterial for THAT member (Prosus)', () => {
    const scale = memberScale([0.0090, 0.1485, 0.70]);
    expect(stepGrowth(0.0090, -0.24, scale)).toBeNull();
  });

  it('keeps a genuinely small series, because the bar is relative (NVIDIA)', () => {
    const scale = memberScale([0.035, 0.05, 0.08, 0.16]);
    expect(stepGrowth(0.035, 0.05, scale)).toBeCloseTo(0.05 / 0.035 - 1);
  });

  it('keeps the lowest legitimate anchor measured (Adyen, 0.150)', () => {
    expect(4.291 / 28.618).toBeGreaterThan(MIN_STEP_BASE_FRACTION);
    expect(stepGrowth(4.291, 28.618, 28.618)).not.toBeNull();
  });

  it('floors a member at −100%, so a chained index can never cross zero', () => {
    expect(stepGrowth(2, -1, 2)).toBe(-1);
    expect(stepGrowth(2, -400, 2)).toBe(-1);
  });
});

describe('memberScale', () => {
  it('is the median, not the mean — the outlier is what it exists to identify', () => {
    expect(memberScale([0.009, 0.14, 0.15, 0.70])).toBeCloseTo(0.145);
  });

  it('is 0 for an empty series, which disables the bar rather than throwing', () => {
    expect(memberScale([])).toBe(0);
  });

  it('matches the backend on an odd-length series', () => {
    expect(memberScale([0.70, 0.009, 0.1485])).toBeCloseTo(0.1485);
  });
});
