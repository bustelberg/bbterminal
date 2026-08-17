import { describe, expect, it } from 'vitest';
import { MAX_STEP_GROWTH, MIN_STEP_BASE_FRACTION, memberScale, stepGrowth } from './stepGrowth';

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

describe('an implausible RESULT is refused, not carried', () => {
  /**
   * ⚠⚠ THE MEASURED CASES. `MIN_STEP_BASE_FRACTION` guarded the divisor and nothing guarded the
   * numerator, so a vendor scale error passed through as growth and the chain multiplied it by the
   * member's weight with no bound. On ACWI's annual FCF/share these two steps moved a line indexed
   * to 100 by +116.12pp and +17.97pp — from constituents weighing 0.07% and 0.04%.
   */
  it('⚠ Mitsubishi Heavy 2024→2025: 50.78 → 86,214.52 is refused', () => {
    // scale = its own median |value|, 39.66. The base passes (50.78 >> 3.97); the result does not.
    expect(stepGrowth(50.78, 86214.52, 39.66)).toBeNull();
  });

  it('⚠ DENSO 2024→2025: 172.97 → 108,415.57 is refused', () => {
    expect(stepGrowth(172.97, 108415.57, 36.22)).toBeNull();
  });

  it('⚠⚠ Bank of America’s +3,818% SURVIVES — the largest step that is unambiguously real', () => {
    // 2008→2009, recovering from the crisis. A ceiling that deletes this is deleting history.
    expect(stepGrowth(0.42, 16.50, 3.17)).toBeCloseTo(38.286, 2);
  });

  it('the p99.99 of the real distribution survives', () => {
    // +6,889% — the top of the legitimate band on 26,160 measured steps.
    expect(stepGrowth(1, 69.89, 1)).toBeCloseTo(68.89, 4);
  });

  it('the boundary is exactly 100x, and 100x itself is kept', () => {
    expect(stepGrowth(1, 101, 1)).toBeCloseTo(100, 6);      // +10,000%, at the bar
    expect(stepGrowth(1, 101.5, 1)).toBeNull();             // over it
  });

  it('⚠ it is one-sided — the downside is already handled by the −100% floor', () => {
    // There is no "too negative" case to catch: a level cannot lose more than all of itself.
    expect(stepGrowth(86214.52, 226.63, 39.66)).toBe(-1);
  });

  it('⚠ REFUSED, NEVER CAPPED — a capped step would be a growth rate nobody reported', () => {
    const got = stepGrowth(172.97, 108415.57, 36.22);
    expect(got).toBeNull();
    expect(got).not.toBe(MAX_STEP_GROWTH);
  });
});
