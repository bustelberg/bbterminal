/**
 * ⚠⚠ THE ONE CLAIM WORTH PINNING IS THAT THE BAND IS CENTRED ON ā AND NOT ON ZERO. Every other
 * assertion here exists to stop that centre drifting back to the benchmark by accident — which is
 * what "TE 12.41%" means to almost every reader, and what the OTHER definition of tracking error
 * (ā not subtracted) would actually give.
 */
import { describe, expect, it } from 'vitest';
import { oneSigmaBand } from './activeBand';

describe('oneSigmaBand', () => {
  it('centres on ā · f, not on the benchmark', () => {
    // 0.06%/week × 52 = +3.12%/yr, widened by the annualised TE.
    const b = oneSigmaBand(0.06, 52, 12.41)!;
    expect(b.centre).toBeCloseTo(3.12, 10);
    expect(b.lo).toBeCloseTo(-9.29, 10);
    expect(b.hi).toBeCloseTo(15.53, 10);
    // ⚠ THE ASYMMETRY IS THE POINT: a symmetric −12.41/+12.41 would be the definition that does
    // not subtract ā, and it is 3.12pp away from this one in both directions.
    expect(b.hi + b.lo).toBeCloseTo(2 * b.centre, 10);
  });

  it('carries the TE through verbatim — no second annualisation', () => {
    // ⚠ `annualized_stats` already applied √f. Doing it again here would be a silent ×7.21 at
    // weekly, and the band would still look plausible.
    for (const f of [12, 52, 252]) {
      expect(oneSigmaBand(0.1, f, 12.41)!.te).toBe(12.41);
    }
  });

  it('scales the centre with f while the spread stays put', () => {
    const weekly = oneSigmaBand(0.06, 52, 12.41)!;
    const monthly = oneSigmaBand(0.26, 12, 12.41)!;
    expect(weekly.centre).toBeCloseTo(3.12, 10);
    expect(monthly.centre).toBeCloseTo(3.12, 10);
    expect(weekly.te).toBe(monthly.te);
  });

  it('puts the band below the index when the sleeve has trailed', () => {
    const b = oneSigmaBand(-0.05, 52, 8.0)!;
    expect(b.centre).toBeCloseTo(-2.6, 10);
    expect(b.lo).toBeCloseTo(-10.6, 10);
    expect(b.hi).toBeCloseTo(5.4, 10);
  });

  it('is exactly centre ∓ te, with no rounding of its own', () => {
    const b = oneSigmaBand(0.0123456, 52, 11.987654)!;
    expect(b.lo).toBe(b.centre - b.te);
    expect(b.hi).toBe(b.centre + b.te);
  });

  it('refuses rather than guessing', () => {
    expect(oneSigmaBand(null, 52, 12.41)).toBeNull();
    expect(oneSigmaBand(0.06, null, 12.41)).toBeNull();
    expect(oneSigmaBand(0.06, 52, null)).toBeNull();
    expect(oneSigmaBand(undefined, undefined, undefined)).toBeNull();
    expect(oneSigmaBand(NaN, 52, 12.41)).toBeNull();
    expect(oneSigmaBand(0.06, 52, NaN)).toBeNull();
  });

  it('refuses a band it cannot lay either side of', () => {
    // ⚠ SAME REFUSAL THE INFORMATION-RATIO TILE MAKES: a ~0 TE is not a zero-width band worth
    // printing, it is a figure with no risk in it.
    expect(oneSigmaBand(0.06, 52, 0)).toBeNull();
    expect(oneSigmaBand(0.06, 52, -1)).toBeNull();
    expect(oneSigmaBand(0.06, 0, 12.41)).toBeNull();
    expect(oneSigmaBand(0.06, -52, 12.41)).toBeNull();
  });

  it('keeps a zero mean symmetric — the only case the two TE definitions agree', () => {
    const b = oneSigmaBand(0, 52, 12.41)!;
    expect(b.centre).toBe(0);
    expect(b.lo).toBe(-12.41);
    expect(b.hi).toBe(12.41);
  });
});
