import { describe, expect, it } from 'vitest';
import { MIN_BASE_FRACTION, medianAbs, negativeRunStart, usableStep } from './positiveChain';

/** `usableStep` reads a row through a lookup, so a test only needs the values in order. */
const from = (vals: readonly (number | null)[]) => (p: string) => vals[Number(p)];
const periods = (vals: readonly unknown[]) => vals.map((_, i) => String(i));
const step = (vals: readonly (number | null)[], i: number, scale: number) =>
  usableStep(from(vals), periods(vals), i, scale);

describe('medianAbs', () => {
  it('takes the middle of the absolute values, ignoring gaps', () => {
    expect(medianAbs([3, -1, 2])).toBe(2);
    expect(medianAbs([5.085, -3.489, 0.458, 6.632])).toBeCloseTo(4.287, 3);
    expect(medianAbs([4, null, undefined, -2])).toBe(3);
  });

  it('is 0 when there is nothing to measure — which refuses every base downstream', () => {
    expect(medianAbs([])).toBe(0);
    expect(medianAbs([null, undefined])).toBe(0);
  });
});

describe('usableStep — the skip rule', () => {
  it('is an ordinary year-on-year when the period behind it is usable', () => {
    const s = step([100, 110], 1, 100)!;
    expect(s.span).toBe(1);
    expect(s.skipped).toBe(0);
    expect(s.growth).toBeCloseTo(0.10, 10);
    // ⚠ over one period the total IS the annual figure — no root taken.
    expect(s.annualised).toBe(s.growth);
  });

  it('walks past a negative base and annualises over the span it really took', () => {
    // The user's case: 2 → −1 → −2 → 4. Both negatives are stepped over, and the answer is that
    // the company DOUBLED over three years — not that it grew 100% in one.
    const s = step([2, -1, -2, 4], 3, medianAbs([2, -1, -2, 4]))!;
    expect(s.from).toBe('0');
    expect(s.span).toBe(3);
    expect(s.skipped).toBe(2);
    expect(s.growth).toBeCloseTo(1.0, 10);
    expect(s.annualised).toBeCloseTo(2 ** (1 / 3) - 1, 10);   // +25.99%/yr
  });

  it('⚠ walks past a POSITIVE base that is immaterial — the case a sign test misses', () => {
    // Eli Lilly ran 5.085 → −3.489 → 0.458 → 6.632. `0.458` is positive, and dividing by it prints
    // +1,348% for a company whose three-year growth was +30.4%. Scale is Lilly's own median |FCF/sh|
    // over its full history (~5.085), so the floor is 0.509 and 0.458 sits under it.
    const lilly = [5.085, -3.489, 0.458, 6.632];
    const s = step(lilly, 3, 5.085)!;
    expect(s.from).toBe('0');
    expect(s.base).toBe(5.085);
    expect(s.span).toBe(3);
    expect(s.growth).toBeCloseTo(0.30423, 5);                 // +30.4% in total
    expect(s.annualised).toBeCloseTo(0.09246, 5);             // +9.2%/yr

    // …and the same base one period earlier is the one that would have been printed instead.
    expect(1.348).toBeCloseTo(6.632 / 0.458 - 1, 2);
  });

  it('refuses a base orders of magnitude below the series, and says so with null', () => {
    // The Japan Post Bank shape. ⚠ THIS TEST IS ABOUT THE DIVISOR ONLY: the base of 4.998 is a
    // rounding error against a series in the thousands and is refused. The FY2025 figure being
    // 1,000x too large (its `shares` cell is 1,000x too small) is a vendor defect in the VALUE,
    // which no growth rule can mend — see `yoyStep`.
    const s = step([4.998, 1_256_901.52], 1, 1623);
    expect(s).toBeNull();
    // …and from a usable base the step is still enormous, correctly: the numerator really is wrong.
    expect(step([1623, 4.998, 1_256_901.52], 2, 1623)!.from).toBe('0');
  });

  it('has nothing to measure at the first period, or with no value of its own', () => {
    expect(step([100, 110], 0, 100)).toBeNull();
    expect(step([100, null], 1, 100)).toBeNull();
    expect(step([100, 110], 5, 100)).toBeNull();
  });

  it('refuses an all-zero series rather than dividing by a floor of zero', () => {
    // scale 0 makes the floor 0, so only the `base > 0` test stands between this and a 0/0.
    expect(step([0, 0, 5], 2, 0)).toBeNull();
  });

  it('gives a one-period fall past zero its exact rate — no root is taken', () => {
    // V₀(1+r)¹ = V₁ is solvable for any V₁, so −150% here is a true statement, not an artefact.
    const s = step([10, 20, -5], 2, 10)!;
    expect(s.span).toBe(1);
    expect(s.growth).toBeCloseTo(-1.25, 10);
    expect(s.annualised).toBe(s.growth);
  });

  it('⚠ refuses a rate for a MULTI-period step ending below zero, because none exists', () => {
    // The company that turned negative and never came back — the one case the skip rule cannot
    // reach: it bridges a dip BETWEEN two positive years, and there is no far side here.
    // `1 + growth ≤ 0` over 2 periods has no real square root.
    const s = step([10, -1, -5], 2, 10)!;
    expect(s.span).toBe(2);
    expect(s.base).toBe(10);                                  // the step is still identified…
    expect(s.growth).toBeCloseTo(-1.5, 10);                   // …and its total is still true…
    expect(s.annualised).toBeNull();                          // …but there is no rate. Not NaN.
  });

  it('telescopes — the retained steps multiply back to the endpoint ratio', () => {
    const vals = [4, -2, 0.05, 6, 9];
    const scale = medianAbs(vals);                            // 4
    let product = 1;
    for (let i = 1; i < vals.length; i += 1) {
      const s = step(vals, i, scale);
      if (s) product *= 1 + s.growth;
    }
    expect(product).toBeCloseTo(9 / 4, 10);
  });

  it('pins the floor at a tenth of the series scale', () => {
    expect(MIN_BASE_FRACTION).toBe(0.10);
    expect(step([1.01, 5], 1, 10)!.from).toBe('0');            // just above 10% of 10
    expect(step([0.99, 5], 1, 10)).toBeNull();                 // just below
  });
});

describe('negativeRunStart', () => {
  const runFrom = (vals: readonly (number | null)[], i: number) =>
    negativeRunStart(from(vals), periods(vals), i);

  it('names the period the series turned, so a rate-less cell can still say something', () => {
    expect(runFrom([5, 4, -1, -2, -3], 4)).toBe('2');
    expect(runFrom([5, 4, -1, -2, -3], 2)).toBe('2');
  });

  it('is null where the value is positive — there is no run to date', () => {
    expect(runFrom([5, 4, -1, 6], 3)).toBeNull();
  });

  it('counts zero as part of the run — a zero base is no more divisible than a negative one', () => {
    expect(runFrom([5, 0, -1], 2)).toBe('1');
  });

  it('⚠ stops at a GAP. An unreported period is not a negative one, and walking through it would '
    + 'date the turn to before a year we have no figure for', () => {
    expect(runFrom([5, -1, null, -3], 3)).toBe('3');
  });
});
