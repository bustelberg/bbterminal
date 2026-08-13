import { describe, expect, it } from 'vitest';
import { transformSeries } from './marginData';

/**
 * The three views behind the drill-down switch (Reported / Rebased / YoY %).
 *
 * ⚠ THE INTERESTING CASES ARE ALL REFUSALS. Every one of these transforms has an arithmetic
 * expression that produces a perfectly ordinary-looking number from an input it has no business
 * accepting — a zero base, a negative base, a gap in the middle of a series. Each of those renders
 * in the same font as a real figure, so the assertions below are mostly that we produce NOTHING.
 */
describe('transformSeries', () => {
  it('reported passes values through, normalising undefined to null', () => {
    expect(transformSeries([1, undefined, 3], 'reported')).toEqual([1, null, 3]);
  });

  describe('rebased', () => {
    it('indexes to 100 at the first REPORTED period, not the first column', () => {
      // The row's history opens in column 1; a leading gap must not become the base.
      expect(transformSeries([null, 200, 300], 'rebased')).toEqual([null, 100, 150]);
    });

    it('carries the hole through — a period not reported has no index level', () => {
      expect(transformSeries([50, null, 150], 'rebased')).toEqual([100, null, 300]);
    });

    it('refuses the WHOLE series on a zero base', () => {
      // 100 × v/0 is undefined. Not "0", not Infinity — the row is simply out of the blend, which
      // is the same call `_fundamental_blend._prepare` makes server-side.
      expect(transformSeries([0, 10, 20], 'rebased')).toEqual([null, null, null]);
    });

    it('refuses the WHOLE series on a negative base, which would INVERT the curve', () => {
      // A company opening with negative equity: 100 × 50/−100 = −50, and growth reads as decline.
      expect(transformSeries([-100, 50, 200], 'rebased')).toEqual([null, null, null]);
    });
  });

  describe('yoy', () => {
    /**
     * ⚠ A GROWTH RATE IS A RATIO, SO IT IS COMPARED APPROXIMATELY. `110/100 − 1` is
     * `0.10000000000000009` in binary floating point, and `toEqual` against a literal `10` fails
     * on the last bit — which is a fact about IEEE-754, not about the function. These three
     * assertions were red for exactly that reason; matching on the value to a sane precision keeps
     * them checking the arithmetic instead of the representation.
     *
     * ⚠ THE NULLS STAY EXACT. Every interesting case in this file is a REFUSAL, and "no value" is
     * the thing actually being asserted — it must never be swallowed by a tolerance.
     */
    const closeTo = (got: (number | null)[], want: (number | null)[]) => {
      expect(got.map((v) => (v === null ? null : 'n'))).toEqual(
        want.map((v) => (v === null ? null : 'n')));
      got.forEach((v, i) => {
        if (v !== null) expect(v).toBeCloseTo(want[i] as number, 9);
      });
    };

    it('has nothing to grow from in its first reported period', () => {
      closeTo(transformSeries([100, 110], 'yoy'), [null, 10]);
    });

    it('measures against the previous period THIS ROW REPORTED, not the previous column', () => {
      // ⚠ The whole point. A skipped period must not silently show two periods of growth in the
      // same ink as everyone else's one: 121 is compared with 100, and the answer is 21%.
      closeTo(transformSeries([100, null, 121], 'yoy'), [null, null, 21]);
    });

    it('states a decline as a negative, not as a hole', () => {
      closeTo(transformSeries([100, 80], 'yoy'), [null, -20]);
    });

    it('refuses only the step off a non-positive base, not the rest of the series', () => {
      // Unlike the rebase — whose base poisons every period — a bad denominator is local. The
      // 0 → 5 step is unstateable; the 5 → 10 step that follows is a perfectly good +100%.
      expect(transformSeries([10, 0, 5, 10], 'yoy')).toEqual([null, -100, null, 100]);
    });

    it('refuses a step off a negative base rather than reporting a sign-flipped growth', () => {
      // −50 → 50 is not "+200% growth"; it is a swing through zero that no growth rate describes.
      expect(transformSeries([-50, 50], 'yoy')).toEqual([null, null]);
    });
  });
});
