import { describe, expect, it } from 'vitest';
import { stepGrowth } from './stepGrowth';

/**
 * The client twin of `backend/tests/test_blend_step_growth.py`, over the same cases — because the
 * drill-down's "Weighted (= the line)" row is supposed to BE the plotted line, and a table that
 * explains a number the chart does not show is worse than no table.
 *
 * ⚠⚠ THE TWO MAGNITUDE HEURISTICS WERE REMOVED ON 2026-09-04, ON REQUEST, and most of what this
 * file used to assert went with them. What is pinned now is that the remaining refusals are
 * ARITHMETIC — a ratio needs a positive divisor, and an index cannot carry a term below −1 — and,
 * just as importantly, that the ones that WERE judgement are gone and stay gone: a big step is now
 * reported as filed.
 *
 * Removed, with the measurements that justified them preserved in the backend's constant block:
 *   * `MIN_STEP_BASE_FRACTION` (0.10) — refused a step whose anchor was under 10% of the member's
 *     own median. Of its 180 refusals across ACWI's five lines, 44 threw away steps that were flat
 *     or falling or under 2x, because it never looked at the step, only at the divisor.
 *   * `MAX_STEP_GROWTH` (100x) — refused a step over 100x in a year.
 */

describe('stepGrowth', () => {
  it('is the plain ratio for a normal step', () => {
    expect(stepGrowth(100, 150)).toBeCloseTo(0.5);
  });

  it('refuses a step it cannot span', () => {
    expect(stepGrowth(null, 150)).toBeNull();
    expect(stepGrowth(100, null)).toBeNull();
    expect(stepGrowth(undefined, undefined)).toBeNull();
  });

  it('⚠ refuses a non-positive anchor — arithmetic, not judgement', () => {
    // There is no ratio to a zero, and a ratio to a negative flips the sign of every later point.
    expect(stepGrowth(0, 150)).toBeNull();
    expect(stepGrowth(-2, 150)).toBeNull();
  });

  it('⚠ floors at −100%, because an index is a product of (1 + g)', () => {
    // A term below −1 does not make the line small, it makes it NEGATIVE — and a negative index is
    // not a low reading, it is not an index. −150% and −400% both read as −100%; that lost
    // distinction is what guarantees the line cannot cross zero, which is the only reason its log
    // axis can be trusted to be showing all of it.
    expect(stepGrowth(0.30, -0.24)).toBeCloseTo(-1);
    expect(stepGrowth(100, -900)).toBeCloseTo(-1);
  });
});

describe('the removed heuristics stay removed', () => {
  it('⚠⚠ a near-zero anchor is now REPORTED, not refused', () => {
    // Prosus: 0.0090 a share against a 0.1485 median. The old bar refused this; it is arithmetic
    // that works, so it is now the answer — a huge number that is what the vendor filed.
    expect(stepGrowth(0.0090, 0.1485)).toBeCloseTo(15.5, 1);
  });

  it('⚠⚠ Industrivärden’s real recovery is counted — the case that ended the bar', () => {
    // FCF/share 1.087 -> 16.18 in 2021, out of a one-year trough in an otherwise 6 -> 21 series.
    // The server refused it and the client did not, which is how ACWI's FCF/share read 18.85% on
    // `Graphs` against 18.90% in `Tables`. Both now count it.
    expect(stepGrowth(1.087, 16.18)).toBeCloseTo(13.885, 3);
  });

  it('⚠⚠ and so is a step past the old 100x ceiling', () => {
    // Mitsubishi Heavy's 50.78 -> 86,214.52 is almost certainly a vendor scale error, and it now
    // reaches the chart as one. That is the agreed trade: a bad figure shows up as a bad number
    // rather than as a silently missing member. Catching it belongs in a STRUCTURAL test on the
    // share count, not in a threshold on the answer.
    expect(stepGrowth(50.78, 86214.52)).toBeCloseTo(1696.8, 1);
    expect(stepGrowth(1, 1e6)).toBeCloseTo(999999);
  });
});
