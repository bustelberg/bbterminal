/**
 * ⚠⚠ AN LTM POINT MUST NEVER RENDER AS A FISCAL QUARTER.
 *
 * MEASURED 2026-08-14 on the ACWI overlay of `EPS (excl. non-recurring)`, Long Equity tab, annual
 * basis. A trailing-twelve-month point is dated to a QUARTER-END, so it lands on a fractional x
 * (2026-06-30 → 2026.25) while every reported point on an annual chart sits on a whole year. Run
 * through `xToPeriod` that x reads **"2026 Q2"** — a fiscal quarter, on an axis that has none, in
 * the one position a reader looks at for the newest figure.
 *
 * It was the INDEX's point. The portfolio blend emitted no LTM row at all (the full read never
 * called `_ltm_blend_rows`; only the narrowed benchmark read did), so the card had no LTM x of its
 * own to match against and nothing could ever be labelled. The chart therefore showed a green line
 * running one quarter past the book's, under a quarter label — which reads as "the index has
 * reported and we have not". Both halves are fixed; this pins the labelling half.
 */
import { describe, expect, it } from 'vitest';
import { periodTick, stepChanges, xToPeriod } from './marginData';

describe('periodTick', () => {
  it('names an LTM x "LTM" rather than the quarter it happens to fall in', () => {
    expect(xToPeriod(2026.25)).toBe('2026 Q2');          // the bug, as the bare formatter sees it
    expect(periodTick(2026.25, new Set([2026.25]))).toBe('LTM');
  });

  it("labels the INDEX's LTM even when the book has none — the measured case", () => {
    // Only the benchmark contributed an LTM x. Keyed on the company's own point, this tick fell
    // through to "2026 Q2"; the set is what makes the benchmark's point nameable.
    expect(periodTick(2026.25, new Set([2026.25]))).toBe('LTM');
    expect(periodTick(2025, new Set([2026.25]))).toBe('2025');
  });

  it('names BOTH windows when the two lines end on different quarters', () => {
    // ⚠ Two "LTM" ticks are the honest answer: each blend is stamped with the newest filing behind
    // it, so these really are two trailing years. Naming one of them "2026 Q1" would say the book
    // filed a quarter, which it did not.
    const xs = new Set([2026.0, 2026.25]);
    expect(periodTick(2026.0, xs)).toBe('LTM');
    expect(periodTick(2026.25, xs)).toBe('LTM');
  });

  it('leaves every ordinary period alone, on both bases', () => {
    const xs = new Set([2026.25]);
    expect(periodTick(2015, xs)).toBe('2015');
    expect(periodTick(2025.5, xs)).toBe('2025 Q3');      // a real quarter on the quarterly basis
    expect(periodTick(2024, undefined)).toBe('2024');    // no LTM on this chart at all
  });
});

/**
 * The per-period step the level cards show on hover.
 *
 * ⚠ WHY THE STEP AND NOT THE GROWTH SINCE THE ANCHOR: both lines are rebased to 100 at the anchor,
 * so how far apart they have drawn on screen IS the since-anchor comparison — a hover repeating it
 * adds nothing. Which line grew faster in the period under the cursor cannot be read off a log
 * axis where both are climbing, and that is the question the hover exists to answer.
 *
 * ⚠ AND WHY IT IS THE ONE HONEST FIGURE A BLENDED LEVEL CAN PUT IN A HOVER: it is a ratio of two of
 * the line's own points, so the units divide out and it is as real for a portfolio — which has no
 * currency and no actual value — as it is for a single company.
 */
describe('stepChanges', () => {
  it('measures against the previous point and reports which one', () => {
    // ⚠ `toBeCloseTo`, NEVER `toEqual`, ON THE PERCENTAGE. `100 * (110/100 - 1)` is
    // 10.000000000000009 in binary floating point, so an exact match fails on a step that is
    // arithmetically exactly 10% — the assertion would be testing IEEE-754, not the rule.
    const s = stepChanges(new Map([[2023, 100], [2024, 110], [2025, 121]]));
    expect(s.get(2024)?.pct).toBeCloseTo(10, 10);
    expect(s.get(2024)?.from).toBe(2023);
    expect(s.get(2025)?.pct).toBeCloseTo(10, 10);
    expect(s.get(2025)?.from).toBe(2024);
  });

  it('gives the first point no step at all', () => {
    // Nothing to measure from. A "0.0%" there would assert a flat year nobody observed; the card
    // renders this absence as a dash.
    expect(stepChanges(new Map([[2023, 100], [2024, 110]])).has(2023)).toBe(false);
  });

  it('refuses a percentage from a NEGATIVE base, and says so with null not 0', () => {
    // ⚠ THE CASE EPS ACTUALLY HITS. −2 → −1 is not "+50% growth" for a company still making a
    // loss, and −1 → +2 is not "+300%" in any sense that compounds. Same refusal as the CAGR tile.
    const s = stepChanges(new Map([[2023, -2], [2024, -1], [2025, 3]]));
    expect(s.get(2024)).toEqual({ pct: null, from: 2023 });
    expect(s.get(2025)).toEqual({ pct: null, from: 2024 });
  });

  it('refuses a ZERO base too — the division nobody notices until it is Infinity', () => {
    expect(stepChanges(new Map([[2023, 0], [2024, 5]])).get(2024))
      .toEqual({ pct: null, from: 2023 });
  });

  it('measures across a HOLE, and names the period it really came from', () => {
    // ⚠ A period the coverage floor withheld is not drawn, so this step spans two years. `from` is
    // what stops it being labelled "YoY" — the caller renders "+21.0% vs 2023", which is true.
    const s = stepChanges(new Map([[2023, 100], [2024, null], [2025, 121]]));
    expect(s.get(2025)).toEqual({ pct: expect.closeTo(21, 10), from: 2023 });
  });

  it('steps into the LTM point from the last full fiscal year', () => {
    // The LTM x is fractional (2026-06-30 → 2026.25) and its interval is a quarter or two, not a
    // year — which is exactly why the caller prints "vs 2025" rather than asserting an annual rate.
    const s = stepChanges(new Map([[2024, 100], [2025, 110], [2026.25, 115]]));
    expect(s.get(2026.25)).toEqual({ pct: expect.closeTo(4.545, 3), from: 2025 });
  });

  it('is unchanged by a rebase — the multiplier divides out', () => {
    // ⚠ WHY IT IS SAFE TO COMPUTE ON THE RAW SERIES AND SHOW IT BESIDE AN INDEXED AXIS. A rebase is
    // one constant per series, so the step cannot disagree with the line it annotates.
    const raw = new Map([[2023, 4.0], [2024, 5.0]]);
    const indexedSeries = new Map([[2023, 100], [2024, 125]]);
    expect(stepChanges(raw).get(2024)?.pct).toBeCloseTo(
      stepChanges(indexedSeries).get(2024)?.pct as number, 10);
  });
});
