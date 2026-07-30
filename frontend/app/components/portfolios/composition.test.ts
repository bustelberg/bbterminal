/**
 * ⚠ THE DANGEROUS SIMPLIFICATION HERE IS `filter(r => r.portfolio_pct > 0)`.
 *
 * It reads like "hide the empty rows" and it deletes the most informative ones: a bucket the book
 * does NOT hold while the benchmark does is an unowned region/sector — the thing Brinson scores as
 * an allocation bet, and the finding a reader is most likely to be looking for. The chart would
 * still look complete afterwards, which is why nothing downstream would catch it.
 */
import { describe, expect, it } from 'vitest';

import { DISPLAY_EPSILON, formatPct, hiddenWeight, visibleBuckets } from './composition';

const row = (bucket: string, portfolio_pct: number | null, benchmark_pct: number | null) =>
  ({ bucket, portfolio_pct, benchmark_pct });

describe('visibleBuckets', () => {
  it('drops a bucket that is empty on BOTH sides', () => {
    const rows = [row('North America', 90, 98), row('Pacific', 0, 0), row('Unclassified', 0, 0)];
    expect(visibleBuckets(rows).map((r) => r.bucket)).toEqual(['North America']);
  });

  it('KEEPS a bucket the portfolio does not hold but the benchmark does', () => {
    // ⚠ The whole point. Measured precedent: a model holding 6% Healthcare was credited +1.73pp
    // of allocation for "avoiding" a sector it actually owned — a false finding born of a bucket
    // silently going missing. Hiding an unowned-but-indexed bucket is the same failure, inverted.
    const rows = [row('Healthcare', 0, 6.2)];
    expect(visibleBuckets(rows).map((r) => r.bucket)).toEqual(['Healthcare']);
  });

  it('keeps a bucket the portfolio holds and the benchmark does not', () => {
    expect(visibleBuckets([row('TWD', 5, 0)]).map((r) => r.bucket)).toEqual(['TWD']);
  });

  it('treats a null side as zero, not as unknown-so-keep', () => {
    expect(visibleBuckets([row('MXN', null, null)])).toEqual([]);
    expect(visibleBuckets([row('CAD', null, 1.2)]).map((r) => r.bucket)).toEqual(['CAD']);
  });

  it('hides only what would have RENDERED as "0%", so no visible row disappears', () => {
    const rows = [row('kept', DISPLAY_EPSILON, 0), row('hidden', DISPLAY_EPSILON / 2, 0)];
    expect(visibleBuckets(rows).map((r) => r.bucket)).toEqual(['kept']);
  });

  it('⚠ hides a bucket that is small-but-nonzero, because it still PRINTS "0%"', () => {
    // The bug this file was written for and did not catch: values render at ZERO decimals, so
    // 0.2% prints "0%". A threshold calibrated to one decimal (0.05) let it through a filter
    // written to remove it, and the reader still saw "Pacific 0%".
    expect(visibleBuckets([row('Pacific', 0, 0.2)])).toEqual([]);
    expect(visibleBuckets([row('Pacific', 0.49, 0)])).toEqual([]);
  });

  it('the filter and the formatter agree by construction', () => {
    // ⚠ THE ACTUAL INVARIANT. A row is hidden if and only if both its values render empty. Two
    // separate notions of "displays as zero" is what broke this the first time.
    for (const v of [0, 0.01, 0.2, 0.49, 0.5, 0.51, 1, 42.4]) {
      const shown = visibleBuckets([row('x', v, 0)]).length === 1;
      expect(shown).toBe(formatPct(v) !== '');
    }
  });

  it('preserves the order it was given', () => {
    const rows = [row('b', 10, 0), row('zero', 0, 0), row('a', 20, 0)];
    expect(visibleBuckets(rows).map((r) => r.bucket)).toEqual(['b', 'a']);
  });

  it('an all-empty axis yields no rows rather than a fabricated one', () => {
    expect(visibleBuckets([row('x', 0, 0), row('y', 0, 0)])).toEqual([]);
  });
});

describe('hiddenWeight', () => {
  it('reports what the hidden rows accounted for, per side', () => {
    // ⚠ Hiding rows makes the visible bars stop summing to 100%. A reader who adds them up has
    // found a discrepancy we created, so the amount is available to be stated.
    // ⚠ BOTH sides must be under the threshold for the row to be hidden at all — a first draft of
    // this fixture gave "dust" a 0.1% benchmark, which correctly KEPT it and reported nothing
    // hidden. That is the rule working, and it is worth a test of its own (below).
    const rows = [row('big', 99.96, 99.98), row('dust', 0.04, 0.02)];
    const h = hiddenWeight(rows);
    expect(h.portfolio).toBeCloseTo(0.04, 6);
    expect(h.benchmark).toBeCloseTo(0.02, 6);
  });

  it('reports nothing hidden when the benchmark side alone keeps a dust row visible', () => {
    // ⚠ The benchmark side must clear the SAME threshold — 0.6 prints "1%", 0.1 prints "0%".
    // An earlier version of this test used 0.1 and only passed because the threshold was
    // mis-set to 0.05; it went green while the feature was visibly broken on screen.
    expect(hiddenWeight([row('big', 99.4, 99.4), row('dust', 0.04, 0.6)]))
      .toEqual({ portfolio: 0, benchmark: 0 });
  });

  it('is zero when nothing is hidden', () => {
    expect(hiddenWeight([row('a', 50, 50), row('b', 50, 50)])).toEqual({ portfolio: 0, benchmark: 0 });
  });
});
