/**
 * ⚠ THE DANGEROUS SIMPLIFICATION HERE IS `filter(r => r.portfolio_pct > 0)`.
 *
 * It reads like "hide the empty rows" and it deletes the most informative ones: a bucket the book
 * does NOT hold while the benchmark does is an unowned region/sector — the thing Brinson scores as
 * an allocation bet, and the finding a reader is most likely to be looking for. The chart would
 * still look complete afterwards, which is why nothing downstream would catch it.
 */
import { describe, expect, it } from 'vitest';

import { DISPLAY_EPSILON, formatPct, visibleBuckets } from './composition';

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
