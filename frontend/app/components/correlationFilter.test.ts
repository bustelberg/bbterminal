/**
 * Filtering an NxN correlation matrix to a subset of portfolios.
 *
 * ⚠ THE BUG THIS EXISTS FOR: SLICING ONLY THE ROWS.
 *
 * `matrix` is NxN over the same index space as `labels`. Filter the rows and you get rows of the
 * ORIGINAL width — so every cell after the first dropped column is read from the wrong portfolio.
 * The result is still rectangular, still renders as a heatmap, still has a plausible diagonal-ish
 * look, and is silently wrong. Both axes must be projected through the same index list.
 */
import { describe, it, expect } from 'vitest';
import { sliceMatrix } from './correlationFilter';

// A 4x4 whose every cell encodes its own coordinates, so a mis-slice is unmistakable.
const M4 = [
  [11, 12, 13, 14],
  [21, 22, 23, 24],
  [31, 32, 33, 34],
  [41, 42, 43, 44],
];

describe('sliceMatrix', () => {
  it('projects BOTH axes through the kept indices', () => {
    // Keep portfolios 1 and 3 -> the 2x2 of their mutual cells.
    expect(sliceMatrix(M4, [1, 3])).toEqual([
      [22, 24],
      [42, 44],
    ]);
  });

  it('does not merely drop rows (the failure mode)', () => {
    const got = sliceMatrix(M4, [1, 3]);
    // Rows-only would give [[21,22,23,24],[41,42,43,44]] — 4 wide, and cell [0][1] would be 22's
    // neighbour rather than the 1-3 pair.
    expect(got[0]).toHaveLength(2);
    expect(got[0][1]).toBe(24);      // the (1,3) pair — NOT 22
  });

  it('keeps the result square', () => {
    for (const keep of [[0], [0, 2], [1, 2, 3], [0, 1, 2, 3]]) {
      const got = sliceMatrix(M4, keep);
      expect(got).toHaveLength(keep.length);
      for (const row of got) expect(row).toHaveLength(keep.length);
    }
  });

  it('preserves the diagonal as the diagonal', () => {
    // A correlation matrix's diagonal is self-vs-self (1.0). If a slice shifted an axis, the
    // diagonal would fill with off-diagonal values and every cell would read plausibly wrong.
    const got = sliceMatrix(M4, [2, 0, 3]);
    expect(got[0][0]).toBe(33);
    expect(got[1][1]).toBe(11);
    expect(got[2][2]).toBe(44);
  });

  it('respects the given order rather than re-sorting', () => {
    expect(sliceMatrix(M4, [3, 0])).toEqual([
      [44, 41],
      [14, 11],
    ]);
  });

  it('keeps nulls as nulls', () => {
    // A null cell means "these two share too few common days" — it must not become a 0, which
    // would read as "uncorrelated", a completely different claim.
    const withNull = [
      [1, null],
      [null, 1],
    ];
    expect(sliceMatrix(withNull, [0, 1])).toEqual([[1, null], [null, 1]]);
  });

  it('handles an empty selection', () => {
    expect(sliceMatrix(M4, [])).toEqual([]);
  });

  it('is null-safe on a ragged/absent source', () => {
    expect(sliceMatrix([], [0, 1])).toEqual([[null, null], [null, null]]);
  });
});
