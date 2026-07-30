/**
 * The diagonal header's height.
 *
 * A 45° label projects `len × cos(45°)` in BOTH axes, so ONE number is the header's height and
 * the room reserved to the right of the table. Get it too small and the longest name is clipped
 * by the scroll container — which is precisely the thing the switch to full names removed, and it
 * would come back silently, on whichever portfolio happens to have the longest chosen name.
 */
import { describe, it, expect } from 'vitest';
import { diagonalExtentPx } from './CorrelationMatrix';

describe('diagonalExtentPx', () => {
  it('grows with the longest label, not the average or the count', () => {
    const short = diagonalExtentPx(['AB', 'CD', 'EF']);
    const one_long = diagonalExtentPx(['AB', 'CD', 'DuurzaamTopSelectie Beperkt Offensief']);
    expect(one_long).toBeGreaterThan(short);
    // A single long name must reserve as much room as a list full of them — the header is as tall
    // as its tallest label, and averaging would clip that one.
    expect(one_long).toBe(diagonalExtentPx(['DuurzaamTopSelectie Beperkt Offensief']));
  });

  it('fits the real longest label at 45 degrees', () => {
    // The longest chosen name measured on the live data (37 chars). At ~5.8px/char that is
    // ~215px of text, projecting to ~152px on each axis.
    const px = diagonalExtentPx(['DuurzaamTopSelectie Beperkt Offensief']);
    expect(px).toBeGreaterThan(140);
    expect(px).toBeLessThan(180);
  });

  it('is the 45-degree projection, not the raw text width', () => {
    // cos(45°) ≈ 0.707. A header as tall as the text is LONG would waste ~40% of the panel.
    const label = 'X'.repeat(40);
    const px = diagonalExtentPx([label]);
    const rawTextPx = 40 * 5.8;
    expect(px).toBeLessThan(rawTextPx);
    // Ceil'd to a whole pixel BEFORE the padding — a fractional header height is a blurry
    // sticky edge, and the +10 is breathing room on top of a whole number.
    expect(px).toBe(Math.ceil(rawTextPx * Math.SQRT1_2) + 10);
  });

  it('has a floor so a short filter still has a header', () => {
    expect(diagonalExtentPx(['AI'])).toBeGreaterThanOrEqual(48);
    expect(diagonalExtentPx([])).toBeGreaterThanOrEqual(48);
  });

  it('has a ceiling so one pathological name cannot push the matrix off screen', () => {
    expect(diagonalExtentPx(['X'.repeat(500)])).toBeLessThanOrEqual(340);
  });

  it('errs generous rather than tight', () => {
    // Under-estimating CLIPS a name; over-estimating costs whitespace. The padding term keeps a
    // borderline label off the edge.
    const px = diagonalExtentPx(['ExactlyTwentyCharsXX']);
    expect(px).toBeGreaterThan(20 * 5.8 * Math.SQRT1_2);
  });
});

/**
 * The header stacking order.
 *
 * ⚠ WHY THIS IS A TEST AND NOT JUST A CONSTANT. Each header cell is an OPAQUE background (it has
 * to be — body rows scroll under it), but a 45° label ascends OUT of its own cell and across every
 * header to its right. At a shared z-index the paint order is DOM order, so each header's white
 * background covers its LEFT neighbour's label and the whole axis reads blank. That is exactly
 * what shipped, and "flatten these to one z-index" is the most natural tidy-up in the file.
 */
import { headerZ, cornerZ, spacerZ } from './CorrelationMatrix';

describe('diagonal header stacking', () => {
  const n = 42;

  it('ranks every label above every header to its RIGHT', () => {
    // The direction is the whole point: labels ascend rightward, so the cells they cross are the
    // ones after them. Ascending z would reproduce the bug exactly.
    for (let j = 0; j < n - 1; j++) {
      expect(headerZ(j, n)).toBeGreaterThan(headerZ(j + 1, n));
    }
  });

  it('puts the right-hand spacer BELOW every label', () => {
    // It is last in DOM order and opaque — at a shared z it covered the longest labels, which are
    // the only reason it exists.
    for (let j = 0; j < n; j++) expect(headerZ(j, n)).toBeGreaterThan(spacerZ());
  });

  it('keeps the sticky corner above every label', () => {
    // Headers scroll left underneath it; if a label out-ranked it, names would slide over the
    // corner as you scroll right.
    for (let j = 0; j < n; j++) expect(cornerZ(n)).toBeGreaterThan(headerZ(j, n));
  });

  it('keeps the whole header band above the row headers and cells', () => {
    const ROW_HEADER_Z = 10;
    expect(spacerZ()).toBeGreaterThan(ROW_HEADER_Z);
    for (let j = 0; j < n; j++) expect(headerZ(j, n)).toBeGreaterThan(ROW_HEADER_Z);
  });

  it('holds for one column and for a filtered handful', () => {
    for (const size of [1, 2, 5, 8]) {
      expect(cornerZ(size)).toBeGreaterThan(headerZ(0, size));
      expect(headerZ(size - 1, size)).toBeGreaterThan(spacerZ());
    }
  });
});
