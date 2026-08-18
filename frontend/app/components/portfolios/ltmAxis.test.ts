import { describe, expect, it } from 'vitest';
import { atSharedX, ltmWindowsDiffer, ltmYearX, sharedLtmX } from './ltmAxis';

/**
 * ⚠⚠ THE REPORTED SYMPTOM (2026-08-18): "the Revenue chart is showing TWO LTM points on the x axis
 * because a company and AEX have different quarters available — LTM should always be a single
 * point."
 *
 * Both stubs were correct and both were labelled correctly. The bug was that there were two of
 * them: `ltmYearX` measures a stub from the entity's OWN last fiscal year end (which is right —
 * three months past it is three months past it, whatever calendar that year was labelled with) and
 * the result was then used as a COORDINATE. Two fiscal calendars, two coordinates, two ticks both
 * reading "LTM", and the trailing points side by side as though the index's twelve months had
 * happened later in time than the book's.
 *
 * Neither rule had a test, and both of them fail into a plausible chart rather than into an error.
 */

const LAST = { year: 2025, date: '2025-12-31' };

describe('ltmYearX — the stub is measured, and clamped inside the year', () => {
  it('a June window on a December filer lands half a year on', () => {
    expect(ltmYearX('2026-06-30', LAST)).toBeCloseTo(2025.5, 2);
  });

  it('⚠ NEVER ON THE FIRST FORECAST. At 1.0 it would sit on FY2026, where the first estimate is', () => {
    // A window a full year past the last close would otherwise land exactly on the next tick.
    expect(ltmYearX('2026-12-31', LAST)).toBeLessThan(2026);
    expect(ltmYearX('2027-12-31', LAST)).toBeLessThan(2026);
  });

  it('⚠ NEVER ON THE LAST ACTUAL EITHER. At 0 it would hide the point it extends', () => {
    expect(ltmYearX('2025-12-31', LAST)).toBeGreaterThan(2025);
    expect(ltmYearX('2025-01-01', LAST)).toBeGreaterThan(2025);
  });

  it('⚠ MEASURED FROM THE ENTITY’S OWN YEAR END, not from a calendar', () => {
    /** An off-calendar filer's FY2026 ends 2026-03-31 and already sits at x=2026; a June window is
     *  ONE quarter past it, not two. Measuring in calendar months would say otherwise. */
    const offCalendar = { year: 2026, date: '2026-03-31' };
    expect(ltmYearX('2026-06-30', offCalendar)).toBeCloseTo(2026.25, 2);
  });

  it('falls back to a quarter on when there is no date to measure from', () => {
    expect(ltmYearX(undefined, LAST)).toBe(2025.25);
    expect(ltmYearX('2026-06-30', { year: 2025 })).toBe(2025.25);
  });

  it('is null with no last reported year — there is nothing to extend', () => {
    expect(ltmYearX('2026-06-30', null)).toBeNull();
  });
});

describe('sharedLtmX — one chart, one LTM position', () => {
  const own = { year: 2025.5, value: 100, date: '2026-06-30' };
  const bench = { year: 2025.75, value: 90, date: '2026-06-30' };

  it('⚠⚠ THE MEASURED CASE — two fiscal calendars must not become two ticks', () => {
    /** The book's last full year ended 2025-12-31 and the index's blend ended 2025-09-30, so the
     *  same June window measured 0.50 and 0.75 of a year on. Two x, both labelled "LTM". */
    expect(sharedLtmX(own, bench)).toBe(2025.5);
    expect(new Set([sharedLtmX(own, bench)]).size).toBe(1);
  });

  it('the subject wins — the card is about the book, the index is an overlay', () => {
    expect(sharedLtmX(own, bench)).toBe(own.year);
  });

  it('the overlay is the slot when the subject has no LTM', () => {
    /** ⚠ THE CASE THAT BROKE THE TICK BEFORE: the blend emitted no LTM row, so nothing could match
     *  and the index's own stub fell through to `xToPeriod` and rendered as a fiscal quarter. */
    expect(sharedLtmX(null, bench)).toBe(2025.75);
  });

  it('is null when neither line has one', () => {
    expect(sharedLtmX(null, null)).toBeNull();
    expect(sharedLtmX(undefined, undefined)).toBeNull();
  });
});

describe('atSharedX — the overlay keeps its window and moves its position', () => {
  const bench = { year: 2025.75, value: 90, date: '2026-06-30' };

  it('moves the x and preserves the value and the real quarter-end', () => {
    const moved = atSharedX(bench, 2025.5);
    expect(moved).toEqual({ year: 2025.5, value: 90, date: '2026-06-30' });
  });

  it('⚠ DOES NOT MUTATE — the original still carries the window the tooltip reads', () => {
    atSharedX(bench, 2025.5);
    expect(bench.year).toBe(2025.75);
  });

  it('passes a point through untouched when there is no shared x', () => {
    expect(atSharedX(bench, null)).toBe(bench);
  });
});

describe('ltmWindowsDiffer — the caveat that survives the collapse', () => {
  it('⚠⚠ COMPARED ON DATES, NOT POSITIONS. Once both share an x, positions can never disagree', () => {
    const own = { year: 2025.5, value: 100, date: '2026-06-30' };
    const bench = { year: 2025.5, value: 90, date: '2026-03-31' };   // already moved onto own's x
    expect(own.year).toBe(bench.year);
    expect(ltmWindowsDiffer(own, bench)).toBe(true);
  });

  it('same quarter-end is not a disagreement', () => {
    expect(ltmWindowsDiffer(
      { year: 2025.5, value: 100, date: '2026-06-30' },
      { year: 2025.5, value: 90, date: '2026-06-30' },
    )).toBe(false);
  });

  it('one line, or an undated point, is not a disagreement', () => {
    const own = { year: 2025.5, value: 100, date: '2026-06-30' };
    expect(ltmWindowsDiffer(own, null)).toBe(false);
    expect(ltmWindowsDiffer(null, own)).toBe(false);
    // A warning that cannot name both quarters explains nothing.
    expect(ltmWindowsDiffer(own, { year: 2025.5, value: 90 })).toBe(false);
  });
});
